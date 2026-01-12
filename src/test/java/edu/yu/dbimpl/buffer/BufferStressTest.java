package edu.yu.dbimpl.buffer;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.PageBase;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BufferStressTest {

    private static int appendDummyLSN(LogMgrBase lm) {
        return ((LogMgr) lm).append(new byte[] { 1 });
    }

    @Test
    @Order(0)
    public void heavy_write_read_throughput() {
        // Fresh DB startup
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        final String dir = "testing/buffer-stress/throughput";
        final String fileName = "stress_tbl";
        final int blockSize = 4096; // 4KB pages
        final int nBuffers = 2000;
        final int maxWaitMs = 500;
        final int nBlocks = 10_000; // sizable but test-friendly
        final int concurrency = 16;

        FileMgrBase fm = new FileMgr(new File(dir), blockSize);
        LogMgrBase lm = new LogMgr(fm, "stress_perf_log");
        BufferMgr bm = new BufferMgr(fm, lm, nBuffers, maxWaitMs);

        final int txId = 1;

        final long t0 = System.nanoTime();
        // Create and write nBlocks
        for (int i = 0; i < nBlocks; i++) {
            BlockId b = new BlockId(fileName, i);
            BufferBase buf = bm.pin(b);
            try {
                PageBase p = buf.contents();
                final int intPos = 0;
                final int strPos = Integer.BYTES;
                final int expectedInt = i * 17 + 13;
                final String expectedStr = "s#" + i;
                p.setInt(intPos, expectedInt);
                p.setString(strPos, expectedStr);
                int lsn = appendDummyLSN(lm);
                buf.setModified(txId, lsn);
            } finally {
                bm.unpin(buf);
            }
        }
        bm.flushAll(txId);
        final long t1 = System.nanoTime();

        // Prepare randomized read order
        List<Integer> indices = new ArrayList<>(nBlocks);
        for (int i = 0; i < nBlocks; i++)
            indices.add(i);
        Collections.shuffle(indices);

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        List<Future<Integer>> futures = new ArrayList<>();
        final AtomicInteger verified = new AtomicInteger(0);

        int chunk = (nBlocks + concurrency - 1) / concurrency;
        for (int t = 0; t < concurrency; t++) {
            final int start = t * chunk;
            final int end = Math.min(indices.size(), start + chunk);
            if (start >= end)
                break;
            final List<Integer> slice = indices.subList(start, end);
            Callable<Integer> task = () -> {
                int local = 0;
                for (int idx : slice) {
                    BlockId b = new BlockId(fileName, idx);
                    BufferBase buf = bm.pin(b);
                    try {
                        PageBase p = buf.contents();
                        final int intPos = 0;
                        final int strPos = Integer.BYTES;
                        final int expectedInt = idx * 17 + 13;
                        final String expectedStr = "s#" + idx;
                        int gotInt = p.getInt(intPos);
                        String gotStr = p.getString(strPos);
                        if (gotInt != expectedInt || !expectedStr.equals(gotStr)) {
                            fail("Data mismatch for block " + idx);
                        }
                        local++;
                    } finally {
                        bm.unpin(buf);
                    }
                }
                verified.addAndGet(local);
                return local;
            };
            futures.add(pool.submit(task));
        }

        for (Future<Integer> f : futures) {
            try {
                f.get();
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                fail("Reader task failed: " + (cause == null ? ee : cause));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for readers");
            }
        }
        pool.shutdown();
        try {
            pool.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted while awaiting pool termination");
        }

        final long t2 = System.nanoTime();

        assertEquals(nBlocks, verified.get(), "All blocks should be verified");

        final long writeMs = (t1 - t0) / 1_000_000L;
        final long readMs = (t2 - t1) / 1_000_000L;
        System.out.printf(
                "Stress throughput: wrote %,d blocks in %d ms, read+verified in %d ms with %d threads (block=%dB, buffers=%d)\n",
                nBlocks, writeMs, readMs, concurrency, blockSize, nBuffers);
    }

    @Test
    @Order(1)
    public void contention_timeouts_under_load() {
        // Fresh DB startup
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        final String dir = "testing/buffer-stress/contention";
        final int blockSize = 256;
        final int bufferSize = 8; // small to induce contention
        final int maxWaitMs = 50; // short wait to force aborts
        final int nBlocks = 64; // working set
        final int concurrency = 32;
        final int attemptsPerThread = 200;

        FileMgrBase fm = new FileMgr(new File(dir), blockSize);
        LogMgrBase lm = new LogMgr(fm, "stress_cont_log");
        BufferMgr bm = new BufferMgr(fm, lm, bufferSize, maxWaitMs);

        // Light initialization
        for (int i = 0; i < nBlocks; i++) {
            BufferBase buf = bm.pin(new BlockId("cont_tbl", i));
            bm.unpin(buf);
        }

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        final AtomicInteger successes = new AtomicInteger(0);
        final AtomicInteger aborts = new AtomicInteger(0);
        final AtomicLong totalLatencyNs = new AtomicLong(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < concurrency; t++) {
            futures.add(pool.submit(() -> {
                long seed = System.nanoTime() ^ System.identityHashCode(Thread.currentThread());
                // Simple LCG
                long a = 2862933555777941757L;
                long c = 3037000493L;
                for (int k = 0; k < attemptsPerThread; k++) {
                    seed = a * seed + c;
                    int idx = (int) (Math.floor(((seed >>> 1) & ((1L << 31) - 1)) / (double) (1L << 31) * nBlocks));
                    BlockId b = new BlockId("cont_tbl", idx);
                    long s = System.nanoTime();
                    try {
                        BufferBase buf = bm.pin(b);
                        try {
                            // light touch
                            buf.contents().getInt(0);
                        } finally {
                            bm.unpin(buf);
                        }
                        long e = System.nanoTime();
                        totalLatencyNs.addAndGet(e - s);
                        successes.incrementAndGet();
                    } catch (BufferAbortException bae) {
                        long e = System.nanoTime();
                        totalLatencyNs.addAndGet(e - s);
                        aborts.incrementAndGet();
                    }
                }
            }));
        }

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                fail("Worker failed: " + (cause == null ? ee : cause));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for workers");
            }
        }
        pool.shutdown();
        try {
            pool.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted while awaiting pool termination");
        }

        final int totalAttempts = concurrency * attemptsPerThread;
        final long avgLatencyUs = totalLatencyNs.get() / Math.max(1, totalAttempts) / 1000L;
        System.out.printf(
                "Contention: attempts=%d, successes=%d, aborts=%d, successRate=%.1f%%, avgLatency=%d µs (buf=%d, wait=%dms)\n",
                totalAttempts, successes.get(), aborts.get(), successes.get() * 100.0 / totalAttempts,
                avgLatencyUs, bufferSize, maxWaitMs);

        assertTrue(successes.get() > 0, "Should have some successful pins");
        assertTrue(aborts.get() > 0, "Should observe some BufferAbortExceptions under contention");
        assertEquals(totalAttempts, successes.get() + aborts.get(), "All attempts accounted for");
    }

    @Test
    @Order(2)
    public void eviction_policy_stress_large() {
        final String baseDir = "testing/buffer-stress/eviction";
        final String fileName = "evict_tbl";

        // Workload that favors a cacheable hot set
        final int hotSize = 5000;
        final int coldSize = 50000;
        final int bufferSize = 10000; // can hold entire hot set
        final int ops = 300_000;
        final double hotProb = 0.6;
        final int[] blockSizes = new int[] { 512, 1024, 4096, 8192 };

        // Track timings for each block size
        Map<Integer, long[]> timingsByBlock = new LinkedHashMap<>(); // blockSize -> [naiveMs, clockMs]

        // Small fresh init helper
        for (int blockSize : blockSizes) {
            System.out.printf("===\tRunning tests for block size %dB\n", blockSize);
            long tNaive = runEvictionWorkload(baseDir + "/naive-" + blockSize + "-" + System.nanoTime(),
                    fileName, blockSize, bufferSize, hotSize, coldSize, ops, hotProb,
                    BufferMgrBase.EvictionPolicy.NAIVE);

            long tClock = runEvictionWorkload(baseDir + "/clock-" + blockSize + "-" + System.nanoTime(),
                    fileName, blockSize, bufferSize, hotSize, coldSize, ops, hotProb,
                    BufferMgrBase.EvictionPolicy.CLOCK);

            timingsByBlock.put(blockSize, new long[] { tNaive, tClock });
            System.out.printf("===\tEvictionPerf (%dB): NAIVE=%d ms, CLOCK=%d ms\n", blockSize, tNaive, tClock);
            assertTrue(tNaive > 0 && tClock > 0);
        }
    }

    // Initializes hot/cold data set and times a mixed-access workload for a policy
    private long runEvictionWorkload(String dir, String fileName, int blockSize, int bufferSize, int hotSize,
            int coldSize, int ops, double hotProb, BufferMgrBase.EvictionPolicy policy) {
        // Fresh DB startup for initialization
        final Properties startup = new Properties();
        startup.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(startup);

        final FileMgrBase fmInit = new FileMgr(new File(dir), blockSize);
        final LogMgrBase lmInit = new LogMgr(fmInit, "evictlog");
        final BufferMgr bmInit = new BufferMgr(fmInit, lmInit, bufferSize, 500, BufferMgrBase.EvictionPolicy.NAIVE);

        // Initialize data on disk
        final int tx = 1;
        for (int i = 0; i < hotSize + coldSize; i++) {
            BlockId b = new BlockId(fileName, i);
            BufferBase buf = bmInit.pin(b);
            try {
                PageBase p = buf.contents();
                p.setInt(0, i);
                int lsn = appendDummyLSN(lmInit);
                buf.setModified(tx, lsn);
            } finally {
                bmInit.unpin(buf);
            }
        }
        bmInit.flushAll(tx);

        // Restart semantics for measured phase
        final Properties restart = new Properties();
        restart.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
        DBConfiguration.INSTANCE.get().setConfiguration(restart);

        final FileMgrBase fm = new FileMgr(new File(dir), blockSize);
        final LogMgrBase lm = new LogMgr(fm, "evictlog");
        final BufferMgr bm = new BufferMgr(fm, lm, bufferSize, 500, policy);

        // Preload hot set
        List<BufferBase> pinned = new ArrayList<>(hotSize);
        for (int i = 0; i < hotSize; i++) {
            BufferBase buf = bm.pin(new BlockId(fileName, i));
            pinned.add(buf);
        }
        for (BufferBase b : pinned)
            bm.unpin(b);

        // Deterministic pseudo-random access sequence
        long seed = 987654321L;
        long a = 2862933555777941757L;
        long c = 3037000493L;

        final long t0 = System.nanoTime();
        for (int k = 0; k < ops; k++) {
            seed = a * seed + c;
            double r = ((seed >>> 1) & ((1L << 53) - 1)) / (double) (1L << 53);
            int idx;
            if (r < hotProb) {
                idx = (int) (Math.floor(r / hotProb * hotSize));
            } else {
                double rc = (r - hotProb) / (1.0 - hotProb);
                idx = hotSize + (int) (Math.floor(rc * coldSize));
            }

            BufferBase buf = bm.pin(new BlockId(fileName, idx));
            try {
                buf.contents().getInt(0);
            } finally {
                bm.unpin(buf);
            }
        }
        final long t1 = System.nanoTime();
        return (t1 - t0) / 1_000_000L;
    }

    // Variant: identical to runEvictionWorkload but uses per-op randomness (seeded)
    // for access selection
    private long runEvictionWorkloadRandomOps(String dir, String fileName, int blockSize, int bufferSize, int hotSize,
            int coldSize, int ops, double hotProb, BufferMgrBase.EvictionPolicy policy, long seedForOps) {
        // Fresh DB startup for initialization
        final Properties startup = new Properties();
        startup.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(startup);

        final FileMgrBase fmInit = new FileMgr(new File(dir), blockSize);
        final LogMgrBase lmInit = new LogMgr(fmInit, "evictlog");
        final BufferMgr bmInit = new BufferMgr(fmInit, lmInit, bufferSize, 500, BufferMgrBase.EvictionPolicy.NAIVE);

        // Initialize data on disk
        final int tx = 1;
        for (int i = 0; i < hotSize + coldSize; i++) {
            BlockId b = new BlockId(fileName, i);
            BufferBase buf = bmInit.pin(b);
            try {
                PageBase p = buf.contents();
                p.setInt(0, i);
                int lsn = appendDummyLSN(lmInit);
                buf.setModified(tx, lsn);
            } finally {
                bmInit.unpin(buf);
            }
        }
        bmInit.flushAll(tx);

        // Restart semantics for measured phase
        final Properties restart = new Properties();
        restart.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
        DBConfiguration.INSTANCE.get().setConfiguration(restart);

        final FileMgrBase fm = new FileMgr(new File(dir), blockSize);
        final LogMgrBase lm = new LogMgr(fm, "evictlog");
        final BufferMgr bm = new BufferMgr(fm, lm, bufferSize, 500, policy);

        // Preload hot set
        List<BufferBase> pinned = new ArrayList<>(hotSize);
        for (int i = 0; i < hotSize; i++) {
            BufferBase buf = bm.pin(new BlockId(fileName, i));
            pinned.add(buf);
        }
        for (BufferBase b : pinned)
            bm.unpin(b);

        final java.util.Random rng = new java.util.Random(seedForOps);

        final long t0 = System.nanoTime();
        for (int k = 0; k < ops; k++) {
            double r = rng.nextDouble();
            int idx;
            if (r < hotProb) {
                idx = (int) (Math.floor(r / hotProb * hotSize));
            } else {
                double rc = (r - hotProb) / (1.0 - hotProb);
                idx = hotSize + (int) (Math.floor(rc * coldSize));
            }

            BufferBase buf = bm.pin(new BlockId(fileName, idx));
            try {
                buf.contents().getInt(0);
            } finally {
                bm.unpin(buf);
            }
        }
        final long t1 = System.nanoTime();
        return (t1 - t0) / 1_000_000L;
    }

    @Test
    @Order(3)
    public void eviction_policy_repeated_statistics_table() {
        // This test runs the eviction_policy_stress_large workload multiple times
        // and prints a compact table of stats per block size and policy.

        final String baseDir = "testing/buffer-stress/eviction-batch";
        final String fileName = "evict_tbl";

        // Keep these aligned with eviction_policy_stress_large
        final int hotSize = 5000;
        final int coldSize = 10000;
        final int bufferSize = 10000; // can hold entire hot set
        final int ops = 300_000;
        // Randomize hotProb per trial within a safe range
        final double minHotProb = 0.25;
        final double maxHotProb = 0.45;
        final java.util.Random rng = new java.util.Random(System.nanoTime());
        final int[] blockSizes = new int[] { 512, 1024, 4096, 8192 };

        final int trials = 5; // number of runs per configuration

        StringBuilder table = new StringBuilder();
        table.append(System.lineSeparator());
        table.append(String.format(
                "Eviction policy repeated runs: trials=%d, ops/run=%d, bufferSize=%d, hotProb=random[%.2f, %.2f]%n",
                trials, ops, bufferSize, minHotProb, maxHotProb));
        table.append(String.format("%n%-10s | %-6s | %8s | %8s | %8s%n", "BlockSize", "Policy", "Avg(ms)",
                "Min(ms)", "Max(ms)"));
        table.append("-----------+--------+----------+----------+----------\n");

        for (int blockSize : blockSizes) {
            long[] naive = new long[trials];
            long[] clock = new long[trials];

            for (int r = 0; r < trials; r++) {
                double hotProb = minHotProb + rng.nextDouble() * (maxHotProb - minHotProb);
                long opSeed = rng.nextLong(); // same per-policy within a trial for fair comparison
                long tNaive = runEvictionWorkloadRandomOps(
                        baseDir + "/naive-" + blockSize + "-" + System.nanoTime(),
                        fileName, blockSize, bufferSize, hotSize, coldSize, ops, hotProb,
                        BufferMgrBase.EvictionPolicy.NAIVE, opSeed);

                long tClock = runEvictionWorkloadRandomOps(
                        baseDir + "/clock-" + blockSize + "-" + System.nanoTime(),
                        fileName, blockSize, bufferSize, hotSize, coldSize, ops, hotProb,
                        BufferMgrBase.EvictionPolicy.CLOCK, opSeed);

                naive[r] = tNaive;
                clock[r] = tClock;
            }

            long[] naiveStats = computeStats(naive);
            long[] clockStats = computeStats(clock);

            // Rows per policy for this block size
            table.append(String.format("%-10d | %-6s | %8d | %8d | %8d%n",
                    blockSize, "NAIVE", naiveStats[0], naiveStats[1], naiveStats[2]));
            table.append(String.format("%-10d | %-6s | %8d | %8d | %8d%n",
                    blockSize, "CLOCK", clockStats[0], clockStats[1], clockStats[2]));

            // Light sanity assertions
            assertTrue(naiveStats[0] > 0 && clockStats[0] > 0,
                    "Average time should be positive for both policies");
        }

        // Print the entire table at the end
        System.out.print(table.toString());
    }

    // Returns [avg, min, max] for the given values
    private static long[] computeStats(long[] values) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long sum = 0L;
        for (long v : values) {
            if (v < min)
                min = v;
            if (v > max)
                max = v;
            sum += v;
        }
        long avg = Math.round(sum / (double) Math.max(1, values.length));
        return new long[] { avg, min, max };
    }

    @Test
    @Order(4)
    public void randomized_mixed_ops_heavy() {
        // Fresh DB startup
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        final String dir = "testing/buffer-stress/random-all";
        final String fileName = "rand_tbl";
        final int blockSize = 4096; // standard 4KB pages
        final int nBuffers = 10_000;
        final int maxWaitMs = 250; // generous given large pool

        final int nBlocks = 20_000;
        final int totalOps = 500_000; // total pin-(read|write)-unpin ops
        final int concurrency = 10;

        FileMgrBase fm = new FileMgr(new File(dir), blockSize);
        LogMgrBase lm = new LogMgr(fm, "rand_ops_log");
        BufferMgr bm = new BufferMgr(fm, lm, nBuffers, maxWaitMs);

        // Initialize on-disk blocks with a consistent baseline value
        final int txId = 1;
        for (int i = 0; i < nBlocks; i++) {
            BufferBase buf = bm.pin(new BlockId(fileName, i));
            try {
                PageBase p = buf.contents();
                p.setInt(0, 0);
                int lsn = appendDummyLSN(lm);
                buf.setModified(txId, lsn);
            } finally {
                bm.unpin(buf);
            }
        }
        bm.flushAll(txId);

        // Distribute operations across threads (last thread takes remainder)
        final int opsPerThreadBase = totalOps / concurrency;
        final int opsRemainder = totalOps % concurrency;

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        List<Future<long[]>> futures = new ArrayList<>();

        final AtomicInteger completed = new AtomicInteger(0);
        final AtomicInteger reads = new AtomicInteger(0);
        final AtomicInteger writes = new AtomicInteger(0);
        final AtomicInteger aborts = new AtomicInteger(0);

        final long t0 = System.nanoTime();
        for (int t = 0; t < concurrency; t++) {
            final int opsForThisThread = opsPerThreadBase + (t == concurrency - 1 ? opsRemainder : 0);
            final long seed = System.nanoTime() ^ (t * 0x9E3779B97F4A7C15L);
            futures.add(pool.submit(() -> {
                java.util.Random rng = new java.util.Random(seed);
                int localCompleted = 0;
                int localReads = 0;
                int localWrites = 0;
                int localAborts = 0;

                for (int k = 0; k < opsForThisThread; k++) {
                    int idx = rng.nextInt(nBlocks);
                    BlockId b = new BlockId(fileName, idx);
                    try {
                        BufferBase buf = bm.pin(b);
                        try {
                            PageBase p = buf.contents();
                            if (rng.nextBoolean()) { // write op
                                int newVal = rng.nextInt();
                                p.setInt(0, newVal);
                                int lsn = appendDummyLSN(lm);
                                buf.setModified(txId, lsn);
                                localWrites++;
                            } else { // read op
                                p.getInt(0); // touch
                                localReads++;
                            }
                        } finally {
                            bm.unpin(buf);
                        }
                        localCompleted++;
                    } catch (BufferAbortException bae) {
                        // Unexpected with large buffer pool, but count and continue
                        localAborts++;
                    }
                }

                completed.addAndGet(localCompleted);
                reads.addAndGet(localReads);
                writes.addAndGet(localWrites);
                aborts.addAndGet(localAborts);
                return new long[] { localCompleted, localReads, localWrites, localAborts };
            }));
        }

        for (Future<long[]> f : futures) {
            try {
                f.get();
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                fail("Worker failed: " + (cause == null ? ee : cause));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for workers");
            }
        }
        pool.shutdown();
        try {
            pool.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted while awaiting pool termination");
        }

        final long t1 = System.nanoTime();

        // Stats and sanity checks
        assertEquals(totalOps, completed.get(), "All operations should complete");
        assertTrue(aborts.get() == 0, "No BufferAbortExceptions expected with large buffer pool");

        final long elapsedMs = (t1 - t0) / 1_000_000L;
        System.out.printf(
                "Random mixed ops: %,d ops in %d ms with %d threads (reads=%,d, writes=%,d, aborts=%d, block=%dB, buffers=%d)\n",
                totalOps, elapsedMs, concurrency, reads.get(), writes.get(), aborts.get(), blockSize, nBuffers);
    }

    // Helper to run the randomized mixed ops workload for a given eviction policy.
    // Returns [elapsedMs, completed, reads, writes, aborts]
    private long[] runMixedOpsWorkloadPerf(String dir, String fileName, int blockSize, int nBuffers, int maxWaitMs,
            int nBlocks, int totalOps, int concurrency, BufferMgrBase.EvictionPolicy policy, long seedBase) {
        // Fresh DB startup for initialization (separate from timing)
        final Properties startup = new Properties();
        startup.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(startup);

        FileMgrBase fmInit = new FileMgr(new File(dir), blockSize);
        LogMgrBase lmInit = new LogMgr(fmInit, "rand_ops_log_init");
        BufferMgr bmInit = new BufferMgr(fmInit, lmInit, nBuffers, maxWaitMs, BufferMgrBase.EvictionPolicy.NAIVE);

        final int txId = 1;
        for (int i = 0; i < nBlocks; i++) {
            BufferBase buf = bmInit.pin(new BlockId(fileName, i));
            try {
                PageBase p = buf.contents();
                p.setInt(0, 0);
                int lsn = appendDummyLSN(lmInit);
                buf.setModified(txId, lsn);
            } finally {
                bmInit.unpin(buf);
            }
        }
        bmInit.flushAll(txId);

        // Restart semantics for measured phase
        final Properties restart = new Properties();
        restart.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
        DBConfiguration.INSTANCE.get().setConfiguration(restart);

        FileMgrBase fm = new FileMgr(new File(dir), blockSize);
        LogMgrBase lm = new LogMgr(fm, "rand_ops_log");
        BufferMgr bm = new BufferMgr(fm, lm, nBuffers, maxWaitMs, policy);

        final int opsPerThreadBase = totalOps / concurrency;
        final int opsRemainder = totalOps % concurrency;

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        List<Future<long[]>> futures = new ArrayList<>();

        final AtomicInteger completed = new AtomicInteger(0);
        final AtomicInteger reads = new AtomicInteger(0);
        final AtomicInteger writes = new AtomicInteger(0);
        final AtomicInteger aborts = new AtomicInteger(0);

        final long t0 = System.nanoTime();
        for (int t = 0; t < concurrency; t++) {
            final int opsForThisThread = opsPerThreadBase + (t == concurrency - 1 ? opsRemainder : 0);
            final long seed = seedBase + t * 0x9E3779B97F4A7C15L;
            futures.add(pool.submit(() -> {
                java.util.Random rng = new java.util.Random(seed);
                int localCompleted = 0;
                int localReads = 0;
                int localWrites = 0;
                int localAborts = 0;

                for (int k = 0; k < opsForThisThread; k++) {
                    int idx = rng.nextInt(nBlocks);
                    BlockId b = new BlockId(fileName, idx);
                    try {
                        BufferBase buf = bm.pin(b);
                        try {
                            PageBase p = buf.contents();
                            if (rng.nextBoolean()) {
                                int newVal = rng.nextInt();
                                p.setInt(0, newVal);
                                int lsn = appendDummyLSN(lm);
                                buf.setModified(txId, lsn);
                                localWrites++;
                            } else {
                                p.getInt(0);
                                localReads++;
                            }
                        } finally {
                            bm.unpin(buf);
                        }
                        localCompleted++;
                    } catch (BufferAbortException bae) {
                        localAborts++;
                    }
                }

                completed.addAndGet(localCompleted);
                reads.addAndGet(localReads);
                writes.addAndGet(localWrites);
                aborts.addAndGet(localAborts);
                return new long[] { localCompleted, localReads, localWrites, localAborts };
            }));
        }

        for (Future<long[]> f : futures) {
            try {
                f.get();
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                fail("Worker failed: " + (cause == null ? ee : cause));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for workers");
            }
        }
        pool.shutdown();
        try {
            pool.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Interrupted while awaiting pool termination");
        }

        final long t1 = System.nanoTime();
        final long elapsedMs = (t1 - t0) / 1_000_000L;

        return new long[] { elapsedMs, completed.get(), reads.get(), writes.get(), aborts.get() };
    }

    @Test
    @Order(5)
    public void randomized_mixed_ops_policy_perf() {
        final String baseDir = "testing/buffer-stress/random-policy";
        final String fileName = "rand_tbl";
        final int blockSize = 4096;
        final int nBuffers = 10_000;
        final int maxWaitMs = 250;
        final int nBlocks = 20_000;
        final int totalOps = 500_000;
        final int concurrency = 10;

        // Same seed for fair comparison between policies
        final long seed = 1234567890123456789L;

        long[] naiveRes = runMixedOpsWorkloadPerf(
                baseDir + "/naive-" + System.nanoTime(), fileName, blockSize, nBuffers, maxWaitMs,
                nBlocks, totalOps, concurrency, BufferMgrBase.EvictionPolicy.NAIVE, seed);

        long[] clockRes = runMixedOpsWorkloadPerf(
                baseDir + "/clock-" + System.nanoTime(), fileName, blockSize, nBuffers, maxWaitMs,
                nBlocks, totalOps, concurrency, BufferMgrBase.EvictionPolicy.CLOCK, seed);

        System.out.printf(
                "Random policy perf: NAIVE=%d ms, CLOCK=%d ms | ops=%,d, threads=%d, block=%dB, buffers=%d | aborts: naive=%d, clock=%d\n",
                naiveRes[0], clockRes[0], totalOps, concurrency, blockSize, nBuffers, naiveRes[4], clockRes[4]);

        // Sanity checks
        assertEquals(totalOps, naiveRes[1], "All NAIVE ops should complete");
        assertEquals(totalOps, clockRes[1], "All CLOCK ops should complete");
        assertTrue(naiveRes[4] == 0 && clockRes[4] == 0, "No aborts expected with large buffer pool");
        assertTrue(naiveRes[0] > 0 && clockRes[0] > 0, "Timings should be positive");
    }
}