package edu.yu.dbimpl.buffer;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Properties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

/**
 * Professor test notes:
 * 
 * • Verification that the buffer module implementation implements the “pin”
 * scenarios discussed in lecture.
 * 
 * • Performance and thread-safety evaluation of the buffer module as a whole.
 * 
 * To give a sense of my expectations: on my Mac Pro (specifications in the
 * FileModule requirements doc), a test
 * 
 * – Creates a BufferMgr with 1,000 buffers and maximum wait time of 500ms
 * 
 * – Creates 10,000 blocks (400 bytes per block) by pinning them into a Buffer
 * and writing a small String and integer into each block
 * 
 * – Reads the blocks randomly into main-memory using the BufferMgr and a
 * concurrency level of 10 and verifies that all data were read successfully
 * 
 * and takes 700 milliseconds.
 * 
 * • Verification that the CLOCK cache eviction algorithm performs better than
 * the NAIVE algorithm when data-access patterns imply that the “hot set” can be
 * cached in memory (such that blocks aren’t “equally valuable”).
 * 
 * – NOTE: I found that no dramatic differences manifest for small block sizes
 * (e.g., 400 bytes). I hypothesize that this is because the os block sizes are
 * much larger and therefore pre-fetch and cache many dbms “blocks/pages” when
 * the dbms accesses a single block. More dramatic results (e.g., CLOCK does
 * 20% better than NAIVE) appear for e.g., 8K block sizes.
 * 
 * • Some amount of testing that the implementation can handle edge-case input
 * to the API.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ProfBufferTest {

    /**
     * Verification that the buffer module implementation implements the “pin”
     * scenarios discussed in lecture.
     * 
     * 4 cases:
     * 1. Client asks the buffer manager to pin block i and block i is currently
     * pinned in a buffer page
     * 2. Client asks the buffer manager to pin block i and block i is currently
     * unpinned in a buffer page
     * 3.Client asks the buffer manager to pin block i and block i is not currently
     * in any buffer page and the buffer pool contains at least one unpinned page
     * 4. Client asks the buffer manager to pin block i and block i is not currently
     * in any buffer page and all pages in the buffer pool are pinned
     */
    @Test
    @Order(0)
    public void VerifyPinScenarioTest() {
        // Fresh DB startup so we don't reuse any on-disk state
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        final String dir = "testing/profbuffer/pin-scenarios";
        final int blockSize = 256;
        final int bufferSize = 2; // small pool to exercise behavior easily
        final int maxWaitMs = 150;

        FileMgrBase fm = new FileMgr(new File(dir), blockSize);
        LogMgrBase lm = new LogMgr(fm, "prof_pin_scenarios_log");
        BufferMgr bm = new BufferMgr(fm, lm, bufferSize, maxWaitMs);

        // Define a few blocks
        BlockId b1 = new BlockId("tbl", 1);
        BlockId b2 = new BlockId("tbl", 2);
        BlockId b3 = new BlockId("tbl", 3);

        // Case 1: pin block i when i is currently pinned in a buffer page
        int startAvailable = bm.available();
        assertEquals(bufferSize, startAvailable, "All buffers should be initially available");

        BufferBase buf1 = bm.pin(b1);
        assertNotNull(buf1);
        assertEquals(bufferSize - 1, bm.available(), "Pin should consume one available permit");

        BufferBase buf1Again = bm.pin(b1);
        assertSame(buf1, buf1Again, "Pinning the same block should return the same Buffer instance");
        assertEquals(bufferSize - 1, bm.available(), "Double-pin on same block must not consume another permit");

        // Case 2: pin block i when i is currently unpinned in a buffer page
        // Unpin twice to drop pin-count to zero (becomes unpinned)
        bm.unpin(buf1);
        assertEquals(bufferSize - 1, bm.available(), "Unpinning once (still pinned) should not change availability");
        bm.unpin(buf1Again);
        assertEquals(bufferSize, bm.available(), "Unpinning to zero should release a permit");

        // Now re-pin the same block that is resident but unpinned
        BufferBase buf1RePinned = bm.pin(b1);
        assertSame(buf1, buf1RePinned, "Re-pinning an unpinned resident block should reuse the same Buffer");
        assertEquals(bufferSize - 1, bm.available(), "Re-pinning should consume one permit");

        // Leave it pinned once for next scenarios

        // Case 3: pin block i not currently in any buffer page, and at least one
        // unpinned page exists
        // Ensure there is at least one unpinned buffer available
        assertEquals(bufferSize - 1, bm.available());
        // Unpin buf1 to free a slot, and pin b2 so that b1 remains resident or
        // replaced, then unpin b2
        bm.unpin(buf1RePinned); // available becomes bufferSize
        assertEquals(bufferSize, bm.available());

        // At this point, the pool has only unpinned pages; choose a block not in any
        // buffer (b3)
        BufferBase buf3 = bm.pin(b3);
        assertNotNull(buf3, "Should be able to pin a block not currently in the pool when a page is available");
        assertEquals(bufferSize - 1, bm.available(), "Pinning b3 should reduce available by one");
        assertEquals(b3, ((Buffer) buf3).block(), "Pinned buffer should reference requested block");

        // Case 4: pin block i not currently in any buffer page and all pages in the
        // buffer pool are pinned
        // Pin another distinct block to exhaust the pool
        BufferBase buf2 = bm.pin(b2);
        assertEquals(0, bm.available(), "Pool should now be fully pinned");

        // Now attempt to pin a new block with the pool exhausted should abort within
        // maxWaitMs
        BlockId b4 = new BlockId("tbl", 4);
        assertThrows(BufferAbortException.class, () -> bm.pin(b4),
                "Pinning with all pages pinned should abort with BufferAbortException");

        // Cleanup to avoid affecting other tests
        bm.unpin(buf2);
        bm.unpin(buf3);
    }

    /**
     * Performance and thread-safety evaluation of the buffer module as a whole.
     * 
     * Tests the following:
     * – Creates a BufferMgr with 1,000 buffers and maximum wait time of 500ms
     * – Creates 10,000 blocks (400 bytes per block) by pinning them into a Buffer
     * and writing a small String and integer into each block
     * – Reads the blocks randomly into main-memory using the BufferMgr and a
     * concurrency level of 10 (threads) and verifies that all data were read
     * successfully
     * 
     * Should take around 1 second.
     */
    @Test
    @Order(1)
    public void PerformanceAndThreadSafetyTest() {
        // Fresh DB startup
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        final String dir = "testing/profbuffer/perf";
        final int blockSize = 400; // bytes per block as specified
        final int nBuffers = 1000;
        final int maxWaitMs = 500;
        final int nBlocks = 10_000;
        final int concurrency = 10;
        final String fileName = "perf_tbl";

        FileMgrBase fm = new FileMgr(new File(dir), blockSize);
        LogMgrBase lm = new LogMgr(fm, "prof_perf_log");
        BufferMgr bm = new BufferMgr(fm, lm, nBuffers, maxWaitMs);

        final int txId = 1;

        final long t0 = System.nanoTime();
        // Create 10,000 blocks, write small String and integer to each
        for (int i = 0; i < nBlocks; i++) {
            BlockId b = new BlockId(fileName, i);
            BufferBase buf = bm.pin(b);
            try {
                PageBase p = buf.contents();
                final int intPos = 0;
                final int strPos = Integer.BYTES; // leave space for int
                final int expectedInt = i * 31 + 7;
                final String expectedStr = "blk#" + i;
                p.setInt(intPos, expectedInt);
                p.setString(strPos, expectedStr);
                // Append a tiny log record to obtain a valid LSN
                int lsn = ((LogMgr) lm).append(new byte[] { 1 });
                buf.setModified(txId, lsn);
            } finally {
                bm.unpin(buf);
            }
        }
        // Ensure all modified buffers are persisted before random reads
        bm.flushAll(txId);
        final long t1 = System.nanoTime();

        // Prepare randomized read order
        List<Integer> indices = new ArrayList<>(nBlocks);
        for (int i = 0; i < nBlocks; i++) {
            indices.add(i);
        }
        Collections.shuffle(indices);

        // Partition work across threads
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        List<Future<Integer>> futures = new ArrayList<>(concurrency);
        final AtomicInteger verified = new AtomicInteger(0);

        int chunk = (nBlocks + concurrency - 1) / concurrency; // ceil
        for (int t = 0; t < concurrency; t++) {
            final int start = t * chunk;
            final int end = Math.min(indices.size(), start + chunk);
            if (start >= end) {
                break;
            }
            final List<Integer> slice = indices.subList(start, end);
            Callable<Integer> task = () -> {
                int localCount = 0;
                for (int idx : slice) {
                    BlockId b = new BlockId(fileName, idx);
                    BufferBase buf = bm.pin(b);
                    try {
                        PageBase p = buf.contents();
                        final int intPos = 0;
                        final int strPos = Integer.BYTES;
                        final int expectedInt = idx * 31 + 7;
                        final String expectedStr = "blk#" + idx;
                        int gotInt = p.getInt(intPos);
                        String gotStr = p.getString(strPos);
                        if (gotInt != expectedInt || !expectedStr.equals(gotStr)) {
                            fail("Data mismatch for block " + idx + ": got (" + gotInt + ", '" + gotStr
                                    + "') expected (" + expectedInt + ", '" + expectedStr + "')");
                        }
                        localCount++;
                    } finally {
                        bm.unpin(buf);
                    }
                }
                verified.addAndGet(localCount);
                return localCount;
            };
            futures.add(pool.submit(task));
        }

        // Wait for completion and propagate any errors
        for (Future<Integer> f : futures) {
            try {
                f.get();
            } catch (ExecutionException ee) {
                // Unwrap and signal failure
                Throwable cause = ee.getCause();
                fail("Reader task failed: " + (cause == null ? ee : cause));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for reader task completion");
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

        // Timing info (non-fatal): write and read phases
        final long writeMs = (t1 - t0) / 1_000_000L;
        final long readMs = (t2 - t1) / 1_000_000L;
        System.out.printf("Perf test: wrote %,d blocks in %d ms, read+verified in %d ms with %d threads%n", nBlocks,
                writeMs, readMs, concurrency);
    }

    /**
     * Verification that the CLOCK cache eviction algorithm performs better than
     * the NAIVE algorithm when data-access patterns imply that the “hot set” can be
     * cached in memory (such that blocks aren’t “equally valuable”).
     * 
     * NOTE: No dramatic differences manifest for small block sizes (e.g., 400
     * bytes). This is likely because the os block sizes are much larger and
     * therefore pre-fetch and cache many dbms “blocks/pages” when the dbms accesses
     * a single block. More dramatic results (e.g., CLOCK does 20% better than
     * NAIVE) appear for e.g., 8K block sizes.
     * 
     * Please test both small and large block sizes and measure the performance.
     */
    @Test
    @Order(2)
    public void AlgorithmPerformanceTest() {
        // Fresh DB startup for clean directories per run
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        // Parameters
        final String baseDir = "testing/profbuffer/algoperf";
        final String fileName = "algofile";

        // Workload characteristics: hot set fits in buffer pool
        final int hotSize = 100;
        final int coldSize = 1000;
        final int bufferSize = hotSize; // ensure hot set can be cached entirely
        final int ops = 20_000; // total accesses per policy
        final double hotProb = 0.9; // 90% hot, 10% cold

        // Small and large block sizes per assignment guidance
        final int[] blockSizes = new int[] { 400, 8192 };

        long naiveSmall = -1, clockSmall = -1, naiveLarge = -1, clockLarge = -1;

        for (int i = 0; i < blockSizes.length; i++) {
            final int blockSize = blockSizes[i];
            final String naiveDir = baseDir + "/naive-" + blockSize + "-" + System.nanoTime();
            final String clockDir = baseDir + "/clock-" + blockSize + "-" + System.nanoTime();

            // NAIVE policy timing
            long tNaive = runEvictionWorkload(naiveDir, fileName, blockSize, bufferSize, hotSize, coldSize, ops,
                    hotProb, BufferMgrBase.EvictionPolicy.NAIVE);

            // CLOCK policy timing
            long tClock = runEvictionWorkload(clockDir, fileName, blockSize, bufferSize, hotSize, coldSize, ops,
                    hotProb, BufferMgrBase.EvictionPolicy.CLOCK);

            if (blockSize == 400) {
                naiveSmall = tNaive;
                clockSmall = tClock;
            } else {
                naiveLarge = tNaive;
                clockLarge = tClock;
            }
        }

        // Report non-fatal timing info
        System.out.printf("AlgPerf (small=400B): NAIVE=%d ms, CLOCK=%d ms%n", naiveSmall, clockSmall);
        System.out.printf("AlgPerf (large=8192B): NAIVE=%d ms, CLOCK=%d ms%n", naiveLarge, clockLarge);

        // Sanity: timings should be positive; we report relative performance but do not
        // hard-assert an ordering
        assertTrue(naiveSmall > 0 && clockSmall > 0 && naiveLarge > 0 && clockLarge > 0,
                "Timings must be positive for both policies and block sizes");
    }

    /**
     * Initializes a dataset with the specified hot and cold set, then runs a mixed
     * access workload and returns elapsed time in milliseconds.
     */
    private long runEvictionWorkload(String dir, String fileName, int blockSize, int bufferSize, int hotSize,
            int coldSize, int ops, double hotProb, BufferMgrBase.EvictionPolicy policy) {
        // Ensure fresh DB directory for initialization
        final Properties startup = new Properties();
        startup.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(startup);

        final FileMgrBase fmInit = new FileMgr(new File(dir), blockSize);
        final LogMgrBase lmInit = new LogMgr(fmInit, "algolog");
        final BufferMgr bmInit = new BufferMgr(fmInit, lmInit, bufferSize, 500, BufferMgrBase.EvictionPolicy.NAIVE);

        // Initialize hot and cold blocks once on disk
        final int tx = 1;
        for (int i = 0; i < hotSize + coldSize; i++) {
            BlockId b = new BlockId(fileName, i);
            BufferBase buf = bmInit.pin(b);
            try {
                PageBase p = buf.contents();
                p.setInt(0, i);
                int lsn = ((LogMgr) lmInit).append(new byte[] { 1 });
                buf.setModified(tx, lsn);
            } finally {
                bmInit.unpin(buf);
            }
        }
        bmInit.flushAll(tx);

        // Restart semantics (no full reset of files), then run measured workload
        final Properties restart = new Properties();
        restart.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
        DBConfiguration.INSTANCE.get().setConfiguration(restart);

        final FileMgrBase fm = new FileMgr(new File(dir), blockSize);
        final LogMgrBase lm = new LogMgr(fm, "algolog");
        final BufferMgr bm = new BufferMgr(fm, lm, bufferSize, 500, policy);

        // Preload hot set into the buffer cache
        List<BufferBase> pinned = new ArrayList<>(hotSize);
        for (int i = 0; i < hotSize; i++) {
            BufferBase buf = bm.pin(new BlockId(fileName, i));
            pinned.add(buf);
        }
        for (BufferBase b : pinned) {
            bm.unpin(b);
        }

        // Deterministic pseudo-random sequence
        long seed = 123456789L;
        long a = 2862933555777941757L; // LCG constants
        long c = 3037000493L;

        final long t0 = System.nanoTime();
        for (int k = 0; k < ops; k++) {
            // LCG step
            seed = a * seed + c;
            // Convert to [0,1)
            double r = ((seed >>> 1) & ((1L << 53) - 1)) / (double) (1L << 53);
            int idx;
            if (r < hotProb) {
                // hot set
                idx = (int) (Math.floor(r / hotProb * hotSize));
            } else {
                // cold set
                double rc = (r - hotProb) / (1.0 - hotProb);
                idx = hotSize + (int) (Math.floor(rc * coldSize));
            }

            BufferBase buf = bm.pin(new BlockId(fileName, idx));
            try {
                // Light read to exercise page without extra logging or writes
                buf.contents().getInt(0);
            } finally {
                bm.unpin(buf);
            }
        }
        final long t1 = System.nanoTime();

        return (t1 - t0) / 1_000_000L; // ms
    }

    /**
     * Test that the implementation can handle edge-case input to the API.
     */
    @Test
    @Order(3)
    public void EdgeCaseTest() {
        // Fresh DB startup so we don't reuse any on-disk state
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        final String dir = "testing/profbuffer/edge";
        final int blockSize = 256;
        final int nBuffers = 2;
        final int maxWaitMs = 100;

        FileMgrBase fm = new FileMgr(new File(dir), blockSize);
        LogMgrBase lm = new LogMgr(fm, "edge_log");

        // Constructor edge cases: non-positive wait time
        assertThrows(IllegalArgumentException.class, () -> new BufferMgr(fm, lm, nBuffers, 0));
        assertThrows(IllegalArgumentException.class, () -> new BufferMgr(fm, lm, nBuffers, -5));
        assertThrows(IllegalArgumentException.class,
                () -> new BufferMgr(fm, lm, nBuffers, 0, BufferMgrBase.EvictionPolicy.NAIVE));

        BufferMgr bm = new BufferMgr(fm, lm, nBuffers, maxWaitMs);

        // pin(null) => IAE
        assertThrows(IllegalArgumentException.class, () -> bm.pin(null));

        // unpin(null) => IAE
        assertThrows(IllegalArgumentException.class, () -> bm.unpin(null));

        // flushAll with negative tx => IAE
        assertThrows(IllegalArgumentException.class, () -> bm.flushAll(-1));

        // Acquire a buffer to exercise Buffer-level edge cases
        BlockId b1 = new BlockId("edge_tbl", 1);
        BufferBase buf1 = bm.pin(b1);
        assertNotNull(buf1);

        // Buffer.setModified: negative txnum => IAE
        assertThrows(IllegalArgumentException.class, () -> ((Buffer) buf1).setModified(-1, -1));

        // Buffer.setModified: invalid positive LSN => IAE (no such log record exists)
        assertThrows(IllegalArgumentException.class, () -> ((Buffer) buf1).setModified(1, 999_999));

        // Buffer.setModified: negative LSN allowed (means no log record was generated)
        ((Buffer) buf1).setModified(1, -1);

        // Double-unpin: second unpin should fail
        bm.unpin(buf1);
        assertThrows(IllegalArgumentException.class, () -> bm.unpin(buf1));

        // Pool exhaustion: with small pool, fully pin and verify timeout on another pin
        BufferBase buf2 = bm.pin(new BlockId("edge_tbl", 2));
        BufferBase buf3 = bm.pin(new BlockId("edge_tbl", 3));
        assertEquals(0, bm.available(), "All buffers should be pinned");
        assertThrows(BufferAbortException.class, () -> bm.pin(new BlockId("edge_tbl", 4)));

        // Cleanup
        bm.unpin(buf2);
        bm.unpin(buf3);
    }
}
