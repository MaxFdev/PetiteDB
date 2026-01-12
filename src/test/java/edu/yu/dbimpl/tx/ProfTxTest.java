package edu.yu.dbimpl.tx;

/**
 * My tests include
 * • Verification that the module implements the classic acid semantics
 * discussed in lecture.
 * 
 * Note: test scenario construction (imho) is more difficult than usual because
 * you need to set up multiple concurrent tasks and (possibly) multiple
 * execution stages. Without such tests, however, it’s doubtful that you can
 * convince yourself that you’ve provided the required semantics.
 * 
 * • Performance and thread-safety evaluation of the transaction module as a
 * whole.
 * 
 * To give a sense of my expectations: on my iMac, a test
 * – Creates 10,000 transactions that execute sequentially, read and update a
 * set of integer, double, string, boolean values
 * – Validates expected behavior (with very detailed logging) and takes 8,100
 * milliseconds.
 * 
 * • Verification of the “block-and-wait” protocol for two conflicting
 * transactions.
 * 
 * • Verification of the “lock compatibility matrix”.
 */

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import edu.yu.dbimpl.buffer.BufferMgr;
import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;

public class ProfTxTest {

    private static final Logger logger = LogManager.getLogger(ProfTxTest.class);

    // Offsets for different value types within a block
    private static final int INT_OFFSET = 0;
    private static final int DOUBLE_OFFSET = 16; // allow room for int
    private static final int BOOLEAN_OFFSET = 32;
    private static final int STRING_OFFSET = 64; // room for primitives before

    private static final String DB_FILE = "profTxTestFile";
    private static final String LOG_FILE = "profTxTestLog";

    private static class Env {
        final TxMgrBase txMgr;
        final BlockIdBase block;

        Env(TxMgrBase t, BlockIdBase blk) {
            this.txMgr = t;
            this.block = blk;
        }
    }

    private Env initEnv(String dirName, int blockSize, int bufferSize, long maxWaitMillis) {
        final Properties dbProperties = new Properties();
        dbProperties.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(dbProperties);
        File dir = new File(dirName);
        dir.mkdirs();
        FileMgrBase fileMgr = new FileMgr(dir, blockSize);
        LogMgrBase logMgr = new LogMgr(fileMgr, LOG_FILE);
        BufferMgrBase bufferMgr = new BufferMgr(fileMgr, logMgr, bufferSize, (int) maxWaitMillis);
        TxMgrBase txMgr = new TxMgr(fileMgr, logMgr, bufferMgr, maxWaitMillis);
        BlockIdBase block = new BlockId(DB_FILE, 1); // assume block 1 exists/created lazily
        logger.info("Initialized environment dir={} blockSize={} bufferSize={} maxWait={}ms", dirName, blockSize,
                bufferSize, maxWaitMillis);
        return new Env(txMgr, block);
    }

    private void primeBlock(Env env, int intVal, double dblVal, boolean boolVal, String strVal) {
        TxBase tx = env.txMgr.newTx();
        tx.pin(env.block);
        tx.setInt(env.block, INT_OFFSET, intVal, true);
        tx.setDouble(env.block, DOUBLE_OFFSET, dblVal, true);
        tx.setBoolean(env.block, BOOLEAN_OFFSET, boolVal, true);
        tx.setString(env.block, STRING_OFFSET, strVal, true);
        logger.info("Primed block with int={}, double={}, boolean={}, string='{}'", intVal, dblVal, boolVal, strVal);
        tx.commit();
    }

    @Test
    @DisplayName("ACID semantics: commit visibility and rollback undo")
    public void acidSemanticsTest() {
        Env env = initEnv("testing/ProfTxTest_acid", 400, 10, 500);
        primeBlock(env, 42, 3.14, true, "fortyTwo");

        // tx1 validates primed state then overwrites and commits
        TxBase tx1 = env.txMgr.newTx();
        tx1.pin(env.block);
        assertEquals(42, tx1.getInt(env.block, INT_OFFSET));
        assertEquals(3.14, tx1.getDouble(env.block, DOUBLE_OFFSET));
        assertEquals(true, tx1.getBoolean(env.block, BOOLEAN_OFFSET));
        assertEquals("fortyTwo", tx1.getString(env.block, STRING_OFFSET));
        tx1.setInt(env.block, INT_OFFSET, 100, true);
        tx1.setDouble(env.block, DOUBLE_OFFSET, 6.28, true);
        tx1.setBoolean(env.block, BOOLEAN_OFFSET, false, true);
        tx1.setString(env.block, STRING_OFFSET, "oneHundred", true);
        logger.info("TX1 updated values then committing");
        tx1.commit();
        assertEquals(TxBase.Status.COMMITTED, tx1.getStatus());

        // tx2 sees tx1 committed values, then overwrites and commits
        TxBase tx2 = env.txMgr.newTx();
        tx2.pin(env.block);
        assertEquals(100, tx2.getInt(env.block, INT_OFFSET));
        assertEquals(6.28, tx2.getDouble(env.block, DOUBLE_OFFSET));
        assertEquals(false, tx2.getBoolean(env.block, BOOLEAN_OFFSET));
        assertEquals("oneHundred", tx2.getString(env.block, STRING_OFFSET));
        tx2.setInt(env.block, INT_OFFSET, 256, true);
        tx2.setDouble(env.block, DOUBLE_OFFSET, -1.0, true);
        tx2.setBoolean(env.block, BOOLEAN_OFFSET, true, true);
        tx2.setString(env.block, STRING_OFFSET, "twoFiveSix", true);
        logger.info("TX2 overwrote values then committing");
        tx2.commit();
        assertEquals(TxBase.Status.COMMITTED, tx2.getStatus());

        // tx3 modifies then rolls back
        TxBase tx3 = env.txMgr.newTx();
        tx3.pin(env.block);
        assertEquals(256, tx3.getInt(env.block, INT_OFFSET));
        tx3.setInt(env.block, INT_OFFSET, 9999, true);
        tx3.setString(env.block, STRING_OFFSET, "tempValue", true);
        logger.info("TX3 made temporary changes, rolling back");
        tx3.rollback();
        assertEquals(TxBase.Status.ROLLED_BACK, tx3.getStatus());

        // tx4 validates rollback restored tx2 values
        TxBase tx4 = env.txMgr.newTx();
        tx4.pin(env.block);
        assertEquals(256, tx4.getInt(env.block, INT_OFFSET));
        assertEquals("twoFiveSix", tx4.getString(env.block, STRING_OFFSET));
        logger.info("TX4 confirms rollback semantics then committing (read-only)");
        tx4.commit();
    }

    @Test
    @DisplayName("Performance & thread-safety: 10,000 sequential transactions")
    public void performanceSequentialTransactionsTest() {
        final int txCount = 10_000;
        Env env = initEnv("testing/ProfTxTest_perf", 400, 20, 500);
        primeBlock(env, 0, 0.0, true, "seed");

        long start = System.nanoTime();
        int expectedInt = 0;
        double expectedDouble = 0.0;
        boolean expectedBool = true;
        String expectedString = "seed";

        for (int i = 1; i <= txCount; i++) {
            TxBase tx = env.txMgr.newTx();
            tx.pin(env.block);
            // validate previous state
            assertEquals(expectedInt, tx.getInt(env.block, INT_OFFSET));
            assertEquals(expectedDouble, tx.getDouble(env.block, DOUBLE_OFFSET));
            assertEquals(expectedBool, tx.getBoolean(env.block, BOOLEAN_OFFSET));
            assertEquals(expectedString, tx.getString(env.block, STRING_OFFSET));
            // update state
            expectedInt += 1;
            expectedDouble -= 0.5; // arbitrary change
            expectedBool = !expectedBool;
            expectedString = "v" + i;
            tx.setInt(env.block, INT_OFFSET, expectedInt, false); // reduce logging overhead
            tx.setDouble(env.block, DOUBLE_OFFSET, expectedDouble, false);
            tx.setBoolean(env.block, BOOLEAN_OFFSET, expectedBool, false);
            tx.setString(env.block, STRING_OFFSET, expectedString, false);
            tx.commit();
            if (i % 1000 == 0) {
                logger.info("Progress: committed {} transactions", i);
            }
        }
        long end = System.nanoTime();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(end - start);
        double tps = txCount / (elapsedMs / 1000.0);
        logger.info("Sequential {} txs completed in {} ms ({} tx/s)", txCount, elapsedMs, String.format("%.2f", tps));

        // Loose upper bound; environment dependent
        assertTrue(elapsedMs < 20_000, "Performance regression: took too long (" + elapsedMs + " ms)");
    }

    @Test
    @DisplayName("Performance & thread-safety: concurrent transactions with contention (16 threads)")
    public void performanceConcurrentTransactionsTest() throws Exception {
        final int threadCount = 16;
        final int txPerThread = 625; // 10,000 total transactions
        final int totalTxCount = threadCount * txPerThread;
        final int blockCount = 2000; // reduced from 1000 to avoid buffer exhaustion
        // 2 second lock wait timeout with larger buffer size to handle concurrent load
        Env env = initEnv("testing/ProfTxTest_concurrent", 400, 1500, 2_000);

        // Prime all blocks with initial deterministic values
        final int initialInt = 1000;
        final double initialDouble = 100.0;
        BlockIdBase[] blocks = new BlockIdBase[blockCount];

        // Track transaction counts per block for deterministic verification
        final java.util.concurrent.ConcurrentHashMap<Integer, java.util.concurrent.atomic.AtomicInteger> blockTxCounts = new java.util.concurrent.ConcurrentHashMap<>();

        for (int b = 0; b < blockCount; b++) {
            blocks[b] = new BlockId(DB_FILE, b + 1);
            blockTxCounts.put(b, new java.util.concurrent.atomic.AtomicInteger(0));
        }

        // Prime blocks one at a time to avoid buffer issues
        for (int b = 0; b < blockCount; b++) {
            TxBase primeTx = env.txMgr.newTx();
            primeTx.pin(blocks[b]);
            primeTx.setInt(blocks[b], INT_OFFSET, initialInt, false);
            primeTx.setDouble(blocks[b], DOUBLE_OFFSET, initialDouble, false);
            primeTx.commit();
            if ((b + 1) % 25 == 0) {
                logger.info("Primed {} / {} blocks", b + 1, blockCount);
            }
        }
        logger.info("Primed all {} blocks for concurrent testing", blockCount);

        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(threadCount);

        long globalStart = System.nanoTime();

        for (int threadId = 0; threadId < threadCount; threadId++) {
            final int tid = threadId;
            exec.submit(() -> {
                try {
                    startLatch.await(); // synchronize start
                    int localSuccessCount = 0;

                    for (int i = 0; i < txPerThread; i++) {
                        // Deterministic block assignment
                        int primaryBlockIndex = (tid * txPerThread + i) % blockCount;
                        BlockIdBase targetBlock = blocks[primaryBlockIndex];

                        TxBase tx = env.txMgr.newTx();

                        // Pin and read current values
                        tx.pin(targetBlock);
                        int currentInt = tx.getInt(targetBlock, INT_OFFSET);
                        double currentDouble = tx.getDouble(targetBlock, DOUBLE_OFFSET);

                        // Deterministic computation: increment int by 1, add 0.5 to double
                        int newInt = currentInt + 1;
                        double newDouble = currentDouble + 0.5;

                        // Write updated values without logging for performance
                        tx.setInt(targetBlock, INT_OFFSET, newInt, false);
                        tx.setDouble(targetBlock, DOUBLE_OFFSET, newDouble, false);

                        // Verify read-after-write consistency every 10th transaction
                        if (i % 10 == 0) {
                            int verifyInt = tx.getInt(targetBlock, INT_OFFSET);
                            double verifyDouble = tx.getDouble(targetBlock, DOUBLE_OFFSET);
                            assertEquals(newInt, verifyInt, "Read-after-write consistency failure for int");
                            assertEquals(newDouble, verifyDouble, 0.0001,
                                    "Read-after-write consistency failure for double");
                        }

                        tx.commit();
                        localSuccessCount++;

                        // Track transaction count per block
                        blockTxCounts.get(primaryBlockIndex).incrementAndGet();

                        if (localSuccessCount % 100 == 0) {
                            logger.info("Thread-{} progress: {} transactions committed", tid, localSuccessCount);
                        }
                    }

                    logger.info("Thread-{} completed all {} transactions successfully", tid, localSuccessCount);
                    assertEquals(txPerThread, localSuccessCount, "Thread " + tid + " should complete all transactions");
                } catch (Exception e) {
                    logger.error("Thread-{} encountered exception", tid, e);
                    fail("Thread " + tid + " failed: " + e.getMessage());
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        logger.info("Starting {} threads with {} transactions each (total: {})", threadCount, txPerThread,
                totalTxCount);
        startLatch.countDown();

        // Wait for completion
        boolean finished = completionLatch.await(20, TimeUnit.SECONDS);
        assertTrue(finished, "Test timed out waiting for threads to complete");

        long globalEnd = System.nanoTime();
        long totalElapsedMs = TimeUnit.NANOSECONDS.toMillis(globalEnd - globalStart);
        double tps = totalTxCount / (totalElapsedMs / 1000.0);

        logger.info("Concurrent {} txs across {} threads completed in {} ms ({} tx/s)",
                totalTxCount, threadCount, totalElapsedMs, String.format("%.2f", tps));

        // Validate final state - one block at a time for deterministic verification
        logger.info("Final state validation across {} blocks (one at a time):", blockCount);
        int validationErrors = 0;

        for (int b = 0; b < blockCount; b++) {
            TxBase finalTx = env.txMgr.newTx();
            finalTx.pin(blocks[b]);

            int blockInt = finalTx.getInt(blocks[b], INT_OFFSET);
            double blockDouble = finalTx.getDouble(blocks[b], DOUBLE_OFFSET);

            finalTx.commit();

            // Calculate expected values based on actual transaction count for this block
            int txCountForBlock = blockTxCounts.get(b).get();
            int expectedBlockInt = initialInt + txCountForBlock;
            double expectedBlockDouble = initialDouble + (txCountForBlock * 0.5);

            // Verify exact deterministic values
            if (blockInt != expectedBlockInt) {
                logger.error("Block {} int mismatch: expected={}, actual={}, txCount={}",
                        b, expectedBlockInt, blockInt, txCountForBlock);
                validationErrors++;
            }
            if (Math.abs(blockDouble - expectedBlockDouble) > 0.0001) {
                logger.error("Block {} double mismatch: expected={}, actual={}, txCount={}",
                        b, expectedBlockDouble, blockDouble, txCountForBlock);
                validationErrors++;
            }

            if ((b + 1) % 25 == 0) {
                logger.info("Validated {} / {} blocks", b + 1, blockCount);
            }
        }

        logger.info("Validation complete: {} errors found", validationErrors);
        assertEquals(0, validationErrors, "All blocks should have deterministic final state");

        // Verify total transaction count
        int totalRecordedTxs = blockTxCounts.values().stream()
                .mapToInt(java.util.concurrent.atomic.AtomicInteger::get)
                .sum();
        assertEquals(totalTxCount, totalRecordedTxs,
                "Total recorded transactions should equal expected transaction count");

        exec.shutdown();
        assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS), "ExecutorService failed to terminate");

        // Performance expectation: <= 10 seconds
        assertTrue(totalElapsedMs <= 10_000,
                "Performance regression: concurrent test took too long (" + totalElapsedMs
                        + " ms, expected <= 10000 ms)");
    }

    @Test
    @DisplayName("Block-and-wait protocol for conflicting write locks")
    public void blockAndWaitProtocolTest() throws Exception {
        Env env = initEnv("testing/ProfTxTest_blockWait", 400, 10, 1_000); // generous wait time
        primeBlock(env, 10, 1.0, true, "init");

        final long holdMillis = 200;
        CountDownLatch lockAcquiredLatch = new CountDownLatch(1);

        Runnable holder = () -> {
            TxBase tx = env.txMgr.newTx();
            tx.pin(env.block);
            tx.setInt(env.block, INT_OFFSET, 11, true); // acquires x-lock
            logger.info("Holder TX obtained x-lock and will hold for {} ms", holdMillis);
            lockAcquiredLatch.countDown();
            try {
                Thread.sleep(holdMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            tx.commit();
            logger.info("Holder TX committed, released x-lock");
        };

        final long[] waitDuration = new long[1];
        Runnable waiter = () -> {
            try {
                lockAcquiredLatch.await();
                long start = System.nanoTime();
                TxBase tx = env.txMgr.newTx();
                tx.pin(env.block);
                tx.setInt(env.block, INT_OFFSET, 22, true); // should block until holder commits
                long end = System.nanoTime();
                waitDuration[0] = TimeUnit.NANOSECONDS.toMillis(end - start);
                logger.info("Waiter TX acquired x-lock after waiting {} ms", waitDuration[0]);
                tx.commit();
            } catch (Exception e) {
                logger.error("Waiter encountered exception", e);
                fail("Waiter should not fail acquiring lock: " + e.getMessage());
            }
        };

        Thread tHolder = new Thread(holder, "holder-thread");
        Thread tWaiter = new Thread(waiter, "waiter-thread");
        tHolder.start();
        tWaiter.start();
        tHolder.join();
        tWaiter.join();

        logger.info("Measured waiter wait duration={} ms, holder holdMillis={} ms", waitDuration[0], holdMillis);
        assertTrue(waitDuration[0] >= holdMillis - 25, "Waiter did not sufficiently block; wait=" + waitDuration[0]);
    }

    @Test
    @DisplayName("Lock compatibility matrix: S/S allowed, S->X upgrade, writer waits for readers")
    public void lockCompatibilityMatrixTest() throws Exception {
        Env env = initEnv("testing/ProfTxTest_lockMatrix", 400, 10, 1_000);
        primeBlock(env, 5, 2.5, true, "start");

        ExecutorService exec = Executors.newFixedThreadPool(4);
        CountDownLatch readersPinned = new CountDownLatch(2);
        CountDownLatch releaseReaders = new CountDownLatch(1);
        final long readerHoldMillis = 250;

        Future<Long> reader1 = exec.submit(() -> {
            TxBase tx = env.txMgr.newTx();
            tx.pin(env.block);
            int val = tx.getInt(env.block, INT_OFFSET); // s-lock
            readersPinned.countDown();
            logger.info("Reader1 pinned & read int={}, holding for {} ms", val, readerHoldMillis);
            releaseReaders.await();
            tx.commit();
            return System.currentTimeMillis();
        });
        Future<Long> reader2 = exec.submit(() -> {
            TxBase tx = env.txMgr.newTx();
            tx.pin(env.block);
            int val = tx.getInt(env.block, INT_OFFSET); // s-lock
            readersPinned.countDown();
            logger.info("Reader2 pinned & read int={}, holding for {} ms", val, readerHoldMillis);
            releaseReaders.await();
            tx.commit();
            return System.currentTimeMillis();
        });

        // writer waits for readers
        Future<Long> writer = exec.submit(() -> {
            readersPinned.await();
            long start = System.nanoTime();
            TxBase tx = env.txMgr.newTx();
            tx.pin(env.block);
            tx.setInt(env.block, INT_OFFSET, 77, true); // x-lock attempt
            long end = System.nanoTime();
            tx.commit();
            long waitMs = TimeUnit.NANOSECONDS.toMillis(end - start);
            logger.info("Writer waited {} ms for readers", waitMs);
            return waitMs;
        });

        // upgrade transaction
        Future<Long> upgrader = exec.submit(() -> {
            TxBase tx = env.txMgr.newTx();
            tx.pin(env.block);
            int before = tx.getInt(env.block, INT_OFFSET); // s-lock
            long start = System.nanoTime();
            tx.setInt(env.block, INT_OFFSET, before + 1, true); // upgrade to x-lock
            long end = System.nanoTime();
            tx.commit();
            long upgradeWaitMs = TimeUnit.NANOSECONDS.toMillis(end - start);
            logger.info("Upgrader performed S->X upgrade wait={} ms", upgradeWaitMs);
            return upgradeWaitMs;
        });

        // hold readers then release
        Thread.sleep(readerHoldMillis);
        releaseReaders.countDown();

        long writerWait = writer.get();
        long upgradeWait = upgrader.get();
        reader1.get();
        reader2.get();
        exec.shutdown();

        // Writer should wait at least readerHoldMillis minus scheduling overhead
        assertTrue(writerWait >= readerHoldMillis - 40, "Writer wait too short, writerWait=" + writerWait);
        // Upgrade should be fast (within readerHoldMillis plus scheduling overhead)
        assertTrue(upgradeWait < readerHoldMillis + 10, "Upgrade took unexpectedly long: " + upgradeWait);
    }
}
