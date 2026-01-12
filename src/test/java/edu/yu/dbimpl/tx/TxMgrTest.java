package edu.yu.dbimpl.tx;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
import edu.yu.dbimpl.tx.concurrency.LockAbortException;
import edu.yu.dbimpl.tx.recovery.LogRecordImpl;

/**
 * Tests focused on TxMgr and Tx integration behaviors not covered by
 * TxProfDemoTest.
 */
public class TxMgrTest {

    private TxMgrBase newTxMgr(String testName, int blockSize, int bufferCount, int waitMillis) {
        Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);
        Path dir = Path.of("testing", "TxMgrTest", testName);
        try {
            Files.createDirectories(dir);
        } catch (Exception e) {
            fail("Failed to create test directory: " + e.getMessage());
        }
        FileMgrBase fm = new FileMgr(dir.toFile(), blockSize);
        LogMgrBase lm = new LogMgr(fm, "txmgr_log");
        BufferMgrBase bm = new BufferMgr(fm, lm, bufferCount, waitMillis);
        return new TxMgr(fm, lm, bm, waitMillis);
    }

    @Test
    @DisplayName("Constructor performs auto recovery: internal tx0 used, next txnum is 1, checkpoint present")
    void autoRecoveryCheckpoint() {
        Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
        DBConfiguration.INSTANCE.get().setConfiguration(props);
        Path dir = Path.of("testing", "TxMgrTest", "autoRecoveryCheckpoint");
        try {
            Files.createDirectories(dir);
        } catch (Exception e) {
            fail(e);
        }
        FileMgrBase fm = new FileMgr(dir.toFile(), 256);
        LogMgrBase lm = new LogMgr(fm, "txmgr_log");
        BufferMgrBase bm = new BufferMgr(fm, lm, 8, 200);
        TxMgrBase txMgr = new TxMgr(fm, lm, bm, 200);

        TxBase external = txMgr.newTx();
        assertEquals(1, external.txnum(), "txnum should start at 1 externally since 0 consumed by recovery");

        boolean checkpointFound = false;
        var it = lm.iterator();
        while (it.hasNext()) {
            LogRecordImpl rec = new LogRecordImpl(it.next());
            if (rec.op() == LogRecordImpl.CHECKPOINT) {
                checkpointFound = true;
                break;
            }
        }
        assertTrue(checkpointFound, "Recovery should append a CHECKPOINT record");
    }

    @Test
    @DisplayName("Sequential transaction numbers increment starting at 1 (0 consumed by recovery)")
    void sequentialTxNumbers() {
        TxMgrBase txMgr = newTxMgr("sequentialTxNumbers", 256, 4, 100);
        List<Integer> nums = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            nums.add(txMgr.newTx().txnum());
        }
        assertEquals(List.of(0, 1, 2, 3, 4), nums, "Tx numbers should increment sequentially");
    }

    @Test
    @DisplayName("resetAllLockState clears global lock map after locks acquired")
    void resetAllLockState() {
        TxMgrBase txMgrBase = newTxMgr("resetAllLockState", 256, 4, 500);
        assertTrue(txMgrBase instanceof TxMgr);
        TxMgr txMgr = (TxMgr) txMgrBase;
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("fileA");
        tx.pin(blk);
        tx.setInt(blk, 0, 123, true); // acquire xLock
        assertFalse(txMgr.getGlobalLockMap().isEmpty(), "Lock map should contain entry after setInt");
        txMgr.resetAllLockState();
        assertTrue(txMgr.getGlobalLockMap().isEmpty(), "Lock map should be empty after resetAllLockState");
    }

    @Test
    @DisplayName("Concurrent shared locks allow parallel reads")
    void concurrentSharedLocks() throws Exception {
        TxMgrBase txMgr = newTxMgr("concurrentSharedLocks", 256, 6, 300);
        TxBase writer = txMgr.newTx();
        BlockIdBase blk = writer.append("fileB");
        writer.pin(blk);
        writer.setInt(blk, 0, 77, true);
        // Commit so readers see stable value
        writer.commit();

        TxBase reader1 = txMgr.newTx();
        TxBase reader2 = txMgr.newTx();
        reader1.pin(blk);
        reader2.pin(blk);

        CountDownLatch latch = new CountDownLatch(1);
        ExecutorService exec = Executors.newFixedThreadPool(2);
        Future<Integer> f1 = exec.submit(() -> {
            latch.await();
            return reader1.getInt(blk, 0);
        });
        Future<Integer> f2 = exec.submit(() -> {
            latch.await();
            return reader2.getInt(blk, 0);
        });
        latch.countDown();
        assertEquals(77, f1.get());
        assertEquals(77, f2.get());
        exec.shutdownNow();
        reader1.rollback();
        reader2.rollback();
    }

    @Test
    @DisplayName("Exclusive lock causes timeout for second writer")
    void exclusiveLockTimeout() throws Exception {
        int waitMillis = 50; // short to force timeout
        TxMgrBase txMgr = newTxMgr("exclusiveLockTimeout", 256, 4, waitMillis);
        TxBase txA = txMgr.newTx();
        BlockIdBase blk = txA.append("fileC");
        txA.pin(blk);
        txA.setInt(blk, 0, 1, true); // acquire xLock and keep ACTIVE

        TxBase txB = txMgr.newTx();
        txB.pin(blk);
        // Attempt to acquire xLock in another thread to simulate contention
        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<Throwable> future = exec.submit(() -> {
            try {
                txB.setInt(blk, 0, 2, true);
                return null; // unexpected success
            } catch (Throwable t) {
                return t;
            }
        });
        Throwable thrown = future.get();
        exec.shutdownNow();
        assertNotNull(thrown, "Second writer should fail acquiring xLock");
        assertTrue(thrown instanceof LockAbortException, "Expected LockAbortException but got: " + thrown);
        txA.rollback();
        txB.rollback();
    }

    @Test
    @DisplayName("availableBuffs guarded after commit")
    void availableBuffsInactiveGuard() {
        TxMgrBase txMgr = newTxMgr("availableBuffsInactiveGuard", 256, 3, 200);
        TxBase tx = txMgr.newTx();
        tx.commit();
        assertThrows(IllegalArgumentException.class, () -> tx.availableBuffs());
    }

    @Test
    @DisplayName("pin after commit throws IllegalStateException")
    void pinAfterCommitGuard() {
        TxMgrBase txMgr = newTxMgr("pinAfterCommitGuard", 256, 3, 200);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("fileD");
        tx.pin(blk);
        tx.commit();
        assertThrows(IllegalStateException.class, () -> tx.pin(blk));
    }

    @Test
    @DisplayName("unpin of block never pinned throws IllegalArgumentException")
    void unpinInvalidBlock() {
        TxMgrBase txMgr = newTxMgr("unpinInvalidBlock", 256, 3, 200);
        TxBase tx = txMgr.newTx();
        BlockIdBase blkPinned = tx.append("fileE");
        tx.pin(blkPinned);
        BlockIdBase fakeBlk = new BlockId("fileE", blkPinned.number() + 1);
        assertThrows(IllegalArgumentException.class, () -> tx.unpin(fakeBlk));
        tx.rollback();
    }

    @Test
    @DisplayName("size and append guarded after commit")
    void sizeAppendInactiveGuard() {
        TxMgrBase txMgr = newTxMgr("sizeAppendInactiveGuard", 256, 3, 200);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("fileF");
        tx.pin(blk);
        tx.commit();
        assertThrows(IllegalStateException.class, () -> tx.size("fileF"));
        assertThrows(IllegalStateException.class, () -> tx.append("fileF"));
    }

    @Test
    @DisplayName("All set methods followed by rollback restore original values")
    void allSetMethodsRollback() {
        TxMgrBase txMgr = newTxMgr("allSetMethodsRollback", 512, 4, 300);

        // Setup: Create initial values with first transaction
        TxBase setupTx = txMgr.newTx();
        BlockIdBase blk = setupTx.append("fileG");
        setupTx.pin(blk);

        int intOffset = 0;
        int boolOffset = 32;
        int doubleOffset = 64;
        int stringOffset = 128;
        int bytesOffset = 256;

        int originalInt = 100;
        boolean originalBool = true;
        double originalDouble = 3.14159;
        String originalString = "original";
        byte[] originalBytes = new byte[] { 1, 2, 3, 4, 5 };

        setupTx.setInt(blk, intOffset, originalInt, false);
        setupTx.setBoolean(blk, boolOffset, originalBool, false);
        setupTx.setDouble(blk, doubleOffset, originalDouble, false);
        setupTx.setString(blk, stringOffset, originalString, false);
        setupTx.setBytes(blk, bytesOffset, originalBytes, false);
        setupTx.commit();

        // Test: Modify all values then rollback
        TxBase testTx = txMgr.newTx();
        testTx.pin(blk);

        // Verify original values are present
        assertEquals(originalInt, testTx.getInt(blk, intOffset));
        assertEquals(originalBool, testTx.getBoolean(blk, boolOffset));
        assertEquals(originalDouble, testTx.getDouble(blk, doubleOffset));
        assertEquals(originalString, testTx.getString(blk, stringOffset));
        assertArrayEquals(originalBytes, testTx.getBytes(blk, bytesOffset));

        // Modify all values
        testTx.setInt(blk, intOffset, 999, true);
        testTx.setBoolean(blk, boolOffset, false, true);
        testTx.setDouble(blk, doubleOffset, 9.99999, true);
        testTx.setString(blk, stringOffset, "modified", true);
        testTx.setBytes(blk, bytesOffset, new byte[] { 9, 8, 7 }, true);

        // Verify modifications within transaction
        assertEquals(999, testTx.getInt(blk, intOffset));
        assertEquals(false, testTx.getBoolean(blk, boolOffset));
        assertEquals(9.99999, testTx.getDouble(blk, doubleOffset));
        assertEquals("modified", testTx.getString(blk, stringOffset));
        assertArrayEquals(new byte[] { 9, 8, 7 }, testTx.getBytes(blk, bytesOffset));

        // Rollback
        testTx.rollback();

        // Verify: New transaction sees original values after rollback
        TxBase verifyTx = txMgr.newTx();
        verifyTx.pin(blk);
        assertEquals(originalInt, verifyTx.getInt(blk, intOffset), "Int should be restored after rollback");
        assertEquals(originalBool, verifyTx.getBoolean(blk, boolOffset), "Boolean should be restored after rollback");
        assertEquals(originalDouble, verifyTx.getDouble(blk, doubleOffset), "Double should be restored after rollback");
        assertEquals(originalString, verifyTx.getString(blk, stringOffset), "String should be restored after rollback");
        assertArrayEquals(originalBytes, verifyTx.getBytes(blk, bytesOffset),
                "Bytes should be restored after rollback");
        verifyTx.commit();
    }

    @Test
    @DisplayName("Pinning and unpinning stress scenarios")
    public void pinAndUnpinTest() {
        TxMgrBase txMgr = newTxMgr("pinAndUnpinTest", 512, 8, 300);

        // Scenario 1: Invalid pin/unpin operations (null, double pin, invalid unpin,
        // post-commit)
        TxBase tx1 = txMgr.newTx();
        BlockIdBase blk1 = tx1.append("fileH");
        BlockIdBase blk2 = tx1.append("fileH");

        assertThrows(IllegalArgumentException.class, () -> tx1.pin(null));
        assertThrows(IllegalArgumentException.class, () -> tx1.unpin(null));

        tx1.pin(blk1);
        assertDoesNotThrow(() -> tx1.pin(blk1), "Double pin should fail");
        assertThrows(IllegalArgumentException.class, () -> tx1.unpin(blk2), "Unpin unpinned block should fail");

        tx1.commit();
        assertThrows(IllegalStateException.class, () -> tx1.pin(blk1), "Pin after commit should fail");
        assertThrows(IllegalStateException.class, () -> tx1.unpin(blk1), "Unpin after commit should fail");

        // Scenario 2: Accessing unpinned blocks and multiple pin/unpin cycles
        TxBase tx2 = txMgr.newTx();
        BlockIdBase blk3 = tx2.append("fileJ");
        BlockIdBase blk4 = tx2.append("fileJ");
        BlockIdBase blk5 = tx2.append("fileJ");

        tx2.pin(blk3);
        tx2.pin(blk4);
        tx2.pin(blk5);
        tx2.setInt(blk3, 0, 100, true);

        tx2.unpin(blk4); // unpin middle block
        assertThrows(IllegalStateException.class, () -> tx2.getInt(blk4, 0), "Access to unpinned block should fail");

        tx2.unpin(blk3);
        tx2.pin(blk3); // repin
        assertEquals(100, tx2.getInt(blk3, 0), "Should see value after repin");

        tx2.commit();

        // Scenario 3: Concurrent transactions with shared/exclusive lock conflicts
        TxBase txWrite = txMgr.newTx();
        BlockIdBase sharedBlk = txWrite.append("fileK");
        txWrite.pin(sharedBlk);
        txWrite.setInt(sharedBlk, 0, 42, true);
        txWrite.commit();

        TxBase txRead1 = txMgr.newTx();
        TxBase txRead2 = txMgr.newTx();
        txRead1.pin(sharedBlk);
        txRead2.pin(sharedBlk);
        assertEquals(42, txRead1.getInt(sharedBlk, 0));
        assertEquals(42, txRead2.getInt(sharedBlk, 0), "Concurrent reads should succeed");
        txRead1.commit();
        txRead2.rollback();
    }

    @Test
    @DisplayName("Lock interaction test")
    public void lockInteractionTest() throws Exception {
        TxMgrBase txMgr = newTxMgr("lockInteractionTest", 256, 4, 15);

        // Test 1: slock + slock (single-threaded)
        TxBase tx1 = txMgr.newTx();
        TxBase tx2 = txMgr.newTx();
        BlockIdBase blk1 = tx1.append("testfile");
        tx1.pin(blk1);
        tx2.pin(blk1);
        assertEquals(0, tx1.getInt(blk1, 0)); // tx1 gets slock
        assertEquals(0, tx2.getInt(blk1, 0)); // tx2 gets slock - should succeed
        tx1.rollback();
        tx2.rollback();

        // Test 2: slock + slock (multi-threaded)
        ExecutorService exec = Executors.newFixedThreadPool(2);
        Future<BlockIdBase> setupFuture = exec.submit(() -> {
            TxBase txA = txMgr.newTx();
            BlockIdBase blk = txA.append("testfile2");
            txA.pin(blk);
            assertEquals(0, txA.getInt(blk, 0)); // txA gets slock
            return blk;
        });
        BlockIdBase blk2 = setupFuture.get();
        Future<Integer> testFuture = exec.submit(() -> {
            TxBase txB = txMgr.newTx();
            txB.pin(blk2);
            return txB.getInt(blk2, 0); // txB gets slock - should succeed
        });
        assertEquals(0, testFuture.get());

        // Test 3: xlock + slock (single-threaded)
        TxBase tx3 = txMgr.newTx();
        TxBase tx4 = txMgr.newTx();
        BlockIdBase blk3 = tx3.append("testfile3");
        tx3.pin(blk3);
        tx4.pin(blk3);
        tx3.setInt(blk3, 0, 100, true); // tx3 gets xlock
        assertThrows(LockAbortException.class, () -> tx4.getInt(blk3, 0)); // tx4 slock fails
        tx3.rollback();
        tx4.rollback();

        // Test 4: slock + xlock (single-threaded)
        TxBase tx5 = txMgr.newTx();
        TxBase tx6 = txMgr.newTx();
        BlockIdBase blk4 = tx5.append("testfile4");
        tx5.pin(blk4);
        tx6.pin(blk4);
        assertEquals(0, tx5.getInt(blk4, 0)); // tx5 gets slock
        assertThrows(LockAbortException.class, () -> tx6.setInt(blk4, 0, 200, true)); // tx6 xlock fails
        tx5.rollback();
        tx6.rollback();

        // Test 5: xlock + xlock (single-threaded)
        TxBase tx7 = txMgr.newTx();
        TxBase tx8 = txMgr.newTx();
        BlockIdBase blk5 = tx7.append("testfile5");
        tx7.pin(blk5);
        tx8.pin(blk5);
        tx7.setInt(blk5, 0, 300, true); // tx7 gets xlock
        assertThrows(LockAbortException.class, () -> tx8.setInt(blk5, 0, 400, true)); // tx8 xlock fails
        tx7.rollback();
        tx8.rollback();

        exec.shutdownNow();
    }

    @Test
    @DisplayName("Deadlock scenario: two transactions acquire locks in opposite order causing mutual timeout")
    void deadlockScenario() throws Exception {
        int waitMillis = 50; // Short timeout to force quick failure
        TxMgrBase txMgr = newTxMgr("deadlockScenario", 256, 6, waitMillis);

        // Setup: Create two separate blocks
        TxBase setupTx = txMgr.newTx();
        BlockIdBase blkA = setupTx.append("deadlockFileA");
        BlockIdBase blkB = setupTx.append("deadlockFileB");
        setupTx.pin(blkA);
        setupTx.pin(blkB);
        setupTx.setInt(blkA, 0, 1, false);
        setupTx.setInt(blkB, 0, 2, false);
        setupTx.commit();

        // Create two transactions that will deadlock
        TxBase txA = txMgr.newTx();
        TxBase txB = txMgr.newTx();
        txA.pin(blkA);
        txA.pin(blkB);
        txB.pin(blkA);
        txB.pin(blkB);

        // Use latches to coordinate lock acquisition timing
        CountDownLatch phase1Latch = new CountDownLatch(2); // Both hold first locks
        CountDownLatch phase2Latch = new CountDownLatch(1); // Signal to attempt second locks

        ExecutorService exec = Executors.newFixedThreadPool(2);

        // Thread 1: txA acquires lock on blkA, then tries blkB
        Future<Throwable> futureA = exec.submit(() -> {
            try {
                // Phase 1: Acquire exclusive lock on blkA
                txA.setInt(blkA, 0, 100, true);
                phase1Latch.countDown();

                // Wait for txB to acquire its first lock
                phase2Latch.await();

                // Phase 2: Try to acquire lock on blkB (will deadlock with txB)
                txA.setInt(blkB, 0, 200, true);
                return null; // Should not reach here
            } catch (Throwable t) {
                return t;
            }
        });

        // Thread 2: txB acquires lock on blkB, then tries blkA
        Future<Throwable> futureB = exec.submit(() -> {
            try {
                // Phase 1: Acquire exclusive lock on blkB
                txB.setInt(blkB, 0, 300, true);
                phase1Latch.countDown();

                // Wait for txA to acquire its first lock
                phase2Latch.await();

                // Phase 2: Try to acquire lock on blkA (will deadlock with txA)
                txB.setInt(blkA, 0, 400, true);
                return null; // Should not reach here
            } catch (Throwable t) {
                return t;
            }
        });

        // Wait for both transactions to acquire their first locks
        phase1Latch.await();

        // Now both hold one lock each; signal them to try acquiring the second lock
        phase2Latch.countDown();

        // Get results - both should timeout with LockAbortException
        Throwable thrownA = futureA.get();
        Throwable thrownB = futureB.get();

        // Cleanup
        exec.shutdownNow();

        // Assert both transactions failed due to timeout
        assertNotNull(thrownA, "txA should timeout trying to acquire blkB");
        assertNotNull(thrownB, "txB should timeout trying to acquire blkA");
        assertTrue(thrownA instanceof LockAbortException,
                "txA should throw LockAbortException, but got: " + thrownA);
        assertTrue(thrownB instanceof LockAbortException,
                "txB should throw LockAbortException, but got: " + thrownB);

        // Rollback both transactions
        txA.rollback();
        txB.rollback();
    }

    @Test
    @DisplayName("Deadlock on lock upgrade: two transactions hold shared locks, both attempt to upgrade to exclusive")
    void deadlockOnLockUpgrade() throws Exception {
        int waitMillis = 50; // Short timeout to force quick failure
        TxMgrBase txMgr = newTxMgr("deadlockOnLockUpgrade", 256, 6, waitMillis);

        // Setup: Create a block with initial value
        TxBase setupTx = txMgr.newTx();
        BlockIdBase blk = setupTx.append("upgradeFile");
        setupTx.pin(blk);
        setupTx.setInt(blk, 0, 42, false);
        setupTx.commit();

        // Create two transactions that will both acquire shared locks
        TxBase tx1 = txMgr.newTx();
        TxBase tx2 = txMgr.newTx();
        tx1.pin(blk);
        tx2.pin(blk);

        // Use latches to coordinate lock acquisition timing
        CountDownLatch sharedLatch = new CountDownLatch(2); // Both hold shared locks
        CountDownLatch upgradeLatch = new CountDownLatch(1); // Signal to attempt upgrades

        ExecutorService exec = Executors.newFixedThreadPool(2);

        // Thread 1: tx1 acquires shared lock, then tries to upgrade to exclusive
        Future<Throwable> future1 = exec.submit(() -> {
            try {
                // Phase 1: Acquire shared lock via getInt
                tx1.getInt(blk, 0);
                sharedLatch.countDown();

                // Wait for tx2 to also acquire shared lock
                upgradeLatch.await();

                // Phase 2: Try to upgrade to exclusive lock (will deadlock with tx2)
                tx1.setInt(blk, 0, 100, true);
                return null; // Should not reach here
            } catch (Throwable t) {
                return t;
            }
        });

        // Thread 2: tx2 acquires shared lock, then tries to upgrade to exclusive
        Future<Throwable> future2 = exec.submit(() -> {
            try {
                // Phase 1: Acquire shared lock via getInt
                tx2.getInt(blk, 0);
                sharedLatch.countDown();

                // Wait for tx1 to also acquire shared lock
                upgradeLatch.await();

                // Phase 2: Try to upgrade to exclusive lock (will deadlock with tx1)
                tx2.setInt(blk, 0, 200, true);
                return null; // Should not reach here
            } catch (Throwable t) {
                return t;
            }
        });

        // Wait for both transactions to acquire shared locks
        sharedLatch.await();

        // Now both hold shared locks; signal them to try upgrading
        upgradeLatch.countDown();

        // Get results - both should timeout with LockAbortException
        Throwable thrown1 = future1.get();
        Throwable thrown2 = future2.get();

        // Cleanup
        exec.shutdownNow();

        // Assert both transactions failed due to timeout on upgrade
        assertNotNull(thrown1, "tx1 should timeout trying to upgrade lock");
        assertNotNull(thrown2, "tx2 should timeout trying to upgrade lock");
        assertTrue(thrown1 instanceof LockAbortException,
                "tx1 should throw LockAbortException, but got: " + thrown1);
        assertTrue(thrown2 instanceof LockAbortException,
                "tx2 should throw LockAbortException, but got: " + thrown2);

        // Rollback both transactions
        tx1.rollback();
        tx2.rollback();
    }

    @Test
    @DisplayName("Three-way circular deadlock: Tx1→A→B, Tx2→B→C, Tx3→C→A")
    void threeWayCircularDeadlock() throws Exception {
        int waitMillis = 100; // Longer timeout to allow all threads to reach waiting state
        TxMgrBase txMgr = newTxMgr("threeWayCircularDeadlock", 256, 8, waitMillis);

        // Setup: Create three separate blocks
        TxBase setupTx = txMgr.newTx();
        BlockIdBase blkA = setupTx.append("circularFileA");
        BlockIdBase blkB = setupTx.append("circularFileB");
        BlockIdBase blkC = setupTx.append("circularFileC");
        setupTx.pin(blkA);
        setupTx.pin(blkB);
        setupTx.pin(blkC);
        setupTx.setInt(blkA, 0, 1, false);
        setupTx.setInt(blkB, 0, 2, false);
        setupTx.setInt(blkC, 0, 3, false);
        setupTx.commit();

        // Create three transactions that will form a circular deadlock
        TxBase tx1 = txMgr.newTx();
        TxBase tx2 = txMgr.newTx();
        TxBase tx3 = txMgr.newTx();
        tx1.pin(blkA);
        tx1.pin(blkB);
        tx2.pin(blkB);
        tx2.pin(blkC);
        tx3.pin(blkC);
        tx3.pin(blkA);

        // Use latches to coordinate lock acquisition timing
        CountDownLatch phase1Latch = new CountDownLatch(3); // All hold first locks
        CountDownLatch phase2Latch = new CountDownLatch(1); // Signal to attempt second locks

        ExecutorService exec = Executors.newFixedThreadPool(3);

        // Thread 1: tx1 holds A, wants B
        Future<Throwable> future1 = exec.submit(() -> {
            try {
                // Phase 1: Acquire exclusive lock on A
                tx1.setInt(blkA, 0, 100, true);
                phase1Latch.countDown();

                // Wait for all transactions to acquire their first locks
                phase2Latch.await();

                // Phase 2: Try to acquire lock on B (will deadlock)
                tx1.setInt(blkB, 0, 110, true);
                return null; // Should not reach here
            } catch (Throwable t) {
                return t;
            }
        });

        // Thread 2: tx2 holds B, wants C
        Future<Throwable> future2 = exec.submit(() -> {
            try {
                // Phase 1: Acquire exclusive lock on B
                tx2.setInt(blkB, 0, 200, true);
                phase1Latch.countDown();

                // Wait for all transactions to acquire their first locks
                phase2Latch.await();

                // Phase 2: Try to acquire lock on C (will deadlock)
                tx2.setInt(blkC, 0, 210, true);
                return null; // Should not reach here
            } catch (Throwable t) {
                return t;
            }
        });

        // Thread 3: tx3 holds C, wants A
        Future<Throwable> future3 = exec.submit(() -> {
            try {
                // Phase 1: Acquire exclusive lock on C
                tx3.setInt(blkC, 0, 300, true);
                phase1Latch.countDown();

                // Wait for all transactions to acquire their first locks
                phase2Latch.await();

                // Phase 2: Try to acquire lock on A (will deadlock)
                tx3.setInt(blkA, 0, 310, true);
                return null; // Should not reach here
            } catch (Throwable t) {
                return t;
            }
        });

        // Wait for all transactions to acquire their first locks
        phase1Latch.await();

        // Now all hold one lock each; signal them to try acquiring second locks
        phase2Latch.countDown();

        // Get results - all three should timeout with LockAbortException
        Throwable thrown1 = future1.get();
        Throwable thrown2 = future2.get();
        Throwable thrown3 = future3.get();

        // Cleanup
        exec.shutdownNow();

        // Assert all three transactions failed due to timeout
        assertNotNull(thrown1, "tx1 should timeout in circular deadlock");
        assertNotNull(thrown2, "tx2 should timeout in circular deadlock");
        assertNotNull(thrown3, "tx3 should timeout in circular deadlock");
        assertTrue(thrown1 instanceof LockAbortException,
                "tx1 should throw LockAbortException, but got: " + thrown1);
        assertTrue(thrown2 instanceof LockAbortException,
                "tx2 should throw LockAbortException, but got: " + thrown2);
        assertTrue(thrown3 instanceof LockAbortException,
                "tx3 should throw LockAbortException, but got: " + thrown3);

        // Rollback all transactions
        tx1.rollback();
        tx2.rollback();
        tx3.rollback();
    }

    @Test
    @DisplayName("Mixed lock type deadlock: exclusive/shared lock combinations in deadlock cycle")
    void mixedLockTypeDeadlock() throws Exception {
        int waitMillis = 50; // Short timeout to force quick failure
        TxMgrBase txMgr = newTxMgr("mixedLockTypeDeadlock", 256, 6, waitMillis);

        // Setup: Create two separate blocks
        TxBase setupTx = txMgr.newTx();
        BlockIdBase blkA = setupTx.append("mixedFileA");
        BlockIdBase blkB = setupTx.append("mixedFileB");
        setupTx.pin(blkA);
        setupTx.pin(blkB);
        setupTx.setInt(blkA, 0, 1, false);
        setupTx.setInt(blkB, 0, 2, false);
        setupTx.commit();

        // Create two transactions with mixed lock types
        TxBase tx1 = txMgr.newTx();
        TxBase tx2 = txMgr.newTx();
        tx1.pin(blkA);
        tx1.pin(blkB);
        tx2.pin(blkA);
        tx2.pin(blkB);

        // Use latches to coordinate lock acquisition timing
        CountDownLatch phase1Latch = new CountDownLatch(2); // Both hold exclusive locks
        CountDownLatch phase2Latch = new CountDownLatch(1); // Signal to attempt shared locks

        ExecutorService exec = Executors.newFixedThreadPool(2);

        // Thread 1: tx1 holds xLock on A, wants sLock on B
        Future<Throwable> future1 = exec.submit(() -> {
            try {
                // Phase 1: Acquire exclusive lock on A
                tx1.setInt(blkA, 0, 100, true);
                phase1Latch.countDown();

                // Wait for tx2 to acquire its exclusive lock
                phase2Latch.await();

                // Phase 2: Try to acquire shared lock on B (blocked by tx2's xLock)
                tx1.getInt(blkB, 0);
                return null; // Should not reach here
            } catch (Throwable t) {
                return t;
            }
        });

        // Thread 2: tx2 holds xLock on B, wants sLock on A
        Future<Throwable> future2 = exec.submit(() -> {
            try {
                // Phase 1: Acquire exclusive lock on B
                tx2.setInt(blkB, 0, 200, true);
                phase1Latch.countDown();

                // Wait for tx1 to acquire its exclusive lock
                phase2Latch.await();

                // Phase 2: Try to acquire shared lock on A (blocked by tx1's xLock)
                tx2.getInt(blkA, 0);
                return null; // Should not reach here
            } catch (Throwable t) {
                return t;
            }
        });

        // Wait for both transactions to acquire their exclusive locks
        phase1Latch.await();

        // Now both hold exclusive locks; signal them to try acquiring shared locks
        phase2Latch.countDown();

        // Get results - both should timeout with LockAbortException
        Throwable thrown1 = future1.get();
        Throwable thrown2 = future2.get();

        // Cleanup
        exec.shutdownNow();

        // Assert both transactions failed due to timeout
        assertNotNull(thrown1, "tx1 should timeout trying to acquire sLock on B");
        assertNotNull(thrown2, "tx2 should timeout trying to acquire sLock on A");
        assertTrue(thrown1 instanceof LockAbortException,
                "tx1 should throw LockAbortException, but got: " + thrown1);
        assertTrue(thrown2 instanceof LockAbortException,
                "tx2 should throw LockAbortException, but got: " + thrown2);

        // Rollback both transactions
        tx1.rollback();
        tx2.rollback();
    }

    @Test
    @DisplayName("Self-deadlock prevention: single transaction safely acquires and upgrades locks it already holds")
    void selfDeadlockPrevention() {
        TxMgrBase txMgr = newTxMgr("selfDeadlockPrevention", 256, 4, 100);

        // Setup: Create a block with initial value
        TxBase setupTx = txMgr.newTx();
        BlockIdBase blk = setupTx.append("selfLockFile");
        setupTx.pin(blk);
        setupTx.setInt(blk, 0, 42, false);
        setupTx.commit();

        // Test: Single transaction performing multiple lock operations on same block
        TxBase tx = txMgr.newTx();
        tx.pin(blk);

        // Acquire shared lock multiple times (should be idempotent/fast-path)
        int value1 = tx.getInt(blk, 0);
        assertEquals(42, value1, "First read should succeed");

        int value2 = tx.getInt(blk, 0);
        assertEquals(42, value2, "Second read should succeed (idempotent sLock)");

        int value3 = tx.getInt(blk, 0);
        assertEquals(42, value3, "Third read should succeed (idempotent sLock)");

        // Upgrade from shared to exclusive lock (should succeed without timeout)
        assertDoesNotThrow(() -> tx.setInt(blk, 0, 100, true),
                "Lock upgrade from sLock to xLock should succeed without deadlock");

        // Verify the write succeeded
        int value4 = tx.getInt(blk, 0);
        assertEquals(100, value4, "Read after upgrade should see new value");

        // Multiple writes with xLock already held (should be idempotent)
        assertDoesNotThrow(() -> tx.setInt(blk, 0, 200, true),
                "Second write should succeed (idempotent xLock)");
        assertDoesNotThrow(() -> tx.setInt(blk, 0, 300, true),
                "Third write should succeed (idempotent xLock)");

        // Verify final value
        int finalValue = tx.getInt(blk, 0);
        assertEquals(300, finalValue, "Final value should reflect last write");

        // Commit and verify persistence
        tx.commit();

        // Verify with new transaction
        TxBase verifyTx = txMgr.newTx();
        verifyTx.pin(blk);
        int persistedValue = verifyTx.getInt(blk, 0);
        assertEquals(300, persistedValue, "Value should persist after commit");
        verifyTx.commit();
    }

}
