package edu.yu.dbimpl.tx;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

import edu.yu.dbimpl.buffer.BufferMgr;
import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.tx.recovery.LogRecordImpl;

/**
 * Comprehensive transaction testing with recovery scenarios.
 * Tests are ordered to ensure recovery test runs after the crash simulation.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TxCaseTesting {

    private static final Path TEST_DIR = Path.of("testing", "TxCaseTesting", "multiTxCrashAndRecover");
    private static final String TEST_FILE = "shared_data";
    private static final int BLOCK_SIZE = 1024;
    private static final int BUFFER_COUNT = 10;
    private static final int WAIT_MILLIS = 500;

    /**
     * Helper method to create a new TxMgr with specified DB startup mode.
     */
    private TxMgrBase newTxMgr(boolean isStartup) {
        Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(isStartup));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        try {
            Files.createDirectories(TEST_DIR);
        } catch (Exception e) {
            fail("Failed to create test directory: " + e.getMessage());
        }

        FileMgrBase fm = new FileMgr(TEST_DIR.toFile(), BLOCK_SIZE);
        LogMgrBase lm = new LogMgr(fm, "crash_recovery_log");
        BufferMgrBase bm = new BufferMgr(fm, lm, BUFFER_COUNT, WAIT_MILLIS);
        return new TxMgr(fm, lm, bm, WAIT_MILLIS);
    }

    @Test
    @Order(1)
    @DisplayName("Multiple transactions with all operations, leaving uncommitted state for recovery")
    void multiTransactionCrashScenario() {
        // Clean start
        TxMgrBase txMgr = newTxMgr(true);

        // ===== Transaction 1: Setup initial data and COMMIT =====
        TxBase tx1 = txMgr.newTx();
        BlockIdBase blk0 = tx1.append(TEST_FILE);
        BlockIdBase blk1 = tx1.append(TEST_FILE);
        BlockIdBase blk2 = tx1.append(TEST_FILE);

        // Pin all blocks
        tx1.pin(blk0);
        tx1.pin(blk1);
        tx1.pin(blk2);

        // Set initial values on block 0 (int, boolean, double, string, bytes)
        int initialInt = 100;
        boolean initialBool = true;
        double initialDouble = 3.14159;
        String initialString = "Transaction1_Initial";
        byte[] initialBytes = new byte[] { 1, 2, 3, 4, 5 };

        tx1.setInt(blk0, 0, initialInt, true);
        tx1.setBoolean(blk0, 32, initialBool, true);
        tx1.setDouble(blk0, 64, initialDouble, true);
        tx1.setString(blk0, 128, initialString, true);
        tx1.setBytes(blk0, 256, initialBytes, true);

        // Set values on block 1
        tx1.setInt(blk1, 0, 200, true);
        tx1.setString(blk1, 64, "Block1_Data", true);

        // Set values on block 2
        tx1.setDouble(blk2, 0, 2.71828, true);
        tx1.setBoolean(blk2, 32, false, true);

        // Verify values before commit
        assertEquals(initialInt, tx1.getInt(blk0, 0));
        assertEquals(initialBool, tx1.getBoolean(blk0, 32));
        assertEquals(initialDouble, tx1.getDouble(blk0, 64), 0.00001);
        assertEquals(initialString, tx1.getString(blk0, 128));
        assertArrayEquals(initialBytes, tx1.getBytes(blk0, 256));

        // Commit tx1 - these values should survive
        tx1.commit();
        assertEquals(TxBase.Status.COMMITTED, tx1.getStatus());

        // ===== Transaction 2: Modify data and COMMIT =====
        TxBase tx2 = txMgr.newTx();
        tx2.pin(blk0);
        tx2.pin(blk1);

        // Read original values
        assertEquals(initialInt, tx2.getInt(blk0, 0));
        assertEquals(initialBool, tx2.getBoolean(blk0, 32));

        // Modify block 0
        int tx2Int = 999;
        String tx2String = "Transaction2_Modified";
        tx2.setInt(blk0, 0, tx2Int, true);
        tx2.setString(blk0, 128, tx2String, true);

        // Modify block 1
        tx2.setInt(blk1, 0, 777, true);

        // Verify modifications
        assertEquals(tx2Int, tx2.getInt(blk0, 0));
        assertEquals(tx2String, tx2.getString(blk0, 128));
        assertEquals(777, tx2.getInt(blk1, 0));

        // Commit tx2 - these values should survive
        tx2.commit();
        assertEquals(TxBase.Status.COMMITTED, tx2.getStatus());

        // ===== Transaction 3: Modify and ROLLBACK =====
        TxBase tx3 = txMgr.newTx();
        tx3.pin(blk0);
        tx3.pin(blk2);

        // Read committed values from tx2
        assertEquals(tx2Int, tx3.getInt(blk0, 0));

        // Make modifications
        tx3.setInt(blk0, 0, 5555, true);
        tx3.setDouble(blk0, 64, 9.999, true);
        tx3.setBoolean(blk2, 32, true, true);
        tx3.setBytes(blk0, 256, new byte[] { 99, 98, 97 }, true);

        // Verify modifications before rollback
        assertEquals(5555, tx3.getInt(blk0, 0));
        assertEquals(9.999, tx3.getDouble(blk0, 64), 0.001);

        // Rollback tx3 - changes should be undone
        tx3.rollback();
        assertEquals(TxBase.Status.ROLLED_BACK, tx3.getStatus());

        // ===== Transaction 4: Modify data but DO NOT COMMIT (simulate crash) =====
        TxBase tx4 = txMgr.newTx();
        tx4.pin(blk0);
        tx4.pin(blk1);

        // Read values that tx2 committed (tx3's changes were rolled back)
        assertEquals(tx2Int, tx4.getInt(blk0, 0));
        assertEquals(tx2String, tx4.getString(blk0, 128));

        // Make modifications that should be undone by recovery
        tx4.setInt(blk0, 0, 11111, true);
        tx4.setBoolean(blk0, 32, false, true);
        tx4.setDouble(blk0, 64, 1.1111, true);
        tx4.setString(blk0, 128, "TX4_UNCOMMITTED", true);
        tx4.setBytes(blk0, 256, new byte[] { 10, 20, 30, 40, 50 }, true);

        tx4.setInt(blk1, 0, 22222, true);
        tx4.setString(blk1, 64, "TX4_BLOCK1_UNCOMMITTED", true);

        // Verify tx4's uncommitted changes are visible to tx4
        assertEquals(11111, tx4.getInt(blk0, 0));
        assertEquals(false, tx4.getBoolean(blk0, 32));
        assertEquals(1.1111, tx4.getDouble(blk0, 64), 0.0001);
        assertEquals("TX4_UNCOMMITTED", tx4.getString(blk0, 128));
        assertArrayEquals(new byte[] { 10, 20, 30, 40, 50 }, tx4.getBytes(blk0, 256));

        // DO NOT COMMIT OR ROLLBACK tx4 - simulate crash
        // tx4 remains ACTIVE, simulating an uncommitted transaction
        assertEquals(TxBase.Status.ACTIVE, tx4.getStatus());

        // ===== Transaction 5: Another uncommitted transaction =====
        TxBase tx5 = txMgr.newTx();
        tx5.pin(blk2);

        // tx5 writes to blk2, which tx4 did not lock, so no conflict
        // But after recovery, these should be undone
        tx5.setDouble(blk2, 0, 8.8888, true);
        tx5.setBoolean(blk2, 32, true, true);
        tx5.setString(blk2, 100, "TX5_ALSO_UNCOMMITTED", true);

        assertEquals(8.8888, tx5.getDouble(blk2, 0), 0.0001);
        assertEquals(true, tx5.getBoolean(blk2, 32));
        assertEquals("TX5_ALSO_UNCOMMITTED", tx5.getString(blk2, 100));

        // DO NOT COMMIT tx5 either
        assertEquals(TxBase.Status.ACTIVE, tx5.getStatus());

        // At this point:
        // - tx1: COMMITTED - should survive
        // - tx2: COMMITTED - should survive
        // - tx3: ROLLED_BACK - already undone
        // - tx4: ACTIVE (uncommitted) - should be undone by recovery
        // - tx5: ACTIVE (uncommitted) - should be undone by recovery
        //
        // Expected final state after recovery:
        // blk0[0] = 999 (from tx2)
        // blk0[32] = true (from tx1)
        // blk0[64] = 3.14159 (from tx1)
        // blk0[128] = "Transaction2_Modified" (from tx2)
        // blk0[256] = {1,2,3,4,5} (from tx1)
        // blk1[0] = 777 (from tx2)
        // blk1[64] = "Block1_Data" (from tx1)
        // blk2[0] = 2.71828 (from tx1)
        // blk2[32] = false (from tx1)
    }

    @Test
    @Order(2)
    @DisplayName("Recovery test: verify uncommitted transactions are rolled back")
    void recoveryAfterCrash() {
        // Initialize with DB_STARTUP=false to trigger recovery
        TxMgrBase txMgr = newTxMgr(false);

        // The TxMgr constructor should have performed recovery
        // Create a new transaction to read the recovered state
        TxBase txRecovery = txMgr.newTx();

        // Check that we have the expected blocks
        assertTrue(txRecovery.size(TEST_FILE) >= 3,
                "File should have at least 3 blocks from previous test");

        // Create block references
        txRecovery.append(TEST_FILE);
        assertEquals(4, txRecovery.size(TEST_FILE),
                "File should have exactly 4 blocks after append");

        // Blocks 0, 1, 2 were created in first test
        BlockIdBase blk0 = new BlockId(TEST_FILE, 0);
        BlockIdBase blk1 = new BlockId(TEST_FILE, 1);
        BlockIdBase blk2 = new BlockId(TEST_FILE, 2);

        txRecovery.pin(blk0);
        txRecovery.pin(blk1);
        txRecovery.pin(blk2);

        // Verify block 0 values (tx4's uncommitted changes should be undone)
        // Expected: tx2's committed values, or tx1's where tx2 didn't change
        assertEquals(999, txRecovery.getInt(blk0, 0),
                "blk0[0] should be 999 from tx2 (tx4's 11111 undone)");
        assertEquals(true, txRecovery.getBoolean(blk0, 32),
                "blk0[32] should be true from tx1 (tx4's false undone)");
        assertEquals(3.14159, txRecovery.getDouble(blk0, 64), 0.00001,
                "blk0[64] should be 3.14159 from tx1 (tx4's 1.1111 undone)");
        assertEquals("Transaction2_Modified", txRecovery.getString(blk0, 128),
                "blk0[128] should be tx2's string (tx4's change undone)");
        assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, txRecovery.getBytes(blk0, 256),
                "blk0[256] should be original bytes from tx1 (tx4's change undone)");

        // Verify block 1 values (tx4 and tx5's uncommitted changes should be undone)
        assertEquals(777, txRecovery.getInt(blk1, 0),
                "blk1[0] should be 777 from tx2 (tx4's 22222 and tx5's 33333 undone)");
        assertEquals("Block1_Data", txRecovery.getString(blk1, 64),
                "blk1[64] should be original from tx1 (tx4 and tx5's changes undone)");

        // Verify block 2 values (tx4's uncommitted changes should be undone)
        assertEquals(2.71828, txRecovery.getDouble(blk2, 0), 0.00001,
                "blk2[0] should be original from tx1 (tx4's 8.8888 undone)");
        assertEquals(false, txRecovery.getBoolean(blk2, 32),
                "blk2[32] should be false from tx1 (tx4's true undone)");

        // Test all get methods again with different offsets to ensure recovery is
        // thorough
        int testInt = 12345;
        boolean testBool = false;
        double testDouble = 6.28318;
        String testString = "RecoveryTestString";
        byte[] testBytes = new byte[] { 11, 22, 33, 44, 55, 66 };

        // Set new values at different offsets
        txRecovery.setInt(blk0, 400, testInt, true);
        txRecovery.setBoolean(blk0, 450, testBool, true);
        txRecovery.setDouble(blk1, 200, testDouble, true);
        txRecovery.setString(blk1, 300, testString, true);
        txRecovery.setBytes(blk2, 100, testBytes, true);

        // Verify new values
        assertEquals(testInt, txRecovery.getInt(blk0, 400));
        assertEquals(testBool, txRecovery.getBoolean(blk0, 450));
        assertEquals(testDouble, txRecovery.getDouble(blk1, 200), 0.00001);
        assertEquals(testString, txRecovery.getString(blk1, 300));
        assertArrayEquals(testBytes, txRecovery.getBytes(blk2, 100));

        // Test unpin/re-pin functionality after recovery
        txRecovery.unpin(blk2);
        txRecovery.pin(blk2);
        assertArrayEquals(testBytes, txRecovery.getBytes(blk2, 100),
                "Values should persist after unpin/repin");

        // Commit this recovery transaction
        txRecovery.commit();
        assertEquals(TxBase.Status.COMMITTED, txRecovery.getStatus());

        // Verify log contains checkpoint from recovery
        FileMgrBase fm = new FileMgr(TEST_DIR.toFile(), BLOCK_SIZE);
        LogMgrBase lm = new LogMgr(fm, "crash_recovery_log");

        boolean checkpointFound = false;
        Iterator<byte[]> logIter = lm.iterator();
        while (logIter.hasNext()) {
            byte[] logBytes = logIter.next();
            LogRecordImpl record = new LogRecordImpl(logBytes);
            if (record.op() == LogRecordImpl.CHECKPOINT) {
                checkpointFound = true;
                break;
            }
        }
        assertTrue(checkpointFound, "Recovery should have written a CHECKPOINT record");
    }

    @Test
    @Order(3)
    @DisplayName("Additional recovery verification: create new transaction and verify state persists")
    void postRecoveryStateVerification() {
        // Start with recovery mode off (database should be in recovered state)
        TxMgrBase txMgr = newTxMgr(false);

        TxBase tx = txMgr.newTx();

        BlockIdBase blk0 = new edu.yu.dbimpl.file.BlockId(TEST_FILE, 0);
        BlockIdBase blk1 = new edu.yu.dbimpl.file.BlockId(TEST_FILE, 1);

        tx.pin(blk0);
        tx.pin(blk1);

        // Verify core recovered values still intact
        assertEquals(999, tx.getInt(blk0, 0),
                "Recovered value should persist across TxMgr instances");
        assertEquals("Transaction2_Modified", tx.getString(blk0, 128),
                "Recovered string should persist across TxMgr instances");
        assertEquals(777, tx.getInt(blk1, 0),
                "Recovered value from block 1 should persist");

        // Make a new modification and commit
        tx.setInt(blk0, 500, 88888, true);
        tx.setString(blk1, 400, "FinalTest", true);

        assertEquals(88888, tx.getInt(blk0, 500));
        assertEquals("FinalTest", tx.getString(blk1, 400));

        tx.commit();
        assertEquals(TxBase.Status.COMMITTED, tx.getStatus());

        // Read back with a new transaction
        TxBase tx2 = txMgr.newTx();
        tx2.pin(blk0);
        tx2.pin(blk1);

        assertEquals(88888, tx2.getInt(blk0, 500),
                "New committed value should be readable");
        assertEquals("FinalTest", tx2.getString(blk1, 400),
                "New committed string should be readable");
        assertEquals(999, tx2.getInt(blk0, 0),
                "Original recovered values should still be intact");

        tx2.rollback();
    }

    // ==================== COMPREHENSIVE RECOVERY STRESS TEST ====================
    private static final Path RECOVERY_STRESS_DIR = Path.of("testing", "TxCaseTesting", "recoveryStressTest");
    private static final String STRESS_FILE = "recovery_stress_data";

    private TxMgrBase newRecoveryStressTxMgr(boolean isStartup) {
        Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(isStartup));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        try {
            Files.createDirectories(RECOVERY_STRESS_DIR);
        } catch (Exception e) {
            fail("Failed to create test directory: " + e.getMessage());
        }

        FileMgrBase fm = new FileMgr(RECOVERY_STRESS_DIR.toFile(), BLOCK_SIZE);
        LogMgrBase lm = new LogMgr(fm, "recovery_stress_log");
        BufferMgrBase bm = new BufferMgr(fm, lm, 12, WAIT_MILLIS);
        return new TxMgr(fm, lm, bm, 25);
    }

    @Test
    @Order(4)
    @DisplayName("COMPREHENSIVE RECOVERY STRESS TEST: Complex interleaved transactions with multiple rollbacks and crashes")
    void comprehensiveRecoveryStressTest() {
        // ========== PHASE 1: Initialize with complex committed state ==========
        TxMgrBase txMgr = newRecoveryStressTxMgr(true);

        // Create 5 blocks with different initial data patterns
        TxBase txInit = txMgr.newTx();
        BlockIdBase blk0 = new BlockId(STRESS_FILE, 0);
        BlockIdBase blk1 = new BlockId(STRESS_FILE, 1);
        BlockIdBase blk2 = new BlockId(STRESS_FILE, 2);
        BlockIdBase blk3 = new BlockId(STRESS_FILE, 3);
        BlockIdBase blk4 = new BlockId(STRESS_FILE, 4);

        txInit.pin(blk0);
        txInit.pin(blk1);
        txInit.pin(blk2);
        txInit.pin(blk3);
        txInit.pin(blk4);

        // Block 0: Sequential integers pattern
        txInit.setInt(blk0, 0, 1000, true);
        txInit.setInt(blk0, 4, 1001, true);
        txInit.setInt(blk0, 8, 1002, true);
        txInit.setInt(blk0, 12, 1003, true);
        txInit.setInt(blk0, 16, 1004, true);
        txInit.setInt(blk0, 20, 1005, true);
        txInit.setBoolean(blk0, 50, true, true);
        txInit.setDouble(blk0, 100, 2.718281828, true);
        txInit.setString(blk0, 200, "INITIAL_BLK0", true);
        txInit.setBytes(blk0, 300, new byte[] { 10, 20, 30, 40, 50 }, true);

        // Block 1: Powers of 2 pattern
        txInit.setInt(blk1, 0, 2, true);
        txInit.setInt(blk1, 4, 4, true);
        txInit.setInt(blk1, 8, 8, true);
        txInit.setInt(blk1, 12, 16, true);
        txInit.setInt(blk1, 16, 32, true);
        txInit.setInt(blk1, 20, 64, true);
        txInit.setBoolean(blk1, 50, false, true);
        txInit.setDouble(blk1, 100, 3.141592653, true);
        txInit.setString(blk1, 200, "INITIAL_BLK1", true);
        txInit.setBytes(blk1, 300, new byte[] { 1, 2, 4, 8, 16 }, true);

        // Block 2: Negative values
        txInit.setInt(blk2, 0, -100, true);
        txInit.setInt(blk2, 4, -200, true);
        txInit.setInt(blk2, 8, -300, true);
        txInit.setInt(blk2, 12, -400, true);
        txInit.setInt(blk2, 16, -500, true);
        txInit.setInt(blk2, 20, -600, true);
        txInit.setBoolean(blk2, 50, true, true);
        txInit.setDouble(blk2, 100, -1.414213562, true);
        txInit.setString(blk2, 200, "INITIAL_BLK2", true);
        txInit.setBytes(blk2, 300, new byte[] { (byte) 255, (byte) 254, (byte) 253 }, true);

        // Block 3: Large values
        txInit.setInt(blk3, 0, 999999, true);
        txInit.setInt(blk3, 4, 888888, true);
        txInit.setInt(blk3, 8, 777777, true);
        txInit.setInt(blk3, 12, 666666, true);
        txInit.setInt(blk3, 16, 555555, true);
        txInit.setInt(blk3, 20, 444444, true);
        txInit.setBoolean(blk3, 50, false, true);
        txInit.setDouble(blk3, 100, 1.618033988, true);
        txInit.setString(blk3, 200, "INITIAL_BLK3", true);
        txInit.setBytes(blk3, 300, new byte[] { 99, 88, 77, 66, 55, 44 }, true);

        // Block 4: Zero and ones
        txInit.setInt(blk4, 0, 0, true);
        txInit.setInt(blk4, 4, 1, true);
        txInit.setInt(blk4, 8, 0, true);
        txInit.setInt(blk4, 12, 1, true);
        txInit.setInt(blk4, 16, 0, true);
        txInit.setInt(blk4, 20, 1, true);
        txInit.setBoolean(blk4, 50, true, true);
        txInit.setDouble(blk4, 100, 0.0, true);
        txInit.setString(blk4, 200, "INITIAL_BLK4", true);
        txInit.setBytes(blk4, 300, new byte[] { 0, 1, 0, 1, 0 }, true);

        txInit.commit();
        assertEquals(TxBase.Status.COMMITTED, txInit.getStatus());

        // ========== PHASE 2: Multiple interleaved transactions with overwrites
        // ==========

        // Transaction A: Modifies blocks 0, 1, 2 and COMMITS
        TxBase txA = txMgr.newTx();
        txA.pin(blk0);
        txA.pin(blk1);
        txA.pin(blk2);

        txA.setInt(blk0, 0, 5000, true);
        txA.setInt(blk0, 4, 5001, true);
        txA.setBoolean(blk0, 50, false, true);
        txA.setString(blk0, 200, "COMMITTED_A_BLK0", true);

        txA.setInt(blk1, 0, 128, true);
        txA.setInt(blk1, 4, 256, true);
        txA.setDouble(blk1, 100, 1.732050807, true);
        txA.setBytes(blk1, 300, new byte[] { 11, 22, 33 }, true);

        txA.setInt(blk2, 0, 42, true);
        txA.setBoolean(blk2, 50, false, true);
        txA.setString(blk2, 200, "COMMITTED_A_BLK2", true);

        txA.commit();

        // Transaction B: Modifies blocks 1, 3 and COMMITS
        TxBase txB = txMgr.newTx();
        txB.pin(blk1);
        txB.pin(blk3);

        txB.setInt(blk1, 8, 512, true);
        txB.setInt(blk1, 12, 1024, true);
        txB.setString(blk1, 200, "COMMITTED_B_BLK1", true);

        txB.setInt(blk3, 0, 111111, true);
        txB.setInt(blk3, 4, 222222, true);
        txB.setDouble(blk3, 100, 2.236067977, true);
        txB.setBytes(blk3, 300, new byte[] { 1, 1, 2, 3, 5, 8 }, true);

        txB.commit();

        // Transaction C: Modifies blocks 0, 2, 4 and COMMITS
        TxBase txC = txMgr.newTx();
        txC.pin(blk0);
        txC.pin(blk2);
        txC.pin(blk4);

        txC.setInt(blk0, 8, 5002, true);
        txC.setInt(blk0, 12, 5003, true);
        txC.setDouble(blk0, 100, 2.828427124, true);

        txC.setInt(blk2, 4, 43, true);
        txC.setInt(blk2, 8, 44, true);

        txC.setInt(blk4, 0, 100, true);
        txC.setInt(blk4, 4, 200, true);
        txC.setBoolean(blk4, 50, false, true);
        txC.setString(blk4, 200, "COMMITTED_C_BLK4", true);

        txC.commit();

        // ========== PHASE 3: Transactions that will be explicitly ROLLED BACK
        // ==========

        // Transaction D: Modifies blocks 0, 1 and ROLLS BACK
        TxBase txD = txMgr.newTx();
        txD.pin(blk0);
        txD.pin(blk1);

        txD.setInt(blk0, 0, 9999, true);
        txD.setInt(blk0, 4, 9998, true);
        txD.setInt(blk0, 8, 9997, true);
        txD.setBoolean(blk0, 50, true, true);
        txD.setDouble(blk0, 100, 99.99, true);
        txD.setString(blk0, 200, "ROLLBACK_D_BLK0", true);
        txD.setBytes(blk0, 300, new byte[] { 99, 99, 99 }, true);

        txD.setInt(blk1, 0, 8888, true);
        txD.setInt(blk1, 4, 7777, true);
        txD.setString(blk1, 200, "ROLLBACK_D_BLK1", true);

        // Verify changes before rollback
        assertEquals(9999, txD.getInt(blk0, 0));
        assertEquals("ROLLBACK_D_BLK0", txD.getString(blk0, 200));

        txD.rollback();
        assertEquals(TxBase.Status.ROLLED_BACK, txD.getStatus());

        // Transaction E: Modifies blocks 2, 3, 4 and ROLLS BACK
        TxBase txE = txMgr.newTx();
        txE.pin(blk2);
        txE.pin(blk3);
        txE.pin(blk4);

        txE.setInt(blk2, 0, 7777, true);
        txE.setInt(blk2, 4, 6666, true);
        txE.setInt(blk2, 8, 5555, true);
        txE.setBoolean(blk2, 50, true, true);
        txE.setString(blk2, 200, "ROLLBACK_E_BLK2", true);

        txE.setInt(blk3, 0, 4444, true);
        txE.setDouble(blk3, 100, 88.88, true);

        txE.setInt(blk4, 0, 3333, true);
        txE.setInt(blk4, 4, 2222, true);
        txE.setBytes(blk4, 300, new byte[] { 77, 77, 77 }, true);

        txE.rollback();
        assertEquals(TxBase.Status.ROLLED_BACK, txE.getStatus());

        // ========== PHASE 4: Multiple UNCOMMITTED transactions (simulate crash)
        // ==========
        // Run each uncommitted transaction in separate threads to avoid lock conflicts
        // Each thread will hold locks on different blocks to prevent deadlocks

        final TxMgrBase txMgrFinal = txMgr;
        final BlockIdBase blk0Final = blk0;
        final BlockIdBase blk1Final = blk1;
        final BlockIdBase blk2Final = blk2;
        final BlockIdBase blk3Final = blk3;
        final BlockIdBase blk4Final = blk4;

        // Track transactions for later verification
        final TxBase[] transactions = new TxBase[3];
        final Thread[] threads = new Thread[3];

        // Transaction F: Modifies blocks 0 and 1, leaves UNCOMMITTED
        threads[0] = new Thread(() -> {
            TxBase txF = txMgrFinal.newTx();
            transactions[0] = txF;
            txF.pin(blk0Final);
            txF.pin(blk1Final);

            txF.setInt(blk0Final, 0, 77777, true);
            txF.setInt(blk0Final, 4, 77778, true);
            txF.setInt(blk0Final, 8, 77779, true);
            txF.setInt(blk0Final, 12, 77780, true);
            txF.setInt(blk0Final, 16, 77781, true);
            txF.setInt(blk0Final, 20, 77782, true);
            txF.setBoolean(blk0Final, 50, true, true);
            txF.setDouble(blk0Final, 100, 77.777, true);
            txF.setString(blk0Final, 200, "UNCOMMITTED_F_BLK0", true);
            txF.setBytes(blk0Final, 300, new byte[] { 77, 77, 77, 77, 77 }, true);

            txF.setInt(blk1Final, 0, 66666, true);
            txF.setInt(blk1Final, 4, 66667, true);
            txF.setInt(blk1Final, 8, 66668, true);
            txF.setInt(blk1Final, 12, 66669, true);
            txF.setInt(blk1Final, 16, 66670, true);
            txF.setInt(blk1Final, 20, 66671, true);
            txF.setBoolean(blk1Final, 50, true, true);
            txF.setDouble(blk1Final, 100, 66.666, true);
            txF.setString(blk1Final, 200, "UNCOMMITTED_F_BLK1", true);
            txF.setBytes(blk1Final, 300, new byte[] { 66, 66, 66 }, true);

            // Verify uncommitted changes visible to txF
            assertEquals(77777, txF.getInt(blk0Final, 0));
            assertEquals("UNCOMMITTED_F_BLK0", txF.getString(blk0Final, 200));
            assertEquals(66666, txF.getInt(blk1Final, 0));

            // DO NOT COMMIT - simulate crash
        });

        // Transaction G: Modifies blocks 2 and 3, leaves UNCOMMITTED
        threads[1] = new Thread(() -> {
            TxBase txG = txMgrFinal.newTx();
            transactions[1] = txG;
            txG.pin(blk2Final);
            txG.pin(blk3Final);

            txG.setInt(blk2Final, 0, 55555, true);
            txG.setInt(blk2Final, 4, 55556, true);
            txG.setInt(blk2Final, 8, 55557, true);
            txG.setInt(blk2Final, 12, 55558, true);
            txG.setBoolean(blk2Final, 50, false, true);
            txG.setString(blk2Final, 200, "UNCOMMITTED_G_BLK2", true);

            txG.setInt(blk3Final, 0, 44444, true);
            txG.setInt(blk3Final, 4, 44445, true);
            txG.setDouble(blk3Final, 100, 44.444, true);
            txG.setBytes(blk3Final, 300, new byte[] { 44, 44, 44, 44 }, true);

            // Verify uncommitted changes visible to txG
            assertEquals(55555, txG.getInt(blk2Final, 0));
            assertEquals(44444, txG.getInt(blk3Final, 0));

            // DO NOT COMMIT - simulate crash
        });

        // Transaction H: Modifies block 4, leaves UNCOMMITTED
        threads[2] = new Thread(() -> {
            TxBase txH = txMgrFinal.newTx();
            transactions[2] = txH;
            txH.pin(blk4Final);

            txH.setInt(blk4Final, 0, 33333, true);
            txH.setInt(blk4Final, 4, 33334, true);
            txH.setBoolean(blk4Final, 50, true, true);
            txH.setString(blk4Final, 200, "UNCOMMITTED_H_BLK4", true);
            txH.setDouble(blk4Final, 100, 98.765, true);
            txH.setBytes(blk4Final, 300, new byte[] { 98, 76, 54 }, true);

            // Verify uncommitted changes visible to txH
            assertEquals(33333, txH.getInt(blk4Final, 0));
            assertEquals("UNCOMMITTED_H_BLK4", txH.getString(blk4Final, 200));

            // DO NOT COMMIT - simulate crash
        });

        for (Thread thread : threads) {
            // Start thread
            thread.start();
            try {
                // Wait for thread to complete
                thread.join();
            } catch (InterruptedException e) {
                fail("Thread interrupted: " + e.getMessage());
            }
        }

        // Verify all transactions are still ACTIVE (uncommitted)
        for (TxBase tx : transactions) {
            if (tx != null) {
                assertEquals(TxBase.Status.ACTIVE, tx.getStatus());
            }
        }

        // ========== PHASE 5: RECOVERY - Restart system and verify ==========
        // Reinitialize with DB_STARTUP=false to trigger recovery
        TxMgrBase txMgrRecovered = newRecoveryStressTxMgr(false);

        // Recovery should undo all uncommitted transactions (F, G, H)
        // Expected state after recovery: committed changes from A, B, C only
        TxBase txVerify = txMgrRecovered.newTx();
        txVerify.pin(blk0);
        txVerify.pin(blk1);
        txVerify.pin(blk2);
        txVerify.pin(blk3);
        txVerify.pin(blk4);

        // Block 0: Should have values from txA and txC (F, H undone)
        assertEquals(5000, txVerify.getInt(blk0, 0), "blk0[0] from txA");
        assertEquals(5001, txVerify.getInt(blk0, 4), "blk0[4] from txA");
        assertEquals(5002, txVerify.getInt(blk0, 8), "blk0[8] from txC");
        assertEquals(5003, txVerify.getInt(blk0, 12), "blk0[12] from txC");
        assertEquals(1004, txVerify.getInt(blk0, 16), "blk0[16] from txInit");
        assertEquals(1005, txVerify.getInt(blk0, 20), "blk0[20] from txInit");
        assertEquals(false, txVerify.getBoolean(blk0, 50), "blk0[50] from txA");
        assertEquals(2.828427124, txVerify.getDouble(blk0, 100), 0.0001, "blk0[100] from txC");
        assertEquals("COMMITTED_A_BLK0", txVerify.getString(blk0, 200), "blk0[200] from txA");
        assertArrayEquals(new byte[] { 10, 20, 30, 40, 50 }, txVerify.getBytes(blk0, 300), "blk0[300] from txInit");

        // Block 1: Should have values from txA and txB (F, G undone)
        assertEquals(128, txVerify.getInt(blk1, 0), "blk1[0] from txA");
        assertEquals(256, txVerify.getInt(blk1, 4), "blk1[4] from txA");
        assertEquals(512, txVerify.getInt(blk1, 8), "blk1[8] from txB");
        assertEquals(1024, txVerify.getInt(blk1, 12), "blk1[12] from txB");
        assertEquals(32, txVerify.getInt(blk1, 16), "blk1[16] from txInit");
        assertEquals(64, txVerify.getInt(blk1, 20), "blk1[20] from txInit");
        assertEquals(false, txVerify.getBoolean(blk1, 50), "blk1[50] from txInit");
        assertEquals(1.732050807, txVerify.getDouble(blk1, 100), 0.0001, "blk1[100] from txA");
        assertEquals("COMMITTED_B_BLK1", txVerify.getString(blk1, 200), "blk1[200] from txB");
        assertArrayEquals(new byte[] { 11, 22, 33 }, txVerify.getBytes(blk1, 300), "blk1[300] from txA");

        // Block 2: Should have values from txA and txC (F, G undone)
        assertEquals(42, txVerify.getInt(blk2, 0), "blk2[0] from txA");
        assertEquals(43, txVerify.getInt(blk2, 4), "blk2[4] from txC");
        assertEquals(44, txVerify.getInt(blk2, 8), "blk2[8] from txC");
        assertEquals(-400, txVerify.getInt(blk2, 12), "blk2[12] from txInit");
        assertEquals(-500, txVerify.getInt(blk2, 16), "blk2[16] from txInit");
        assertEquals(-600, txVerify.getInt(blk2, 20), "blk2[20] from txInit");
        assertEquals(false, txVerify.getBoolean(blk2, 50), "blk2[50] from txA");
        assertEquals(-1.414213562, txVerify.getDouble(blk2, 100), 0.0001, "blk2[100] from txInit");
        assertEquals("COMMITTED_A_BLK2", txVerify.getString(blk2, 200), "blk2[200] from txA");
        assertArrayEquals(new byte[] { (byte) 255, (byte) 254, (byte) 253 }, txVerify.getBytes(blk2, 300),
                "blk2[300] from txInit");

        // Block 3: Should have values from txB (F, G undone)
        assertEquals(111111, txVerify.getInt(blk3, 0), "blk3[0] from txB");
        assertEquals(222222, txVerify.getInt(blk3, 4), "blk3[4] from txB");
        assertEquals(777777, txVerify.getInt(blk3, 8), "blk3[8] from txInit");
        assertEquals(666666, txVerify.getInt(blk3, 12), "blk3[12] from txInit");
        assertEquals(555555, txVerify.getInt(blk3, 16), "blk3[16] from txInit");
        assertEquals(444444, txVerify.getInt(blk3, 20), "blk3[20] from txInit");
        assertEquals(false, txVerify.getBoolean(blk3, 50), "blk3[50] from txInit");
        assertEquals(2.236067977, txVerify.getDouble(blk3, 100), 0.0001, "blk3[100] from txB");
        assertEquals("INITIAL_BLK3", txVerify.getString(blk3, 200), "blk3[200] from txInit");
        assertArrayEquals(new byte[] { 1, 1, 2, 3, 5, 8 }, txVerify.getBytes(blk3, 300), "blk3[300] from txB");

        // Block 4: Should have values from txC (F, H undone)
        assertEquals(100, txVerify.getInt(blk4, 0), "blk4[0] from txC");
        assertEquals(200, txVerify.getInt(blk4, 4), "blk4[4] from txC");
        assertEquals(0, txVerify.getInt(blk4, 8), "blk4[8] from txInit");
        assertEquals(1, txVerify.getInt(blk4, 12), "blk4[12] from txInit");
        assertEquals(0, txVerify.getInt(blk4, 16), "blk4[16] from txInit");
        assertEquals(1, txVerify.getInt(blk4, 20), "blk4[20] from txInit");
        assertEquals(false, txVerify.getBoolean(blk4, 50), "blk4[50] from txC");
        assertEquals(0.0, txVerify.getDouble(blk4, 100), 0.0001, "blk4[100] from txInit");
        assertEquals("COMMITTED_C_BLK4", txVerify.getString(blk4, 200), "blk4[200] from txC");
        assertArrayEquals(new byte[] { 0, 1, 0, 1, 0 }, txVerify.getBytes(blk4, 300), "blk4[300] from txInit");

        txVerify.commit();

        // ========== PHASE 6: Verify checkpoint record exists ==========
        FileMgrBase fm = new FileMgr(RECOVERY_STRESS_DIR.toFile(), BLOCK_SIZE);
        LogMgrBase lm = new LogMgr(fm, "recovery_stress_log");

        boolean checkpointFound = false;
        Iterator<byte[]> logIter = lm.iterator();
        while (logIter.hasNext()) {
            byte[] logBytes = logIter.next();
            LogRecordImpl record = new LogRecordImpl(logBytes);
            if (record.op() == LogRecordImpl.CHECKPOINT) {
                checkpointFound = true;
                break;
            }
        }
        assertTrue(checkpointFound, "Recovery should have written a CHECKPOINT record to the log");

        // ========== PHASE 7: Additional verification with new transaction ==========
        TxBase txFinal = txMgrRecovered.newTx();
        txFinal.pin(blk0);
        txFinal.pin(blk1);
        txFinal.pin(blk2);

        // Perform some new operations to ensure system still works correctly
        txFinal.setInt(blk0, 500, 88888, true);
        txFinal.setString(blk1, 400, "POST_RECOVERY_TEST", true);
        txFinal.setDouble(blk2, 400, 6.28318, true);

        assertEquals(88888, txFinal.getInt(blk0, 500));
        assertEquals("POST_RECOVERY_TEST", txFinal.getString(blk1, 400));
        assertEquals(6.28318, txFinal.getDouble(blk2, 400), 0.00001);

        // Verify old committed values still intact
        assertEquals(5000, txFinal.getInt(blk0, 0));
        assertEquals(128, txFinal.getInt(blk1, 0));
        assertEquals(42, txFinal.getInt(blk2, 0));

        txFinal.commit();
    }

    // ==================== CRASH RECOVERY TEST ====================
    @Test
    @Order(5)
    @DisplayName("Crash Recovery: Verify committed Tx survives and uncommitted Tx is rolled back")
    void crashRecoveryTest() {
        // Test-specific constants
        final Path CRASH_RECOVERY_DIR = Path.of("testing", "TxCaseTesting", "crashRecovery");
        final String CRASH_FILE = "crash_recovery_data";

        // Helper class: Custom transaction that simulates a crash during commit
        class CrashingTx extends Tx {
            public CrashingTx(FileMgrBase fileMgr, BufferMgrBase bufferMgr, TxMgrBase txMgr, LogMgrBase logMgr,
                    int txnum) {
                super(fileMgr, bufferMgr, txMgr, logMgr, txnum);
            }

            @Override
            public void commit() {
                if (getStatus() != Status.ACTIVE) {
                    throw new IllegalStateException("Can't commit if tx is not active");
                }

                // Flush all dirty buffers to disk (simulating what happens before crash)
                getBufferMgr().flushAll(txnum());

                // CRASH HERE - do NOT write commit log record
                throw new RuntimeException("SIMULATED CRASH during commit after flush");
            }
        }

        // ========== PHASE 1: Create committed data then crash during second Tx
        // ==========
        // Setup TxMgr for initial state
        Properties props1 = new Properties();
        props1.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props1);

        try {
            Files.createDirectories(CRASH_RECOVERY_DIR);
        } catch (Exception e) {
            fail("Failed to create test directory: " + e.getMessage());
        }

        FileMgrBase fm1 = new FileMgr(CRASH_RECOVERY_DIR.toFile(), BLOCK_SIZE);
        LogMgrBase lm1 = new LogMgr(fm1, "crash_recovery_log");
        BufferMgrBase bm1 = new BufferMgr(fm1, lm1, BUFFER_COUNT, WAIT_MILLIS);
        TxMgrBase tm1 = new TxMgr(fm1, lm1, bm1, WAIT_MILLIS);

        // Transaction 1: Write and commit baseline data
        TxBase tx1 = tm1.newTx();
        BlockIdBase blk0 = tx1.append(CRASH_FILE);
        BlockIdBase blk1 = tx1.append(CRASH_FILE);
        BlockIdBase blk2 = tx1.append(CRASH_FILE);

        tx1.pin(blk0);
        tx1.pin(blk1);
        tx1.pin(blk2);

        // Write diverse data types across multiple blocks
        tx1.setInt(blk0, 0, 1000, false);
        tx1.setInt(blk0, 4, 1001, false);
        tx1.setBoolean(blk0, 50, true, false);
        tx1.setDouble(blk0, 100, 3.14159, false);
        tx1.setString(blk0, 200, "TX1_COMMITTED_BLK0", false);
        tx1.setBytes(blk0, 400, new byte[] { 10, 20, 30, 40, 50 }, false);

        tx1.setInt(blk1, 0, 2000, false);
        tx1.setInt(blk1, 4, 2001, false);
        tx1.setBoolean(blk1, 50, false, false);
        tx1.setDouble(blk1, 100, 2.71828, false);
        tx1.setString(blk1, 200, "TX1_COMMITTED_BLK1", false);
        tx1.setBytes(blk1, 400, new byte[] { 1, 2, 3, 4, 5 }, false);

        tx1.setInt(blk2, 0, 3000, false);
        tx1.setBoolean(blk2, 50, true, false);
        tx1.setString(blk2, 200, "TX1_COMMITTED_BLK2", false);

        tx1.commit();
        assertEquals(TxBase.Status.COMMITTED, tx1.getStatus());

        // Transaction 2: Write new data AND overwrite some tx1 data, then crash before
        // commit
        CrashingTx tx2 = new CrashingTx(fm1, bm1, tm1, lm1, 1);

        BlockIdBase blk0_tx2 = new BlockId(CRASH_FILE, 0);
        BlockIdBase blk1_tx2 = new BlockId(CRASH_FILE, 1);
        BlockIdBase blk2_tx2 = new BlockId(CRASH_FILE, 2);
        tx2.pin(blk0_tx2);
        tx2.pin(blk1_tx2);
        tx2.pin(blk2_tx2);

        // Overwrite some tx1 values (these should be undone by recovery)
        tx2.setInt(blk0_tx2, 0, 9999, true); // overwrites tx1's 1000
        tx2.setString(blk0_tx2, 200, "TX2_POISON_BLK0", true); // overwrites tx1's string

        // Write new data at different offsets (these should also be undone)
        tx2.setInt(blk0_tx2, 8, 8888, true); // new data
        tx2.setBoolean(blk0_tx2, 50, false, true); // overwrites tx1's true
        tx2.setDouble(blk0_tx2, 150, 9.999, true); // new data

        // Overwrite and add new data in block 1
        tx2.setInt(blk1_tx2, 0, 7777, true); // overwrites tx1's 2000
        tx2.setInt(blk1_tx2, 8, 7778, true); // new data
        tx2.setString(blk1_tx2, 200, "TX2_POISON_BLK1", true); // overwrites tx1's string
        tx2.setBytes(blk1_tx2, 400, new byte[] { 99, 98, 97 }, true); // overwrites tx1's bytes

        // Add new data to block 2
        tx2.setInt(blk2_tx2, 4, 6666, true); // new data
        tx2.setDouble(blk2_tx2, 100, 6.666, true); // new data
        tx2.setString(blk2_tx2, 300, "TX2_NEW_DATA", true); // new data

        // Verify tx2's changes are visible before crash
        assertEquals(9999, tx2.getInt(blk0_tx2, 0));
        assertEquals(8888, tx2.getInt(blk0_tx2, 8));
        assertEquals("TX2_POISON_BLK0", tx2.getString(blk0_tx2, 200));
        assertEquals(7777, tx2.getInt(blk1_tx2, 0));

        // Attempt to commit - this will flush data to disk then crash
        try {
            tx2.commit();
            fail("Expected RuntimeException for simulated crash");
        } catch (RuntimeException e) {
            assertEquals("SIMULATED CRASH during commit after flush", e.getMessage());
        }

        // ========== PHASE 2: Recovery and verification ==========
        // Setup TxMgr for recovery
        Properties props2 = new Properties();
        props2.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
        DBConfiguration.INSTANCE.get().setConfiguration(props2);

        FileMgrBase fm2 = new FileMgr(CRASH_RECOVERY_DIR.toFile(), BLOCK_SIZE);
        LogMgrBase lm2 = new LogMgr(fm2, "crash_recovery_log");
        BufferMgrBase bm2 = new BufferMgr(fm2, lm2, BUFFER_COUNT, WAIT_MILLIS);
        TxMgrBase txMgrRecovered = new TxMgr(fm2, lm2, bm2, WAIT_MILLIS);

        TxBase txVerify = txMgrRecovered.newTx();
        BlockIdBase blk0_verify = new BlockId(CRASH_FILE, 0);
        BlockIdBase blk1_verify = new BlockId(CRASH_FILE, 1);
        BlockIdBase blk2_verify = new BlockId(CRASH_FILE, 2);
        txVerify.pin(blk0_verify);
        txVerify.pin(blk1_verify);
        txVerify.pin(blk2_verify);

        // Verify ALL tx1's committed values are present (even those overwritten by tx2)
        assertEquals(1000, txVerify.getInt(blk0_verify, 0), "tx1's value should be restored");
        assertEquals(1001, txVerify.getInt(blk0_verify, 4), "tx1's value should persist");
        assertEquals(true, txVerify.getBoolean(blk0_verify, 50), "tx1's boolean should be restored");
        assertEquals(3.14159, txVerify.getDouble(blk0_verify, 100), 0.00001, "tx1's double should persist");
        assertEquals("TX1_COMMITTED_BLK0", txVerify.getString(blk0_verify, 200), "tx1's string should be restored");
        assertArrayEquals(new byte[] { 10, 20, 30, 40, 50 }, txVerify.getBytes(blk0_verify, 400),
                "tx1's bytes should persist");

        assertEquals(2000, txVerify.getInt(blk1_verify, 0), "tx1's value should be restored");
        assertEquals(2001, txVerify.getInt(blk1_verify, 4), "tx1's value should persist");
        assertEquals(false, txVerify.getBoolean(blk1_verify, 50), "tx1's boolean should persist");
        assertEquals(2.71828, txVerify.getDouble(blk1_verify, 100), 0.00001, "tx1's double should persist");
        assertEquals("TX1_COMMITTED_BLK1", txVerify.getString(blk1_verify, 200), "tx1's string should be restored");
        assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 }, txVerify.getBytes(blk1_verify, 400),
                "tx1's bytes should be restored");

        assertEquals(3000, txVerify.getInt(blk2_verify, 0), "tx1's value should persist");
        assertEquals(true, txVerify.getBoolean(blk2_verify, 50), "tx1's boolean should persist");
        assertEquals("TX1_COMMITTED_BLK2", txVerify.getString(blk2_verify, 200), "tx1's string should persist");

        // Verify ALL tx2's changes are NOT present (both overwrites and new data)
        // Check that tx2's new data at offset 8 was undone (should be 0 or initial
        // value)
        int blk0_offset8 = txVerify.getInt(blk0_verify, 8);
        assertNotEquals(8888, blk0_offset8, "tx2's new data should be undone");

        // Check that tx2's new data at offset 150 was undone
        double blk0_offset150 = txVerify.getDouble(blk0_verify, 150);
        assertNotEquals(9.999, blk0_offset150, 0.001, "tx2's new data should be undone");

        // Check that tx2's new data in block 1 at offset 8 was undone
        int blk1_offset8 = txVerify.getInt(blk1_verify, 8);
        assertNotEquals(7778, blk1_offset8, "tx2's new data should be undone");

        // Check that tx2's new data in block 2 was undone
        int blk2_offset4 = txVerify.getInt(blk2_verify, 4);
        assertNotEquals(6666, blk2_offset4, "tx2's new data should be undone");

        double blk2_offset100 = txVerify.getDouble(blk2_verify, 100);
        assertNotEquals(6.666, blk2_offset100, 0.001, "tx2's new data should be undone");

        // Verify checkpoint was written during recovery
        FileMgrBase fmCheck = new FileMgr(CRASH_RECOVERY_DIR.toFile(), BLOCK_SIZE);
        LogMgrBase lmCheck = new LogMgr(fmCheck, "crash_recovery_log");

        boolean checkpointFound = false;
        Iterator<byte[]> logIter = lmCheck.iterator();
        while (logIter.hasNext()) {
            byte[] logBytes = logIter.next();
            LogRecordImpl record = new LogRecordImpl(logBytes);
            if (record.op() == LogRecordImpl.CHECKPOINT) {
                checkpointFound = true;
                break;
            }
        }
        assertTrue(checkpointFound, "Recovery should write a CHECKPOINT record");

        txVerify.commit();
    }

    // ==================== BUFFER SIZE 1 CRASH TEST ====================
    @Test
    @Order(6)
    @DisplayName("Buffer Size 1: Uncommitted transaction data lost when buffer is evicted before crash")
    void bufferSize1CrashTest() {
        // Test-specific constants
        final Path BUFFER_1_DIR = Path.of("testing", "TxCaseTesting", "bufferSize1Crash");
        final String BUFFER_1_FILE = "buffer_1_data";
        final int BUFFER_SIZE_1 = 1; // Only 1 buffer available

        // ========== PHASE 1: Initialize DB with buffer size 1, create transactions
        // ===========
        Properties props1 = new Properties();
        props1.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props1);

        try {
            Files.createDirectories(BUFFER_1_DIR);
        } catch (Exception e) {
            fail("Failed to create test directory: " + e.getMessage());
        }

        FileMgrBase fm1 = new FileMgr(BUFFER_1_DIR.toFile(), BLOCK_SIZE);
        LogMgrBase lm1 = new LogMgr(fm1, "buffer_1_log");
        BufferMgrBase bm1 = new BufferMgr(fm1, lm1, BUFFER_SIZE_1, WAIT_MILLIS);
        TxMgrBase tm1 = new TxMgr(fm1, lm1, bm1, WAIT_MILLIS);

        // Transaction 1: Pin block 0, write data, unpin
        TxBase tx1 = tm1.newTx();
        BlockIdBase blk0 = tx1.append(BUFFER_1_FILE);

        tx1.pin(blk0);

        // Write various data types to block 0
        tx1.setInt(blk0, 0, 12345, true);
        tx1.setBoolean(blk0, 50, true, true);
        tx1.setDouble(blk0, 100, 9.8765, true);
        tx1.setString(blk0, 200, "TX1_UNCOMMITTED_DATA", true);
        tx1.setBytes(blk0, 400, new byte[] { 11, 22, 33, 44, 55 }, true);

        // Verify writes are visible to tx1
        assertEquals(12345, tx1.getInt(blk0, 0));
        assertEquals(true, tx1.getBoolean(blk0, 50));
        assertEquals(9.8765, tx1.getDouble(blk0, 100), 0.0001);
        assertEquals("TX1_UNCOMMITTED_DATA", tx1.getString(blk0, 200));
        assertArrayEquals(new byte[] { 11, 22, 33, 44, 55 }, tx1.getBytes(blk0, 400));

        // Unpin block 0 - this makes the buffer available for reuse
        tx1.unpin(blk0);

        // DO NOT COMMIT tx1 - leave it active
        assertEquals(TxBase.Status.ACTIVE, tx1.getStatus());

        // Transaction 2: Pin block 1
        // With buffer size of 1, this should evict block 0's buffer
        // Since tx1 didn't commit and didn't log the changes, the dirty data may be
        // lost
        TxBase tx2 = tm1.newTx();
        BlockIdBase blk1 = tx2.append(BUFFER_1_FILE);

        tx2.pin(blk1);

        // DO NOT COMMIT tx2 either
        assertEquals(TxBase.Status.ACTIVE, tx2.getStatus());

        // At this point:
        // - tx1 wrote to block 0 but didn't commit (and unpinned the buffer)
        // - tx2 pinned block 1, evicting block 0's buffer
        // - Both transactions are uncommitted
        // - When we simulate a crash and recover, tx1's changes should NOT be persisted
        // because they were never committed and may have been evicted from the buffer

        // ========== PHASE 2: Simulate crash and recovery ==========
        Properties props2 = new Properties();
        props2.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
        DBConfiguration.INSTANCE.get().setConfiguration(props2);

        FileMgrBase fm2 = new FileMgr(BUFFER_1_DIR.toFile(), BLOCK_SIZE);
        LogMgrBase lm2 = new LogMgr(fm2, "buffer_1_log");
        BufferMgrBase bm2 = new BufferMgr(fm2, lm2, BUFFER_COUNT, WAIT_MILLIS); // Use normal buffer count for recovery
        TxMgrBase tmRecovered = new TxMgr(fm2, lm2, bm2, WAIT_MILLIS);

        // Create a new transaction to verify the recovered state
        TxBase txVerify = tmRecovered.newTx();

        // Verify file has 2 blocks (created by tx1 and tx2)
        assertEquals(2, txVerify.size(BUFFER_1_FILE), "File should have 2 blocks");

        BlockIdBase blk0_verify = new BlockId(BUFFER_1_FILE, 0);
        BlockIdBase blk1_verify = new BlockId(BUFFER_1_FILE, 1);

        txVerify.pin(blk0_verify);
        txVerify.pin(blk1_verify);

        // ========== PHASE 3: Verify tx1's uncommitted data is NOT persisted ==========
        // Since tx1 never committed and its buffer may have been evicted,
        // the values should NOT be the ones tx1 wrote

        int recoveredInt = txVerify.getInt(blk0_verify, 0);
        assertNotEquals(12345, recoveredInt,
                "tx1's uncommitted int should not persist after crash (was " + recoveredInt + ")");

        double recoveredDouble = txVerify.getDouble(blk0_verify, 100);
        assertNotEquals(9.8765, recoveredDouble, 0.0001,
                "tx1's uncommitted double should not persist after crash (was " + recoveredDouble + ")");

        String recoveredString = txVerify.getString(blk0_verify, 200);
        assertNotEquals("TX1_UNCOMMITTED_DATA", recoveredString,
                "tx1's uncommitted string should not persist after crash (was '" + recoveredString + "')");

        byte[] recoveredBytes = txVerify.getBytes(blk0_verify, 400);
        assertFalse(java.util.Arrays.equals(new byte[] { 11, 22, 33, 44, 55 }, recoveredBytes),
                "tx1's uncommitted bytes should not persist after crash");

        // Verify tx2's uncommitted data is also NOT persisted
        int recoveredInt2 = txVerify.getInt(blk1_verify, 0);
        assertNotEquals(99999, recoveredInt2,
                "tx2's uncommitted int should not persist after crash (was " + recoveredInt2 + ")");

        // Verify we can write new data and commit successfully after recovery
        txVerify.setInt(blk0_verify, 0, 77777, true);
        txVerify.setString(blk0_verify, 200, "POST_RECOVERY", true);

        assertEquals(77777, txVerify.getInt(blk0_verify, 0));
        assertEquals("POST_RECOVERY", txVerify.getString(blk0_verify, 200));

        txVerify.commit();
        assertEquals(TxBase.Status.COMMITTED, txVerify.getStatus());

        // Verify the committed data persists in a new transaction
        TxBase txFinal = tmRecovered.newTx();
        txFinal.pin(blk0_verify);

        assertEquals(77777, txFinal.getInt(blk0_verify, 0),
                "Newly committed data should persist");
        assertEquals("POST_RECOVERY", txFinal.getString(blk0_verify, 200),
                "Newly committed string should persist");

        txFinal.rollback();
    }

}
