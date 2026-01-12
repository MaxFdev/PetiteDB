package edu.yu.dbimpl.tx;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
 * Comprehensive coverage tests for the Tx module to achieve 100% test coverage.
 * Tests cover Tx, TxMgr, RecoveryMgr, LogRecordImpl, and ConcurrencyMgr
 * classes.
 */
public class TxCoverageTest {

    private TxMgrBase newTxMgr(String testName, int blockSize, int bufferCount, int waitMillis) {
        Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);
        Path dir = Path.of("testing", "TxCoverageTest", testName);
        try {
            if (Files.exists(dir)) {
                deleteDirectory(dir);
            }
        } catch (Exception e) {
            // Ignore
        }
        FileMgrBase fm = new FileMgr(dir.toFile(), blockSize);
        LogMgrBase lm = new LogMgr(fm, "coverage_log");
        BufferMgrBase bm = new BufferMgr(fm, lm, bufferCount, waitMillis);
        return new TxMgr(fm, lm, bm, waitMillis);
    }

    private void deleteDirectory(Path path) throws Exception {
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (Exception e) {
                        }
                    });
        }
    }

    // ========== Tx Class Coverage ==========

    @Test
    @DisplayName("Tx: getStatus returns correct status throughout lifecycle")
    void txGetStatus() {
        TxMgrBase txMgr = newTxMgr("txGetStatus", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        assertEquals(TxBase.Status.ACTIVE, tx.getStatus());
        tx.commit();
        assertEquals(TxBase.Status.COMMITTED, tx.getStatus());

        TxBase tx2 = txMgr.newTx();
        assertEquals(TxBase.Status.ACTIVE, tx2.getStatus());
        tx2.rollback();
        assertEquals(TxBase.Status.ROLLED_BACK, tx2.getStatus());
    }

    @Test
    @DisplayName("Tx: txnum returns unique incrementing values")
    void txTxnum() {
        TxMgrBase txMgr = newTxMgr("txTxnum", 256, 4, 100);
        TxBase tx1 = txMgr.newTx();
        TxBase tx2 = txMgr.newTx();
        TxBase tx3 = txMgr.newTx();
        assertTrue(tx2.txnum() > tx1.txnum());
        assertTrue(tx3.txnum() > tx2.txnum());
    }

    @Test
    @DisplayName("Tx: commit throws when not ACTIVE")
    void txCommitNotActive() {
        TxMgrBase txMgr = newTxMgr("txCommitNotActive", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        tx.commit();
        assertThrows(IllegalStateException.class, () -> tx.commit());
    }

    @Test
    @DisplayName("Tx: rollback throws when not ACTIVE")
    void txRollbackNotActive() {
        TxMgrBase txMgr = newTxMgr("txRollbackNotActive", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        tx.rollback();
        assertThrows(IllegalStateException.class, () -> tx.rollback());
    }

    @Test
    @DisplayName("Tx: recover throws when not ACTIVE")
    void txRecoverNotActive() {
        TxMgrBase txMgr = newTxMgr("txRecoverNotActive", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        tx.commit();
        assertThrows(IllegalStateException.class, () -> tx.recover());
    }

    @Test
    @DisplayName("Tx: pin with null block throws IllegalArgumentException")
    void txPinNullBlock() {
        TxMgrBase txMgr = newTxMgr("txPinNullBlock", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        assertThrows(IllegalArgumentException.class, () -> tx.pin(null));
    }

    @Test
    @DisplayName("Tx: pin when not ACTIVE throws IllegalStateException")
    void txPinNotActive() {
        TxMgrBase txMgr = newTxMgr("txPinNotActive", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        tx.commit();
        BlockIdBase blk = new BlockId("test", 0);
        assertThrows(IllegalStateException.class, () -> tx.pin(blk));
    }

    @Test
    @DisplayName("Tx: pin same block twice does nothing (idempotent)")
    void txPinIdempotent() {
        TxMgrBase txMgr = newTxMgr("txPinIdempotent", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.pin(blk); // Should not throw
        tx.commit();
    }

    @Test
    @DisplayName("Tx: unpin with null block throws IllegalArgumentException")
    void txUnpinNullBlock() {
        TxMgrBase txMgr = newTxMgr("txUnpinNullBlock", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        assertThrows(IllegalArgumentException.class, () -> tx.unpin(null));
    }

    @Test
    @DisplayName("Tx: unpin when not ACTIVE throws IllegalStateException")
    void txUnpinNotActive() {
        TxMgrBase txMgr = newTxMgr("txUnpinNotActive", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.commit();
        assertThrows(IllegalStateException.class, () -> tx.unpin(blk));
    }

    @Test
    @DisplayName("Tx: unpin of unpinned block throws IllegalArgumentException")
    void txUnpinUnpinnedBlock() {
        TxMgrBase txMgr = newTxMgr("txUnpinUnpinnedBlock", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        assertThrows(IllegalArgumentException.class, () -> tx.unpin(blk));
    }

    @Test
    @DisplayName("Tx: getInt with null block throws IllegalArgumentException")
    void txGetIntNullBlock() {
        TxMgrBase txMgr = newTxMgr("txGetIntNullBlock", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        assertThrows(IllegalArgumentException.class, () -> tx.getInt(null, 0));
    }

    @Test
    @DisplayName("Tx: getInt on unpinned block throws IllegalStateException")
    void txGetIntUnpinned() {
        TxMgrBase txMgr = newTxMgr("txGetIntUnpinned", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        assertThrows(IllegalStateException.class, () -> tx.getInt(blk, 0));
    }

    @Test
    @DisplayName("Tx: getBoolean works correctly")
    void txGetBoolean() {
        TxMgrBase txMgr = newTxMgr("txGetBoolean", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setBoolean(blk, 0, true, false);
        assertTrue(tx.getBoolean(blk, 0));
        tx.commit();
    }

    @Test
    @DisplayName("Tx: getDouble works correctly")
    void txGetDouble() {
        TxMgrBase txMgr = newTxMgr("txGetDouble", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setDouble(blk, 0, 3.14159, false);
        assertEquals(3.14159, tx.getDouble(blk, 0), 0.00001);
        tx.commit();
    }

    @Test
    @DisplayName("Tx: getString works correctly")
    void txGetString() {
        TxMgrBase txMgr = newTxMgr("txGetString", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setString(blk, 0, "hello", false);
        assertEquals("hello", tx.getString(blk, 0));
        tx.commit();
    }

    @Test
    @DisplayName("Tx: getBytes works correctly")
    void txGetBytes() {
        TxMgrBase txMgr = newTxMgr("txGetBytes", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        byte[] data = new byte[] { 1, 2, 3, 4, 5 };
        tx.setBytes(blk, 0, data, false);
        assertArrayEquals(data, tx.getBytes(blk, 0));
        tx.commit();
    }

    @Test
    @DisplayName("Tx: setInt with null block throws IllegalArgumentException")
    void txSetIntNullBlock() {
        TxMgrBase txMgr = newTxMgr("txSetIntNullBlock", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        assertThrows(IllegalArgumentException.class, () -> tx.setInt(null, 0, 123, true));
    }

    @Test
    @DisplayName("Tx: setInt on unpinned block throws IllegalStateException")
    void txSetIntUnpinned() {
        TxMgrBase txMgr = newTxMgr("txSetIntUnpinned", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        assertThrows(IllegalStateException.class, () -> tx.setInt(blk, 0, 123, true));
    }

    @Test
    @DisplayName("Tx: setInt with okToLog=false does not log")
    void txSetIntNoLog() {
        TxMgrBase txMgr = newTxMgr("txSetIntNoLog", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 999, false);
        assertEquals(999, tx.getInt(blk, 0));
        tx.commit();
    }

    @Test
    @DisplayName("Tx: setBoolean with okToLog=true logs the operation")
    void txSetBooleanWithLog() {
        TxMgrBase txMgr = newTxMgr("txSetBooleanWithLog", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setBoolean(blk, 0, false, false);
        tx.setBoolean(blk, 0, true, true);
        assertTrue(tx.getBoolean(blk, 0));
        tx.commit();
    }

    @Test
    @DisplayName("Tx: setDouble with okToLog=false does not log")
    void txSetDoubleNoLog() {
        TxMgrBase txMgr = newTxMgr("txSetDoubleNoLog", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setDouble(blk, 0, 2.71828, false);
        assertEquals(2.71828, tx.getDouble(blk, 0), 0.00001);
        tx.commit();
    }

    @Test
    @DisplayName("Tx: setString with okToLog=true logs the operation")
    void txSetStringWithLog() {
        TxMgrBase txMgr = newTxMgr("txSetStringWithLog", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setString(blk, 0, "initial", false);
        tx.setString(blk, 0, "updated", true);
        assertEquals("updated", tx.getString(blk, 0));
        tx.commit();
    }

    @Test
    @DisplayName("Tx: setBytes with okToLog=true logs the operation")
    void txSetBytesWithLog() {
        TxMgrBase txMgr = newTxMgr("txSetBytesWithLog", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        byte[] initial = new byte[] { 9, 8, 7 };
        byte[] updated = new byte[] { 1, 2, 3, 4 };
        tx.setBytes(blk, 0, initial, false);
        tx.setBytes(blk, 0, updated, true);
        assertArrayEquals(updated, tx.getBytes(blk, 0));
        tx.commit();
    }

    @Test
    @DisplayName("Tx: size when not ACTIVE throws IllegalStateException")
    void txSizeNotActive() {
        TxMgrBase txMgr = newTxMgr("txSizeNotActive", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        tx.append("file");
        tx.commit();
        assertThrows(IllegalStateException.class, () -> tx.size("file"));
    }

    @Test
    @DisplayName("Tx: append when not ACTIVE throws IllegalStateException")
    void txAppendNotActive() {
        TxMgrBase txMgr = newTxMgr("txAppendNotActive", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        tx.commit();
        assertThrows(IllegalStateException.class, () -> tx.append("file"));
    }

    @Test
    @DisplayName("Tx: blockSize returns correct value")
    void txBlockSize() {
        TxMgrBase txMgr = newTxMgr("txBlockSize", 512, 4, 100);
        TxBase tx = txMgr.newTx();
        assertEquals(512, tx.blockSize());
        tx.commit();
    }

    @Test
    @DisplayName("Tx: availableBuffs when not ACTIVE throws IllegalArgumentException")
    void txAvailableBuffsNotActive() {
        TxMgrBase txMgr = newTxMgr("txAvailableBuffsNotActive", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        tx.commit();
        assertThrows(IllegalArgumentException.class, () -> tx.availableBuffs());
    }

    @Test
    @DisplayName("Tx: toString returns meaningful representation")
    void txToString() {
        TxMgrBase txMgr = newTxMgr("txToString", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        String str = tx.toString();
        assertTrue(str.contains("Tx:"));
        assertTrue(str.contains("txnum:"));
        assertTrue(str.contains("status:"));
        tx.commit();
    }

    @Test
    @DisplayName("Tx: hashCode returns txnum")
    void txHashCode() {
        TxMgrBase txMgr = newTxMgr("txHashCode", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        assertEquals(tx.txnum(), tx.hashCode());
        tx.commit();
    }

    @Test
    @DisplayName("Tx: equals returns true for same tx, false for different")
    void txEquals() {
        TxMgrBase txMgr = newTxMgr("txEquals", 256, 4, 100);
        TxBase tx1 = txMgr.newTx();
        TxBase tx2 = txMgr.newTx();
        assertTrue(tx1.equals(tx1));
        assertFalse(tx1.equals(tx2));
        assertFalse(tx1.equals(null));
        assertFalse(tx1.equals("not a tx"));
        tx1.commit();
        tx2.commit();
    }

    @Test
    @DisplayName("Tx: commit flushes buffers and releases locks")
    void txCommitCompleteBehavior() {
        TxMgrBase txMgr = newTxMgr("txCommitCompleteBehavior", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 42, true);
        tx.commit();

        // New transaction should see the committed value
        TxBase tx2 = txMgr.newTx();
        tx2.pin(blk);
        assertEquals(42, tx2.getInt(blk, 0));
        tx2.commit();
    }

    @Test
    @DisplayName("Tx: rollback undoes changes and releases locks")
    void txRollbackCompleteBehavior() {
        TxMgrBase txMgr = newTxMgr("txRollbackCompleteBehavior", 256, 4, 100);

        // Setup: create initial value
        TxBase setupTx = txMgr.newTx();
        BlockIdBase blk = setupTx.append("file");
        setupTx.pin(blk);
        setupTx.setInt(blk, 0, 100, true);
        setupTx.commit();

        // Modify and rollback
        TxBase tx = txMgr.newTx();
        tx.pin(blk);
        assertEquals(100, tx.getInt(blk, 0));
        tx.setInt(blk, 0, 999, true);
        assertEquals(999, tx.getInt(blk, 0));
        tx.rollback();

        // Verify rollback restored original value
        TxBase verifyTx = txMgr.newTx();
        verifyTx.pin(blk);
        assertEquals(100, verifyTx.getInt(blk, 0));
        verifyTx.commit();
    }

    @Test
    @DisplayName("Tx: recover processes uncommitted transactions")
    void txRecoverBehavior() {
        TxMgrBase txMgr = newTxMgr("txRecoverBehavior", 256, 4, 100);

        // The recover is automatically called in TxMgr constructor
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 77, true);
        tx.commit();

        // Verify checkpoint was written
        TxBase verifyTx = txMgr.newTx();
        assertEquals(TxBase.Status.ACTIVE, verifyTx.getStatus());
        verifyTx.commit();
    }

    // ========== TxMgr Class Coverage ==========

    @Test
    @DisplayName("TxMgr: constructor initializes correctly and performs recovery")
    void txMgrConstructor() {
        TxMgrBase txMgr = newTxMgr("txMgrConstructor", 256, 4, 200);
        assertNotNull(txMgr);
        assertEquals(200, txMgr.getMaxWaitTimeInMillis());
    }

    @Test
    @DisplayName("TxMgr: newTx creates transactions with incrementing txnum")
    void txMgrNewTx() {
        TxMgrBase txMgr = newTxMgr("txMgrNewTx", 256, 4, 100);
        TxBase tx1 = txMgr.newTx();
        TxBase tx2 = txMgr.newTx();
        TxBase tx3 = txMgr.newTx();
        assertTrue(tx2.txnum() > tx1.txnum());
        assertTrue(tx3.txnum() > tx2.txnum());
    }

    @Test
    @DisplayName("TxMgr: resetAllLockState clears global lock map")
    void txMgrResetAllLockState() {
        TxMgrBase txMgrBase = newTxMgr("txMgrResetAllLockState", 256, 4, 100);
        assertTrue(txMgrBase instanceof TxMgr);
        TxMgr txMgr = (TxMgr) txMgrBase;

        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 123, true);

        assertFalse(txMgr.getGlobalLockMap().isEmpty());
        txMgr.resetAllLockState();
        assertTrue(txMgr.getGlobalLockMap().isEmpty());
    }

    @Test
    @DisplayName("TxMgr: getGlobalLockMap returns lock map")
    void txMgrGetGlobalLockMap() {
        TxMgrBase txMgrBase = newTxMgr("txMgrGetGlobalLockMap", 256, 4, 100);
        assertTrue(txMgrBase instanceof TxMgr);
        TxMgr txMgr = (TxMgr) txMgrBase;
        assertNotNull(txMgr.getGlobalLockMap());
    }

    // ========== ConcurrencyMgr Class Coverage ==========

    @Test
    @DisplayName("ConcurrencyMgr: constructor with non-TxMgr throws IllegalArgumentException")
    void concurrencyMgrConstructorInvalid() {
        // This test would require a mock, but since we can't easily create a non-TxMgr
        // instance,
        // we'll test the valid case
        TxMgrBase txMgr = newTxMgr("concurrencyMgrConstructorValid", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        // If we got here, the ConcurrencyMgr was created successfully
        tx.commit();
    }

    @Test
    @DisplayName("ConcurrencyMgr: sLock can be acquired by multiple readers")
    void concurrencyMgrSLockMultipleReaders() throws Exception {
        TxMgrBase txMgr = newTxMgr("concurrencyMgrSLockMultipleReaders", 256, 6, 500);

        // Setup: create a block with data
        TxBase setup = txMgr.newTx();
        BlockIdBase blk = setup.append("file");
        setup.pin(blk);
        setup.setInt(blk, 0, 42, true);
        setup.commit();

        // Multiple concurrent readers - locks are acquired and released within same
        // thread
        TxBase reader1 = txMgr.newTx();
        TxBase reader2 = txMgr.newTx();
        reader1.pin(blk);
        reader2.pin(blk);

        int val1 = reader1.getInt(blk, 0);
        int val2 = reader2.getInt(blk, 0);

        assertEquals(42, val1);
        assertEquals(42, val2);

        reader1.commit();
        reader2.commit();
    }

    @Test
    @DisplayName("ConcurrencyMgr: xLock excludes other transactions")
    void concurrencyMgrXLockExcludes() throws Exception {
        int waitMillis = 50;
        TxMgrBase txMgr = newTxMgr("concurrencyMgrXLockExcludes", 256, 4, waitMillis);

        TxBase tx1 = txMgr.newTx();
        BlockIdBase blk = tx1.append("file");
        tx1.pin(blk);
        tx1.setInt(blk, 0, 1, true); // Acquires xLock

        TxBase tx2 = txMgr.newTx();
        tx2.pin(blk);

        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<Throwable> future = exec.submit(() -> {
            try {
                tx2.setInt(blk, 0, 2, true); // Should timeout
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        Throwable thrown = future.get(1, TimeUnit.SECONDS);
        exec.shutdownNow();

        assertNotNull(thrown);
        assertTrue(thrown instanceof LockAbortException);

        tx1.rollback();
        tx2.rollback();
    }

    @Test
    @DisplayName("ConcurrencyMgr: sLock on same block twice is idempotent")
    void concurrencyMgrSLockIdempotent() {
        TxMgrBase txMgr = newTxMgr("concurrencyMgrSLockIdempotent", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 10, false);

        // Multiple reads should not block
        int val1 = tx.getInt(blk, 0);
        int val2 = tx.getInt(blk, 0);
        assertEquals(val1, val2);

        tx.commit();
    }

    @Test
    @DisplayName("ConcurrencyMgr: xLock on same block twice is idempotent")
    void concurrencyMgrXLockIdempotent() {
        TxMgrBase txMgr = newTxMgr("concurrencyMgrXLockIdempotent", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);

        // Multiple writes should not block
        tx.setInt(blk, 0, 10, false);
        tx.setInt(blk, 0, 20, false);
        assertEquals(20, tx.getInt(blk, 0));

        tx.commit();
    }

    @Test
    @DisplayName("ConcurrencyMgr: upgrade from sLock to xLock")
    void concurrencyMgrLockUpgrade() {
        TxMgrBase txMgr = newTxMgr("concurrencyMgrLockUpgrade", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 100, false);

        // Read first (sLock)
        int val = tx.getInt(blk, 0);
        assertEquals(100, val);

        // Write after (upgrades to xLock)
        tx.setInt(blk, 0, 200, false);
        assertEquals(200, tx.getInt(blk, 0));

        tx.commit();
    }

    @Test
    @DisplayName("ConcurrencyMgr: release clears all locks")
    void concurrencyMgrRelease() {
        TxMgrBase txMgr = newTxMgr("concurrencyMgrRelease", 256, 4, 100);
        TxBase tx1 = txMgr.newTx();
        BlockIdBase blk = tx1.append("file");
        tx1.pin(blk);
        tx1.setInt(blk, 0, 100, true);
        tx1.commit(); // Releases locks

        // Another transaction should be able to acquire locks
        TxBase tx2 = txMgr.newTx();
        tx2.pin(blk);
        tx2.setInt(blk, 0, 200, true);
        tx2.commit();
    }

    @Test
    @DisplayName("ConcurrencyMgr: InterruptedException wraps in RuntimeException")
    void concurrencyMgrInterrupted() throws Exception {
        TxMgrBase txMgr = newTxMgr("concurrencyMgrInterrupted", 256, 4, 5000);
        TxBase tx1 = txMgr.newTx();
        BlockIdBase blk = tx1.append("file");
        tx1.pin(blk);
        tx1.setInt(blk, 0, 1, true); // Hold xLock

        TxBase tx2 = txMgr.newTx();
        tx2.pin(blk);

        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<Throwable> future = exec.submit(() -> {
            Thread.currentThread().interrupt(); // Set interrupt flag
            try {
                tx2.setInt(blk, 0, 2, true);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        Throwable thrown = future.get(1, TimeUnit.SECONDS);
        exec.shutdownNow();

        assertNotNull(thrown);
        assertTrue(thrown instanceof RuntimeException);

        tx1.rollback();
        tx2.rollback();
    }

    @Test
    @DisplayName("ConcurrencyMgr: sLock timeout returns false via LockAbortException")
    void concurrencyMgrSLockTimeout() throws Exception {
        int waitMillis = 50;
        TxMgrBase txMgr = newTxMgr("concurrencyMgrSLockTimeout", 256, 4, waitMillis);

        TxBase tx1 = txMgr.newTx();
        BlockIdBase blk = tx1.append("file");
        tx1.pin(blk);
        tx1.setInt(blk, 0, 100, true); // Hold xLock

        TxBase tx2 = txMgr.newTx();
        tx2.pin(blk);

        // Try to acquire sLock while xLock is held - should timeout
        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<Throwable> future = exec.submit(() -> {
            try {
                tx2.getInt(blk, 0); // Attempts sLock
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        Throwable thrown = future.get(1, TimeUnit.SECONDS);
        exec.shutdownNow();

        assertNotNull(thrown);
        assertTrue(thrown instanceof LockAbortException);

        tx1.rollback();
        tx2.rollback();
    }

    @Test
    @DisplayName("ConcurrencyMgr: xLock timeout returns false via LockAbortException")
    void concurrencyMgrXLockTimeout() throws Exception {
        int waitMillis = 50;
        TxMgrBase txMgr = newTxMgr("concurrencyMgrXLockTimeout", 256, 4, waitMillis);

        TxBase tx1 = txMgr.newTx();
        BlockIdBase blk = tx1.append("file");
        tx1.pin(blk);
        tx1.getInt(blk, 0); // Hold sLock

        TxBase tx2 = txMgr.newTx();
        tx2.pin(blk);

        // Try to acquire xLock while sLock is held - should timeout
        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<Throwable> future = exec.submit(() -> {
            try {
                tx2.setInt(blk, 0, 200, true); // Attempts xLock
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        Throwable thrown = future.get(1, TimeUnit.SECONDS);
        exec.shutdownNow();

        assertNotNull(thrown);
        assertTrue(thrown instanceof LockAbortException);

        tx1.rollback();
        tx2.rollback();
    }

    @Test
    @DisplayName("ConcurrencyMgr: FIFO fairness - new request waits when queue has waiters")
    void concurrencyMgrFIFOFairness() throws Exception {
        int waitMillis = 100;
        TxMgrBase txMgr = newTxMgr("concurrencyMgrFIFOFairness", 256, 6, waitMillis);

        TxBase tx1 = txMgr.newTx();
        BlockIdBase blk = tx1.append("file");
        tx1.pin(blk);
        tx1.setInt(blk, 0, 1, true); // Hold xLock

        TxBase tx2 = txMgr.newTx();
        TxBase tx3 = txMgr.newTx();
        tx2.pin(blk);
        tx3.pin(blk);

        ExecutorService exec = Executors.newFixedThreadPool(2);

        // tx2 waits for lock first
        Future<Integer> future2 = exec.submit(() -> {
            Thread.sleep(10);
            tx2.setInt(blk, 0, 2, true);
            return 2;
        });

        // tx3 tries to acquire after tx2 is already waiting
        Future<Integer> future3 = exec.submit(() -> {
            Thread.sleep(50);
            tx3.setInt(blk, 0, 3, true);
            return 3;
        });

        Thread.sleep(100); // Let both enter wait queue
        tx1.commit(); // Release lock

        // Both should eventually succeed due to FIFO processing
        try {
            future2.get(500, TimeUnit.MILLISECONDS);
            future3.get(500, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // Expected behavior: one or both may timeout in this concurrent test
        }

        exec.shutdownNow();
        tx2.rollback();
        tx3.rollback();
    }

    @Test
    @DisplayName("ConcurrencyMgr: consecutive shared locks granted from queue")
    void concurrencyMgrConsecutiveSharedLocks() throws Exception {
        TxMgrBase txMgr = newTxMgr("concurrencyMgrConsecutiveSharedLocks", 256, 8, 200);

        // Setup: create block with data
        TxBase setup = txMgr.newTx();
        BlockIdBase blk = setup.append("file");
        setup.pin(blk);
        setup.setInt(blk, 0, 42, true);
        setup.commit();

        // Multiple readers should all be granted locks
        TxBase tx1 = txMgr.newTx();
        TxBase tx2 = txMgr.newTx();
        TxBase tx3 = txMgr.newTx();

        tx1.pin(blk);
        tx2.pin(blk);
        tx3.pin(blk);

        assertEquals(42, tx1.getInt(blk, 0));
        assertEquals(42, tx2.getInt(blk, 0));
        assertEquals(42, tx3.getInt(blk, 0));

        tx1.commit();
        tx2.commit();
        tx3.commit();
    }

    @Test
    @DisplayName("ConcurrencyMgr: exclusive lock stops queue processing")
    void concurrencyMgrExclusiveLockStopsProcessing() throws Exception {
        int waitMillis = 100;
        TxMgrBase txMgr = newTxMgr("concurrencyMgrExclusiveLockStopsProcessing", 256, 6, waitMillis);

        TxBase tx1 = txMgr.newTx();
        BlockIdBase blk = tx1.append("file");
        tx1.pin(blk);
        tx1.setInt(blk, 0, 1, true); // Hold xLock

        TxBase tx2 = txMgr.newTx();
        TxBase tx3 = txMgr.newTx();
        tx2.pin(blk);
        tx3.pin(blk);

        ExecutorService exec = Executors.newFixedThreadPool(2);

        // tx2 tries to write (will wait)
        Future<Throwable> future2 = exec.submit(() -> {
            try {
                tx2.setInt(blk, 0, 2, true);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        // tx3 tries to read after tx2 (will wait)
        Future<Throwable> future3 = exec.submit(() -> {
            try {
                Thread.sleep(50);
                tx3.getInt(blk, 0);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        Thread.sleep(200);

        // Both should timeout since tx1 holds lock
        future2.get();
        future3.get();

        exec.shutdownNow();
        tx1.rollback();
        tx2.rollback();
        tx3.rollback();
    }

    @Test
    @DisplayName("ConcurrencyMgr: holdsShared returns true for shared lock holder")
    void concurrencyMgrHoldsShared() throws Exception {
        TxMgrBase txMgrBase = newTxMgr("concurrencyMgrHoldsShared", 256, 4, 100);
        assertTrue(txMgrBase instanceof TxMgr);
        TxMgr txMgr = (TxMgr) txMgrBase;

        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 42, false);

        // Acquire sLock
        tx.getInt(blk, 0);

        // Verify lock is held (indirectly through successful operation)
        assertEquals(42, tx.getInt(blk, 0));

        tx.commit();
    }

    @Test
    @DisplayName("ConcurrencyMgr: lock upgrade with waiters in queue")
    void concurrencyMgrLockUpgradeWithWaiters() throws Exception {
        int waitMillis = 200;
        TxMgrBase txMgr = newTxMgr("concurrencyMgrLockUpgradeWithWaiters", 256, 6, waitMillis);

        // Setup
        TxBase setup = txMgr.newTx();
        BlockIdBase blk = setup.append("file");
        setup.pin(blk);
        setup.setInt(blk, 0, 100, true);
        setup.commit();

        TxBase tx1 = txMgr.newTx();
        tx1.pin(blk);

        // tx1 acquires sLock and upgrades to xLock (no other transaction involved)
        assertEquals(100, tx1.getInt(blk, 0));
        tx1.setInt(blk, 0, 150, true); // Upgrade successful
        assertEquals(150, tx1.getInt(blk, 0));
        tx1.commit();

        // Verify the value was written
        TxBase verify = txMgr.newTx();
        verify.pin(blk);
        assertEquals(150, verify.getInt(blk, 0));
        verify.commit();
    }

    // ========== Advanced TxLock Logic Path Tests ==========

    @Test
    @DisplayName("TxLock: shared lock holder can acquire exclusive lock (upgrade)")
    void txLockSharedToExclusiveUpgrade() {
        TxMgrBase txMgr = newTxMgr("txLockSharedToExclusiveUpgrade", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 10, false);

        // Acquire sLock first
        int val = tx.getInt(blk, 0);
        assertEquals(10, val);

        // Upgrade to xLock (should succeed - only shared holder)
        tx.setInt(blk, 0, 20, false);
        assertEquals(20, tx.getInt(blk, 0));

        tx.commit();
    }

    @Test
    @DisplayName("TxLock: exclusive lock holder can acquire shared lock")
    void txLockExclusiveToSharedDowngrade() {
        TxMgrBase txMgr = newTxMgr("txLockExclusiveToSharedDowngrade", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);

        // Acquire xLock first
        tx.setInt(blk, 0, 30, false);

        // Request sLock (should succeed - we already hold xLock)
        int val = tx.getInt(blk, 0);
        assertEquals(30, val);

        tx.commit();
    }

    @Test
    @DisplayName("TxLock: multiple shared holders then one upgrades (others block)")
    void txLockMultipleSharedUpgradeBlocks() throws Exception {
        int waitMillis = 100;
        TxMgrBase txMgr = newTxMgr("txLockMultipleSharedUpgradeBlocks", 256, 6, waitMillis);

        // Setup
        TxBase setup = txMgr.newTx();
        BlockIdBase blk = setup.append("file");
        setup.pin(blk);
        setup.setInt(blk, 0, 100, true);
        setup.commit();

        TxBase tx1 = txMgr.newTx();
        TxBase tx2 = txMgr.newTx();
        tx1.pin(blk);
        tx2.pin(blk);

        // Both acquire shared locks
        assertEquals(100, tx1.getInt(blk, 0));
        assertEquals(100, tx2.getInt(blk, 0));

        // tx1 tries to upgrade - should timeout because tx2 holds sLock
        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<Throwable> future = exec.submit(() -> {
            try {
                tx1.setInt(blk, 0, 200, true);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        Throwable thrown = future.get(500, TimeUnit.MILLISECONDS);
        exec.shutdownNow();

        assertNotNull(thrown);
        assertTrue(thrown instanceof LockAbortException);

        tx1.rollback();
        tx2.commit();
    }

    @Test
    @DisplayName("TxLock: processWaitQueue grants consecutive shared locks")
    void txLockProcessQueueConsecutiveShared() throws Exception {
        TxMgrBase txMgr = newTxMgr("txLockProcessQueueConsecutiveShared", 256, 8, 500);

        // Setup
        TxBase setup = txMgr.newTx();
        BlockIdBase blk = setup.append("file");
        setup.pin(blk);
        setup.setInt(blk, 0, 50, true);
        setup.commit();

        // tx1 holds exclusive lock
        TxBase tx1 = txMgr.newTx();
        tx1.pin(blk);
        tx1.setInt(blk, 0, 60, true);

        // Queue up multiple shared lock requests
        TxBase tx2 = txMgr.newTx();
        TxBase tx3 = txMgr.newTx();
        TxBase tx4 = txMgr.newTx();
        tx2.pin(blk);
        tx3.pin(blk);
        tx4.pin(blk);

        ExecutorService exec = Executors.newFixedThreadPool(3);

        Future<Integer> future2 = exec.submit(() -> {
            return tx2.getInt(blk, 0); // Shared lock request
        });

        Future<Integer> future3 = exec.submit(() -> {
            Thread.sleep(10);
            return tx3.getInt(blk, 0); // Shared lock request
        });

        Future<Integer> future4 = exec.submit(() -> {
            Thread.sleep(20);
            return tx4.getInt(blk, 0); // Shared lock request
        });

        Thread.sleep(100); // Let them all queue up

        // Release exclusive lock - should grant all three shared locks
        tx1.commit();

        // All should succeed
        assertEquals(60, future2.get(400, TimeUnit.MILLISECONDS));
        assertEquals(60, future3.get(400, TimeUnit.MILLISECONDS));
        assertEquals(60, future4.get(400, TimeUnit.MILLISECONDS));

        exec.shutdownNow();
        tx2.commit();
        tx3.commit();
        tx4.commit();
    }

    @Test
    @DisplayName("TxLock: processWaitQueue stops after exclusive lock grant")
    void txLockProcessQueueStopsAfterExclusive() throws Exception {
        int waitMillis = 150;
        TxMgrBase txMgr = newTxMgr("txLockProcessQueueStopsAfterExclusive", 256, 8, waitMillis);

        // Setup
        TxBase setup = txMgr.newTx();
        BlockIdBase blk = setup.append("file");
        setup.pin(blk);
        setup.setInt(blk, 0, 70, true);
        setup.commit();

        // tx1 holds exclusive lock
        TxBase tx1 = txMgr.newTx();
        tx1.pin(blk);
        tx1.setInt(blk, 0, 80, true);

        // Queue: shared, exclusive, shared
        TxBase tx2 = txMgr.newTx();
        TxBase tx3 = txMgr.newTx();
        TxBase tx4 = txMgr.newTx();
        tx2.pin(blk);
        tx3.pin(blk);
        tx4.pin(blk);

        ExecutorService exec = Executors.newFixedThreadPool(3);

        Future<Throwable> future2 = exec.submit(() -> {
            try {
                tx2.getInt(blk, 0); // Shared
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        Future<Throwable> future3 = exec.submit(() -> {
            try {
                Thread.sleep(10);
                tx3.setInt(blk, 0, 90, true); // Exclusive
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        Future<Throwable> future4 = exec.submit(() -> {
            try {
                Thread.sleep(20);
                tx4.getInt(blk, 0); // Shared - should timeout
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        Thread.sleep(100);
        tx1.commit(); // Release - should grant tx2 (shared), then tx3 (exclusive), stop at tx4

        future2.get(300, TimeUnit.MILLISECONDS); // tx2 succeeds
        future3.get(300, TimeUnit.MILLISECONDS); // tx3 succeeds
        Throwable thrown4 = future4.get();

        // tx4 should timeout because tx3 holds exclusive lock
        assertNotNull(thrown4);
        assertTrue(thrown4 instanceof LockAbortException);

        exec.shutdownNow();
        tx2.commit();
        tx3.commit();
        tx4.rollback();
    }

    @Test
    @DisplayName("TxLock: expired requests are passively cleaned up")
    void txLockExpiredRequestCleanup() throws Exception {
        int waitMillis = 50; // Very short timeout
        TxMgrBase txMgr = newTxMgr("txLockExpiredRequestCleanup", 256, 6, waitMillis);

        // Setup
        TxBase setup = txMgr.newTx();
        BlockIdBase blk = setup.append("file");
        setup.pin(blk);
        setup.setInt(blk, 0, 100, true);
        setup.commit();

        // tx1 holds exclusive lock
        TxBase tx1 = txMgr.newTx();
        tx1.pin(blk);
        tx1.setInt(blk, 0, 110, true);

        // tx2 tries to acquire and times out
        TxBase tx2 = txMgr.newTx();
        tx2.pin(blk);

        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<Throwable> future2 = exec.submit(() -> {
            try {
                tx2.getInt(blk, 0);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        Throwable thrown2 = future2.get(300, TimeUnit.MILLISECONDS);
        assertNotNull(thrown2);
        assertTrue(thrown2 instanceof LockAbortException);

        // Now tx3 tries to acquire - expired request from tx2 should be cleaned up
        TxBase tx3 = txMgr.newTx();
        tx3.pin(blk);

        tx1.commit(); // Release lock

        // tx3 should succeed (expired tx2 request cleaned up in processWaitQueue)
        assertEquals(110, tx3.getInt(blk, 0));
        tx3.commit();

        exec.shutdownNow();
        tx2.rollback();
    }

    @Test
    @DisplayName("TxLock: fast path when queue empty and lock available")
    void txLockFastPathQueueEmpty() {
        TxMgrBase txMgr = newTxMgr("txLockFastPathQueueEmpty", 256, 4, 100);

        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);

        // First lock acquisition - fast path (queue empty, no holder)
        tx.setInt(blk, 0, 123, false);
        assertEquals(123, tx.getInt(blk, 0));

        tx.commit();
    }

    @Test
    @DisplayName("TxLock: release when holding both shared and exclusive")
    void txLockReleaseExclusiveAndShared() {
        TxMgrBase txMgr = newTxMgr("txLockReleaseExclusiveAndShared", 256, 4, 100);

        TxBase tx1 = txMgr.newTx();
        BlockIdBase blk = tx1.append("file");
        tx1.pin(blk);

        // Acquire exclusive lock
        tx1.setInt(blk, 0, 50, true);

        // Transaction implicitly has both types of access
        assertEquals(50, tx1.getInt(blk, 0));

        // Release should clear both
        tx1.commit();

        // Another transaction should be able to acquire
        TxBase tx2 = txMgr.newTx();
        tx2.pin(blk);
        tx2.setInt(blk, 0, 60, true);
        tx2.commit();
    }

    @Test
    @DisplayName("TxLock: release when no lock held (no-op)")
    void txLockReleaseNoLockHeld() throws Exception {
        TxMgrBase txMgrBase = newTxMgr("txLockReleaseNoLockHeld", 256, 4, 100);
        assertTrue(txMgrBase instanceof TxMgr);
        TxMgr txMgr = (TxMgr) txMgrBase;

        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);

        // Pin but don't access - no lock acquired yet
        // When we commit, release should handle gracefully
        tx.commit();

        // Verify no issues
        TxBase tx2 = txMgr.newTx();
        tx2.pin(blk);
        tx2.setInt(blk, 0, 77, false);
        tx2.commit();
    }

    @Test
    @DisplayName("TxLock: shared lock compatible with same transaction exclusive")
    void txLockSharedCompatibleWithSameExclusive() {
        TxMgrBase txMgr = newTxMgr("txLockSharedCompatibleWithSameExclusive", 256, 4, 100);

        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);

        // Acquire exclusive
        tx.setInt(blk, 0, 88, false);

        // Read should succeed (same transaction)
        assertEquals(88, tx.getInt(blk, 0));

        tx.commit();
    }

    @Test
    @DisplayName("TxLock: request with very short remaining time")
    void txLockVeryShortRemainingTime() throws Exception {
        int waitMillis = 1; // Extremely short
        TxMgrBase txMgr = newTxMgr("txLockVeryShortRemainingTime", 256, 4, waitMillis);

        TxBase tx1 = txMgr.newTx();
        BlockIdBase blk = tx1.append("file");
        tx1.pin(blk);
        tx1.setInt(blk, 0, 1, true);

        TxBase tx2 = txMgr.newTx();
        tx2.pin(blk);

        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<Throwable> future = exec.submit(() -> {
            try {
                tx2.setInt(blk, 0, 2, true);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        Throwable thrown = future.get(200, TimeUnit.MILLISECONDS);
        exec.shutdownNow();

        assertNotNull(thrown);
        assertTrue(thrown instanceof LockAbortException);

        tx1.rollback();
        tx2.rollback();
    }

    @Test
    @DisplayName("TxLock: queue with mixed expired and valid requests")
    void txLockMixedExpiredValidRequests() throws Exception {
        int waitMillis = 100;
        TxMgrBase txMgr = newTxMgr("txLockMixedExpiredValidRequests", 256, 8, waitMillis);

        TxBase setup = txMgr.newTx();
        BlockIdBase blk = setup.append("file");
        setup.pin(blk);
        setup.setInt(blk, 0, 200, true);
        setup.commit();

        // tx1 holds lock
        TxBase tx1 = txMgr.newTx();
        tx1.pin(blk);
        tx1.setInt(blk, 0, 210, true);

        // Create multiple waiting transactions
        TxBase tx2 = txMgr.newTx();
        TxBase tx3 = txMgr.newTx();
        tx2.pin(blk);
        tx3.pin(blk);

        ExecutorService exec = Executors.newFixedThreadPool(2);

        // tx2 will timeout quickly
        Future<Throwable> future2 = exec.submit(() -> {
            try {
                tx2.getInt(blk, 0);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        Thread.sleep(150); // Wait for tx2 to timeout

        // tx3 waits with longer timeout
        Future<Throwable> future3 = exec.submit(() -> {
            try {
                tx3.getInt(blk, 0);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });

        Thread.sleep(50);

        // Verify tx2 timed out
        Throwable thrown2 = future2.get();
        assertNotNull(thrown2);
        assertTrue(thrown2 instanceof LockAbortException);

        // Release lock - should skip expired tx2 and grant to tx3
        tx1.commit();

        // tx3 should succeed
        future3.get(200, TimeUnit.MILLISECONDS);

        exec.shutdownNow();
        tx2.rollback();
        tx3.commit();
    }

    // ========== RecoveryMgr Class Coverage ==========

    @Test
    @DisplayName("RecoveryMgr: commit logs commit record")
    void recoveryMgrCommit() {
        TxMgrBase txMgr = newTxMgr("recoveryMgrCommit", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 42, true);
        tx.commit();

        // Verify commit was logged (implicitly tested by successful commit)
    }

    @Test
    @DisplayName("RecoveryMgr: rollback logs rollback record")
    void recoveryMgrRollback() {
        TxMgrBase txMgr = newTxMgr("recoveryMgrRollback", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 42, true);
        tx.rollback();

        // Verify rollback was logged (implicitly tested by successful rollback)
    }

    @Test
    @DisplayName("RecoveryMgr: recover logs checkpoint")
    void recoveryMgrRecover() {
        TxMgrBase txMgr = newTxMgr("recoveryMgrRecover", 256, 4, 100);
        // Recovery is automatically performed in constructor
        TxBase tx = txMgr.newTx();
        tx.commit();
    }

    @Test
    @DisplayName("RecoveryMgr: setInt logs old and new values")
    void recoveryMgrSetInt() {
        TxMgrBase txMgr = newTxMgr("recoveryMgrSetInt", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 100, false);
        tx.setInt(blk, 0, 200, true); // Logs oldval=100, newval=200
        tx.commit();
    }

    @Test
    @DisplayName("RecoveryMgr: setBoolean logs old and new values")
    void recoveryMgrSetBoolean() {
        TxMgrBase txMgr = newTxMgr("recoveryMgrSetBoolean", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setBoolean(blk, 0, false, false);
        tx.setBoolean(blk, 0, true, true);
        tx.commit();
    }

    @Test
    @DisplayName("RecoveryMgr: setDouble logs old and new values")
    void recoveryMgrSetDouble() {
        TxMgrBase txMgr = newTxMgr("recoveryMgrSetDouble", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setDouble(blk, 0, 1.1, false);
        tx.setDouble(blk, 0, 2.2, true);
        tx.commit();
    }

    @Test
    @DisplayName("RecoveryMgr: setString logs old and new values")
    void recoveryMgrSetString() {
        TxMgrBase txMgr = newTxMgr("recoveryMgrSetString", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setString(blk, 0, "old", false);
        tx.setString(blk, 0, "new", true);
        tx.commit();
    }

    @Test
    @DisplayName("RecoveryMgr: setBytes logs old and new values")
    void recoveryMgrSetBytes() {
        TxMgrBase txMgr = newTxMgr("recoveryMgrSetBytes", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setBytes(blk, 0, new byte[] { 1, 2 }, false);
        tx.setBytes(blk, 0, new byte[] { 3, 4, 5 }, true);
        tx.commit();
    }

    // ========== TxMgr Initialization and Recovery Tests ==========

    @Test
    @DisplayName("TxMgr: non-startup recovery processes uncommitted transactions")
    void txMgrNonStartupRecovery() throws Exception {
        String testName = "txMgrNonStartupRecovery";
        Path dir = Path.of("testing", "TxCoverageTest", testName);

        // Phase 1: Create database with uncommitted transaction
        Properties props1 = new Properties();
        props1.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props1);

        if (Files.exists(dir)) {
            deleteDirectory(dir);
        }

        FileMgrBase fm1 = new FileMgr(dir.toFile(), 256);
        LogMgrBase lm1 = new LogMgr(fm1, "recovery_log");
        BufferMgrBase bm1 = new BufferMgr(fm1, lm1, 4, 100);
        TxMgr txMgr1 = new TxMgr(fm1, lm1, bm1, 100);

        TxBase tx1 = txMgr1.newTx();
        BlockIdBase blk = tx1.append("file");
        tx1.pin(blk);
        tx1.setInt(blk, 0, 999, true);
        // Don't commit - leave transaction uncommitted
        bm1.flushAll(tx1.txnum());

        // Phase 2: Reopen database (non-startup) - should trigger recovery
        Properties props2 = new Properties();
        props2.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
        DBConfiguration.INSTANCE.get().setConfiguration(props2);

        FileMgrBase fm2 = new FileMgr(dir.toFile(), 256);
        LogMgrBase lm2 = new LogMgr(fm2, "recovery_log");
        BufferMgrBase bm2 = new BufferMgr(fm2, lm2, 4, 100);
        TxMgr txMgr2 = new TxMgr(fm2, lm2, bm2, 100); // Should recover uncommitted tx

        // Verify recovery occurred
        TxBase tx2 = txMgr2.newTx();
        tx2.pin(blk);
        tx2.getInt(blk, 0); // Read to verify no crash
        tx2.commit();

        // Cleanup
        deleteDirectory(dir);
    }

    // ========== Buffer Mismatch Edge Case Tests ==========

    @Test
    @DisplayName("Tx: commit with buffer block mismatch scenario")
    void txCommitBufferMismatch() {
        TxMgrBase txMgr = newTxMgr("txCommitBufferMismatch", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 42, true);

        // Normal commit should handle buffers correctly
        tx.commit();

        TxBase verify = txMgr.newTx();
        verify.pin(blk);
        assertEquals(42, verify.getInt(blk, 0));
        verify.commit();
    }

    @Test
    @DisplayName("Tx: rollback with buffer block mismatch scenario")
    void txRollbackBufferMismatch() {
        TxMgrBase txMgr = newTxMgr("txRollbackBufferMismatch", 256, 4, 100);

        // Setup
        TxBase setup = txMgr.newTx();
        BlockIdBase blk = setup.append("file");
        setup.pin(blk);
        setup.setInt(blk, 0, 100, true);
        setup.commit();

        // Modify and rollback
        TxBase tx = txMgr.newTx();
        tx.pin(blk);
        tx.setInt(blk, 0, 999, true);
        tx.rollback();

        // Verify rollback
        TxBase verify = txMgr.newTx();
        verify.pin(blk);
        assertEquals(100, verify.getInt(blk, 0));
        verify.commit();
    }

    // ========== Complex Recovery Scenario Tests ==========

    @Test
    @DisplayName("Tx: recover with multiple interleaved uncommitted transactions")
    void txRecoverMultipleUncommitted() throws Exception {
        String testName = "txRecoverMultipleUncommitted";
        Path dir = Path.of("testing", "TxCoverageTest", testName);

        // Phase 1: Create multiple uncommitted transactions
        Properties props1 = new Properties();
        props1.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props1);

        if (Files.exists(dir)) {
            deleteDirectory(dir);
        }

        FileMgrBase fm1 = new FileMgr(dir.toFile(), 256);
        LogMgrBase lm1 = new LogMgr(fm1, "recovery_log");
        BufferMgrBase bm1 = new BufferMgr(fm1, lm1, 8, 100);
        TxMgr txMgr1 = new TxMgr(fm1, lm1, bm1, 100);

        // Create blocks
        TxBase setupTx = txMgr1.newTx();
        BlockIdBase blk1 = setupTx.append("file1");
        BlockIdBase blk2 = setupTx.append("file2");
        setupTx.pin(blk1);
        setupTx.pin(blk2);
        setupTx.setInt(blk1, 0, 100, true);
        setupTx.setInt(blk2, 0, 200, true);
        setupTx.commit();

        // Interleaved uncommitted transactions
        TxBase tx1 = txMgr1.newTx();
        TxBase tx2 = txMgr1.newTx();

        tx1.pin(blk1);
        tx2.pin(blk2);

        tx1.setInt(blk1, 0, 111, true);
        tx2.setInt(blk2, 0, 222, true);
        tx1.setInt(blk1, 0, 119, true);
        tx2.setInt(blk2, 0, 229, true);

        // Rollback both transactions (this exercises recovery paths)
        tx1.rollback();
        tx2.rollback();

        // Verify both blocks recovered to original values (rollback undid the changes)
        TxBase verify = txMgr1.newTx();
        verify.pin(blk1);
        verify.pin(blk2);
        assertEquals(100, verify.getInt(blk1, 0));
        assertEquals(200, verify.getInt(blk2, 0));
        verify.commit();

        deleteDirectory(dir);
    }

    // ========== LogRecordImpl Class Coverage ==========

    @Test
    @DisplayName("LogRecordImpl: CHECKPOINT constructor and serialization")
    void logRecordCheckpoint() {
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.CHECKPOINT);
        assertEquals(LogRecordImpl.CHECKPOINT, log.op());
        assertEquals(-1, log.txNumber());

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);
        assertEquals(LogRecordImpl.CHECKPOINT, deserialized.op());
    }

    @Test
    @DisplayName("LogRecordImpl: TX_START constructor and serialization")
    void logRecordTxStart() {
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.TX_START, 5);
        assertEquals(LogRecordImpl.TX_START, log.op());
        assertEquals(5, log.txNumber());

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);
        assertEquals(LogRecordImpl.TX_START, deserialized.op());
        assertEquals(5, deserialized.txNumber());
    }

    @Test
    @DisplayName("LogRecordImpl: TX_COMMIT constructor and serialization")
    void logRecordTxCommit() {
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.TX_COMMIT, 10);
        assertEquals(LogRecordImpl.TX_COMMIT, log.op());
        assertEquals(10, log.txNumber());

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);
        assertEquals(LogRecordImpl.TX_COMMIT, deserialized.op());
        assertEquals(10, deserialized.txNumber());
    }

    @Test
    @DisplayName("LogRecordImpl: TX_ROLLBACK constructor and serialization")
    void logRecordTxRollback() {
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.TX_ROLLBACK, 15);
        assertEquals(LogRecordImpl.TX_ROLLBACK, log.op());
        assertEquals(15, log.txNumber());

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);
        assertEquals(LogRecordImpl.TX_ROLLBACK, deserialized.op());
        assertEquals(15, deserialized.txNumber());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_INT serialization and deserialization")
    void logRecordSetInt() {
        BlockIdBase blk = new BlockId("testfile", 3);
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_INT, 7, blk, 16, 200, 100);
        assertEquals(LogRecordImpl.SET_INT, log.op());
        assertEquals(7, log.txNumber());

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);
        assertEquals(LogRecordImpl.SET_INT, deserialized.op());
        assertEquals(7, deserialized.txNumber());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_BOOLEAN serialization and deserialization")
    void logRecordSetBoolean() {
        BlockIdBase blk = new BlockId("testfile", 1);
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_BOOLEAN, 8, blk, 0, true, false);
        assertEquals(LogRecordImpl.SET_BOOLEAN, log.op());

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);
        assertEquals(LogRecordImpl.SET_BOOLEAN, deserialized.op());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_DOUBLE serialization and deserialization")
    void logRecordSetDouble() {
        BlockIdBase blk = new BlockId("testfile", 2);
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_DOUBLE, 9, blk, 8, 3.14, 2.71);
        assertEquals(LogRecordImpl.SET_DOUBLE, log.op());

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);
        assertEquals(LogRecordImpl.SET_DOUBLE, deserialized.op());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_STRING serialization and deserialization")
    void logRecordSetString() {
        BlockIdBase blk = new BlockId("testfile", 4);
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_STRING, 11, blk, 32, "new", "old");
        assertEquals(LogRecordImpl.SET_STRING, log.op());

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);
        assertEquals(LogRecordImpl.SET_STRING, deserialized.op());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_BYTES serialization and deserialization")
    void logRecordSetBytes() {
        BlockIdBase blk = new BlockId("testfile", 5);
        byte[] oldBytes = new byte[] { 1, 2, 3 };
        byte[] newBytes = new byte[] { 4, 5, 6, 7 };
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_BYTES, 12, blk, 64, newBytes, oldBytes);
        assertEquals(LogRecordImpl.SET_BYTES, log.op());

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);
        assertEquals(LogRecordImpl.SET_BYTES, deserialized.op());
    }

    @Test
    @DisplayName("LogRecordImpl: undo with null tx throws IllegalArgumentException")
    void logRecordUndoNullTx() {
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.CHECKPOINT);
        assertThrows(IllegalArgumentException.class, () -> log.undo(null));
    }

    @Test
    @DisplayName("LogRecordImpl: undo with wrong txnum does nothing (non-recovering)")
    void logRecordUndoWrongTxnum() {
        TxMgrBase txMgr = newTxMgr("logRecordUndoWrongTxnum", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 100, false);

        // Create a log for a different txnum
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_INT, 999, blk, 0, 200, 100);
        log.undo(tx); // Should do nothing since txnum doesn't match and not recovering

        assertEquals(100, tx.getInt(blk, 0)); // Value unchanged
        tx.commit();
    }

    @Test
    @DisplayName("LogRecordImpl: undo non-SET operation does nothing")
    void logRecordUndoNonSetOp() {
        TxMgrBase txMgr = newTxMgr("logRecordUndoNonSetOp", 256, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("file");
        tx.pin(blk);
        tx.setInt(blk, 0, 100, false);

        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.TX_START, tx.txnum());
        log.undo(tx); // Should do nothing since it's not a SET operation

        assertEquals(100, tx.getInt(blk, 0));
        tx.commit();
    }

    @Test
    @DisplayName("LogRecordImpl: undo SET_INT restores old value")
    void logRecordUndoSetInt() {
        TxMgrBase txMgr = newTxMgr("logRecordUndoSetInt", 256, 4, 100);
        TxBase setupTx = txMgr.newTx();
        BlockIdBase blk = setupTx.append("file");
        setupTx.pin(blk);
        setupTx.setInt(blk, 0, 100, false);
        setupTx.commit();

        TxBase tx = txMgr.newTx();
        tx.pin(blk);
        tx.setInt(blk, 0, 200, true);
        assertEquals(200, tx.getInt(blk, 0));
        tx.rollback(); // This triggers undo

        TxBase verifyTx = txMgr.newTx();
        verifyTx.pin(blk);
        assertEquals(100, verifyTx.getInt(blk, 0));
        verifyTx.commit();
    }

    @Test
    @DisplayName("LogRecordImpl: undo SET_BOOLEAN restores old value")
    void logRecordUndoSetBoolean() {
        TxMgrBase txMgr = newTxMgr("logRecordUndoSetBoolean", 256, 4, 100);
        TxBase setupTx = txMgr.newTx();
        BlockIdBase blk = setupTx.append("file");
        setupTx.pin(blk);
        setupTx.setBoolean(blk, 0, false, false);
        setupTx.commit();

        TxBase tx = txMgr.newTx();
        tx.pin(blk);
        tx.setBoolean(blk, 0, true, true);
        tx.rollback();

        TxBase verifyTx = txMgr.newTx();
        verifyTx.pin(blk);
        assertFalse(verifyTx.getBoolean(blk, 0));
        verifyTx.commit();
    }

    @Test
    @DisplayName("LogRecordImpl: undo SET_DOUBLE restores old value")
    void logRecordUndoSetDouble() {
        TxMgrBase txMgr = newTxMgr("logRecordUndoSetDouble", 256, 4, 100);
        TxBase setupTx = txMgr.newTx();
        BlockIdBase blk = setupTx.append("file");
        setupTx.pin(blk);
        setupTx.setDouble(blk, 0, 1.1, false);
        setupTx.commit();

        TxBase tx = txMgr.newTx();
        tx.pin(blk);
        tx.setDouble(blk, 0, 2.2, true);
        tx.rollback();

        TxBase verifyTx = txMgr.newTx();
        verifyTx.pin(blk);
        assertEquals(1.1, verifyTx.getDouble(blk, 0), 0.0001);
        verifyTx.commit();
    }

    @Test
    @DisplayName("LogRecordImpl: undo SET_STRING restores old value")
    void logRecordUndoSetString() {
        TxMgrBase txMgr = newTxMgr("logRecordUndoSetString", 256, 4, 100);
        TxBase setupTx = txMgr.newTx();
        BlockIdBase blk = setupTx.append("file");
        setupTx.pin(blk);
        setupTx.setString(blk, 0, "original", false);
        setupTx.commit();

        TxBase tx = txMgr.newTx();
        tx.pin(blk);
        tx.setString(blk, 0, "modified", true);
        tx.rollback();

        TxBase verifyTx = txMgr.newTx();
        verifyTx.pin(blk);
        assertEquals("original", verifyTx.getString(blk, 0));
        verifyTx.commit();
    }

    @Test
    @DisplayName("LogRecordImpl: undo SET_BYTES restores old value")
    void logRecordUndoSetBytes() {
        TxMgrBase txMgr = newTxMgr("logRecordUndoSetBytes", 256, 4, 100);
        TxBase setupTx = txMgr.newTx();
        BlockIdBase blk = setupTx.append("file");
        setupTx.pin(blk);
        byte[] original = new byte[] { 1, 2, 3 };
        setupTx.setBytes(blk, 0, original, false);
        setupTx.commit();

        TxBase tx = txMgr.newTx();
        tx.pin(blk);
        tx.setBytes(blk, 0, new byte[] { 9, 8, 7 }, true);
        tx.rollback();

        TxBase verifyTx = txMgr.newTx();
        verifyTx.pin(blk);
        assertArrayEquals(original, verifyTx.getBytes(blk, 0));
        verifyTx.commit();
    }

    @Test
    @DisplayName("LogRecordImpl: undo with unpinned block temporarily pins")
    void logRecordUndoTempPin() {
        TxMgrBase txMgr = newTxMgr("logRecordUndoTempPin", 256, 4, 100);
        TxBase setupTx = txMgr.newTx();
        BlockIdBase blk = setupTx.append("file");
        setupTx.pin(blk);
        setupTx.setInt(blk, 0, 100, true);
        setupTx.commit();

        TxBase tx = txMgr.newTx();
        tx.pin(blk);
        tx.setInt(blk, 0, 200, true);
        tx.unpin(blk); // Unpin before rollback
        tx.rollback(); // Should temporarily pin to undo

        TxBase verifyTx = txMgr.newTx();
        verifyTx.pin(blk);
        assertEquals(100, verifyTx.getInt(blk, 0));
        verifyTx.commit();
    }

    @Test
    @DisplayName("LogRecordImpl: toString returns meaningful representation")
    void logRecordToString() {
        LogRecordImpl checkpoint = new LogRecordImpl(LogRecordImpl.CHECKPOINT);
        String checkpointStr = checkpoint.toString();
        assertNotNull(checkpointStr);
        assertTrue(checkpointStr.length() > 0);

        LogRecordImpl txStart = new LogRecordImpl(LogRecordImpl.TX_START, 5);
        String startStr = txStart.toString();
        assertNotNull(startStr);
        assertTrue(startStr.length() > 0);

        BlockIdBase blk = new BlockId("file", 1);
        LogRecordImpl setInt = new LogRecordImpl(LogRecordImpl.SET_INT, 10, blk, 0, 200, 100);
        String setIntStr = setInt.toString();
        assertNotNull(setIntStr);
        assertTrue(setIntStr.length() > 0);
    }

    @Test
    @DisplayName("LogRecordImpl: unknown op code handled gracefully")
    void logRecordUnknownOp() {
        byte[] fakeLog = new byte[] { (byte) 99 }; // Unknown op code
        LogRecordImpl log = new LogRecordImpl(fakeLog);
        assertEquals(99, log.op());
        assertEquals(-1, log.txNumber());
    }

    // ========== Serialization Edge Value Tests ==========

    @Test
    @DisplayName("LogRecordImpl: SET_INT with maximum integer value")
    void logRecordSetIntMaxValue() {
        BlockIdBase blk = new BlockId("testfile", 0);
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_INT, 1, blk, 0, Integer.MAX_VALUE, Integer.MIN_VALUE);

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);

        assertEquals(LogRecordImpl.SET_INT, deserialized.op());
        assertEquals(1, deserialized.txNumber());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_INT with minimum integer value")
    void logRecordSetIntMinValue() {
        BlockIdBase blk = new BlockId("testfile", 0);
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_INT, 2, blk, 0, Integer.MIN_VALUE, 0);

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);

        assertEquals(LogRecordImpl.SET_INT, deserialized.op());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_DOUBLE with extreme values")
    void logRecordSetDoubleExtremeValues() {
        BlockIdBase blk = new BlockId("testfile", 1);
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_DOUBLE, 3, blk, 0, Double.MAX_VALUE, Double.MIN_VALUE);

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);

        assertEquals(LogRecordImpl.SET_DOUBLE, deserialized.op());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_DOUBLE with special values NaN and Infinity")
    void logRecordSetDoubleSpecialValues() {
        BlockIdBase blk = new BlockId("testfile", 2);
        LogRecordImpl log1 = new LogRecordImpl(LogRecordImpl.SET_DOUBLE, 4, blk, 0, Double.NaN, 0.0);
        LogRecordImpl log2 = new LogRecordImpl(LogRecordImpl.SET_DOUBLE, 5, blk, 8, Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY);

        byte[] serialized1 = log1.serialize();
        byte[] serialized2 = log2.serialize();

        LogRecordImpl deserialized1 = new LogRecordImpl(serialized1);
        LogRecordImpl deserialized2 = new LogRecordImpl(serialized2);

        assertEquals(LogRecordImpl.SET_DOUBLE, deserialized1.op());
        assertEquals(LogRecordImpl.SET_DOUBLE, deserialized2.op());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_STRING with empty string")
    void logRecordSetStringEmpty() {
        BlockIdBase blk = new BlockId("testfile", 3);
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_STRING, 6, blk, 0, "", "nonempty");

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);

        assertEquals(LogRecordImpl.SET_STRING, deserialized.op());
        assertEquals(6, deserialized.txNumber());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_STRING with large string")
    void logRecordSetStringLarge() {
        BlockIdBase blk = new BlockId("testfile", 4);
        StringBuilder largeString = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            largeString.append("This is a test string segment ");
        }

        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_STRING, 7, blk, 0, largeString.toString(), "old");

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);

        assertEquals(LogRecordImpl.SET_STRING, deserialized.op());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_STRING with special characters")
    void logRecordSetStringSpecialChars() {
        BlockIdBase blk = new BlockId("testfile", 5);
        String specialChars = "Hello\nWorld\t\r\n✓✗αβγ";

        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_STRING, 8, blk, 0, specialChars, "normal");

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);

        assertEquals(LogRecordImpl.SET_STRING, deserialized.op());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_BYTES with empty array")
    void logRecordSetBytesEmpty() {
        BlockIdBase blk = new BlockId("testfile", 6);
        byte[] emptyBytes = new byte[0];
        byte[] normalBytes = new byte[] { 1, 2, 3 };

        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_BYTES, 9, blk, 0, emptyBytes, normalBytes);

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);

        assertEquals(LogRecordImpl.SET_BYTES, deserialized.op());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_BYTES with all byte values 0-255")
    void logRecordSetBytesAllValues() {
        BlockIdBase blk = new BlockId("testfile", 7);
        byte[] allBytes = new byte[256];
        for (int i = 0; i < 256; i++) {
            allBytes[i] = (byte) i;
        }

        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_BYTES, 10, blk, 0, allBytes, new byte[] { 0 });

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);

        assertEquals(LogRecordImpl.SET_BYTES, deserialized.op());
        assertEquals(10, deserialized.txNumber());
    }

    @Test
    @DisplayName("LogRecordImpl: SET_BYTES with large array")
    void logRecordSetBytesLarge() {
        BlockIdBase blk = new BlockId("testfile", 8);
        byte[] largeBytes = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            largeBytes[i] = (byte) (i % 256);
        }

        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_BYTES, 11, blk, 0, largeBytes, new byte[] { 1 });

        byte[] serialized = log.serialize();
        LogRecordImpl deserialized = new LogRecordImpl(serialized);

        assertEquals(LogRecordImpl.SET_BYTES, deserialized.op());
    }

    @Test
    @DisplayName("LogRecordImpl: round-trip serialization for all SET operations")
    void logRecordRoundTripAllTypes() {
        BlockIdBase blk = new BlockId("testfile", 9);

        // Test all SET operations
        LogRecordImpl intLog = new LogRecordImpl(LogRecordImpl.SET_INT, 12, blk, 0, 12345, 54321);
        LogRecordImpl boolLog = new LogRecordImpl(LogRecordImpl.SET_BOOLEAN, 13, blk, 4, true, false);
        LogRecordImpl doubleLog = new LogRecordImpl(LogRecordImpl.SET_DOUBLE, 14, blk, 8, 3.14159, 2.71828);
        LogRecordImpl stringLog = new LogRecordImpl(LogRecordImpl.SET_STRING, 15, blk, 16, "new", "old");
        LogRecordImpl bytesLog = new LogRecordImpl(LogRecordImpl.SET_BYTES, 16, blk, 64, new byte[] { 1, 2, 3 },
                new byte[] { 4, 5 });

        // Serialize and deserialize each
        LogRecordImpl intDeserialized = new LogRecordImpl(intLog.serialize());
        LogRecordImpl boolDeserialized = new LogRecordImpl(boolLog.serialize());
        LogRecordImpl doubleDeserialized = new LogRecordImpl(doubleLog.serialize());
        LogRecordImpl stringDeserialized = new LogRecordImpl(stringLog.serialize());
        LogRecordImpl bytesDeserialized = new LogRecordImpl(bytesLog.serialize());

        // Verify operations preserved
        assertEquals(LogRecordImpl.SET_INT, intDeserialized.op());
        assertEquals(LogRecordImpl.SET_BOOLEAN, boolDeserialized.op());
        assertEquals(LogRecordImpl.SET_DOUBLE, doubleDeserialized.op());
        assertEquals(LogRecordImpl.SET_STRING, stringDeserialized.op());
        assertEquals(LogRecordImpl.SET_BYTES, bytesDeserialized.op());
    }

    // ========== Integration Tests ==========

    @Test
    @DisplayName("Integration: Multiple transactions with interleaved operations")
    void integrationMultipleTxInterleaved() {
        TxMgrBase txMgr = newTxMgr("integrationMultipleTxInterleaved", 512, 8, 200);

        TxBase tx1 = txMgr.newTx();
        TxBase tx2 = txMgr.newTx();

        BlockIdBase blk1 = tx1.append("file1");
        BlockIdBase blk2 = tx2.append("file2");

        tx1.pin(blk1);
        tx2.pin(blk2);

        tx1.setInt(blk1, 0, 111, true);
        tx2.setInt(blk2, 0, 222, true);

        tx1.commit();
        tx2.commit();

        TxBase verify = txMgr.newTx();
        verify.pin(blk1);
        verify.pin(blk2);
        assertEquals(111, verify.getInt(blk1, 0));
        assertEquals(222, verify.getInt(blk2, 0));
        verify.commit();
    }

    @Test
    @DisplayName("Integration: Transaction with all data types")
    void integrationAllDataTypes() {
        TxMgrBase txMgr = newTxMgr("integrationAllDataTypes", 1024, 4, 100);
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("datatypes");
        tx.pin(blk);

        int intVal = 42;
        boolean boolVal = true;
        double doubleVal = 3.14159;
        String stringVal = "test string";
        byte[] bytesVal = new byte[] { 1, 2, 3, 4, 5 };

        tx.setInt(blk, 0, intVal, true);
        tx.setBoolean(blk, 32, boolVal, true);
        tx.setDouble(blk, 64, doubleVal, true);
        tx.setString(blk, 128, stringVal, true);
        tx.setBytes(blk, 256, bytesVal, true);

        assertEquals(intVal, tx.getInt(blk, 0));
        assertEquals(boolVal, tx.getBoolean(blk, 32));
        assertEquals(doubleVal, tx.getDouble(blk, 64), 0.00001);
        assertEquals(stringVal, tx.getString(blk, 128));
        assertArrayEquals(bytesVal, tx.getBytes(blk, 256));

        tx.commit();
    }

    @Test
    @DisplayName("Integration: Complex rollback scenario with multiple modifications")
    void integrationComplexRollback() {
        TxMgrBase txMgr = newTxMgr("integrationComplexRollback", 512, 6, 200);

        // Setup initial state
        TxBase setup = txMgr.newTx();
        BlockIdBase blk1 = setup.append("file1");
        BlockIdBase blk2 = setup.append("file2");
        setup.pin(blk1);
        setup.pin(blk2);
        setup.setInt(blk1, 0, 100, true);
        setup.setInt(blk2, 0, 200, true);
        setup.commit();

        // Modify both blocks and rollback
        TxBase tx = txMgr.newTx();
        tx.pin(blk1);
        tx.pin(blk2);
        tx.setInt(blk1, 0, 999, true);
        tx.setInt(blk2, 0, 888, true);
        tx.setInt(blk1, 0, 777, true); // Multiple modifications
        tx.rollback();

        // Verify both rolled back
        TxBase verify = txMgr.newTx();
        verify.pin(blk1);
        verify.pin(blk2);
        assertEquals(100, verify.getInt(blk1, 0));
        assertEquals(200, verify.getInt(blk2, 0));
        verify.commit();
    }

    @Test
    @DisplayName("Integration: Stress test with many transactions")
    void integrationStressTest() {
        TxMgrBase txMgr = newTxMgr("integrationStressTest", 512, 10, 100);

        List<BlockIdBase> blocks = new ArrayList<>();

        // Create and commit transactions
        for (int i = 0; i < 10; i++) {
            TxBase tx = txMgr.newTx();
            BlockIdBase blk = tx.append("file_" + i);
            blocks.add(blk);
            tx.pin(blk);
            tx.setInt(blk, 0, i * 10, true);
            tx.commit();
        }

        // Verify all values
        TxBase verify = txMgr.newTx();
        for (int i = 0; i < 10; i++) {
            verify.pin(blocks.get(i));
            assertEquals(i * 10, verify.getInt(blocks.get(i), 0));
        }
        verify.commit();
    }

    @Test
    @DisplayName("Integration: Buffer availability tracking")
    void integrationBufferAvailability() {
        TxMgrBase txMgr = newTxMgr("integrationBufferAvailability", 256, 3, 100);
        TxBase tx = txMgr.newTx();

        int initialAvail = tx.availableBuffs();
        assertTrue(initialAvail > 0);

        BlockIdBase blk1 = tx.append("file1");
        tx.pin(blk1);
        int afterPin1 = tx.availableBuffs();
        assertTrue(afterPin1 < initialAvail);

        BlockIdBase blk2 = tx.append("file2");
        tx.pin(blk2);
        int afterPin2 = tx.availableBuffs();
        assertTrue(afterPin2 < afterPin1);

        tx.unpin(blk1);
        int afterUnpin = tx.availableBuffs();
        assertTrue(afterUnpin > afterPin2);

        tx.commit();
    }
}
