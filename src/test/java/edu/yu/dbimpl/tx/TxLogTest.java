package edu.yu.dbimpl.tx;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Properties;

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
import edu.yu.dbimpl.tx.recovery.LogRecordImpl;

/**
 * Flattened tests for LogRecordImpl functionality: serialization,
 * deserialization,
 * undo semantics, unknown op, formatting, and negative construction scenarios.
 */
public class TxLogTest {
    private static final int INT_OFFSET = 0;
    private static final int BOOL_OFFSET = 32;
    private static final int DOUBLE_OFFSET = 64;
    private static final int STRING_OFFSET = 96;
    private static final int BYTES_OFFSET = 160;

    private TxMgrBase newTxMgr(String dir) {
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);
        File d = new File(dir);
        d.mkdirs();
        FileMgrBase fm = new FileMgr(d, 256);
        LogMgrBase lm = new LogMgr(fm, "lrTestLog");
        BufferMgrBase bm = new BufferMgr(fm, lm, 8, 500);
        return new TxMgr(fm, lm, bm, 500);
    }

    @Test
    @DisplayName("Checkpoint serialize/deserialize")
    void checkpointRecord() {
        LogRecordImpl rec = new LogRecordImpl(LogRecordImpl.CHECKPOINT);
        byte[] ser = rec.serialize();
        assertEquals(1, ser.length);
        LogRecordImpl back = new LogRecordImpl(ser);
        assertEquals(LogRecordImpl.CHECKPOINT, back.op());
        assertEquals(-1, back.txNumber());
    }

    @Test
    @DisplayName("TX start/commit/rollback serialize/deserialize")
    void txLifecycleRecords() {
        int txnum = 17;
        for (int op : new int[] { LogRecordImpl.TX_START, LogRecordImpl.TX_COMMIT, LogRecordImpl.TX_ROLLBACK }) {
            LogRecordImpl rec = new LogRecordImpl(op, txnum);
            byte[] ser = rec.serialize();
            assertEquals(1 + Integer.BYTES, ser.length);
            LogRecordImpl back = new LogRecordImpl(ser);
            assertEquals(op, back.op());
            assertEquals(txnum, back.txNumber());
        }
    }

    @Test
    @DisplayName("SET_INT serialize round-trip equality")
    void setIntRoundTrip() {
        BlockIdBase blk = new BlockId("fileA", 3);
        LogRecordImpl rec = new LogRecordImpl(LogRecordImpl.SET_INT, 9, blk, INT_OFFSET, 777, 111);
        byte[] ser = rec.serialize();
        LogRecordImpl back = new LogRecordImpl(ser);
        assertEquals(LogRecordImpl.SET_INT, back.op());
        assertEquals(9, back.txNumber());
        assertArrayEquals(ser, back.serialize());
    }

    @Test
    @DisplayName("SET_BOOLEAN serialize/deserialize")
    void setBooleanRoundTrip() {
        BlockIdBase blk = new BlockId("fileB", 0);
        LogRecordImpl rec = new LogRecordImpl(LogRecordImpl.SET_BOOLEAN, 2, blk, BOOL_OFFSET, true, false);
        byte[] ser = rec.serialize();
        LogRecordImpl back = new LogRecordImpl(ser);
        assertEquals(LogRecordImpl.SET_BOOLEAN, back.op());
        assertEquals(2, back.txNumber());
    }

    @Test
    @DisplayName("SET_DOUBLE serialize/deserialize")
    void setDoubleRoundTrip() {
        BlockIdBase blk = new BlockId("fileC", 1);
        LogRecordImpl rec = new LogRecordImpl(LogRecordImpl.SET_DOUBLE, 5, blk, DOUBLE_OFFSET, 9.99, -0.5);
        byte[] ser = rec.serialize();
        LogRecordImpl back = new LogRecordImpl(ser);
        assertEquals(LogRecordImpl.SET_DOUBLE, back.op());
        assertEquals(5, back.txNumber());
    }

    @Test
    @DisplayName("SET_STRING serialize empty and non-empty re-serialize equality")
    void setStringRoundTrip() {
        BlockIdBase blk = new BlockId("fileD", 2);
        LogRecordImpl rec = new LogRecordImpl(LogRecordImpl.SET_STRING, 6, blk, STRING_OFFSET, "new", "old");
        byte[] ser = rec.serialize();
        LogRecordImpl back = new LogRecordImpl(ser);
        assertArrayEquals(ser, back.serialize());

        LogRecordImpl emptyStrRec = new LogRecordImpl(LogRecordImpl.SET_STRING, 6, blk, STRING_OFFSET, "", "x");
        byte[] serEmpty = emptyStrRec.serialize();
        LogRecordImpl backEmpty = new LogRecordImpl(serEmpty);
        assertArrayEquals(serEmpty, backEmpty.serialize());
    }

    @Test
    @DisplayName("SET_BYTES serialize zero-length and filled re-serialize equality")
    void setBytesRoundTrip() {
        BlockIdBase blk = new BlockId("fileE", 4);
        byte[] newVal = new byte[0];
        byte[] oldVal = new byte[] { 1, 2, 3 };
        LogRecordImpl rec = new LogRecordImpl(LogRecordImpl.SET_BYTES, 7, blk, BYTES_OFFSET, newVal, oldVal);
        byte[] ser = rec.serialize();
        LogRecordImpl back = new LogRecordImpl(ser);
        assertArrayEquals(ser, back.serialize());
    }

    @Test
    @DisplayName("Unknown op deserialization minimal state")
    void unknownOpMinimal() {
        byte[] raw = new byte[] { (byte) 99 };
        LogRecordImpl rec = new LogRecordImpl(raw);
        assertEquals(99, rec.op());
        assertEquals("<INCOMPLETE_LOG>", rec.toString());
        byte[] ser = rec.serialize();
        assertEquals(1, ser.length);
        assertEquals((byte) 99, ser[0]);
    }

    @Test
    @DisplayName("Undo restores old INT value when txnum matches")
    void undoInt() {
        TxMgrBase txMgr = newTxMgr("testing/undoInt");
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("undoFileInt");
        tx.pin(blk);
        tx.setInt(blk, INT_OFFSET, 999, false);
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_INT, tx.txnum(), blk, INT_OFFSET, 111, 222);
        log.undo(tx);
        assertEquals(222, tx.getInt(blk, INT_OFFSET));
        tx.commit();
    }

    @Test
    @DisplayName("Undo skip when txnum mismatches and status ACTIVE")
    void undoSkipMismatchedTxnum() {
        TxMgrBase txMgr = newTxMgr("testing/undoSkipMismatch");
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("undoFileSkip");
        tx.pin(blk);
        tx.setBoolean(blk, BOOL_OFFSET, true, false);
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_BOOLEAN, tx.txnum() + 1, blk, BOOL_OFFSET, false, true);
        log.undo(tx);
        assertTrue(tx.getBoolean(blk, BOOL_OFFSET));
        tx.commit();
    }

    @Test
    @DisplayName("Undo proceeds during RECOVERING despite txnum mismatch")
    void undoDuringRecoveringMismatchedTxnum() throws Exception {
        TxMgrBase txMgr = newTxMgr("testing/undoRecoverMismatch");
        Tx tx = (Tx) txMgr.newTx();
        BlockIdBase blk = tx.append("undoFileRec");
        tx.pin(blk);
        tx.setDouble(blk, DOUBLE_OFFSET, 3.14, false);
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_DOUBLE, tx.txnum() + 99, blk, DOUBLE_OFFSET, 1.23,
                4.56);
        Field status = Tx.class.getDeclaredField("status");
        status.setAccessible(true);
        status.set(tx, TxBase.Status.RECOVERING);
        log.undo(tx);
        assertEquals(4.56, tx.getDouble(blk, DOUBLE_OFFSET));
    }

    @Test
    @DisplayName("Undo temporary pin/unpin when block not in map")
    void undoTempPin() {
        TxMgrBase txMgr = newTxMgr("testing/undoTempPin");
        Tx tx = (Tx) txMgr.newTx();
        BlockIdBase blk = tx.append("undoFileTemp");
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_STRING, tx.txnum(), blk, STRING_OFFSET, "new", "old");
        assertThrows(IllegalStateException.class, () -> tx.getString(blk, STRING_OFFSET));
        log.undo(tx);
        tx.pin(blk);
        assertEquals("old", tx.getString(blk, STRING_OFFSET));
        tx.commit();
    }

    @Test
    void toStringCheckpoint() {
        assertEquals("<CHECKPOINT>", new LogRecordImpl(LogRecordImpl.CHECKPOINT).toString());
    }

    @Test
    void toStringTxStart() {
        assertTrue(new LogRecordImpl(LogRecordImpl.TX_START, 5).toString().startsWith("<START, 5"));
    }

    @Test
    void toStringSetInt() {
        assertTrue(new LogRecordImpl(LogRecordImpl.SET_INT, 3, new BlockId("f", 0), INT_OFFSET, 10, 1).toString()
                .contains("SET_INT"));
    }

    @Test
    @DisplayName("Serialize with wrong value types causes ClassCastException")
    void wrongTypes() {
        LogRecordImpl rec = new LogRecordImpl(LogRecordImpl.SET_INT, 1, new BlockId("g", 1), INT_OFFSET, "notInt", 5);
        assertThrows(ClassCastException.class, rec::serialize);
    }

    // --- Additional Coverage Tests ---

    @Test
    @DisplayName("TX commit and rollback serialize/deserialize + toString")
    void txCommitRollbackRecords() {
        int txnum = 42;
        LogRecordImpl commit = new LogRecordImpl(LogRecordImpl.TX_COMMIT, txnum);
        LogRecordImpl rollback = new LogRecordImpl(LogRecordImpl.TX_ROLLBACK, txnum);
        byte[] cSer = commit.serialize();
        byte[] rSer = rollback.serialize();
        assertEquals(1 + Integer.BYTES, cSer.length);
        assertEquals(1 + Integer.BYTES, rSer.length);
        assertTrue(commit.toString().startsWith("<COMMIT, 42"));
        assertTrue(rollback.toString().startsWith("<ROLLBACK, 42"));
        LogRecordImpl cBack = new LogRecordImpl(cSer);
        LogRecordImpl rBack = new LogRecordImpl(rSer);
        assertEquals(LogRecordImpl.TX_COMMIT, cBack.op());
        assertEquals(LogRecordImpl.TX_ROLLBACK, rBack.op());
    }

    // Helpers for expected serialized length of SET_* records
    private int expectedSetLength(String filename, int txnum, int offset, Object oldVal, Object newVal, int op) {
        int len = 1 + Integer.BYTES; // op + txnum
        byte[] fnameBytes = filename.getBytes(edu.yu.dbimpl.file.Page.CHARSET);
        len += Integer.BYTES + fnameBytes.length; // filename length prefix + bytes
        len += Integer.BYTES; // block number
        len += Integer.BYTES; // offset
        len += valueSize(op, oldVal);
        len += valueSize(op, newVal);
        return len;
    }

    private int valueSize(int op, Object v) {
        return switch (op) {
            case LogRecordImpl.SET_INT -> Integer.BYTES;
            case LogRecordImpl.SET_BOOLEAN -> 1;
            case LogRecordImpl.SET_DOUBLE -> Double.BYTES;
            case LogRecordImpl.SET_STRING ->
                Integer.BYTES + ((String) v).getBytes(edu.yu.dbimpl.file.Page.CHARSET).length;
            case LogRecordImpl.SET_BYTES -> Integer.BYTES + ((byte[]) v).length;
            default -> 0;
        };
    }

    @Test
    @DisplayName("Serialized length matches specification for each SET_* op")
    void serializedLengthSetOps() {
        BlockIdBase blk = new BlockId("lenFile", 9);
        LogRecordImpl iRec = new LogRecordImpl(LogRecordImpl.SET_INT, 3, blk, INT_OFFSET, 10, -5);
        LogRecordImpl bRec = new LogRecordImpl(LogRecordImpl.SET_BOOLEAN, 3, blk, BOOL_OFFSET, true, false);
        LogRecordImpl dRec = new LogRecordImpl(LogRecordImpl.SET_DOUBLE, 3, blk, DOUBLE_OFFSET, 6.28, -1.0);
        LogRecordImpl sRec = new LogRecordImpl(LogRecordImpl.SET_STRING, 3, blk, STRING_OFFSET, "abc", "XYZ");
        LogRecordImpl eStrRec = new LogRecordImpl(LogRecordImpl.SET_STRING, 3, blk, STRING_OFFSET, "", "p");
        byte[] bytesVal = new byte[] { 9, 8, 7, 6 };
        byte[] oldBytesVal = new byte[] { 1 };
        LogRecordImpl byRec = new LogRecordImpl(LogRecordImpl.SET_BYTES, 3, blk, BYTES_OFFSET, bytesVal, oldBytesVal);

        assertEquals(expectedSetLength("lenFile", 3, INT_OFFSET, -5, 10, LogRecordImpl.SET_INT),
                iRec.serialize().length);
        assertEquals(expectedSetLength("lenFile", 3, BOOL_OFFSET, false, true, LogRecordImpl.SET_BOOLEAN),
                bRec.serialize().length);
        assertEquals(expectedSetLength("lenFile", 3, DOUBLE_OFFSET, -1.0, 6.28, LogRecordImpl.SET_DOUBLE),
                dRec.serialize().length);
        assertEquals(expectedSetLength("lenFile", 3, STRING_OFFSET, "XYZ", "abc", LogRecordImpl.SET_STRING),
                sRec.serialize().length);
        assertEquals(expectedSetLength("lenFile", 3, STRING_OFFSET, "p", "", LogRecordImpl.SET_STRING),
                eStrRec.serialize().length);
        assertEquals(expectedSetLength("lenFile", 3, BYTES_OFFSET, oldBytesVal, bytesVal, LogRecordImpl.SET_BYTES),
                byRec.serialize().length);
    }

    @Test
    @DisplayName("Undo restores old BOOLEAN value")
    void undoBoolean() {
        TxMgrBase txMgr = newTxMgr("testing/undoBool");
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("undoFileBool");
        tx.pin(blk);
        tx.setBoolean(blk, BOOL_OFFSET, true, false);
        // Constructor signature is (op, txnum, block, offset, newVal, oldVal). Old
        // value before setBoolean was false.
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_BOOLEAN, tx.txnum(), blk, BOOL_OFFSET, true, false);
        log.undo(tx);
        assertFalse(tx.getBoolean(blk, BOOL_OFFSET));
        tx.commit();
    }

    @Test
    @DisplayName("Undo restores old DOUBLE including NaN")
    void undoDoubleNaN() {
        TxMgrBase txMgr = newTxMgr("testing/undoDoubleNaN");
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("undoFileDoubleNaN");
        tx.pin(blk);
        tx.setDouble(blk, DOUBLE_OFFSET, 1.11, false);
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_DOUBLE, tx.txnum(), blk, DOUBLE_OFFSET, 2.22,
                Double.NaN);
        log.undo(tx);
        assertTrue(Double.isNaN(tx.getDouble(blk, DOUBLE_OFFSET)));
        tx.commit();
    }

    @Test
    @DisplayName("Undo restores old STRING value")
    void undoString() {
        TxMgrBase txMgr = newTxMgr("testing/undoString");
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("undoFileString");
        tx.pin(blk);
        tx.setString(blk, STRING_OFFSET, "temp", false);
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_STRING, tx.txnum(), blk, STRING_OFFSET, "newer",
                "prev");
        log.undo(tx);
        assertEquals("prev", tx.getString(blk, STRING_OFFSET));
        tx.commit();
    }

    @Test
    @DisplayName("Undo restores old BYTES value including zero-length newVal")
    void undoBytes() {
        TxMgrBase txMgr = newTxMgr("testing/undoBytes");
        TxBase tx = txMgr.newTx();
        BlockIdBase blk = tx.append("undoFileBytes");
        tx.pin(blk);
        tx.setBytes(blk, BYTES_OFFSET, new byte[] { 5, 5 }, false);
        byte[] oldBytes = new byte[] { 1, 2, 3, 4 };
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.SET_BYTES, tx.txnum(), blk, BYTES_OFFSET, new byte[0],
                oldBytes);
        log.undo(tx);
        assertArrayEquals(oldBytes, tx.getBytes(blk, BYTES_OFFSET));
        tx.commit();
    }

    @Test
    @DisplayName("Undo null Tx guard")
    void undoNullTxThrows() {
        LogRecordImpl rec = new LogRecordImpl(LogRecordImpl.SET_INT, 1, new BlockId("guard", 0), INT_OFFSET, 5, 10);
        assertThrows(IllegalArgumentException.class, () -> rec.undo(null));
    }

    @Test
    @DisplayName("Undo wrong Tx type guard")
    void undoWrongTxTypeThrows() {
        LogRecordImpl rec = new LogRecordImpl(LogRecordImpl.SET_INT, 1, new BlockId("guard2", 0), INT_OFFSET, 5, 10);
        TxBase dummy = new TxBase() {
            @Override
            public Status getStatus() {
                return Status.ACTIVE;
            }

            @Override
            public int txnum() {
                return 999;
            }

            @Override
            public void commit() {
            }

            @Override
            public void rollback() {
            }

            @Override
            public void recover() {
            }

            @Override
            public void pin(BlockIdBase blk) {
            }

            @Override
            public void unpin(BlockIdBase blk) {
            }

            @Override
            public int getInt(BlockIdBase blk, int offset) {
                return 0;
            }

            @Override
            public boolean getBoolean(BlockIdBase blk, int offset) {
                return false;
            }

            @Override
            public double getDouble(BlockIdBase blk, int offset) {
                return 0;
            }

            @Override
            public String getString(BlockIdBase blk, int offset) {
                return "";
            }

            @Override
            public byte[] getBytes(BlockIdBase blk, int offset) {
                return new byte[0];
            }

            @Override
            public void setInt(BlockIdBase blk, int offset, int val, boolean okToLog) {
            }

            @Override
            public void setBoolean(BlockIdBase blk, int offset, boolean val, boolean okToLog) {
            }

            @Override
            public void setDouble(BlockIdBase blk, int offset, double val, boolean okToLog) {
            }

            @Override
            public void setString(BlockIdBase blk, int offset, String val, boolean okToLog) {
            }

            @Override
            public void setBytes(BlockIdBase blk, int offset, byte[] val, boolean okToLog) {
            }

            @Override
            public int size(String filename) {
                return 0;
            }

            @Override
            public BlockIdBase append(String filename) {
                return new BlockId(filename, 0);
            }

            @Override
            public int blockSize() {
                return 256;
            }

            @Override
            public int availableBuffs() {
                return 0;
            }
        };
        assertThrows(IllegalArgumentException.class, () -> rec.undo(dummy));
    }
}
