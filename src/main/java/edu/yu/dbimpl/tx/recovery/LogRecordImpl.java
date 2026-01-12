package edu.yu.dbimpl.tx.recovery;

import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.Page;
import edu.yu.dbimpl.buffer.BufferBase;
import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.tx.Tx;
import edu.yu.dbimpl.tx.TxBase;
import edu.yu.dbimpl.tx.Tx.BufferTracker;
import edu.yu.dbimpl.tx.TxBase.Status;

import java.nio.ByteBuffer;
import java.util.Map;

public class LogRecordImpl implements LogRecord {

    public static final int CHECKPOINT = 0;
    public static final int TX_START = 1;
    public static final int TX_COMMIT = 2;
    public static final int TX_ROLLBACK = 3;
    public static final int SET_INT = 4;
    public static final int SET_BOOLEAN = 5;
    public static final int SET_DOUBLE = 6;
    public static final int SET_STRING = 7;
    public static final int SET_BYTES = 8;

    private final int op;
    private final int txnum;
    private final BlockIdBase block;
    private final int offset;
    private final Object oldVal;
    private final Object newVal;

    public LogRecordImpl(int op) {
        this.op = op;
        this.txnum = -1;
        this.block = null;
        this.offset = -1;
        this.oldVal = null;
        this.newVal = null;
    }

    public LogRecordImpl(int op, int txnum) {
        this.op = op;
        this.txnum = txnum;
        this.block = null;
        this.offset = -1;
        this.oldVal = null;
        this.newVal = null;
    }

    public LogRecordImpl(int op, int txnum, BlockIdBase block, int offset, Object newVal, Object oldVal) {
        this.op = op;
        this.txnum = txnum;
        this.block = block;
        this.offset = offset;
        this.oldVal = oldVal;
        this.newVal = newVal;
    }

    public LogRecordImpl(byte[] serialLog) {
        ByteBuffer buf = ByteBuffer.wrap(serialLog);
        int opVal = buf.get() & 0xFF;

        if (opVal == CHECKPOINT) {
            this.op = opVal;
            this.txnum = -1;
            this.block = null;
            this.offset = -1;
            this.oldVal = null;
            this.newVal = null;
            return;
        }

        if (opVal == TX_START || opVal == TX_COMMIT || opVal == TX_ROLLBACK) {
            this.op = opVal;
            this.txnum = buf.getInt();
            this.block = null;
            this.offset = -1;
            this.oldVal = null;
            this.newVal = null;
            return;
        }

        if (opVal >= SET_INT && opVal <= SET_BYTES) {
            int txnum = buf.getInt();
            String filename = getString(buf);
            int blknum = buf.getInt();
            int offset = buf.getInt();
            Object oldVal = readValue(opVal, buf);
            Object newVal = readValue(opVal, buf);

            this.op = opVal;
            this.txnum = txnum;
            this.block = new BlockId(filename, blknum);
            this.offset = offset;
            this.oldVal = oldVal;
            this.newVal = newVal;
            return;
        }

        // Unknown op: keep minimal state
        this.op = opVal;
        this.txnum = -1;
        this.block = null;
        this.offset = -1;
        this.oldVal = null;
        this.newVal = null;
    }

    @Override
    public int op() {
        return this.op;
    }

    @Override
    public int txNumber() {
        return this.txnum;
    }

    @Override
    public void undo(TxBase tx) {
        if (tx == null) {
            throw new IllegalArgumentException("Tx cannot be null");
        }

        if (!(tx instanceof Tx)) {
            throw new IllegalArgumentException("Tx must be of type Tx");
        }

        // only undo logs with matching txnum unless its a recovery tx
        if (this.txnum != tx.txnum() && tx.getStatus() != Status.RECOVERING) {
            return;
        }

        // only undo set operations belonging
        if (this.op < SET_INT || this.op > SET_BYTES) {
            return;
        }

        Tx concreteTx = (Tx) tx;
        Map<BlockIdBase, BufferTracker> buffMap = concreteTx.getBuffersMap();
        BufferTracker bufferTracker = buffMap.get(this.block);
        BufferBase buffer = bufferTracker == null ? null : bufferTracker.buffer;
        boolean tempPin = buffer == null;
        if (tempPin) {
            // pin the block temporarily
            buffer = concreteTx.getBufferMgr().pin(block);
        }

        // Restore old value using tx setters with logging disabled.
        switch (this.op) {
            case SET_INT -> buffer.contents().setInt(this.offset, (Integer) this.oldVal);
            case SET_BOOLEAN -> buffer.contents().setBoolean(this.offset, (Boolean) this.oldVal);
            case SET_DOUBLE -> buffer.contents().setDouble(this.offset, (Double) this.oldVal);
            case SET_STRING -> buffer.contents().setString(this.offset, (String) this.oldVal);
            case SET_BYTES -> buffer.contents().setBytes(this.offset, (byte[]) this.oldVal);
        }

        // set the buffer as modified
        buffer.setModified(concreteTx.txnum(), -1);

        if (tempPin) {
            // unpin the buffer
            concreteTx.getBufferMgr().unpin(buffer);
        }
    }

    /**
     * Create a serial version of the log record as a byte array.
     * Checkpoint:
     * [op: byte]
     * Start, commit, rollback:
     * [op: byte, txnum: int]
     * Set ops:
     * [op: byte, txnum: int, filename: string, block: int,
     * offset: int, oldVal: object, newVal: object]
     * 
     * @return
     */
    public byte[] serialize() {
        int size = 1; // op as a single byte

        if (this.op == CHECKPOINT) {
            ByteBuffer buf = ByteBuffer.allocate(size);
            buf.put((byte) this.op);
            return buf.array();
        }

        // TX_START, TX_COMMIT, TX_ROLLBACK have fixed size: op + txnum
        if (this.op == TX_START || this.op == TX_COMMIT || this.op == TX_ROLLBACK) {
            size += Integer.BYTES; // txnum
            ByteBuffer buf = ByteBuffer.allocate(size);
            buf.put((byte) this.op);
            buf.putInt(this.txnum);
            return buf.array();
        }

        // SET_* records: op, txnum, filename, blockNo, offset, oldVal, newVal
        if (this.op >= SET_INT && this.op <= SET_BYTES) {
            size += Integer.BYTES; // txnum

            // filename (length-prefixed UTF-8)
            String filename = this.block.fileName();
            size += sizeOfString(filename);

            // block number and offset
            size += Integer.BYTES; // block number
            size += Integer.BYTES; // offset

            // old and new values (typed based on op)
            size += sizeOfValue(this.op, this.oldVal);
            size += sizeOfValue(this.op, this.newVal);

            ByteBuffer buf = ByteBuffer.allocate(size);
            buf.put((byte) this.op);
            buf.putInt(this.txnum);
            putString(buf, filename);
            buf.putInt(this.block.number());
            buf.putInt(this.offset);
            putValue(buf, this.op, this.oldVal);
            putValue(buf, this.op, this.newVal);
            return buf.array();
        }

        // Fallback: only op if unknown/incomplete
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) this.op);
        return buf.array();
    }

    private static int sizeOfString(String s) {
        byte[] bytes = s.getBytes(Page.CHARSET);
        return Integer.BYTES + bytes.length; // length prefix + bytes
    }

    private static void putString(ByteBuffer buf, String s) {
        byte[] bytes = s.getBytes(Page.CHARSET);
        buf.putInt(bytes.length);
        buf.put(bytes);
    }

    private static String getString(ByteBuffer buf) {
        int len = buf.getInt();
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, Page.CHARSET);
    }

    private static int sizeOfBytes(byte[] b) {
        return Integer.BYTES + b.length; // length prefix + bytes
    }

    private static void putBytes(ByteBuffer buf, byte[] b) {
        buf.putInt(b.length);
        buf.put(b);
    }

    private static byte[] getBytes(ByteBuffer buf) {
        int len = buf.getInt();
        byte[] b = new byte[len];
        buf.get(b);
        return b;
    }

    private static int sizeOfValue(int op, Object v) {
        switch (op) {
            case SET_INT:
                return Integer.BYTES;
            case SET_BOOLEAN:
                return 1;
            case SET_DOUBLE:
                return Double.BYTES;
            case SET_STRING:
                return sizeOfString((String) v);
            case SET_BYTES:
                return sizeOfBytes((byte[]) v);
            default:
                return 0;
        }
    }

    private static void putValue(ByteBuffer buf, int op, Object v) {
        switch (op) {
            case SET_INT:
                buf.putInt((Integer) v);
                break;
            case SET_BOOLEAN:
                buf.put((byte) ((Boolean) v ? 1 : 0));
                break;
            case SET_DOUBLE:
                buf.putDouble((Double) v);
                break;
            case SET_STRING:
                putString(buf, (String) v);
                break;
            case SET_BYTES:
                putBytes(buf, (byte[]) v);
                break;
            default:
                break;
        }
    }

    private static Object readValue(int op, ByteBuffer buf) {
        switch (op) {
            case SET_INT:
                return buf.getInt();
            case SET_BOOLEAN:
                return buf.get() != 0;
            case SET_DOUBLE:
                return buf.getDouble();
            case SET_STRING:
                return getString(buf);
            case SET_BYTES:
                return getBytes(buf);
            default:
                return null;
        }
    }

    /**
     * Get the string value for the operation.
     * 
     * @return string representation of the operation in the log
     */
    private String getOperation() {
        switch (this.op) {
            case CHECKPOINT:
                return "CHECKPOINT";
            case TX_START:
                return "START";
            case TX_COMMIT:
                return "COMMIT";
            case TX_ROLLBACK:
                return "ROLLBACK";
            case SET_INT:
                return "SET_INT";
            case SET_BOOLEAN:
                return "SET_BOOLEAN";
            case SET_DOUBLE:
                return "SET_DOUBLE";
            case SET_STRING:
                return "SET_STRING";
            case SET_BYTES:
                return "SET_BYTES";
            default:
                return "INCOMPLETE_LOG";
        }
    }

    @Override
    public String toString() {
        StringBuilder stb = new StringBuilder();
        stb.append("<").append(getOperation());

        if (this.op >= TX_START && this.op <= SET_BYTES) {
            stb.append(", ").append(this.txnum);
        }

        if (this.op >= SET_INT && this.op <= SET_BYTES) {
            stb.append(this.block.fileName()).append(", ")
                    .append(this.block.number()).append(", ")
                    .append(this.offset).append(", ")
                    .append(this.oldVal).append(", ")
                    .append(this.newVal);
        }

        stb.append(">");
        return stb.toString();
    }

}
