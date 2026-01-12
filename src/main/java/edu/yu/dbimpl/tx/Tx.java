package edu.yu.dbimpl.tx;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import edu.yu.dbimpl.buffer.BufferBase;
import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.tx.concurrency.ConcurrencyMgr;
import edu.yu.dbimpl.tx.recovery.LogRecordImpl;
import edu.yu.dbimpl.tx.recovery.RecoveryMgr;

public class Tx implements TxBase {

    private final FileMgrBase fileMgr;
    private final BufferMgrBase bufferMgr;
    private final LogMgrBase logMgr;
    private final ConcurrencyMgr concurrencyMgr;
    private final RecoveryMgr recoveryMgr;
    private final int txnum;
    private final Map<BlockIdBase, BufferTracker> bufferMap;
    private Status status;

    public Tx(FileMgrBase fileMgr, BufferMgrBase bufferMgr, TxMgrBase txMgr, LogMgrBase logMgr, int txnum) {
        this.fileMgr = fileMgr;
        this.bufferMgr = bufferMgr;
        this.logMgr = logMgr;
        this.concurrencyMgr = new ConcurrencyMgr(txMgr, txnum);
        this.recoveryMgr = new RecoveryMgr(this, logMgr, bufferMgr);
        this.txnum = txnum;
        this.bufferMap = new ConcurrentHashMap<>();
        this.status = Status.ACTIVE;

        // log tx start
        logMgr.append(new LogRecordImpl(LogRecordImpl.TX_START, txnum).serialize());
    }

    @Override
    public Status getStatus() {
        return this.status;
    }

    @Override
    public int txnum() {
        return this.txnum;
    }

    @Override
    public void commit() {
        if (this.status != Status.ACTIVE) {
            throw new IllegalStateException("Can't commit if tx is not active");
        }

        this.status = Status.COMMITTING;

        // undo-only commit model
        this.bufferMgr.flushAll(this.txnum);
        this.recoveryMgr.commit();
        this.concurrencyMgr.release();
        for (Map.Entry<BlockIdBase, BufferTracker> entry : this.bufferMap.entrySet()) {
            // check if the buffer needs to be unpinned
            if (entry.getKey().equals(entry.getValue().buffer.block())) {
                this.bufferMgr.unpin(entry.getValue().buffer);
            }
        }
        this.bufferMap.clear();

        this.status = Status.COMMITTED;
    }

    @Override
    public void rollback() {
        if (this.status != Status.ACTIVE) {
            throw new IllegalStateException("Can't rollback if tx is not active");
        }

        this.status = Status.ROLLING_BACK;

        // undo this tx's
        Iterator<byte[]> logBytes = this.logMgr.iterator();
        while (logBytes.hasNext()) {
            LogRecordImpl log = new LogRecordImpl(logBytes.next());
            log.undo(this);
            if (log.op() == LogRecordImpl.TX_START && log.txNumber() == this.txnum) {
                break;
            }
        }

        this.bufferMgr.flushAll(this.txnum);
        this.recoveryMgr.rollback();
        this.concurrencyMgr.release();
        for (Map.Entry<BlockIdBase, BufferTracker> entry : this.bufferMap.entrySet()) {
            // check if the buffer needs to be unpinned
            if (entry.getKey().equals(entry.getValue().buffer.block())) {
                this.bufferMgr.unpin(entry.getValue().buffer);
            }
        }
        this.bufferMap.clear();

        this.status = Status.ROLLED_BACK;
    }

    @Override
    public void recover() {
        if (this.status != Status.ACTIVE) {
            throw new IllegalStateException("Can't recover if tx is not active");
        }

        this.status = Status.RECOVERING;

        // go through the log and undo any uncommitted records
        Set<Integer> stableTxs = new HashSet<>();
        Iterator<byte[]> logBytes = this.logMgr.iterator();
        while (logBytes.hasNext()) {
            LogRecordImpl log = new LogRecordImpl(logBytes.next());
            if (log.op() == LogRecordImpl.CHECKPOINT) {
                break;
            } else if (log.op() == LogRecordImpl.TX_COMMIT || log.op() == LogRecordImpl.TX_ROLLBACK) {
                stableTxs.add(log.txNumber());
            } else if (!stableTxs.contains(log.txNumber())) {
                log.undo(this);
            }
        }

        // flush the recovery
        this.bufferMgr.flushAll(this.txnum);

        // write checkpoint
        this.recoveryMgr.recover();

        this.status = Status.RECOVERED;
    }

    /**
     * Helper method to enable logs undo functionality.
     * 
     * @return the buffer map
     */
    public Map<BlockIdBase, BufferTracker> getBuffersMap() {
        return this.bufferMap;
    }

    /**
     * Retrieve the buffer manager for rollback and recovery.
     * 
     * @return the buffer manager
     */
    public BufferMgrBase getBufferMgr() {
        return this.bufferMgr;
    }

    @Override
    public void pin(BlockIdBase blk) {
        if (blk == null) {
            throw new IllegalArgumentException("Block can't be null");
        }

        if (this.status != Status.ACTIVE) {
            throw new IllegalStateException("Can't pin while tx is not active");
        }

        BufferTracker bufferTracker = this.bufferMap.get(blk);
        if (bufferTracker == null || !bufferTracker.buffer.block().equals(blk)) {
            this.bufferMap.put(blk, new BufferTracker(this.bufferMgr.pin(blk)));
        } else {
            bufferTracker.txPins.getAndIncrement();
        }
    }

    @Override
    public void unpin(BlockIdBase blk) {
        if (this.status != Status.ACTIVE) {
            throw new IllegalStateException("Can't unpin while tx is not active");
        }

        if (blk == null) {
            throw new IllegalArgumentException("Block can't be null");
        }

        BufferTracker bufferTracker = this.bufferMap.get(blk);
        if (bufferTracker == null || !blk.equals(bufferTracker.buffer.block())) {
            throw new IllegalArgumentException("Can't unpin a block which the tx did not pin");
        }

        if (bufferTracker.txPins.decrementAndGet() == 0) {
            this.bufferMgr.unpin(bufferTracker.buffer);
            this.bufferMap.remove(blk);
        }
    }

    /**
     * Helper method for acquiring an sLock.
     * 
     * @param blk
     * @return the buffer that the lock was acquired for
     */
    private BufferBase sLock(BlockIdBase blk) {
        if (blk == null) {
            throw new IllegalArgumentException("Block can't be null");
        }

        BufferTracker bufferTracker = this.bufferMap.get(blk);
        if (bufferTracker == null || !bufferTracker.buffer.block().equals(blk)) {
            throw new IllegalStateException("Block must be pinned to operate on");
        }

        this.concurrencyMgr.sLock(blk);
        return bufferTracker.buffer;
    }

    @Override
    public int getInt(BlockIdBase blk, int offset) {
        return sLock(blk).contents().getInt(offset);
    }

    @Override
    public boolean getBoolean(BlockIdBase blk, int offset) {
        return sLock(blk).contents().getBoolean(offset);
    }

    @Override
    public double getDouble(BlockIdBase blk, int offset) {
        return sLock(blk).contents().getDouble(offset);
    }

    @Override
    public String getString(BlockIdBase blk, int offset) {
        return sLock(blk).contents().getString(offset);
    }

    @Override
    public byte[] getBytes(BlockIdBase blk, int offset) {
        return sLock(blk).contents().getBytes(offset);
    }

    /**
     * Helper method for acquiring an xLock.
     * 
     * @param blk
     * @return the buffer that the lock was acquired for
     */
    private BufferBase xLock(BlockIdBase blk) {
        if (blk == null) {
            throw new IllegalArgumentException("Block can't be null");
        }

        BufferTracker bufferTracker = this.bufferMap.get(blk);
        if (bufferTracker == null || !bufferTracker.buffer.block().equals(blk)) {
            throw new IllegalStateException("Block must be pinned to operate on");
        }

        this.concurrencyMgr.xLock(blk);
        return bufferTracker.buffer;
    }

    @Override
    public void setInt(BlockIdBase blk, int offset, int val, boolean okToLog) {
        BufferBase buffer = xLock(blk);

        // check if the operation should be logged
        int lsn;
        if (okToLog) {
            // log the operation and get the lsn
            lsn = this.recoveryMgr.setInt(buffer, offset, val);
        } else {
            // set the lsn to an non-loggable value
            lsn = -1;
        }

        buffer.contents().setInt(offset, val);
        buffer.setModified(this.txnum, lsn);
    }

    @Override
    public void setBoolean(BlockIdBase blk, int offset, boolean val, boolean okToLog) {
        BufferBase buffer = xLock(blk);

        // check if the operation should be logged
        int lsn;
        if (okToLog) {
            // log the operation and get the lsn
            lsn = this.recoveryMgr.setBoolean(buffer, offset, val);
        } else {
            // set the lsn to an non-loggable value
            lsn = -1;
        }

        buffer.contents().setBoolean(offset, val);
        buffer.setModified(this.txnum, lsn);
    }

    @Override
    public void setDouble(BlockIdBase blk, int offset, double val, boolean okToLog) {
        BufferBase buffer = xLock(blk);

        // check if the operation should be logged
        int lsn;
        if (okToLog) {
            // log the operation and get the lsn
            lsn = this.recoveryMgr.setDouble(buffer, offset, val);
        } else {
            // set the lsn to an non-loggable value
            lsn = -1;
        }

        buffer.contents().setDouble(offset, val);
        buffer.setModified(this.txnum, lsn);
    }

    @Override
    public void setString(BlockIdBase blk, int offset, String val, boolean okToLog) {
        BufferBase buffer = xLock(blk);

        // check if the operation should be logged
        int lsn;
        if (okToLog) {
            // log the operation and get the lsn
            lsn = this.recoveryMgr.setString(buffer, offset, val);
        } else {
            // set the lsn to an non-loggable value
            lsn = -1;
        }

        buffer.contents().setString(offset, val);
        buffer.setModified(this.txnum, lsn);
    }

    @Override
    public void setBytes(BlockIdBase blk, int offset, byte[] val, boolean okToLog) {
        BufferBase buffer = xLock(blk);

        // check if the operation should be logged
        int lsn;
        if (okToLog) {
            // log the operation and get the lsn
            lsn = this.recoveryMgr.setBytes(buffer, offset, val);
        } else {
            // set the lsn to an non-loggable value
            lsn = -1;
        }

        buffer.contents().setBytes(offset, val);
        buffer.setModified(this.txnum, lsn);
    }

    @Override
    public int size(String filename) {
        if (this.status != Status.ACTIVE) {
            throw new IllegalStateException("Can't read file size while tx is not active");
        }

        return this.fileMgr.length(filename);
    }

    @Override
    public BlockIdBase append(String filename) {
        if (this.status != Status.ACTIVE) {
            throw new IllegalStateException("Can't append block to file while tx is not active");
        }

        return this.fileMgr.append(filename);
    }

    @Override
    public int blockSize() {
        return this.fileMgr.blockSize();
    }

    @Override
    public int availableBuffs() {
        if (this.status != Status.ACTIVE) {
            throw new IllegalArgumentException("Can't check buffer availability while tx is not active");
        }

        return this.bufferMgr.available();
    }

    @Override
    public String toString() {
        StringBuilder stb = new StringBuilder();
        stb.append("Tx: [txnum: ")
                .append(this.txnum)
                .append(", status: ")
                .append(this.status)
                .append("]");
        return stb.toString();
    }

    @Override
    public int hashCode() {
        return this.txnum;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof Tx) ? ((Tx) o).txnum == this.txnum : false;
    }

    /**
     * A helper class for keep track of how many pins a Tx assigns to a given
     * buffer.
     */
    public final static class BufferTracker {
        public final BufferBase buffer;
        public final AtomicInteger txPins;

        BufferTracker(BufferBase buffer) {
            this.buffer = buffer;
            this.txPins = new AtomicInteger(1);
        }
    }
}
