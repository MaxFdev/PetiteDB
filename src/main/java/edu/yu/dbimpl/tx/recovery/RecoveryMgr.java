package edu.yu.dbimpl.tx.recovery;

import edu.yu.dbimpl.buffer.BufferBase;
import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.tx.TxBase;

public class RecoveryMgr extends RecoveryMgrBase {

    private TxBase tx;
    private LogMgrBase logMgr;

    public RecoveryMgr(TxBase tx, LogMgrBase logMgr, BufferMgrBase bufferMgr) {
        super(tx, logMgr, bufferMgr);

        this.tx = tx;
        this.logMgr = logMgr;
    }

    @Override
    public void commit() {
        // create commit log
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.TX_COMMIT, this.tx.txnum());

        // serialize and append log
        int lsn = this.logMgr.append(log.serialize());

        // flush all logs up to the commit
        this.logMgr.flush(lsn);
    }

    @Override
    public void rollback() {
        // create rollback log
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.TX_ROLLBACK, this.tx.txnum());

        // serialize and append log
        int lsn = this.logMgr.append(log.serialize());

        // flush all logs up to the commit
        this.logMgr.flush(lsn);
    }

    @Override
    public void recover() {
        // create commit log
        LogRecordImpl log = new LogRecordImpl(LogRecordImpl.CHECKPOINT, this.tx.txnum());

        // serialize and append log
        int lsn = this.logMgr.append(log.serialize());

        // flush all logs up to the commit
        this.logMgr.flush(lsn);
    }

    @Override
    public int setInt(BufferBase buff, int offset, int newval) {
        // create a new log
        LogRecordImpl log = new LogRecordImpl(
                LogRecordImpl.SET_INT,
                this.tx.txnum(),
                buff.block(),
                offset,
                newval,
                buff.contents().getInt(offset));

        // serialize and append the log
        return this.logMgr.append(log.serialize());
    }

    @Override
    public int setBoolean(BufferBase buff, int offset, boolean newval) {
        // create a new log
        LogRecordImpl log = new LogRecordImpl(
                LogRecordImpl.SET_BOOLEAN,
                this.tx.txnum(),
                buff.block(),
                offset,
                newval,
                buff.contents().getBoolean(offset));

        // serialize and append the log
        return this.logMgr.append(log.serialize());
    }

    @Override
    public int setDouble(BufferBase buff, int offset, double newval) {
        // create a new log
        LogRecordImpl log = new LogRecordImpl(
                LogRecordImpl.SET_DOUBLE,
                this.tx.txnum(),
                buff.block(),
                offset,
                newval,
                buff.contents().getDouble(offset));

        // serialize and append the log
        return this.logMgr.append(log.serialize());
    }

    @Override
    public int setString(BufferBase buff, int offset, String newval) {
        // create a new log
        LogRecordImpl log = new LogRecordImpl(
                LogRecordImpl.SET_STRING,
                this.tx.txnum(),
                buff.block(),
                offset,
                newval,
                buff.contents().getString(offset));

        // serialize and append the log
        return this.logMgr.append(log.serialize());
    }

    @Override
    public int setBytes(BufferBase buff, int offset, byte[] newval) {
        // create a new log
        LogRecordImpl log = new LogRecordImpl(
                LogRecordImpl.SET_BYTES,
                this.tx.txnum(),
                buff.block(),
                offset,
                newval,
                buff.contents().getBytes(offset));

        // serialize and append the log
        return this.logMgr.append(log.serialize());
    }

}
