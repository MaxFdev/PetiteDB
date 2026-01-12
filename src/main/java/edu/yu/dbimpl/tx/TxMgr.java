package edu.yu.dbimpl.tx;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.tx.concurrency.ConcurrencyMgr.TxLock;

public class TxMgr extends TxMgrBase {

    private static final Logger logger = LogManager.getLogger(TxMgr.class);

    private final FileMgrBase fm;
    private final LogMgrBase lm;
    private final BufferMgrBase bm;
    private final long maxWaitTimeInMillis;
    private final Map<BlockIdBase, TxLock> globalLockMap;
    private final AtomicInteger txnumCounter;

    public TxMgr(FileMgrBase fm, LogMgrBase lm, BufferMgrBase bm, long maxWaitTimeInMillis) {
        super(fm, lm, bm, maxWaitTimeInMillis);

        logger.info("(Re)initialize database: {}", DBConfiguration.INSTANCE.isDBStartup());

        this.fm = fm;
        this.lm = lm;
        this.bm = bm;
        this.maxWaitTimeInMillis = maxWaitTimeInMillis;
        this.globalLockMap = new ConcurrentHashMap<>();
        this.txnumCounter = new AtomicInteger();

        // check config to see if this is a new db or existing
        boolean startup;
        try {
            startup = DBConfiguration.INSTANCE.isDBStartup();
        } catch (Exception e) {
            // throw IllegalStateException if db config can't supply info
            throw new IllegalStateException("Config couldn't supply startup info", e);
        }

        if (!startup) {
            // initiate a rollback for uncommitted transactions
            newTx().recover();
        }
    }

    @Override
    public long getMaxWaitTimeInMillis() {
        return this.maxWaitTimeInMillis;
    }

    @Override
    public TxBase newTx() {
        return new Tx(this.fm, this.bm, this, this.lm, this.txnumCounter.getAndIncrement());
    }

    @Override
    public void resetAllLockState() {
        this.globalLockMap.clear();
    }

    /**
     * Retrieve the global lock map for read/write access to blocks.
     * 
     * @return the global lock map
     */
    public Map<BlockIdBase, TxLock> getGlobalLockMap() {
        return this.globalLockMap;
    }

}
