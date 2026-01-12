package edu.yu.dbimpl.tx;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import edu.yu.dbimpl.buffer.BufferMgr;
import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;

/**
 * Smoke-test transferring logic from SampleTxModuleDemo into JUnit.
 * Verifies commit/rollback semantics over a sequence of transactions.
 */
public class TxProfDemoTest {

    @Test
    public void demoUsageOfTxModule() {
        logger.info("Entered main");

        final Properties dbProperties = new Properties();
        dbProperties.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        logger.info("Setting DBConfiguration properties with: {}", dbProperties);
        DBConfiguration.INSTANCE.get().setConfiguration(dbProperties);

        final String dirName = "testing/SampleTxModuleDemoTx";
        final String logFile = "temp_logfile";
        final String dbFile = "testfile";
        final File dbDirectory = new File(dirName);
        final int blockSize = 400;
        final int bufferSize = 10;
        final int maxWaitTime = 500; // ms
        final int maxTxWaitTime = 500; // ms
        final int integerPos = 80;
        final int stringPos = 200;
        final int fortyTwo = 42;
        final String notFortyTwo = "notFortyTwo";
        final BlockId block1 = new BlockId(dbFile, 1);
        final boolean okToLock = true;

        final FileMgrBase fileMgr = new FileMgr(dbDirectory, blockSize);
        final LogMgrBase logMgr = new LogMgr(fileMgr, logFile);
        final BufferMgrBase bufferMgr = new BufferMgr(fileMgr, logMgr, bufferSize, maxWaitTime);
        logger.info("Created BufferMgr with a buffer size of {} and maxWaitTime of {}", bufferSize, maxWaitTime);
        final TxMgrBase txMgr = new TxMgr(fileMgr, logMgr, bufferMgr, maxTxWaitTime);

        // tx1: initial writes, commit
        final TxBase tx1 = txMgr.newTx();
        tx1.pin(block1);
        tx1.setInt(block1, integerPos, fortyTwo, !okToLock);
        tx1.setString(block1, stringPos, notFortyTwo, !okToLock);
        printActiveState(tx1);
        tx1.commit();
        printState(tx1);
        logger.info("Committed {}", tx1);

        // tx2: verify tx1 values, then overwrite and commit
        final TxBase tx2 = txMgr.newTx();
        tx2.pin(block1);
        logger.info("Someone set blk {}:offset {} to {}", block1, integerPos, tx2.getInt(block1, integerPos));
        logger.info("Someone set blk {}:offset {} to {}", block1, stringPos, tx2.getString(block1, stringPos));
        assertEquals(fortyTwo, tx2.getInt(block1, integerPos), "tx2 should see tx1's committed int");
        assertEquals(notFortyTwo, tx2.getString(block1, stringPos), "tx2 should see tx1's committed string");
        tx2.setInt(block1, integerPos, fortyTwo + 1, okToLock);
        tx2.setString(block1, stringPos, String.valueOf(fortyTwo), okToLock);
        printActiveState(tx2);
        tx2.commit();
        logger.info("Committed {}", tx2);
        printState(tx2);

        // tx3: verify tx2 values, then overwrite and commit
        final TxBase tx3 = txMgr.newTx();
        tx3.pin(block1);
        logger.info("Someone set blk {}:offset {} to {}", block1, integerPos, tx3.getInt(block1, integerPos));
        logger.info("Someone set blk {}:offset {} to {}", block1, stringPos, tx3.getString(block1, stringPos));
        assertEquals(fortyTwo + 1, tx3.getInt(block1, integerPos), "tx3 should see tx2's committed int");
        assertEquals(String.valueOf(fortyTwo), tx3.getString(block1, stringPos),
                "tx3 should see tx2's committed string");
        tx3.setInt(block1, integerPos, fortyTwo - 10, okToLock); // 32
        tx3.setString(block1, stringPos, String.valueOf(fortyTwo - 10), okToLock); // "32"
        printActiveState(tx3);
        tx3.commit();
        logger.info("Committed {}", tx3);
        printState(tx3);

        // tx4: verify tx3 values, then modify and rollback
        final TxBase tx4 = txMgr.newTx();
        tx4.pin(block1);
        logger.info("Someone set blk {}:offset {} to {}", block1, integerPos, tx4.getInt(block1, integerPos));
        logger.info("Someone set blk {}:offset {} to {}", block1, stringPos, tx4.getString(block1, stringPos));
        assertEquals(fortyTwo - 10, tx4.getInt(block1, integerPos), "tx4 should see tx3's committed int");
        assertEquals(String.valueOf(fortyTwo - 10), tx4.getString(block1, stringPos),
                "tx4 should see tx3's committed string");
        tx4.setInt(block1, integerPos, Integer.MAX_VALUE, okToLock);
        tx4.setString(block1, stringPos, String.valueOf(Integer.MAX_VALUE), okToLock);
        printActiveState(tx4);
        tx4.rollback();
        logger.info("Rolled back {}", tx4);
        printState(tx4);

        // tx5: verify rollback of tx4 restored tx3 values
        final TxBase tx5 = txMgr.newTx();
        tx5.pin(block1);
        logger.info("Someone set blk {}:offset {} to {}", block1, integerPos, tx5.getInt(block1, integerPos));
        logger.info("Someone set blk {}:offset {} to {}", block1, stringPos, tx5.getString(block1, stringPos));
        assertEquals(fortyTwo - 10, tx5.getInt(block1, integerPos), "tx5 should NOT see tx4's rolled-back int");
        assertEquals(String.valueOf(fortyTwo - 10), tx5.getString(block1, stringPos),
                "tx5 should NOT see tx4's rolled-back string");
        printActiveState(tx5);
        tx5.rollback();
        logger.info("Rolled back {}", tx5);
        printState(tx5);

        logger.info("Exiting main");
    }

    private static void printActiveState(final TxBase tx) {
        logger.info("Tx {} reports that block size is {} and that there are {} available buffers", tx.txnum(),
                tx.blockSize(), tx.availableBuffs());
    }

    private static void printState(final TxBase tx) {
        logger.info("Status of tx {} is {}", tx.txnum(), tx.getStatus());
    }

    private static final Logger logger = LogManager.getLogger(TxProfDemoTest.class);
}
