package edu.yu.dbimpl.buffer;

import java.io.File;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.PageBase;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;

public class ProfDemoTest {
    private final static Logger logger = LogManager.getLogger(ProfDemoTest.class);

    @Test
    public void demoTest() {
        try {
            logger.info("Entered main");

            final Properties dbProperties = new Properties();
            dbProperties.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
            logger.info("Setting DBConfiguration properties with: {}", dbProperties);
            DBConfiguration.INSTANCE.get().setConfiguration(dbProperties);

            final String dirName = "testing/SampleBufferModuleDemo";
            final String logFile = "temp_logfile";
            final String dbFile = "testfile";
            final File dbDirectory = new File(dirName);
            final int blockSize = 400;
            final int bufferSize = 3;
            final int maxWaitTime = 500; // ms
            final int integerPos = 80;
            final int fortyTwo = 42;
            final int placeHolderTxn = 1;
            final int placeHolderLSN = -1;

            final FileMgrBase fileMgr = new FileMgr(dbDirectory, blockSize);
            final LogMgrBase logMgr = new LogMgr(fileMgr, logFile);
            final BufferMgrBase bufferMgr = new BufferMgr(fileMgr, logMgr, bufferSize, maxWaitTime);
            logger.info("Created BufferMgr with a buffer size of {} and maxWaitTime " +
                    "of {}", bufferSize, maxWaitTime);

            final BlockId block1 = new BlockId(dbFile, 1);
            final BufferBase buffer1 = bufferMgr.pin(block1);
            final PageBase p = buffer1.contents();
            logger.info("Buffer wraps disk block {} and currently associated with " +
                    "main-memory Page {}", buffer1.block(), buffer1.contents());
            int n = p.getInt(integerPos);
            logger.info("Current 'integer' value at position {}: {}", integerPos, n);
            logger.info("Setting int value at position {} to {}", integerPos, n + 1);
            p.setInt(integerPos, n + 1);

            buffer1.setModified(placeHolderTxn, placeHolderLSN);
            logger.info("Is the buffer pinned? {}", buffer1.isPinned());
            logger.info("Current 'integer' value at position {}: {}",
                    integerPos, buffer1.contents().getInt(integerPos));
            logger.info("Number of available buffers: {}", bufferMgr.available());
            bufferMgr.unpin(buffer1);
            logger.info("After unpinning buffer, number of available buffers: {}",
                    bufferMgr.available());

            logger.info("Note: buffer has NOT been explicitly flushed to disk");

            logger.info("Creating & pinning 3 new Buffers to wrap blocks 2, 3, 4: " +
                    "one of these pins should implicitly flush buffer 1 to disk");

            BufferBase buffer2 = bufferMgr.pin(new BlockId(dbFile, 2));
            BufferBase buffer3 = bufferMgr.pin(new BlockId(dbFile, 3));
            BufferBase buffer4 = bufferMgr.pin(new BlockId(dbFile, 4));
            logger.info("Created {}, {}, {}: ", buffer2.block(), buffer3.block(),
                    buffer4.block());

            logger.info("Unpinning {} and creating another buffer to wrap disk block {}",
                    buffer2, block1);
            bufferMgr.unpin(buffer2);
            final BufferBase buffer5 = bufferMgr.pin(block1);
            PageBase p2 = buffer2.contents();
            logger.info("Current 'integer' value at position {}: {}",
                    integerPos, p2.getInt(integerPos));
            logger.info("Setting int value at position {} to {}", integerPos, fortyTwo);
            p2.setInt(integerPos, fortyTwo);
            buffer5.setModified(placeHolderTxn, placeHolderLSN);
            logger.info("Set modified bit on buffer, but NOT expecting to seeing new " +
                    "value flushed to disk: DIDN'T invoke BufferMgr.flushAll()");

            final Properties dbProperties2 = new Properties();
            dbProperties2.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
            logger.info("Setting DBConfiguration properties with: {}", dbProperties2);
            DBConfiguration.INSTANCE.get().setConfiguration(dbProperties2);
            logger.info("Creating NEW {File,Log,Buffer} Mgr instances");

            final FileMgrBase fileMgr2 = new FileMgr(dbDirectory, blockSize);
            final LogMgrBase logMgr2 = new LogMgr(fileMgr2, logFile);
            final BufferMgrBase bufferMgr2 = new BufferMgr(fileMgr2, logMgr2, bufferSize, maxWaitTime);
            logger.info("Created BufferMgr with a buffer size of {} and maxWaitTime " +
                    "of {}", bufferSize, maxWaitTime);

            final BufferBase buffer1b = bufferMgr2.pin(block1);
            final PageBase p3 = buffer1b.contents();
            logger.info("Buffer wraps disk block {} and currently associated with " +
                    "main-memory Page {}", buffer1b.block(), buffer1b.contents());

            n = p3.getInt(integerPos);
            logger.info("Nu ... what is the integer value stored at position {}? {}",
                    integerPos, n);

        } catch (Exception e) {
            logger.error("Problem: ", e);
            throw new RuntimeException(e);
        } finally {
            logger.info("Exiting main");
        }
    }
}
