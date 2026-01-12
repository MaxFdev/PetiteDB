package edu.yu.dbimpl.file;

import java.io.File;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.yu.dbimpl.config.DBConfiguration;

public class ProfDemoTest {
    @Test
    public void Test() {
        try {
            logger.info("Entered main");

            final Properties dbProperties = new Properties();
            dbProperties.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
            logger.info("Setting DBConfiguration properties with: {}", dbProperties);
            DBConfiguration.INSTANCE.get().setConfiguration(dbProperties);

            final String dirName = "testing/SampleFileModuleDemo";
            final File dbDirectory = new File(dirName);
            final int blockSize = 400;
            final String stringData = "abcdefghijklm";
            final int intData = 345;
            int stringDataPos = 88;

            logger.info("Creating a FileMgr with blockSize of {}", blockSize);
            final FileMgrBase fm = new FileMgr(dbDirectory, blockSize);
            final BlockIdBase blk = new BlockId("testfile", 2);
            logger.info("Created a BlockId: {}", blk);

            final PageBase p1 = new Page(fm.blockSize());
            logger.info("Created a Page: {}", p1);

            logger.info("Setting String {} at position {}", stringData, stringDataPos);
            p1.setString(stringDataPos, stringData);
            final int size = PageBase.maxLength(stringData.length());

            final int intDataPos = stringDataPos + size;
            logger.info("Setting int {} at position {}", intData, intDataPos);
            p1.setInt(intDataPos, intData);

            logger.info("Writing the page to disk");
            fm.write(blk, p1);

            final PageBase p2 = new Page(fm.blockSize());
            logger.info("Created a second page into which the data will be read");
            logger.info("Reading the block's data back into the second page");
            fm.read(blk, p2);

            logger.info("Retrieved the string: {}", p2.getString(stringDataPos));
            logger.info("Retrieved the int: {}", p2.getInt(intDataPos));

            final Properties dbProperties2 = new Properties();
            dbProperties2.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
            logger.info("Setting DBConfiguration properties with: {}", dbProperties2);
            DBConfiguration.INSTANCE.get().setConfiguration(dbProperties2);
            logger.info("Creating a NEW FileMgr instance with blockSize of {}", blockSize);
            final FileMgrBase fm2 = new FileMgr(dbDirectory, blockSize);

            logger.info("Hmm ... was the first file mgr's data persisted?");
            final PageBase p3 = new Page(fm2.blockSize());
            logger.info("Created a third page into which the data will be read");
            logger.info("Reading the block's data back into the third page");
            fm2.read(blk, p3);
            logger.info("Retrieved the string: {}", p3.getString(stringDataPos));
            logger.info("Retrieved the int: {}", p3.getInt(intDataPos));

            final Properties dbProperties3 = new Properties();
            dbProperties3.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
            logger.info("Setting DBConfiguration properties with: {}", dbProperties3);
            logger.info("Creating a third file mgr, should reinitialize the system");
            DBConfiguration.INSTANCE.get().setConfiguration(dbProperties3);
            logger.info("Creating a NEW FileMgr instance with blockSize of {}", blockSize);
            final FileMgrBase fm3 = new FileMgr(dbDirectory, blockSize);

            logger.info("Hmm ... the first file mgr's data should have been reinitialized?");
            final PageBase p4 = new Page(fm3.blockSize());
            logger.info("Created a fourth page into which the data will be read");
            logger.info("Reading the block's data back into the fourth page");
            fm3.read(blk, p4);
            logger.info("Retrieved the string: {}", p4.getString(stringDataPos));
            logger.info("Retrieved the int: {}", p4.getInt(intDataPos));

        } catch (Exception e) {
            logger.error("Problem: ", e);
        } finally {
            logger.info("Exiting main");
        }
    }

    private final static Logger logger = LogManager.getLogger(ProfDemoTest.class);

}
