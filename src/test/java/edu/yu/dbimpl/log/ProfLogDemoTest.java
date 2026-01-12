package edu.yu.dbimpl.log;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.Page;
import edu.yu.dbimpl.file.PageBase;

/** Moves the SampleLogModuleDemo into a JUnit test for smoke verification. */
public class ProfLogDemoTest {

    @Test
    public void demoUsageOfLogModule() {
        logger.info("Entered demoUsageOfLogModule");

        final Properties dbProperties = new Properties();
        dbProperties.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        logger.info("Setting DBConfiguration properties with: {}", dbProperties);
        DBConfiguration.INSTANCE.get().setConfiguration(dbProperties);

        final String dirName = "testing/SampleLogModuleDemo";
        final File dbDirectory = new File(dirName);
        final int blockSize = 400;
        logger.info("Test will append 2 log record and retrieve via LogIterator");

        final FileMgrBase fileMgr = new FileMgr(dbDirectory, blockSize);
        final String logFile = "temp_logfile";
        final LogMgrBase logMgr = new LogMgr(fileMgr, logFile);
        final String s = "Length7";
        final int x = 42;
        logger.info("Creating byte array containing String {} and int {}", s, x);
        logger.info("'maxLength' of this string is {}", PageBase.maxLength(s.length()));

        final int spos = 0;
        final int npos = spos + PageBase.maxLength(s.length());
        final byte[] byteArray = new byte[npos + Integer.BYTES];
        logger.info("Array length before being passed to Page ctor: {}", byteArray.length);
        final PageBase p1 = new Page(byteArray);
        logger.info("Storing string at position {}", spos);
        logger.info("Storing int at position {}", npos);
        p1.setString(spos, s);
        p1.setInt(npos, x);
        logger.info("Byte array length after being passed to Page ctor: {}", byteArray.length);
        final int lsn = logMgr.append(byteArray);
        logger.info("Appended to LogMgr, received LSN of {}", lsn);
        assertEquals(0, lsn, "First LSN should be 0 on fresh startup");

        final String s2 = "len4";
        final int x2 = 99;
        logger.info("Creating byte array containing String {} and int {}", s2, x2);
        logger.info("'maxLength' of this string is {}", PageBase.maxLength(s2.length()));

        final int spos2 = 0;
        int npos2 = spos2 + PageBase.maxLength(s2.length());
        final byte[] byteArray2 = new byte[Integer.BYTES + PageBase.maxLength(s2.length())];
        logger.info("Array length before being passed to Page ctor: {}", byteArray2.length);
        final PageBase p2 = new Page(byteArray); // intentionally uses byteArray as in demo
        logger.info("Storing string at position {}", spos2);
        logger.info("Storing int at position {}", npos2);
        p2.setString(spos2, s2);
        p2.setInt(npos2, x2);
        logger.info("Byte array length after being passed to Page ctor: {}", byteArray2.length);
        final int lsn2 = logMgr.append(byteArray); // intentionally appends byteArray as in demo
        logger.info("Appended to LogMgr, received LSN of {}", lsn2);
        assertEquals(1, lsn2, "Second LSN should be 1");

        final List<byte[]> logRecords1 = new ArrayList<byte[]>();
        logMgr.iterator().forEachRemaining(logRecords1::add);
        logger.info("# of log records: {}", logRecords1.size());
        assertEquals(2, logRecords1.size(), "Expected exactly 2 log records");

        logger.info("Creating LogMgr.iterator() and invoking next()");
        Iterator<byte[]> iter = logMgr.iterator();
        List<String> strs = new ArrayList<>();
        List<Integer> vals = new ArrayList<>();
        List<Integer> lengths = new ArrayList<>();
        while (iter.hasNext()) {
            byte[] rec = iter.next();
            lengths.add(rec.length);
            logger.info("LogIterator returned byte array of length {}", rec.length);
            logger.info("Log record size is {}", rec.length);
            final PageBase p = new Page(rec);
            logger.info("Accessing string at position 0");
            final String str = p.getString(0);
            final int npos3 = PageBase.maxLength(str.length());
            logger.info("Accessing int at position {}", npos3);
            final int val = p.getInt(npos3);
            logger.info("Deserialized log record into: [" + str + ", " + val + "]");
            strs.add(str);
            vals.add(val);
        }

        // Assertions based on expected output ordering and sizes
        assertEquals(2, strs.size(), "Iterator should return two records");
        assertEquals("len4", strs.get(0), "Most recent record should have string 'len4'");
        assertEquals(99, vals.get(0), "Most recent record should have int 99");
        assertEquals("Length7", strs.get(1), "Older record should have string 'Length7'");
        assertEquals(42, vals.get(1), "Older record should have int 42");
        // Both byte arrays are length 15 in the demo output (second reuses first array)
        assertEquals(2, lengths.size());
        assertEquals(15, lengths.get(0));
        assertEquals(15, lengths.get(1));

        logger.info("Exiting demoUsageOfLogModule");
    }

    private static final Logger logger = LogManager.getLogger(ProfLogDemoTest.class);
}
