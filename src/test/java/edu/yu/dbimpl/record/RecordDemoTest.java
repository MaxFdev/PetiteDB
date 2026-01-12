
package edu.yu.dbimpl.record;

import org.junit.jupiter.api.Test;
import java.io.File;
import java.sql.Types;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

import edu.yu.dbimpl.buffer.*;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.*;
import edu.yu.dbimpl.log.*;
import edu.yu.dbimpl.tx.*;
import static edu.yu.dbimpl.record.RecordPageBase.BEFORE_FIRST_SLOT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RecordDemoTest {

    private final static Logger logger = LogManager.getLogger(RecordDemoTest.class);
    private final static String fieldA = "A";
    private final static String fieldB = "B";
    private final static String fieldC = "C";
    private final static int logicalLengthOfFieldB = 12;
    private final static String TEST_DIR = "testing/SampleRecordModuleDemo";
    private final static String LOG_FILE = "temp_logfile";

    @Test
    public void testSampleRecordModuleDemo() {
        try {
            logger.info("Entered testSampleRecordModuleDemo");

            final Properties dbProperties = new Properties();
            dbProperties.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
            logger.info("Setting DBConfiguration properties with: {}", dbProperties);
            DBConfiguration.INSTANCE.get().setConfiguration(dbProperties);

            final File dbDirectory = new File(TEST_DIR);
            final String logFile = LOG_FILE;
            final int blockSize = 400;
            final int bufferSize = 10;
            final int maxWaitTime = 500; // ms
            final int maxTxWaitTime = 500; // ms

            final FileMgrBase fileMgr = new FileMgr(dbDirectory, blockSize);
            final LogMgrBase logMgr = new LogMgr(fileMgr, logFile);
            final BufferMgrBase bufferMgr = new BufferMgr(fileMgr, logMgr, bufferSize, maxWaitTime);
            logger.info("Created BufferMgr with a buffer size of {} and maxWaitTime " +
                    "of {}", bufferSize, maxWaitTime);
            final TxMgrBase txMgr = new TxMgr(fileMgr, logMgr, bufferMgr, maxTxWaitTime);
            final TxBase tx = txMgr.newTx();

            logger.info("Creating a schema");
            final SchemaBase schema = new Schema();
            schema.addIntField(fieldA);
            schema.addStringField(fieldB, logicalLengthOfFieldB);
            logger.info("Added int field {} and string field {}",
                    fieldA, fieldB);

            logger.info("schema fields: {}", schema.fields());
            assertTrue(schema.fields().contains(fieldA));
            assertTrue(schema.fields().contains(fieldB));
            logger.info("does schema include {}? {}", fieldA, schema.hasField(fieldA));
            assertTrue(schema.hasField(fieldA));
            logger.info("does schema include {}? {}", fieldB, schema.hasField(fieldB));
            assertTrue(schema.hasField(fieldB));
            logger.info("does schema include {}? {}", fieldC, schema.hasField(fieldC));
            assertFalse(schema.hasField(fieldC));
            logger.info("type of {}: {}", fieldA, schema.type(fieldA));
            assertEquals(Types.INTEGER, schema.type(fieldA));
            logger.info("type of {}: {}", fieldB, schema.type(fieldB));
            assertEquals(Types.VARCHAR, schema.type(fieldB));
            logger.info("length of {}: {}", fieldA, schema.length(fieldA));
            assertEquals(Integer.BYTES, schema.length(fieldA));
            logger.info("length of {}: {}", fieldB, schema.length(fieldB));
            assertTrue(schema.length(fieldB) >= logicalLengthOfFieldB);

            logger.info("Creating Layout to extend the schema");
            final LayoutBase layout = new Layout(schema);
            for (String fldname : layout.schema().fields()) {
                int offset = layout.offset(fldname);
                logger.info("Field {} has offset {}", fldname, offset);
                assertTrue(offset >= 0);
            }

            logger.info("Logical storage for field {} is {}, physical storage " +
                    "allocated is {}",
                    fieldB, logicalLengthOfFieldB,
                    PageBase.maxLength(logicalLengthOfFieldB));
            logger.info("Record slot size: {}", layout.slotSize());
            assertTrue(layout.slotSize() > 0);
            final BlockIdBase blk1 = tx.append("temp_testfile");
            tx.pin(blk1);
            final RecordPageBase recordPage1 = new RecordPage(tx, blk1, layout);
            logger.info("About to manage {} using a RecordPage", blk1);
            manipulateABlock(recordPage1);
            assertTrue(recordPage1.nextAfter(BEFORE_FIRST_SLOT) >= 0);
            tx.unpin(blk1);

            final BlockIdBase blk2 = tx.append("temp_testfile");
            tx.pin(blk2);
            final RecordPageBase recordPage2 = new RecordPage(tx, blk2, layout);
            logger.info("About to manage {} using a RecordPage", blk2);
            manipulateABlock(recordPage2);
            assertTrue(recordPage2.nextAfter(BEFORE_FIRST_SLOT) >= 0);
            tx.unpin(blk2);

            logger.info("Now showing how TableScan is a higher-level of " +
                    "abstraction than RecordPage");
            final String tableName = "T";
            final TableScanBase ts = new TableScan(tx, tableName, layout);

            final int nRecords = 50; // to ensure multiple blocks
            RID rid30 = null;

            logger.info("Filling table {} with {} records", tableName, nRecords);
            for (int i = 0; i < nRecords; i++) {
                ts.insert(); // position cursor on new, empty, record
                ts.setInt(fieldA, i);
                ts.setString(fieldB, "record" + i);
                assertEquals(i, ts.getInt(fieldA));
                assertEquals("record" + i, ts.getString(fieldB));
                if (i == 30) {
                    rid30 = ts.getRid();
                }
            } // creating records

            logger.info("Finished filling table");
            final int threshold = 30;
            logger.info("Deleting all records whose {} values are less than {}",
                    fieldA, threshold);
            int count = 0;
            ts.beforeFirst();
            while (ts.next()) {
                final int a = ts.getInt(fieldA);
                if (a < threshold) {
                    count++;
                    logger.info("Scanning record {}: int field={}", ts.getRid(), a);
                    ts.delete();
                    logger.info("Deleted the record");
                }
            } // iterating over all records
            assertEquals(threshold, count);

            logger.info("Verifying count of UNDELETED records");
            count = 0;
            ts.beforeFirst();
            while (ts.next()) {
                final int a = ts.getInt(fieldA);
                final String b = ts.getString(fieldB);
                count++;
                logger.info("Scanning record {}: '{},{}'", ts.getRid(), a, b);
                assertTrue(a >= threshold);
                assertTrue(b.startsWith("record"));
            }
            logger.info("Number of UNDELETED records: {}", count);
            assertEquals(nRecords - threshold, count);

            logger.info("Now jumpting to & evaluating record at RID: {}", rid30);
            ts.moveToRid(rid30);
            logger.info("Current RID's record: {}={} and {}={}", fieldA,
                    ts.getInt(fieldA), fieldB, ts.getString(fieldB));
            assertEquals(30, ts.getInt(fieldA));
            assertEquals("record30", ts.getString(fieldB));

            ts.close();
            tx.commit();
        } catch (Exception e) {
            logger.error("Problem: ", e);
            throw new RuntimeException(e);
        } finally {
            logger.info("Exiting testSampleRecordModuleDemo");
        }
    }

    private static void manipulateABlock(final RecordPageBase recordPage) {
        logger.info("Record page block size is {}", recordPage.block());
        logger.info("Formatting the block ...");
        recordPage.format();
        logger.info("Completed block formatting");

        logger.info("Filling the block with records consisting of an " +
                "int, String pair");
        int nRecordsInserted = 0;
        int slot = recordPage.insertAfter(BEFORE_FIRST_SLOT);
        while (slot >= 0) {
            int n = slot;
            recordPage.setInt(slot, fieldA, n);
            recordPage.setString(slot, fieldB, "record" + n);
            logger.info("inserting into slot {}: { {},record{} }",
                    slot, n, n);
            slot = recordPage.insertAfter(slot);
            nRecordsInserted++;
        }

        logger.info("Inserted {} records", nRecordsInserted);
        logger.info("Retrieving these records");

        slot = recordPage.nextAfter(BEFORE_FIRST_SLOT);
        while (slot >= 0) {
            final int a = recordPage.getInt(slot, fieldA);
            final String b = recordPage.getString(slot, fieldB);
            logger.info("slot {} : { {},{} }", slot, a, b);
            slot = recordPage.nextAfter(slot);
        }
    }

}
