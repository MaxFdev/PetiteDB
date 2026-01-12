package edu.yu.dbimpl.index;

import java.io.File;
import java.util.Properties;
import java.util.Set;

import edu.yu.dbimpl.buffer.*;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.*;
import edu.yu.dbimpl.log.*;
import edu.yu.dbimpl.metadata.TableMgr;
import edu.yu.dbimpl.metadata.TableMgrBase;
import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.query.Datum;
import edu.yu.dbimpl.tx.*;
import edu.yu.dbimpl.record.*;
import static edu.yu.dbimpl.index.IndexMgrBase.IndexType;
import static edu.yu.dbimpl.index.IndexMgrBase.IndexType.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

public class IndexDemoTest {

    private final static Logger logger = LogManager.getLogger(IndexDemoTest.class);

    private final static String fieldA = "A";
    private final static String fieldB = "B";
    private final static String fieldC = "C";
    private final static int logicalLengthOfFieldB = 12;

    @Test
    public void testIndexModuleDemo() {
        try {
            logger.info("Entered main");

            final Properties dbProperties = new Properties();
            dbProperties.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
            logger.info("Setting DBConfiguration properties with: {}", dbProperties);
            DBConfiguration.INSTANCE.get().setConfiguration(dbProperties);

            final String dirName = "testing/SampleIndexModuleDemo";
            final String logFile = "temp_logfile";
            final String dbFile = "testfile";
            final File dbDirectory = new File(dirName);
            final int blockSize = 400;
            final int bufferSize = 1_000;
            final int maxWaitTime = 500; // ms
            final int maxTxWaitTime = 500; // ms

            final FileMgrBase fileMgr = new FileMgr(dbDirectory, blockSize);
            final LogMgrBase logMgr = new LogMgr(fileMgr, logFile);
            final BufferMgrBase bufferMgr = new BufferMgr(fileMgr, logMgr, bufferSize, maxWaitTime);
            logger.info("Created BufferMgr with a buffer size of {} and maxWaitTime " +
                    "of {}", bufferSize, maxWaitTime);
            final TxMgrBase txMgr = new TxMgr(fileMgr, logMgr, bufferMgr, maxTxWaitTime);
            final TxBase tx = txMgr.newTx();

            final String tableName = "students";

            final TableMgrBase tableMgr = new TableMgr(tx);
            final IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            final String studentFieldNameName = "name";
            final String studentFieldNameGPA = "gpa";

            final SchemaBase schema = new Schema();
            schema.addStringField(studentFieldNameName, 20);
            schema.addDoubleField(studentFieldNameGPA);
            final LayoutBase layout = tableMgr.createTable(tableName, schema, tx);
            logger.info("Created table {} metadata with schema {}", tableName, schema);

            final TableScanBase ts = new TableScan(tx, tableName, layout);
            logger.info("Created a TableScan on table {}", tableName);

            // final String studentNameIndex = "studentName";
            final IndexMgr.IndexType indexType = STATIC_HASH;
            final int nameIndexId = indexMgr
                    .persistIndexDescriptor(tx, tableName,
                            studentFieldNameName, indexType);
            logger.info("IndexMgr.persistIndexDescriptor({},{},{}) returned {}",
                    tableName, studentFieldNameName, indexType,
                    nameIndexId);
            final Set<Integer> ids = indexMgr.indexIds(tx, tableName);
            logger.info("IndexMgr.indexIds returned the following index ids: {}", ids);

            final IndexDescriptorBase studentNameDescriptor = indexMgr.get(tx, nameIndexId);
            logger.info("IndexMgr.get({}) returned {}", nameIndexId, studentNameDescriptor);

            final IndexBase nameIndex = indexMgr.instantiate(tx, nameIndexId);

            final int nRecords = 10; // to ensure multiple blocks
            logger.info("Creating {} records in table {} AND creating corresponding " +
                    "index records", nRecords, tableName);
            for (int i = 0; i < nRecords; i++) {
                ts.insert(); // position cursor on new, empty, record
                final String name = studentFieldNameName + "_" + String.valueOf(i);
                final double gpa = Double.valueOf(i % 4) + 0.3;
                ts.setString(studentFieldNameName, name);
                ts.setDouble(studentFieldNameGPA, gpa);
                logger.info("Created a student record with fields: {}, {}",
                        name, gpa);

                final RID rid = ts.getRid();

                final DatumBase nameValue = new Datum(name);
                nameIndex.insert(nameValue, rid);
                logger.info("Inserted an 'name' index record: {},{}", nameValue, rid);

            }

            ts.close();
            nameIndex.close();
            tx.commit();
            logger.info("Closed the scan, committed the transaction");

            final TxBase tx2 = txMgr.newTx();
            final DatumBase name7 = new Datum(studentFieldNameName + "_" + String.valueOf(7));
            final TableScanBase ts2 = new TableScan(tx2, tableName, layout);
            final IndexBase nameIndex2 = indexMgr.instantiate(tx2, nameIndexId);

            logger.info("Using this search key to retrieve a student: {}", name7);
            nameIndex2.beforeFirst(name7);

            while (nameIndex2.next()) {
                final RID rid = nameIndex2.getRID();
                logger.info("Moving TableScan to RID {}", rid);
                ts2.moveToRid(rid);
                final double retrievedGPA = ts2.getDouble(studentFieldNameGPA);
                logger.info("The student gpa field for {} is {}",
                        name7, retrievedGPA);
            }

            nameIndex2.close();
            tx2.commit();
        } catch (Exception e) {
            logger.error("Problem: ", e);
            throw new RuntimeException(e);
        } finally {
            logger.info("Exiting main");
        }
    }
}
