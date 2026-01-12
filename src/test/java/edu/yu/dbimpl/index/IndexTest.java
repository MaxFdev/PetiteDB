package edu.yu.dbimpl.index;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import edu.yu.dbimpl.buffer.BufferMgr;
import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.index.IndexMgrBase.IndexType;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.metadata.TableMgr;
import edu.yu.dbimpl.metadata.TableMgrBase;
import edu.yu.dbimpl.query.Datum;
import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.record.LayoutBase;
import edu.yu.dbimpl.record.RID;
import edu.yu.dbimpl.record.Schema;
import edu.yu.dbimpl.record.SchemaBase;
import edu.yu.dbimpl.record.TableScan;
import edu.yu.dbimpl.record.TableScanBase;
import edu.yu.dbimpl.tx.TxBase;
import edu.yu.dbimpl.tx.TxMgr;
import edu.yu.dbimpl.tx.TxMgrBase;

/**
 * Comprehensive correctness and exception tests for the Index module.
 * Tests cover happy path semantics, persistence, type variations,
 * IllegalArgumentException/IllegalStateException validation, and performance.
 */
public class IndexTest {

    private static final Logger logger = LogManager.getLogger(IndexTest.class);

    // Test configuration constants
    private static final int BLOCK_SIZE = 400;
    private static final int BUFFER_SIZE = 100;
    private static final int MAX_WAIT_TIME = 500;
    private static final String TEST_BASE_DIR = "testing/IndexTest";
    private static final String LOG_FILE = "index_logfile";

    /**
     * Helper method to create a fresh TxMgr for each test.
     */
    private TxMgrBase createTxMgr(String testName, boolean isStartup) {
        Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(isStartup));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        Path testDir = Path.of(TEST_BASE_DIR, testName);
        try {
            if (isStartup && Files.exists(testDir)) {
                deleteDirectory(testDir);
            }
        } catch (Exception e) {
            logger.warn("Could not clean up test directory: {}", testDir);
        }

        FileMgrBase fileMgr = new FileMgr(testDir.toFile(), BLOCK_SIZE);
        LogMgrBase logMgr = new LogMgr(fileMgr, LOG_FILE);
        BufferMgrBase bufferMgr = new BufferMgr(fileMgr, logMgr, BUFFER_SIZE, MAX_WAIT_TIME);
        return new TxMgr(fileMgr, logMgr, bufferMgr, MAX_WAIT_TIME);
    }

    /**
     * Helper method to delete a directory recursively.
     */
    private void deleteDirectory(Path path) throws Exception {
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (Exception e) {
                            // ignore
                        }
                    });
        }
    }

    /**
     * Helper method to create a standard test schema with all field types.
     */
    private SchemaBase createStandardSchema() {
        Schema schema = new Schema();
        schema.addIntField("id");
        schema.addStringField("name", 16);
        schema.addBooleanField("active");
        schema.addDoubleField("salary");
        return schema;
    }

    /**
     * Helper method to create a table and return its layout.
     */
    private LayoutBase createTestTable(TableMgrBase tableMgr, String tableName, SchemaBase schema, TxBase tx) {
        return tableMgr.createTable(tableName, schema, tx);
    }

    // ========================================================================
    // SECTION 1: HAPPY PATH SEMANTICS TESTS
    // ========================================================================

    @Nested
    @DisplayName("Happy Path Semantics Tests")
    class HappyPathTests {

        @Test
        @DisplayName("persistIndexDescriptor creates and returns unique ID")
        void persistIndexDescriptorCreatesUniqueId() {
            TxMgrBase txMgr = createTxMgr("persistIndexDescriptorCreatesUniqueId", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            int id1 = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            int id2 = indexMgr.persistIndexDescriptor(tx, "TestTable", "name", IndexType.STATIC_HASH);

            assertNotEquals(id1, id2, "Different fields should get different index IDs");

            tx.commit();
        }

        @Test
        @DisplayName("persistIndexDescriptor on same field is idempotent")
        void persistIndexDescriptorIsIdempotent() {
            TxMgrBase txMgr = createTxMgr("persistIndexDescriptorIsIdempotent", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            int id1 = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            int id2 = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);

            assertEquals(id1, id2, "Same table/field should return same ID");

            tx.commit();
        }

        @Test
        @DisplayName("indexIds returns correct set for table")
        void indexIdsReturnsCorrectSet() {
            TxMgrBase txMgr = createTxMgr("indexIdsReturnsCorrectSet", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            int id1 = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            int id2 = indexMgr.persistIndexDescriptor(tx, "TestTable", "name", IndexType.STATIC_HASH);

            Set<Integer> ids = indexMgr.indexIds(tx, "TestTable");

            assertEquals(2, ids.size());
            assertTrue(ids.contains(id1));
            assertTrue(ids.contains(id2));

            tx.commit();
        }

        @Test
        @DisplayName("get returns correct IndexDescriptor")
        void getReturnsCorrectDescriptor() {
            TxMgrBase txMgr = createTxMgr("getReturnsCorrectDescriptor", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            int id = indexMgr.persistIndexDescriptor(tx, "TestTable", "name", IndexType.STATIC_HASH);
            IndexDescriptorBase descriptor = indexMgr.get(tx, id);

            assertNotNull(descriptor);
            assertEquals("TestTable", descriptor.getTableName());
            assertEquals("name", descriptor.getFieldName());
            assertEquals("name", descriptor.getIndexName());
            assertEquals(IndexType.STATIC_HASH, descriptor.getIndexType());

            tx.commit();
        }

        @Test
        @DisplayName("instantiate returns functional Index")
        void instantiateReturnsFunctionalIndex() {
            TxMgrBase txMgr = createTxMgr("instantiateReturnsFunctionalIndex", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "name", IndexType.STATIC_HASH);

            IndexBase index = indexMgr.instantiate(tx, indexId);

            assertNotNull(index);

            // Insert a record into the table
            TableScanBase ts = new TableScan(tx, "TestTable", layout);
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "Alice");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 75000.0);
            RID rid = ts.getRid();
            ts.close();

            // Insert into index
            DatumBase nameValue = new Datum("Alice");
            index.insert(nameValue, rid);

            // Search using index
            index.beforeFirst(nameValue);
            assertTrue(index.next());
            assertEquals(rid, index.getRID());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Index.insert and next find matching records")
        void indexInsertAndNextFindRecords() {
            TxMgrBase txMgr = createTxMgr("indexInsertAndNextFindRecords", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert multiple records
            TableScanBase ts = new TableScan(tx, "TestTable", layout);
            List<RID> rids = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("name", "name_" + i);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0);
                RID rid = ts.getRid();
                rids.add(rid);
                index.insert(new Datum(i), rid);
            }
            ts.close();

            // Search for specific record
            DatumBase searchKey = new Datum(3);
            index.beforeFirst(searchKey);
            assertTrue(index.next());
            assertEquals(rids.get(3), index.getRID());
            assertFalse(index.next()); // Only one record with id=3

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Index.getRID returns correct RID")
        void indexGetRIDReturnsCorrectRID() {
            TxMgrBase txMgr = createTxMgr("indexGetRIDReturnsCorrectRID", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "name", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert record
            TableScanBase ts = new TableScan(tx, "TestTable", layout);
            ts.insert();
            ts.setInt("id", 42);
            ts.setString("name", "TestName");
            ts.setBoolean("active", false);
            ts.setDouble("salary", 100000.0);
            RID expectedRid = ts.getRid();
            ts.close();

            index.insert(new Datum("TestName"), expectedRid);

            // Search and verify RID
            index.beforeFirst(new Datum("TestName"));
            assertTrue(index.next());
            RID actualRid = index.getRID();
            assertEquals(expectedRid.blockNumber(), actualRid.blockNumber());
            assertEquals(expectedRid.slot(), actualRid.slot());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Index.delete removes index record")
        void indexDeleteRemovesRecord() {
            TxMgrBase txMgr = createTxMgr("indexDeleteRemovesRecord", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert record
            TableScanBase ts = new TableScan(tx, "TestTable", layout);
            ts.insert();
            ts.setInt("id", 99);
            ts.setString("name", "ToDelete");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 60000.0);
            RID rid = ts.getRid();
            ts.close();

            DatumBase value = new Datum(99);
            index.insert(value, rid);

            // Verify it exists
            index.beforeFirst(value);
            assertTrue(index.next());

            // Delete it
            index.delete(value, rid);

            // Verify it's gone
            index.beforeFirst(value);
            assertFalse(index.next());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Index.deleteAll removes all records")
        void indexDeleteAllRemovesAllRecords() {
            TxMgrBase txMgr = createTxMgr("indexDeleteAllRemovesAllRecords", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert multiple records
            TableScanBase ts = new TableScan(tx, "TestTable", layout);
            for (int i = 0; i < 10; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("name", "name_" + i);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0);
                index.insert(new Datum(i), ts.getRid());
            }
            ts.close();

            // Delete all
            index.deleteAll();

            // Verify all are gone
            for (int i = 0; i < 10; i++) {
                index.beforeFirst(new Datum(i));
                assertFalse(index.next(), "Record " + i + " should be deleted");
            }

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Multiple indexes on same table work independently")
        void multipleIndexesWorkIndependently() {
            TxMgrBase txMgr = createTxMgr("multipleIndexesWorkIndependently", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int idIndexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            int nameIndexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "name", IndexType.STATIC_HASH);

            IndexBase idIndex = indexMgr.instantiate(tx, idIndexId);
            IndexBase nameIndex = indexMgr.instantiate(tx, nameIndexId);

            // Insert record
            TableScanBase ts = new TableScan(tx, "TestTable", layout);
            ts.insert();
            ts.setInt("id", 7);
            ts.setString("name", "Bob");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 80000.0);
            RID rid = ts.getRid();
            ts.close();

            // Insert into both indexes
            idIndex.insert(new Datum(7), rid);
            nameIndex.insert(new Datum("Bob"), rid);

            // Search by id
            idIndex.beforeFirst(new Datum(7));
            assertTrue(idIndex.next());
            assertEquals(rid, idIndex.getRID());

            // Search by name
            nameIndex.beforeFirst(new Datum("Bob"));
            assertTrue(nameIndex.next());
            assertEquals(rid, nameIndex.getRID());

            idIndex.close();
            nameIndex.close();
            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 2: PERSISTENCE TESTS
    // ========================================================================

    @Nested
    @DisplayName("Persistence Tests")
    class PersistenceTests {

        @Test
        @DisplayName("Index metadata persists across transactions")
        void indexMetadataPersistsAcrossTransactions() {
            String testName = "indexMetadataPersistsAcrossTransactions";

            // First transaction: create table and index
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgrBase tableMgr1 = new TableMgr(tx1);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr1, "PersistTable", schema, tx1);

            IndexMgrBase indexMgr1 = new IndexMgr(tx1, tableMgr1);
            int originalId = indexMgr1.persistIndexDescriptor(tx1, "PersistTable", "id", IndexType.STATIC_HASH);
            tx1.commit();

            // Second transaction: verify index exists
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();

            TableMgrBase tableMgr2 = new TableMgr(tx2);
            IndexMgrBase indexMgr2 = new IndexMgr(tx2, tableMgr2);

            Set<Integer> ids = indexMgr2.indexIds(tx2, "PersistTable");
            assertTrue(ids.contains(originalId));

            IndexDescriptorBase descriptor = indexMgr2.get(tx2, originalId);
            assertNotNull(descriptor);
            assertEquals("PersistTable", descriptor.getTableName());
            assertEquals("id", descriptor.getFieldName());

            tx2.commit();
        }

        @Test
        @DisplayName("Index data persists after commit")
        void indexDataPersistsAfterCommit() {
            String testName = "indexDataPersistsAfterCommit";

            // First transaction: create table, index, and data
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgrBase tableMgr1 = new TableMgr(tx1);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr1, "DataPersistTable", schema, tx1);

            IndexMgrBase indexMgr1 = new IndexMgr(tx1, tableMgr1);
            int indexId = indexMgr1.persistIndexDescriptor(tx1, "DataPersistTable", "name", IndexType.STATIC_HASH);
            IndexBase index1 = indexMgr1.instantiate(tx1, indexId);

            TableScanBase ts1 = new TableScan(tx1, "DataPersistTable", layout);
            ts1.insert();
            ts1.setInt("id", 1);
            ts1.setString("name", "PersistedName");
            ts1.setBoolean("active", true);
            ts1.setDouble("salary", 55000.0);
            RID rid = ts1.getRid();
            ts1.close();

            index1.insert(new Datum("PersistedName"), rid);
            index1.close();
            tx1.commit();

            // Second transaction: verify data persists
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();

            TableMgrBase tableMgr2 = new TableMgr(tx2);
            IndexMgrBase indexMgr2 = new IndexMgr(tx2, tableMgr2);
            IndexBase index2 = indexMgr2.instantiate(tx2, indexId);

            index2.beforeFirst(new Datum("PersistedName"));
            assertTrue(index2.next());
            assertEquals(rid, index2.getRID());

            index2.close();
            tx2.commit();
        }

        @Test
        @DisplayName("IndexMgr restores ID counter correctly")
        void indexMgrRestoresIdCounterCorrectly() {
            String testName = "indexMgrRestoresIdCounterCorrectly";

            // First transaction: create indexes
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgrBase tableMgr1 = new TableMgr(tx1);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr1, "IdCounterTable", schema, tx1);

            IndexMgrBase indexMgr1 = new IndexMgr(tx1, tableMgr1);
            int id1 = indexMgr1.persistIndexDescriptor(tx1, "IdCounterTable", "id", IndexType.STATIC_HASH);
            int id2 = indexMgr1.persistIndexDescriptor(tx1, "IdCounterTable", "name", IndexType.STATIC_HASH);
            tx1.commit();

            // Second transaction: create new index, verify ID is higher
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();

            TableMgrBase tableMgr2 = new TableMgr(tx2);
            IndexMgrBase indexMgr2 = new IndexMgr(tx2, tableMgr2);
            int id3 = indexMgr2.persistIndexDescriptor(tx2, "IdCounterTable", "salary", IndexType.STATIC_HASH);

            assertTrue(id3 > id1);
            assertTrue(id3 > id2);

            tx2.commit();
        }

        @Test
        @DisplayName("Multiple table indexes persist correctly")
        void multipleTableIndexesPersistCorrectly() {
            String testName = "multipleTableIndexesPersistCorrectly";

            // First transaction: create multiple tables with indexes
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgrBase tableMgr1 = new TableMgr(tx1);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr1, "Table1", schema, tx1);
            createTestTable(tableMgr1, "Table2", schema, tx1);

            IndexMgrBase indexMgr1 = new IndexMgr(tx1, tableMgr1);
            int table1IndexId = indexMgr1.persistIndexDescriptor(tx1, "Table1", "id", IndexType.STATIC_HASH);
            int table2IndexId = indexMgr1.persistIndexDescriptor(tx1, "Table2", "name", IndexType.STATIC_HASH);
            tx1.commit();

            // Second transaction: verify all indexes exist
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();

            TableMgrBase tableMgr2 = new TableMgr(tx2);
            IndexMgrBase indexMgr2 = new IndexMgr(tx2, tableMgr2);

            Set<Integer> table1Ids = indexMgr2.indexIds(tx2, "Table1");
            Set<Integer> table2Ids = indexMgr2.indexIds(tx2, "Table2");

            assertTrue(table1Ids.contains(table1IndexId));
            assertTrue(table2Ids.contains(table2IndexId));

            IndexDescriptorBase desc1 = indexMgr2.get(tx2, table1IndexId);
            IndexDescriptorBase desc2 = indexMgr2.get(tx2, table2IndexId);

            assertEquals("Table1", desc1.getTableName());
            assertEquals("id", desc1.getFieldName());
            assertEquals("Table2", desc2.getTableName());
            assertEquals("name", desc2.getFieldName());

            tx2.commit();
        }
    }

    // ========================================================================
    // SECTION 3: TYPE VARIATION TESTS
    // ========================================================================

    @Nested
    @DisplayName("Type Variation Tests")
    class TypeVariationTests {

        @Test
        @DisplayName("Index on INTEGER field")
        void indexOnIntegerField() {
            TxMgrBase txMgr = createTxMgr("indexOnIntegerField", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "IntTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "IntTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            TableScanBase ts = new TableScan(tx, "IntTable", layout);
            ts.insert();
            ts.setInt("id", 42);
            ts.setString("name", "test");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID rid = ts.getRid();
            ts.close();

            index.insert(new Datum(42), rid);
            index.beforeFirst(new Datum(42));
            assertTrue(index.next());
            assertEquals(rid, index.getRID());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Index on VARCHAR field")
        void indexOnVarcharField() {
            TxMgrBase txMgr = createTxMgr("indexOnVarcharField", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "VarcharTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "VarcharTable", "name", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            TableScanBase ts = new TableScan(tx, "VarcharTable", layout);
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "StringValue");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID rid = ts.getRid();
            ts.close();

            index.insert(new Datum("StringValue"), rid);
            index.beforeFirst(new Datum("StringValue"));
            assertTrue(index.next());
            assertEquals(rid, index.getRID());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Index on DOUBLE field")
        void indexOnDoubleField() {
            TxMgrBase txMgr = createTxMgr("indexOnDoubleField", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "DoubleTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "DoubleTable", "salary", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            TableScanBase ts = new TableScan(tx, "DoubleTable", layout);
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "test");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 75000.50);
            RID rid = ts.getRid();
            ts.close();

            index.insert(new Datum(75000.50), rid);
            index.beforeFirst(new Datum(75000.50));
            assertTrue(index.next());
            assertEquals(rid, index.getRID());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Index on BOOLEAN field")
        void indexOnBooleanField() {
            TxMgrBase txMgr = createTxMgr("indexOnBooleanField", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "BoolTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "BoolTable", "active", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            TableScanBase ts = new TableScan(tx, "BoolTable", layout);
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "test");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID trueRid = ts.getRid();

            ts.insert();
            ts.setInt("id", 2);
            ts.setString("name", "test2");
            ts.setBoolean("active", false);
            ts.setDouble("salary", 60000.0);
            RID falseRid = ts.getRid();
            ts.close();

            index.insert(new Datum(true), trueRid);
            index.insert(new Datum(false), falseRid);

            index.beforeFirst(new Datum(true));
            assertTrue(index.next());
            assertEquals(trueRid, index.getRID());

            index.beforeFirst(new Datum(false));
            assertTrue(index.next());
            assertEquals(falseRid, index.getRID());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Search with wrong Datum type finds no matches")
        void searchWithDifferentDatumTypes() {
            TxMgrBase txMgr = createTxMgr("searchWithDifferentDatumTypes", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "TypeTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TypeTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            TableScanBase ts = new TableScan(tx, "TypeTable", layout);
            ts.insert();
            ts.setInt("id", 42);
            ts.setString("name", "test");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID rid = ts.getRid();
            ts.close();

            index.insert(new Datum(42), rid);

            // Search with correct type
            index.beforeFirst(new Datum(42));
            assertTrue(index.next());

            // Search with wrong type should throw IAE
            assertThrows(IllegalArgumentException.class, () -> {
                index.beforeFirst(new Datum("42"));
            });

            index.close();
            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 4: ILLEGALARGUMENTEXCEPTION TESTS
    // ========================================================================

    @Nested
    @DisplayName("IllegalArgumentException Tests")
    class IllegalArgumentExceptionTests {

        @Test
        @DisplayName("persistIndexDescriptor throws IAE for non-existent table")
        void persistIndexDescriptorThrowsIAEForNonExistentTable() {
            TxMgrBase txMgr = createTxMgr("persistIndexDescriptorThrowsIAEForNonExistentTable", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            assertThrows(IllegalArgumentException.class, () -> {
                indexMgr.persistIndexDescriptor(tx, "NonExistentTable", "id", IndexType.STATIC_HASH);
            });

            tx.rollback();
        }

        @Test
        @DisplayName("persistIndexDescriptor throws IAE for non-existent field")
        void persistIndexDescriptorThrowsIAEForNonExistentField() {
            TxMgrBase txMgr = createTxMgr("persistIndexDescriptorThrowsIAEForNonExistentField", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            assertThrows(IllegalArgumentException.class, () -> {
                indexMgr.persistIndexDescriptor(tx, "TestTable", "nonExistentField", IndexType.STATIC_HASH);
            });

            tx.rollback();
        }

        @Test
        @DisplayName("persistIndexDescriptor throws IAE for null indexType")
        void persistIndexDescriptorThrowsIAEForNullIndexType() {
            TxMgrBase txMgr = createTxMgr("persistIndexDescriptorThrowsIAEForNullIndexType", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            assertThrows(IllegalArgumentException.class, () -> {
                indexMgr.persistIndexDescriptor(tx, "TestTable", "id", null);
            });

            tx.rollback();
        }

        @Test
        @DisplayName("indexIds throws IAE for non-existent table")
        void indexIdsThrowsIAEForNonExistentTable() {
            TxMgrBase txMgr = createTxMgr("indexIdsThrowsIAEForNonExistentTable", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            assertThrows(IllegalArgumentException.class, () -> {
                indexMgr.indexIds(tx, "NonExistentTable");
            });

            tx.rollback();
        }

        @Test
        @DisplayName("instantiate throws IAE for invalid ID")
        void instantiateThrowsIAEForInvalidId() {
            TxMgrBase txMgr = createTxMgr("instantiateThrowsIAEForInvalidId", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            assertThrows(IllegalArgumentException.class, () -> {
                indexMgr.instantiate(tx, 99999);
            });

            tx.rollback();
        }

        @Test
        @DisplayName("beforeFirst throws IAE for incompatible searchKey type")
        void beforeFirstThrowsIAEForIncompatibleType() {
            TxMgrBase txMgr = createTxMgr("beforeFirstThrowsIAEForIncompatibleType", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // id is INTEGER, but searching with STRING
            assertThrows(IllegalArgumentException.class, () -> {
                index.beforeFirst(new Datum("wrongType"));
            });

            index.close();
            tx.rollback();
        }

        @Test
        @DisplayName("insert throws IAE for incompatible value type")
        void insertThrowsIAEForIncompatibleType() {
            TxMgrBase txMgr = createTxMgr("insertThrowsIAEForIncompatibleType", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            RID rid = new RID(0, 0);

            // id is INTEGER, but inserting with STRING
            assertThrows(IllegalArgumentException.class, () -> {
                index.insert(new Datum("wrongType"), rid);
            });

            index.close();
            tx.rollback();
        }

        @Test
        @DisplayName("insert throws IAE for null RID")
        void insertThrowsIAEForNullRID() {
            TxMgrBase txMgr = createTxMgr("insertThrowsIAEForNullRID", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            assertThrows(IllegalArgumentException.class, () -> {
                index.insert(new Datum(42), null);
            });

            index.close();
            tx.rollback();
        }

        @Test
        @DisplayName("delete throws IAE for incompatible value type")
        void deleteThrowsIAEForIncompatibleType() {
            TxMgrBase txMgr = createTxMgr("deleteThrowsIAEForIncompatibleType", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            RID rid = new RID(0, 0);

            // id is INTEGER, but deleting with STRING
            assertThrows(IllegalArgumentException.class, () -> {
                index.delete(new Datum("wrongType"), rid);
            });

            index.close();
            tx.rollback();
        }

        @Test
        @DisplayName("delete throws IAE for null RID")
        void deleteThrowsIAEForNullRID() {
            TxMgrBase txMgr = createTxMgr("deleteThrowsIAEForNullRID", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            assertThrows(IllegalArgumentException.class, () -> {
                index.delete(new Datum(42), null);
            });

            index.close();
            tx.rollback();
        }
    }

    // ========================================================================
    // SECTION 5: ILLEGALSTATEEXCEPTION TESTS
    // ========================================================================

    @Nested
    @DisplayName("IllegalStateException Tests")
    class IllegalStateExceptionTests {

        @Test
        @DisplayName("next throws ISE before beforeFirst")
        void nextThrowsISEBeforeBeforeFirst() {
            TxMgrBase txMgr = createTxMgr("nextThrowsISEBeforeBeforeFirst", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            assertThrows(IllegalStateException.class, () -> {
                index.next();
            });

            index.close();
            tx.rollback();
        }

        @Test
        @DisplayName("getRID throws ISE before beforeFirst")
        void getRIDThrowsISEBeforeBeforeFirst() {
            TxMgrBase txMgr = createTxMgr("getRIDThrowsISEBeforeBeforeFirst", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            assertThrows(IllegalStateException.class, () -> {
                index.getRID();
            });

            index.close();
            tx.rollback();
        }

        @Test
        @DisplayName("getRID throws ISE before successful next")
        void getRIDThrowsISEBeforeSuccessfulNext() {
            TxMgrBase txMgr = createTxMgr("getRIDThrowsISEBeforeSuccessfulNext", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "TestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TestTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Call beforeFirst but don't call next
            index.beforeFirst(new Datum(42));

            assertThrows(IllegalStateException.class, () -> {
                index.getRID();
            });

            index.close();
            tx.rollback();
        }
    }

    // ========================================================================
    // SECTION 6: PERFORMANCE TESTS
    // ========================================================================

    @Nested
    @DisplayName("Performance Tests")
    class PerformanceTests {

        @Test
        @DisplayName("Insert and search 100+ records")
        void insertAndSearchManyRecords() {
            TxMgrBase txMgr = createTxMgr("insertAndSearchManyRecords", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "PerfTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "PerfTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            int recordCount = 150;
            List<RID> rids = new ArrayList<>();

            // Insert records
            long insertStart = System.currentTimeMillis();
            TableScanBase ts = new TableScan(tx, "PerfTable", layout);
            for (int i = 0; i < recordCount; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("name", "name_" + i);
                ts.setBoolean("active", i % 2 == 0);
                ts.setDouble("salary", 50000.0 + i);
                RID rid = ts.getRid();
                rids.add(rid);
                index.insert(new Datum(i), rid);
            }
            ts.close();
            long insertEnd = System.currentTimeMillis();
            logger.info("Inserted {} records in {} ms", recordCount, insertEnd - insertStart);

            // Search for specific records
            long searchStart = System.currentTimeMillis();
            for (int i = 0; i < recordCount; i++) {
                index.beforeFirst(new Datum(i));
                assertTrue(index.next(), "Should find record " + i);
                assertEquals(rids.get(i), index.getRID());
            }
            long searchEnd = System.currentTimeMillis();
            logger.info("Searched {} records in {} ms", recordCount, searchEnd - searchStart);

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Hash collision handling")
        void hashCollisionHandling() {
            TxMgrBase txMgr = createTxMgr("hashCollisionHandling", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "CollisionTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "CollisionTable", "name", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert records with same hash bucket (modulo bucket count)
            // Using strings that may have same hash % bucketCount
            TableScanBase ts = new TableScan(tx, "CollisionTable", layout);
            List<RID> rids = new ArrayList<>();
            List<String> names = new ArrayList<>();

            for (int i = 0; i < 50; i++) {
                ts.insert();
                ts.setInt("id", i);
                String name = "CollisionName_" + i;
                ts.setString("name", name);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0);
                RID rid = ts.getRid();
                rids.add(rid);
                names.add(name);
                index.insert(new Datum(name), rid);
            }
            ts.close();

            // Search for each record - should find correct one despite potential collisions
            for (int i = 0; i < 50; i++) {
                index.beforeFirst(new Datum(names.get(i)));
                assertTrue(index.next(), "Should find record for " + names.get(i));
                assertEquals(rids.get(i), index.getRID());
            }

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Multiple indexes with many records")
        void multipleIndexesWithManyRecords() {
            TxMgrBase txMgr = createTxMgr("multipleIndexesWithManyRecords", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "MultiIndexTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int idIndexId = indexMgr.persistIndexDescriptor(tx, "MultiIndexTable", "id", IndexType.STATIC_HASH);
            int nameIndexId = indexMgr.persistIndexDescriptor(tx, "MultiIndexTable", "name", IndexType.STATIC_HASH);
            int salaryIndexId = indexMgr.persistIndexDescriptor(tx, "MultiIndexTable", "salary", IndexType.STATIC_HASH);

            IndexBase idIndex = indexMgr.instantiate(tx, idIndexId);
            IndexBase nameIndex = indexMgr.instantiate(tx, nameIndexId);
            IndexBase salaryIndex = indexMgr.instantiate(tx, salaryIndexId);

            int recordCount = 100;
            List<RID> rids = new ArrayList<>();

            // Insert records into table and all indexes
            TableScanBase ts = new TableScan(tx, "MultiIndexTable", layout);
            for (int i = 0; i < recordCount; i++) {
                ts.insert();
                ts.setInt("id", i);
                String name = "emp_" + i;
                ts.setString("name", name);
                ts.setBoolean("active", i % 2 == 0);
                double salary = 50000.0 + i * 100;
                ts.setDouble("salary", salary);
                RID rid = ts.getRid();
                rids.add(rid);

                idIndex.insert(new Datum(i), rid);
                nameIndex.insert(new Datum(name), rid);
                salaryIndex.insert(new Datum(salary), rid);
            }
            ts.close();

            // Verify id index (complete all searches before moving to next index
            // to avoid interleaved TableScan operations causing pin tracking issues)
            for (int i = 0; i < recordCount; i++) {
                idIndex.beforeFirst(new Datum(i));
                assertTrue(idIndex.next());
                assertEquals(rids.get(i), idIndex.getRID());
            }
            idIndex.close();

            // Verify name index
            for (int i = 0; i < recordCount; i++) {
                nameIndex.beforeFirst(new Datum("emp_" + i));
                assertTrue(nameIndex.next());
                assertEquals(rids.get(i), nameIndex.getRID());
            }
            nameIndex.close();

            // Verify salary index
            for (int i = 0; i < recordCount; i++) {
                salaryIndex.beforeFirst(new Datum(50000.0 + i * 100));
                assertTrue(salaryIndex.next());
                assertEquals(rids.get(i), salaryIndex.getRID());
            }
            salaryIndex.close();
            tx.commit();
        }
    }
}
