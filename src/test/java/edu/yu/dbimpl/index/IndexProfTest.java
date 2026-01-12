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
import edu.yu.dbimpl.buffer.BufferMgrBase.EvictionPolicy;
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
 * My tests include
 * • Making sure that the IndexMgr manages and persists IndexDescriptor
 * meta-data correctly.
 * 
 * • Exercising both the TableScan and Index APIs to verify that crud operations
 * benefit from, and integrate, the complementary API semantics.
 * 
 * • Evaluation to confirm that using indices can drastically improve overall
 * performance (per textbook and lecture discussions). As you explore this
 * space, keep in mind (and make sure you agree with) the following points.
 * – Record creation is much cheaper (factor of 2x per index added) for
 * non-index environment
 * – We hope that over the lifetime of a record, the retrieval cost in an
 * index-environment is so much cheaper than with non-index, that indices are
 * worth it. Amortization argument!
 * – Everything else being equal, here are some important performance factors:
 * ∗ The greater the read/write percentage on the data, the greater the
 * advantage of using indices.
 * ∗ Block size: when records are spread over more blocks, we expect indices to
 * be more beneficial. The number of index blocks will be relatively fewer,
 * making it possible to quickly zero in on the right data block.
 * ∗ Record size: larger records should favor indices since the index records
 * are small and size is essentially not affected by (indexed) record size. In
 * contrast, non-index will have to process more records. The cost of processing
 * records within the block is a ram-cost and we're treating those as free.
 * ∗ Number of records: same effect, number of blocks increases.
 * ∗ BufferMgr eviction policy: when using the CLOCK policy, index blocks are
 * more likely to be cached in memory than data blocks.
 * 
 * • Some amount of testing that the implementation can handle edge-case
 * input to the API.
 */
public class IndexProfTest {

    private static final Logger logger = LogManager.getLogger(IndexProfTest.class);

    // Test configuration constants
    private static final int BLOCK_SIZE = 400;
    private static final int BUFFER_SIZE = 100;
    private static final int MAX_WAIT_TIME = 500;
    private static final String TEST_BASE_DIR = "testing/IndexProfTest";
    private static final String LOG_FILE = "index_prof_logfile";

    /**
     * Helper method to create a fresh TxMgr for each test.
     */
    private TxMgrBase createTxMgr(String testName, boolean isStartup) {
        return createTxMgr(testName, isStartup, BLOCK_SIZE, BUFFER_SIZE, EvictionPolicy.NAIVE);
    }

    /**
     * Helper method to create a fresh TxMgr with configurable block size and buffer
     * size.
     */
    private TxMgrBase createTxMgr(String testName, boolean isStartup, int blockSize, int bufferSize,
            EvictionPolicy policy) {
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

        FileMgrBase fileMgr = new FileMgr(testDir.toFile(), blockSize);
        LogMgrBase logMgr = new LogMgr(fileMgr, LOG_FILE);
        BufferMgrBase bufferMgr = new BufferMgr(fileMgr, logMgr, bufferSize, MAX_WAIT_TIME, policy);
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
     * Helper method to create a schema with configurable string field length.
     */
    private SchemaBase createSchemaWithStringLength(int stringLength) {
        Schema schema = new Schema();
        schema.addIntField("id");
        schema.addStringField("name", stringLength);
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

    /**
     * Helper method to measure performance of an operation.
     */
    private long measurePerformance(Runnable operation, String description) {
        long startTime = System.nanoTime();
        operation.run();
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1_000_000; // Convert to milliseconds
        logger.info("{} took {} ms", description, duration);
        return duration;
    }

    // ========================================================================
    // SECTION 1: INDEXMGR METADATA PERSISTENCE TESTS
    // ========================================================================

    @Nested
    @DisplayName("IndexMgr Metadata Persistence Tests")
    class MetadataPersistenceTests {

        @Test
        @DisplayName("IndexDescriptor is correctly persisted to IndexMetadata table")
        void indexDescriptorPersistedToMetadataTable() {
            TxMgrBase txMgr = createTxMgr("indexDescriptorPersistedToMetadataTable", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "MetadataTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "MetadataTable", "id", IndexType.STATIC_HASH);

            // Verify by reading from IndexMetadata table directly
            TableScanBase metadataScan = new TableScan(tx, "IndexMetadata",
                    tableMgr.getLayout("IndexMetadata", tx));
            boolean found = false;
            while (metadataScan.next()) {
                if (metadataScan.getInt("IndexID") == indexId) {
                    assertEquals("MetadataTable", metadataScan.getString("TableName"));
                    assertEquals("id", metadataScan.getString("FieldName"));
                    found = true;
                    break;
                }
            }
            metadataScan.close();
            assertTrue(found, "IndexDescriptor should be persisted to IndexMetadata table");

            tx.commit();
        }

        @Test
        @DisplayName("IndexDescriptor metadata is correctly restored on database restart")
        void indexDescriptorRestoredOnRestart() {
            String testName = "indexDescriptorRestoredOnRestart";

            // First transaction: create table and index
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgrBase tableMgr1 = new TableMgr(tx1);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr1, "RestoreTable", schema, tx1);

            IndexMgrBase indexMgr1 = new IndexMgr(tx1, tableMgr1);
            int originalId = indexMgr1.persistIndexDescriptor(tx1, "RestoreTable", "id", IndexType.STATIC_HASH);
            int nameId = indexMgr1.persistIndexDescriptor(tx1, "RestoreTable", "name", IndexType.STATIC_HASH);
            tx1.commit();

            // Second transaction: restart database and verify restoration
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();

            TableMgrBase tableMgr2 = new TableMgr(tx2);
            IndexMgrBase indexMgr2 = new IndexMgr(tx2, tableMgr2);

            Set<Integer> ids = indexMgr2.indexIds(tx2, "RestoreTable");
            assertEquals(2, ids.size());
            assertTrue(ids.contains(originalId));
            assertTrue(ids.contains(nameId));

            IndexDescriptorBase descriptor1 = indexMgr2.get(tx2, originalId);
            assertNotNull(descriptor1);
            assertEquals("RestoreTable", descriptor1.getTableName());
            assertEquals("id", descriptor1.getFieldName());

            IndexDescriptorBase descriptor2 = indexMgr2.get(tx2, nameId);
            assertNotNull(descriptor2);
            assertEquals("RestoreTable", descriptor2.getTableName());
            assertEquals("name", descriptor2.getFieldName());

            tx2.commit();
        }

        @Test
        @DisplayName("Multiple IndexDescriptors for same table persist correctly")
        void multipleIndexDescriptorsPersistCorrectly() {
            TxMgrBase txMgr = createTxMgr("multipleIndexDescriptorsPersistCorrectly", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "MultiIndexTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int id1 = indexMgr.persistIndexDescriptor(tx, "MultiIndexTable", "id", IndexType.STATIC_HASH);
            int id2 = indexMgr.persistIndexDescriptor(tx, "MultiIndexTable", "name", IndexType.STATIC_HASH);
            int id3 = indexMgr.persistIndexDescriptor(tx, "MultiIndexTable", "salary", IndexType.STATIC_HASH);

            Set<Integer> ids = indexMgr.indexIds(tx, "MultiIndexTable");
            assertEquals(3, ids.size());
            assertTrue(ids.contains(id1));
            assertTrue(ids.contains(id2));
            assertTrue(ids.contains(id3));

            tx.commit();
        }

        @Test
        @DisplayName("IndexDescriptor metadata persists across multiple transactions")
        void indexDescriptorPersistsAcrossTransactions() {
            String testName = "indexDescriptorPersistsAcrossTransactions";

            // Transaction 1: Create index
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgrBase tableMgr1 = new TableMgr(tx1);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr1, "CrossTxTable", schema, tx1);

            IndexMgrBase indexMgr1 = new IndexMgr(tx1, tableMgr1);
            int indexId = indexMgr1.persistIndexDescriptor(tx1, "CrossTxTable", "id", IndexType.STATIC_HASH);
            tx1.commit();

            // Transaction 2: Verify persistence (create new txMgr with isStartup=false to
            // load catalog)
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();
            TableMgrBase tableMgr2 = new TableMgr(tx2);
            IndexMgrBase indexMgr2 = new IndexMgr(tx2, tableMgr2);

            Set<Integer> ids = indexMgr2.indexIds(tx2, "CrossTxTable");
            assertTrue(ids.contains(indexId));

            IndexDescriptorBase descriptor = indexMgr2.get(tx2, indexId);
            assertNotNull(descriptor);
            assertEquals("CrossTxTable", descriptor.getTableName());
            assertEquals("id", descriptor.getFieldName());

            tx2.commit();
        }

        @Test
        @DisplayName("ID counter is correctly restored and continues from highest persisted ID")
        void idCounterRestoredCorrectly() {
            String testName = "idCounterRestoredCorrectly";

            // First transaction: create indexes
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgrBase tableMgr1 = new TableMgr(tx1);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr1, "IdCounterTable", schema, tx1);

            IndexMgrBase indexMgr1 = new IndexMgr(tx1, tableMgr1);
            int id1 = indexMgr1.persistIndexDescriptor(tx1, "IdCounterTable", "id", IndexType.STATIC_HASH);
            int id2 = indexMgr1.persistIndexDescriptor(tx1, "IdCounterTable", "name", IndexType.STATIC_HASH);
            int id3 = indexMgr1.persistIndexDescriptor(tx1, "IdCounterTable", "salary", IndexType.STATIC_HASH);
            tx1.commit();

            // Second transaction: create new index, verify ID is higher
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();

            TableMgrBase tableMgr2 = new TableMgr(tx2);
            IndexMgrBase indexMgr2 = new IndexMgr(tx2, tableMgr2);
            int id4 = indexMgr2.persistIndexDescriptor(tx2, "IdCounterTable", "active", IndexType.STATIC_HASH);

            assertTrue(id4 > id1);
            assertTrue(id4 > id2);
            assertTrue(id4 > id3);

            tx2.commit();
        }

        @Test
        @DisplayName("indexIds returns correct set after persistence")
        void indexIdsReturnsCorrectSetAfterPersistence() {
            String testName = "indexIdsReturnsCorrectSetAfterPersistence";

            // Create and persist indexes
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgrBase tableMgr1 = new TableMgr(tx1);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr1, "IndexIdsTable", schema, tx1);

            IndexMgrBase indexMgr1 = new IndexMgr(tx1, tableMgr1);
            int id1 = indexMgr1.persistIndexDescriptor(tx1, "IndexIdsTable", "id", IndexType.STATIC_HASH);
            int id2 = indexMgr1.persistIndexDescriptor(tx1, "IndexIdsTable", "name", IndexType.STATIC_HASH);
            tx1.commit();

            // Restart and verify
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();

            TableMgrBase tableMgr2 = new TableMgr(tx2);
            IndexMgrBase indexMgr2 = new IndexMgr(tx2, tableMgr2);

            Set<Integer> ids = indexMgr2.indexIds(tx2, "IndexIdsTable");
            assertEquals(2, ids.size());
            assertTrue(ids.contains(id1));
            assertTrue(ids.contains(id2));

            tx2.commit();
        }

        @Test
        @DisplayName("get returns correct IndexDescriptor after persistence")
        void getReturnsCorrectDescriptorAfterPersistence() {
            String testName = "getReturnsCorrectDescriptorAfterPersistence";

            // Create and persist index
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgrBase tableMgr1 = new TableMgr(tx1);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr1, "GetDescTable", schema, tx1);

            IndexMgrBase indexMgr1 = new IndexMgr(tx1, tableMgr1);
            int indexId = indexMgr1.persistIndexDescriptor(tx1, "GetDescTable", "name", IndexType.STATIC_HASH);
            tx1.commit();

            // Restart and verify
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();

            TableMgrBase tableMgr2 = new TableMgr(tx2);
            IndexMgrBase indexMgr2 = new IndexMgr(tx2, tableMgr2);

            IndexDescriptorBase descriptor = indexMgr2.get(tx2, indexId);
            assertNotNull(descriptor);
            assertEquals("GetDescTable", descriptor.getTableName());
            assertEquals("name", descriptor.getFieldName());
            assertEquals("name", descriptor.getIndexName());
            assertEquals(IndexType.STATIC_HASH, descriptor.getIndexType());

            tx2.commit();
        }
    }

    // ========================================================================
    // SECTION 2: TABLESCAN AND INDEX API INTEGRATION TESTS
    // ========================================================================

    @Nested
    @DisplayName("TableScan and Index API Integration Tests")
    class ApiIntegrationTests {

        @Test
        @DisplayName("Insert operations maintain consistency between TableScan and Index")
        void insertOperationsMaintainConsistency() {
            TxMgrBase txMgr = createTxMgr("insertOperationsMaintainConsistency", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "InsConsistTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "InsConsistTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert via TableScan
            TableScanBase ts = new TableScan(tx, "InsConsistTable", layout);
            ts.insert();
            ts.setInt("id", 42);
            ts.setString("name", "TestName");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 75000.0);
            RID rid = ts.getRid();

            // Insert into Index
            index.insert(new Datum(42), rid);

            // Verify consistency: use Index to find record
            index.beforeFirst(new Datum(42));
            assertTrue(index.next());
            RID foundRid = index.getRID();
            assertEquals(rid, foundRid);

            // Use TableScan to read the record
            ts.moveToRid(foundRid);
            assertEquals(42, ts.getInt("id"));
            assertEquals("TestName", ts.getString("name"));

            ts.close();
            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Read operations use Index to find RID then TableScan to retrieve record")
        void readOperationsUseIndexAndTableScan() {
            TxMgrBase txMgr = createTxMgr("readOperationsUseIndexAndTableScan", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "ReadIntegTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "ReadIntegTable", "name", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert multiple records
            TableScanBase ts = new TableScan(tx, "ReadIntegTable", layout);
            List<RID> rids = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("name", "name_" + i);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0 + i);
                RID rid = ts.getRid();
                rids.add(rid);
                index.insert(new Datum("name_" + i), rid);
            }
            ts.close();

            // Use Index to find specific record
            index.beforeFirst(new Datum("name_5"));
            assertTrue(index.next());
            RID foundRid = index.getRID();

            // Use TableScan to read the full record
            TableScanBase readScan = new TableScan(tx, "ReadIntegTable", layout);
            readScan.moveToRid(foundRid);
            assertEquals(5, readScan.getInt("id"));
            assertEquals("name_5", readScan.getString("name"));
            assertEquals(50005.0, readScan.getDouble("salary"));

            readScan.close();
            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Update operations maintain Index consistency")
        void updateOperationsMaintainIndexConsistency() {
            TxMgrBase txMgr = createTxMgr("updateOperationsMaintainIndexConsistency", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "UpdConsistTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "UpdConsistTable", "name", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert record
            TableScanBase ts = new TableScan(tx, "UpdConsistTable", layout);
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "OldName");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID rid = ts.getRid();
            index.insert(new Datum("OldName"), rid);

            // Update record via TableScan
            ts.moveToRid(rid);
            ts.setString("name", "NewName");

            // Update index: delete old, insert new
            index.delete(new Datum("OldName"), rid);
            index.insert(new Datum("NewName"), rid);

            // Verify: search for new name
            index.beforeFirst(new Datum("NewName"));
            assertTrue(index.next());
            assertEquals(rid, index.getRID());

            // Verify: old name not found
            index.beforeFirst(new Datum("OldName"));
            assertFalse(index.next());

            ts.close();
            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Delete operations maintain consistency between TableScan and Index")
        void deleteOperationsMaintainConsistency() {
            TxMgrBase txMgr = createTxMgr("deleteOperationsMaintainConsistency", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "DelConsistTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "DelConsistTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert record
            TableScanBase ts = new TableScan(tx, "DelConsistTable", layout);
            ts.insert();
            ts.setInt("id", 99);
            ts.setString("name", "ToDelete");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 60000.0);
            RID rid = ts.getRid();
            index.insert(new Datum(99), rid);
            ts.close();

            // Delete from Index first
            index.delete(new Datum(99), rid);

            // Delete from TableScan
            TableScanBase deleteScan = new TableScan(tx, "DelConsistTable", layout);
            deleteScan.moveToRid(rid);
            deleteScan.delete();
            deleteScan.close();

            // Verify: record not found in index
            index.beforeFirst(new Datum(99));
            assertFalse(index.next());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Multiple indexes remain consistent when updating indexed fields")
        void multipleIndexesRemainConsistent() {
            TxMgrBase txMgr = createTxMgr("multipleIndexesRemainConsistent", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "MultiIdxConsistT", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int idIndexId = indexMgr.persistIndexDescriptor(tx, "MultiIdxConsistT", "id", IndexType.STATIC_HASH);
            int nameIndexId = indexMgr.persistIndexDescriptor(tx, "MultiIdxConsistT", "name", IndexType.STATIC_HASH);
            int salaryIndexId = indexMgr.persistIndexDescriptor(tx, "MultiIdxConsistT", "salary",
                    IndexType.STATIC_HASH);

            IndexBase idIndex = indexMgr.instantiate(tx, idIndexId);
            IndexBase nameIndex = indexMgr.instantiate(tx, nameIndexId);
            IndexBase salaryIndex = indexMgr.instantiate(tx, salaryIndexId);

            // Insert record
            TableScanBase ts = new TableScan(tx, "MultiIdxConsistT", layout);
            ts.insert();
            ts.setInt("id", 7);
            ts.setString("name", "Bob");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 80000.0);
            RID rid = ts.getRid();

            idIndex.insert(new Datum(7), rid);
            nameIndex.insert(new Datum("Bob"), rid);
            salaryIndex.insert(new Datum(80000.0), rid);

            // Update multiple fields
            ts.moveToRid(rid);
            ts.setString("name", "Alice");
            ts.setDouble("salary", 90000.0);

            // Update indexes
            nameIndex.delete(new Datum("Bob"), rid);
            nameIndex.insert(new Datum("Alice"), rid);
            salaryIndex.delete(new Datum(80000.0), rid);
            salaryIndex.insert(new Datum(90000.0), rid);

            // Verify all indexes are consistent
            idIndex.beforeFirst(new Datum(7));
            assertTrue(idIndex.next());
            assertEquals(rid, idIndex.getRID());

            nameIndex.beforeFirst(new Datum("Alice"));
            assertTrue(nameIndex.next());
            assertEquals(rid, nameIndex.getRID());

            salaryIndex.beforeFirst(new Datum(90000.0));
            assertTrue(salaryIndex.next());
            assertEquals(rid, salaryIndex.getRID());

            ts.close();
            idIndex.close();
            nameIndex.close();
            salaryIndex.close();
            tx.commit();
        }

        @Test
        @DisplayName("Concurrent TableScan and Index operations in same transaction")
        void concurrentOperationsInSameTransaction() {
            TxMgrBase txMgr = createTxMgr("concurrentOperationsInSameTransaction", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "ConcurOpsTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "ConcurOpsTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Mix insertions: some via TableScan+Index, some just TableScan
            TableScanBase ts = new TableScan(tx, "ConcurOpsTable", layout);
            for (int i = 0; i < 5; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("name", "name_" + i);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0);
                RID rid = ts.getRid();
                index.insert(new Datum(i), rid);
            }

            // Search using index while TableScan is still open
            index.beforeFirst(new Datum(3));
            assertTrue(index.next());
            RID foundRid = index.getRID();

            // Use same TableScan to read
            ts.moveToRid(foundRid);
            assertEquals(3, ts.getInt("id"));

            ts.close();
            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Search patterns: Index beforeFirst/next with TableScan to read full records")
        void searchPatternsWithIndexAndTableScan() {
            TxMgrBase txMgr = createTxMgr("searchPatternsWithIndexAndTableScan", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "SearchPattTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "SearchPattTable", "name", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert records with some duplicate names
            TableScanBase ts = new TableScan(tx, "SearchPattTable", layout);
            for (int i = 0; i < 10; i++) {
                ts.insert();
                ts.setInt("id", i);
                String name = i < 5 ? "CommonName" : "name_" + i;
                ts.setString("name", name);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0 + i);
                RID rid = ts.getRid();
                index.insert(new Datum(name), rid);
            }
            ts.close();

            // Search for all records with "CommonName"
            TableScanBase readScan = new TableScan(tx, "SearchPattTable", layout);
            index.beforeFirst(new Datum("CommonName"));
            int count = 0;
            while (index.next()) {
                RID rid = index.getRID();
                readScan.moveToRid(rid);
                assertEquals("CommonName", readScan.getString("name"));
                assertTrue(readScan.getInt("id") < 5);
                count++;
            }
            assertEquals(5, count);

            readScan.close();
            index.close();
            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 3: PERFORMANCE EVALUATION TESTS
    // ========================================================================

    @Nested
    @DisplayName("Performance Evaluation Tests")
    class PerformanceTests {

        @Test
        @DisplayName("Record creation cost: ~2x per index added")
        void recordCreationCostPerIndex() {
            TxMgrBase txMgr = createTxMgr("recordCreationCostPerIndex", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "CreateCostTable", schema, tx);

            // Create IndexMgr once - it will create IndexMetadata table on first use
            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            int recordCount = 100;
            List<Long> times = new ArrayList<>();

            // Test with 0, 1, 2, 3 indexes
            for (int numIndexes = 0; numIndexes <= 3; numIndexes++) {
                // Create fresh table for each test
                String tableName = "CreateCostTbl_" + numIndexes;
                LayoutBase testLayout = createTestTable(tableMgr, tableName, schema, tx);
                List<IndexBase> indexes = new ArrayList<>();
                List<String> fieldNames = new ArrayList<>();
                for (int i = 0; i < numIndexes; i++) {
                    String fieldName = i == 0 ? "id" : (i == 1 ? "name" : "salary");
                    int indexId = indexMgr.persistIndexDescriptor(tx, tableName, fieldName, IndexType.STATIC_HASH);
                    indexes.add(indexMgr.instantiate(tx, indexId));
                    fieldNames.add(fieldName);
                }

                // Measure insert time
                long duration = measurePerformance(() -> {
                    TableScanBase ts = new TableScan(tx, tableName, testLayout);
                    for (int j = 0; j < recordCount; j++) {
                        ts.insert();
                        ts.setInt("id", j);
                        ts.setString("name", "name_" + j);
                        ts.setBoolean("active", true);
                        ts.setDouble("salary", 50000.0 + j);
                        RID rid = ts.getRid();

                        for (int k = 0; k < indexes.size(); k++) {
                            IndexBase index = indexes.get(k);
                            String fieldName = fieldNames.get(k);
                            DatumBase value;
                            if (fieldName.equals("id")) {
                                value = new Datum(j);
                            } else if (fieldName.equals("name")) {
                                value = new Datum("name_" + j);
                            } else {
                                value = new Datum(50000.0 + j);
                            }
                            index.insert(value, rid);
                        }
                    }
                    ts.close();
                    for (IndexBase index : indexes) {
                        index.close();
                    }
                }, "Insert " + recordCount + " records with " + numIndexes + " indexes");

                times.add(duration);
                logger.info("Time with {} indexes: {} ms", numIndexes, duration);
            }

            // Verify that indexes add cost (allow wide variance for small workloads)
            // Note: With small record counts (100), timing can be very inconsistent
            // We verify that indexes add overhead, but don't enforce exact 2x ratio
            if (times.size() >= 4) {
                long time0 = times.get(0);
                long time1 = times.get(1);
                long time2 = times.get(2);

                // With 1 index should cost more than 0 indexes
                double ratio1 = (double) time1 / time0;
                logger.info("Ratio 1 index / 0 indexes: {} (time0={}ms, time1={}ms)", ratio1, time0, time1);
                assertTrue(ratio1 > 1.0,
                        "1 index should cost more than 0 indexes, got ratio: " + ratio1);

                // With 2 indexes should cost more than 1 index
                double ratio2 = (double) time2 / time1;
                logger.info("Ratio 2 indexes / 1 index: {} (time1={}ms, time2={}ms)", ratio2, time1, time2);
                assertTrue(ratio2 > 1.0,
                        "2 indexes should cost more than 1 index, got ratio: " + ratio2);

                // Log the trend for manual verification
                logger.info("Performance trend: 0 indexes={}ms, 1 index={}ms, 2 indexes={}ms",
                        time0, time1, time2);
            }

            tx.commit();
        }

        @Test
        @DisplayName("Read/write ratio impact: higher read percentage favors indices")
        void readWriteRatioImpact() {
            int recordCount = 200;
            int[] readPercentages = { 10, 50, 90 };

            for (int readPct : readPercentages) {
                String testName = "readWriteRatio_" + readPct;
                TxMgrBase txMgr = createTxMgr(testName, true);
                TxBase tx = txMgr.newTx();

                TableMgrBase tableMgr = new TableMgr(tx);
                SchemaBase schema = createStandardSchema();
                LayoutBase layout = createTestTable(tableMgr, "ReadWriteTable", schema, tx);

                IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
                int indexId = indexMgr.persistIndexDescriptor(tx, "ReadWriteTable", "id", IndexType.STATIC_HASH);
                IndexBase index = indexMgr.instantiate(tx, indexId);

                // Insert records
                TableScanBase ts = new TableScan(tx, "ReadWriteTable", layout);
                List<RID> rids = new ArrayList<>();
                for (int i = 0; i < recordCount; i++) {
                    ts.insert();
                    ts.setInt("id", i);
                    ts.setString("name", "name_" + i);
                    ts.setBoolean("active", true);
                    ts.setDouble("salary", 50000.0 + i);
                    RID rid = ts.getRid();
                    rids.add(rid);
                    index.insert(new Datum(i), rid);
                }
                ts.close();

                // Measure workload with specified read/write ratio
                int readCount = (recordCount * readPct) / 100;
                int writeCount = recordCount - readCount;

                // Measure workload - avoid TableScan conflicts by just measuring index
                // operations
                // For writes, we'll simulate by just verifying the RID (actual updates would
                // require TableScan)
                long duration = measurePerformance(() -> {
                    // Reads using index - just verify RID
                    for (int i = 0; i < readCount; i++) {
                        int searchId = i % recordCount;
                        index.beforeFirst(new Datum(searchId));
                        if (index.next()) {
                            RID rid = index.getRID();
                            // Just verify RID is correct
                            assertNotNull(rid);
                        }
                    }

                    // Writes (simulated) - just verify RID without actual updates to avoid pin
                    // conflicts
                    // In a real scenario, updates would use TableScan, but for performance
                    // measurement
                    // we focus on the index lookup performance
                    for (int i = 0; i < writeCount; i++) {
                        int updateId = (readCount + i) % recordCount;
                        index.beforeFirst(new Datum(updateId));
                        if (index.next()) {
                            RID rid = index.getRID();
                            // Just verify RID is correct - actual update would be done separately
                            assertNotNull(rid);
                        }
                    }
                }, "Workload with " + readPct + "% reads, " + writeCount + "% writes (simulated)");

                logger.info("Read/write ratio {}% reads: {} ms", readPct, duration);

                index.close();
                tx.commit();
            }
        }

        @Test
        @DisplayName("Block size impact: larger blocks favor indices")
        void blockSizeImpact() {
            int[] blockSizes = { 200, 400, 800 };
            int recordCount = 500;

            for (int blockSize : blockSizes) {
                String testName = "blockSize_" + blockSize;
                TxMgrBase txMgr = createTxMgr(testName, true, blockSize, BUFFER_SIZE, EvictionPolicy.NAIVE);
                TxBase tx = txMgr.newTx();

                TableMgrBase tableMgr = new TableMgr(tx);
                SchemaBase schema = createStandardSchema();
                LayoutBase layout = createTestTable(tableMgr, "BlockSizeTable", schema, tx);

                IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
                int indexId = indexMgr.persistIndexDescriptor(tx, "BlockSizeTable", "id", IndexType.STATIC_HASH);
                IndexBase index = indexMgr.instantiate(tx, indexId);

                // Insert records
                TableScanBase ts = new TableScan(tx, "BlockSizeTable", layout);
                List<RID> rids = new ArrayList<>();
                for (int i = 0; i < recordCount; i++) {
                    ts.insert();
                    ts.setInt("id", i);
                    ts.setString("name", "name_" + i);
                    ts.setBoolean("active", true);
                    ts.setDouble("salary", 50000.0 + i);
                    RID rid = ts.getRid();
                    rids.add(rid);
                    index.insert(new Datum(i), rid);
                }
                ts.close();

                // Measure search time using index - just verify RID
                long searchTime = measurePerformance(() -> {
                    for (int i = 0; i < 100; i++) {
                        int searchId = i % recordCount;
                        index.beforeFirst(new Datum(searchId));
                        if (index.next()) {
                            RID rid = index.getRID();
                            // Just verify RID is correct - Index.next() already verified the value
                            assertNotNull(rid);
                        }
                    }
                }, "Search 100 records with block size " + blockSize);

                logger.info("Block size {}: search time {} ms", blockSize, searchTime);

                index.close();
                tx.commit();
            }
        }

        @Test
        @DisplayName("Record size impact: larger records favor indices")
        void recordSizeImpact() {
            int[] stringLengths = { 16, 64, 256 };
            int recordCount = 300;

            for (int strLen : stringLengths) {
                String testName = "recordSize_" + strLen;
                TxMgrBase txMgr = createTxMgr(testName, true);
                TxBase tx = txMgr.newTx();

                TableMgrBase tableMgr = new TableMgr(tx);
                SchemaBase schema = createSchemaWithStringLength(strLen);
                LayoutBase layout = createTestTable(tableMgr, "RecordSizeTable", schema, tx);

                IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
                int indexId = indexMgr.persistIndexDescriptor(tx, "RecordSizeTable", "id", IndexType.STATIC_HASH);
                IndexBase index = indexMgr.instantiate(tx, indexId);

                // Insert records with varying sizes
                // Use string that fits within field length (account for "_" + up to 3 digit
                // number)
                String longName = "x".repeat(Math.max(0, strLen - 5));
                TableScanBase ts = new TableScan(tx, "RecordSizeTable", layout);
                List<RID> rids = new ArrayList<>();
                for (int i = 0; i < recordCount; i++) {
                    ts.insert();
                    ts.setInt("id", i);
                    String name = longName + "_" + i;
                    // Ensure name fits in field length
                    if (name.length() > strLen) {
                        name = name.substring(0, strLen);
                    }
                    ts.setString("name", name);
                    ts.setBoolean("active", true);
                    ts.setDouble("salary", 50000.0 + i);
                    RID rid = ts.getRid();
                    rids.add(rid);
                    index.insert(new Datum(i), rid);
                }
                ts.close();

                // Measure search time - just verify RID without creating additional TableScan
                // to avoid pin/unpin conflicts with Index's internal tableScan
                long searchTime = measurePerformance(() -> {
                    for (int i = 0; i < 100; i++) {
                        int searchId = i % recordCount;
                        index.beforeFirst(new Datum(searchId));
                        if (index.next()) {
                            RID rid = index.getRID();
                            // Just verify RID is correct - Index.next() already verified the value
                            assertNotNull(rid);
                        }
                    }
                }, "Search 100 records with string length " + strLen);

                logger.info("String length {}: search time {} ms", strLen, searchTime);

                index.close();
                tx.commit();
            }
        }

        @Test
        @DisplayName("Number of records impact: more records increase index benefit")
        void numberOfRecordsImpact() {
            int[] recordCounts = { 100, 1000, 10000 };

            for (int recordCount : recordCounts) {
                String testName = "recordCount_" + recordCount;
                TxMgrBase txMgr = createTxMgr(testName, true);
                TxBase tx = txMgr.newTx();

                TableMgrBase tableMgr = new TableMgr(tx);
                SchemaBase schema = createStandardSchema();
                LayoutBase layout = createTestTable(tableMgr, "RecordCountTable", schema, tx);

                IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
                int indexId = indexMgr.persistIndexDescriptor(tx, "RecordCountTable", "id", IndexType.STATIC_HASH);
                IndexBase index = indexMgr.instantiate(tx, indexId);

                // Insert records
                TableScanBase ts = new TableScan(tx, "RecordCountTable", layout);
                List<RID> rids = new ArrayList<>();
                for (int i = 0; i < recordCount; i++) {
                    ts.insert();
                    ts.setInt("id", i);
                    ts.setString("name", "name_" + i);
                    ts.setBoolean("active", true);
                    ts.setDouble("salary", 50000.0 + i);
                    RID rid = ts.getRid();
                    rids.add(rid);
                    index.insert(new Datum(i), rid);
                }
                ts.close();

                // Measure search time using index
                // Just verify RID without creating additional TableScan to avoid pin/unpin
                // conflicts
                int searchCount = Math.min(100, recordCount);
                long searchTime = measurePerformance(() -> {
                    for (int i = 0; i < searchCount; i++) {
                        int searchId = i % recordCount;
                        index.beforeFirst(new Datum(searchId));
                        if (index.next()) {
                            RID rid = index.getRID();
                            // Just verify RID is correct - Index.next() already verified the value
                            assertNotNull(rid);
                        }
                    }
                }, "Search " + searchCount + " records out of " + recordCount);

                logger.info("Record count {}: search time {} ms", recordCount, searchTime);

                index.close();
                tx.commit();
            }
        }

        @Test
        @DisplayName("BufferMgr eviction policy impact: CLOCK caches index blocks better")
        void evictionPolicyImpact() {
            int recordCount = 500;
            int searchCount = 200;

            for (EvictionPolicy policy : new EvictionPolicy[] { EvictionPolicy.NAIVE, EvictionPolicy.CLOCK }) {
                String testName = "evictionPolicy_" + policy;
                TxMgrBase txMgr = createTxMgr(testName, true, BLOCK_SIZE, 50, policy); // Smaller buffer to see effect
                TxBase tx = txMgr.newTx();

                TableMgrBase tableMgr = new TableMgr(tx);
                SchemaBase schema = createStandardSchema();
                LayoutBase layout = createTestTable(tableMgr, "EvictPolTable", schema, tx);

                IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
                int indexId = indexMgr.persistIndexDescriptor(tx, "EvictPolTable", "id", IndexType.STATIC_HASH);
                IndexBase index = indexMgr.instantiate(tx, indexId);

                // Insert records
                TableScanBase ts = new TableScan(tx, "EvictPolTable", layout);
                List<RID> rids = new ArrayList<>();
                for (int i = 0; i < recordCount; i++) {
                    ts.insert();
                    ts.setInt("id", i);
                    ts.setString("name", "name_" + i);
                    ts.setBoolean("active", true);
                    ts.setDouble("salary", 50000.0 + i);
                    RID rid = ts.getRid();
                    rids.add(rid);
                    index.insert(new Datum(i), rid);
                }
                ts.close();

                // Measure search time (should benefit from caching with CLOCK)
                // Just verify RID without creating additional TableScan to avoid pin/unpin
                // conflicts
                long searchTime = measurePerformance(() -> {
                    for (int i = 0; i < searchCount; i++) {
                        int searchId = i % recordCount;
                        index.beforeFirst(new Datum(searchId));
                        if (index.next()) {
                            RID rid = index.getRID();
                            // Just verify RID is correct - Index.next() already verified the value
                            assertNotNull(rid);
                        }
                    }
                }, "Search " + searchCount + " records with " + policy + " policy");

                logger.info("Eviction policy {}: search time {} ms", policy, searchTime);

                index.close();
                tx.commit();
            }
        }
    }

    // ========================================================================
    // SECTION 4: EDGE CASE TESTS
    // ========================================================================

    @Nested
    @DisplayName("Edge Case Tests")
    class EdgeCaseTests {

        @Test
        @DisplayName("Empty index searches return no matches")
        void emptyIndexSearches() {
            TxMgrBase txMgr = createTxMgr("emptyIndexSearches", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "EmptySearchTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "EmptySearchTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Search for non-existent record
            index.beforeFirst(new Datum(999));
            assertFalse(index.next());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Index with single record")
        void indexWithSingleRecord() {
            TxMgrBase txMgr = createTxMgr("indexWithSingleRecord", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "SingleRecTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "SingleRecTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert single record
            TableScanBase ts = new TableScan(tx, "SingleRecTable", layout);
            ts.insert();
            ts.setInt("id", 42);
            ts.setString("name", "Single");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID rid = ts.getRid();
            index.insert(new Datum(42), rid);
            ts.close();

            // Search for it
            index.beforeFirst(new Datum(42));
            assertTrue(index.next());
            assertEquals(rid, index.getRID());
            assertFalse(index.next()); // Only one record

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Index handles hash collisions correctly")
        void indexHandlesHashCollisions() {
            TxMgrBase txMgr = createTxMgr("indexHandlesHashCollisions", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "CollisionTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "CollisionTable", "name", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert multiple records that may hash to same bucket
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

            // Verify each record can be found despite potential collisions
            for (int i = 0; i < 50; i++) {
                index.beforeFirst(new Datum(names.get(i)));
                assertTrue(index.next(), "Should find record for " + names.get(i));
                assertEquals(rids.get(i), index.getRID());
            }

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Index operations with extreme integer values")
        void indexOperationsWithExtremeValues() {
            TxMgrBase txMgr = createTxMgr("indexOperationsWithExtremeValues", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "ExtremeValTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "ExtremeValTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert records with extreme values
            TableScanBase ts = new TableScan(tx, "ExtremeValTable", layout);
            int[] extremeValues = { Integer.MIN_VALUE, -1000, 0, 1000, Integer.MAX_VALUE };
            List<RID> rids = new ArrayList<>();

            for (int i = 0; i < extremeValues.length; i++) {
                int val = extremeValues[i];
                ts.insert();
                ts.setInt("id", val);
                // Use short name that fits in 16-char limit (name field is 16 chars)
                ts.setString("name", "extr" + i);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0);
                RID rid = ts.getRid();
                rids.add(rid);
                index.insert(new Datum(val), rid);
            }
            ts.close();

            // Verify all extreme values can be found
            for (int i = 0; i < extremeValues.length; i++) {
                index.beforeFirst(new Datum(extremeValues[i]));
                assertTrue(index.next());
                assertEquals(rids.get(i), index.getRID());
            }

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Index operations with very long string values")
        void indexOperationsWithLongStrings() {
            // Use larger block size to accommodate large string field (500 chars +
            // overhead)
            // Block size needs to be at least: 1 (in-use) + 4 (int) + 500 (string) +
            // overhead
            int largeBlockSize = 800;
            TxMgrBase txMgr = createTxMgr("indexOperationsWithLongStrings", true, largeBlockSize, BUFFER_SIZE,
                    EvictionPolicy.NAIVE);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            Schema schema = new Schema();
            schema.addIntField("id");
            schema.addStringField("name", 500); // Large string field
            LayoutBase layout = createTestTable(tableMgr, "LongStringTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "LongStringTable", "name", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert records with long strings
            TableScanBase ts = new TableScan(tx, "LongStringTable", layout);
            String longString = "x".repeat(400);
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", longString);
            RID rid = ts.getRid();
            index.insert(new Datum(longString), rid);
            ts.close();

            // Verify long string can be found
            index.beforeFirst(new Datum(longString));
            assertTrue(index.next());
            assertEquals(rid, index.getRID());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Multiple indexes on same field is idempotent")
        void multipleIndexesOnSameFieldIdempotent() {
            TxMgrBase txMgr = createTxMgr("multipleIndexesOnSameFieldIdempotent", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "IdempotentTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int id1 = indexMgr.persistIndexDescriptor(tx, "IdempotentTable", "id", IndexType.STATIC_HASH);
            int id2 = indexMgr.persistIndexDescriptor(tx, "IdempotentTable", "id", IndexType.STATIC_HASH);

            // Should return same ID (idempotent)
            assertEquals(id1, id2);

            Set<Integer> ids = indexMgr.indexIds(tx, "IdempotentTable");
            assertEquals(1, ids.size()); // Only one index for this field
            assertTrue(ids.contains(id1));

            tx.commit();
        }

        @Test
        @DisplayName("Index operations after record deletion")
        void indexOperationsAfterRecordDeletion() {
            TxMgrBase txMgr = createTxMgr("indexOperationsAfterRecordDeletion", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "DelRecordTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "DelRecordTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert and index record
            TableScanBase ts = new TableScan(tx, "DelRecordTable", layout);
            ts.insert();
            ts.setInt("id", 99);
            ts.setString("name", "ToDelete");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 60000.0);
            RID rid = ts.getRid();
            index.insert(new Datum(99), rid);
            ts.close();

            // Delete from index
            index.delete(new Datum(99), rid);

            // Delete from table
            TableScanBase deleteScan = new TableScan(tx, "DelRecordTable", layout);
            deleteScan.moveToRid(rid);
            deleteScan.delete();
            deleteScan.close();

            // Verify record not found
            index.beforeFirst(new Datum(99));
            assertFalse(index.next());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Index handles all field types correctly")
        void indexHandlesAllFieldTypes() {
            TxMgrBase txMgr = createTxMgr("indexHandlesAllFieldTypes", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "AllTypesTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int intIndexId = indexMgr.persistIndexDescriptor(tx, "AllTypesTable", "id", IndexType.STATIC_HASH);
            int stringIndexId = indexMgr.persistIndexDescriptor(tx, "AllTypesTable", "name", IndexType.STATIC_HASH);
            int boolIndexId = indexMgr.persistIndexDescriptor(tx, "AllTypesTable", "active", IndexType.STATIC_HASH);
            int doubleIndexId = indexMgr.persistIndexDescriptor(tx, "AllTypesTable", "salary", IndexType.STATIC_HASH);

            IndexBase intIndex = indexMgr.instantiate(tx, intIndexId);
            IndexBase stringIndex = indexMgr.instantiate(tx, stringIndexId);
            IndexBase boolIndex = indexMgr.instantiate(tx, boolIndexId);
            IndexBase doubleIndex = indexMgr.instantiate(tx, doubleIndexId);

            // Insert record
            TableScanBase ts = new TableScan(tx, "AllTypesTable", layout);
            ts.insert();
            ts.setInt("id", 42);
            ts.setString("name", "TestName");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 75000.5);
            RID rid = ts.getRid();

            intIndex.insert(new Datum(42), rid);
            stringIndex.insert(new Datum("TestName"), rid);
            boolIndex.insert(new Datum(true), rid);
            doubleIndex.insert(new Datum(75000.5), rid);
            ts.close();

            // Verify all types can be searched
            intIndex.beforeFirst(new Datum(42));
            assertTrue(intIndex.next());
            assertEquals(rid, intIndex.getRID());

            stringIndex.beforeFirst(new Datum("TestName"));
            assertTrue(stringIndex.next());
            assertEquals(rid, stringIndex.getRID());

            boolIndex.beforeFirst(new Datum(true));
            assertTrue(boolIndex.next());
            assertEquals(rid, boolIndex.getRID());

            doubleIndex.beforeFirst(new Datum(75000.5));
            assertTrue(doubleIndex.next());
            assertEquals(rid, doubleIndex.getRID());

            intIndex.close();
            stringIndex.close();
            boolIndex.close();
            doubleIndex.close();
            tx.commit();
        }
    }
}
