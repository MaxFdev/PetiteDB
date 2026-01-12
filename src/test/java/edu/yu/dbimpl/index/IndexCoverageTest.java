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
 * Coverage tests for the Index module to achieve near-100% code coverage.
 * Focuses on edge cases, exception paths, and code branches not covered by
 * IndexTest.java and IndexProfTest.java.
 */
public class IndexCoverageTest {

    private static final Logger logger = LogManager.getLogger(IndexCoverageTest.class);

    // Test configuration constants
    private static final int BLOCK_SIZE = 400;
    private static final int BUFFER_SIZE = 100;
    private static final int MAX_WAIT_TIME = 500;
    private static final String TEST_BASE_DIR = "testing/IndexCoverageTest";
    private static final String LOG_FILE = "index_coverage_logfile";

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
    // SECTION 1: IndexMgr.deleteAll Edge Cases
    // ========================================================================

    @Nested
    @DisplayName("IndexMgr.deleteAll Edge Cases")
    class IndexMgrDeleteAllTests {

        @Test
        @DisplayName("deleteAll on table with no indexes (val == null)")
        void deleteAllOnTableWithNoIndexes() {
            TxMgrBase txMgr = createTxMgr("deleteAllOnTableWithNoIndexes", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "NoIndexTable", schema, tx);

            // Insert some data so the table has records to delete
            TableScanBase ts = new TableScan(tx, "NoIndexTable", layout);
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "test");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            ts.close();

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            // Delete table that has no indexes - should not throw
            indexMgr.deleteAll(tx, "NoIndexTable");

            tx.commit();
        }

        @Test
        @DisplayName("deleteAll with multiple indexes (break condition)")
        void deleteAllWithMultipleIndexes() {
            TxMgrBase txMgr = createTxMgr("deleteAllWithMultipleIndexes", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "MultiIndexTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int id1 = indexMgr.persistIndexDescriptor(tx, "MultiIndexTable", "id", IndexType.STATIC_HASH);
            int id2 = indexMgr.persistIndexDescriptor(tx, "MultiIndexTable", "name", IndexType.STATIC_HASH);
            int id3 = indexMgr.persistIndexDescriptor(tx, "MultiIndexTable", "salary", IndexType.STATIC_HASH);

            // Insert some data into indexes
            IndexBase index1 = indexMgr.instantiate(tx, id1);
            IndexBase index2 = indexMgr.instantiate(tx, id2);
            IndexBase index3 = indexMgr.instantiate(tx, id3);

            TableScanBase ts = new TableScan(tx, "MultiIndexTable", layout);
            for (int i = 0; i < 5; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("name", "name_" + i);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0 + i);
                RID rid = ts.getRid();
                index1.insert(new Datum(i), rid);
                index2.insert(new Datum("name_" + i), rid);
                index3.insert(new Datum(50000.0 + i), rid);
            }
            ts.close();
            index1.close();
            index2.close();
            index3.close();

            // Delete all - should hit break condition when deletedCount < val.size()
            // Note: The table must have data records for deleteAll to work properly
            indexMgr.deleteAll(tx, "MultiIndexTable");

            tx.commit();
        }

        @Test
        @DisplayName("deleteAll with single index (no break)")
        void deleteAllWithSingleIndex() {
            TxMgrBase txMgr = createTxMgr("deleteAllWithSingleIndex", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "SingleIndexTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int id = indexMgr.persistIndexDescriptor(tx, "SingleIndexTable", "id", IndexType.STATIC_HASH);

            // Insert some data
            IndexBase index = indexMgr.instantiate(tx, id);
            TableScanBase ts = new TableScan(tx, "SingleIndexTable", layout);
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "test");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            index.insert(new Datum(1), ts.getRid());
            ts.close();
            index.close();

            // Delete all - single index, deletedCount will equal val.size() so no break
            // Note: The table must have data records for deleteAll to work properly
            indexMgr.deleteAll(tx, "SingleIndexTable");

            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 2: IndexMgr Static Methods
    // ========================================================================

    @Nested
    @DisplayName("IndexMgr Static Methods")
    class IndexMgrStaticMethodTests {

        @Test
        @DisplayName("getBucketTableName with various IDs and buckets")
        void getBucketTableNameVariousParameters() {
            // Test various combinations
            assertEquals("0>0", IndexMgr.getBucketTableName(0, 0));
            assertEquals("123>5", IndexMgr.getBucketTableName(123, 5));
            assertEquals("999999>42", IndexMgr.getBucketTableName(999999, 42));
            assertEquals("1>0", IndexMgr.getBucketTableName(1, 0));
            assertEquals("100>100", IndexMgr.getBucketTableName(100, 100));
        }

        @Test
        @DisplayName("static delete() with empty table scan")
        void staticDeleteWithEmptyTableScan() {
            TxMgrBase txMgr = createTxMgr("staticDeleteWithEmptyTableScan", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "EmptyTable", schema, tx);

            // Create empty table scan (no records inserted)
            TableScanBase emptyScan = new TableScan(tx, "EmptyTable", layout);

            // Static delete on empty scan should not throw
            IndexMgr.delete(emptyScan);
            emptyScan.close();

            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 3: Index.next() Edge Cases
    // ========================================================================

    @Nested
    @DisplayName("Index.next() Edge Cases")
    class IndexNextEdgeCasesTests {

        @Test
        @DisplayName("Hash collision where hash matches but value doesn't")
        void hashCollisionHashMatchesButValueDoesnt() {
            TxMgrBase txMgr = createTxMgr("hashCollisionHashMatchesButValueDoesnt", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "CollisionTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "CollisionTable", "name", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Create strings that might have same hash code but different values
            // We'll insert one value and search for a different value with same hash
            TableScanBase ts = new TableScan(tx, "CollisionTable", layout);

            // Insert record with value "target"
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "target");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID targetRid = ts.getRid();
            index.insert(new Datum("target"), targetRid);

            // Insert another record with different value but potentially same hash bucket
            ts.insert();
            ts.setInt("id", 2);
            ts.setString("name", "different");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 60000.0);
            RID differentRid = ts.getRid();
            index.insert(new Datum("different"), differentRid);
            ts.close();

            // Search for "target" - should find it even if "different" is in same bucket
            index.beforeFirst(new Datum("target"));
            assertTrue(index.next());
            assertEquals(targetRid, index.getRID());
            assertFalse(index.next()); // Should only find one match

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("next() handles multiple records in same bucket")
        void nextHandlesMultipleRecordsInSameBucket() {
            TxMgrBase txMgr = createTxMgr("nextHandlesMultipleRecordsInSameBucket", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "MultiBucketTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "MultiBucketTable", "name", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert multiple records with same value (will be in same bucket)
            TableScanBase ts = new TableScan(tx, "MultiBucketTable", layout);
            List<RID> rids = new ArrayList<>();
            String searchValue = "duplicate";

            for (int i = 0; i < 5; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("name", searchValue);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0 + i);
                RID rid = ts.getRid();
                rids.add(rid);
                index.insert(new Datum(searchValue), rid);
            }
            ts.close();

            // Search should find all 5 records
            index.beforeFirst(new Datum(searchValue));
            int foundCount = 0;
            while (index.next()) {
                RID foundRid = index.getRID();
                assertTrue(rids.contains(foundRid));
                foundCount++;
            }
            assertEquals(5, foundCount);

            index.close();
            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 4: Index.insert() Edge Cases
    // ========================================================================

    @Nested
    @DisplayName("Index.insert() Edge Cases")
    class IndexInsertEdgeCasesTests {

        @Test
        @DisplayName("insert when no search is active (searchKey == null)")
        void insertWhenNoSearchActive() {
            TxMgrBase txMgr = createTxMgr("insertWhenNoSearchActive", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "NoSearchTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "NoSearchTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert without calling beforeFirst (searchKey will be null)
            TableScanBase ts = new TableScan(tx, "NoSearchTable", layout);
            ts.insert();
            ts.setInt("id", 42);
            ts.setString("name", "test");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID rid = ts.getRid();
            ts.close();

            // Insert into index - searchKey is null, so tempRID should be null
            index.insert(new Datum(42), rid);

            // Verify it was inserted
            index.beforeFirst(new Datum(42));
            assertTrue(index.next());
            assertEquals(rid, index.getRID());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("insert when search is active (searchKey != null, tempRID != null)")
        void insertWhenSearchActive() {
            TxMgrBase txMgr = createTxMgr("insertWhenSearchActive", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "ActiveSearchTbl", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "ActiveSearchTbl", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert first record
            TableScanBase ts = new TableScan(tx, "ActiveSearchTbl", layout);
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "first");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID rid1 = ts.getRid();
            index.insert(new Datum(1), rid1);

            // Start a search
            index.beforeFirst(new Datum(1));
            index.next(); // Position on first record
            RID searchRid = index.getRID();

            // Insert another record while search is active
            ts.insert();
            ts.setInt("id", 2);
            ts.setString("name", "second");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 60000.0);
            RID rid2 = ts.getRid();
            index.insert(new Datum(2), rid2);

            // Search should still be positioned correctly
            // The insert should have preserved search position
            assertEquals(searchRid, index.getRID());

            ts.close();
            index.close();
            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 5: Index.delete() Edge Cases
    // ========================================================================

    @Nested
    @DisplayName("Index.delete() Edge Cases")
    class IndexDeleteEdgeCasesTests {

        @Test
        @DisplayName("delete when no search is active")
        void deleteWhenNoSearchActive() {
            TxMgrBase txMgr = createTxMgr("deleteWhenNoSearchActive", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "NoSearchDelTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "NoSearchDelTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert record
            TableScanBase ts = new TableScan(tx, "NoSearchDelTable", layout);
            ts.insert();
            ts.setInt("id", 99);
            ts.setString("name", "toDelete");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID rid = ts.getRid();
            ts.close();

            index.insert(new Datum(99), rid);

            // Delete without calling beforeFirst (searchKey will be null)
            index.delete(new Datum(99), rid);

            // Verify it was deleted
            index.beforeFirst(new Datum(99));
            assertFalse(index.next());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("delete when search is active")
        void deleteWhenSearchActive() {
            TxMgrBase txMgr = createTxMgr("deleteWhenSearchActive", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "ActiveSearchDelT", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "ActiveSearchDelT", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert records
            TableScanBase ts = new TableScan(tx, "ActiveSearchDelT", layout);
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "first");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID rid1 = ts.getRid();
            index.insert(new Datum(1), rid1);

            ts.insert();
            ts.setInt("id", 2);
            ts.setString("name", "second");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 60000.0);
            RID rid2 = ts.getRid();
            index.insert(new Datum(2), rid2);
            ts.close();

            // Start a search
            index.beforeFirst(new Datum(1));
            index.next();
            RID searchRid = index.getRID();

            // Delete another record while search is active
            index.delete(new Datum(2), rid2);

            // Search should still be positioned correctly
            assertEquals(searchRid, index.getRID());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("delete where RID matches but value doesn't (continues loop)")
        void deleteWhereRidMatchesButValueDoesnt() {
            TxMgrBase txMgr = createTxMgr("deleteWhereRidMatchesButValueDoesnt", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "RidMatchTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "RidMatchTable", "name", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert records with different names but potentially same hash bucket
            TableScanBase ts = new TableScan(tx, "RidMatchTable", layout);

            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "value1");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID rid1 = ts.getRid();
            index.insert(new Datum("value1"), rid1);

            ts.insert();
            ts.setInt("id", 2);
            ts.setString("name", "value2");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 60000.0);
            RID rid2 = ts.getRid();
            index.insert(new Datum("value2"), rid2);
            ts.close();

            // Try to delete with wrong value but correct RID
            // This should not delete anything because value doesn't match
            index.delete(new Datum("wrongValue"), rid1);

            // Verify value1 still exists
            index.beforeFirst(new Datum("value1"));
            assertTrue(index.next());
            assertEquals(rid1, index.getRID());

            index.close();
            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 6: Index.setIndexScan() Edge Cases
    // ========================================================================

    @Nested
    @DisplayName("Index.setIndexScan() Edge Cases")
    class IndexSetIndexScanEdgeCasesTests {

        @Test
        @DisplayName("Switching between buckets (closes old scan)")
        void switchingBetweenBuckets() {
            TxMgrBase txMgr = createTxMgr("switchingBetweenBuckets", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "BucketSwitchTbl", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "BucketSwitchTbl", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert records that will hash to different buckets
            // Use values that will definitely hash to different buckets
            TableScanBase ts = new TableScan(tx, "BucketSwitchTbl", layout);

            // Insert record with id that will hash to bucket 0
            ts.insert();
            ts.setInt("id", 0);
            ts.setString("name", "zero");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID rid0 = ts.getRid();
            index.insert(new Datum(0), rid0);

            // Insert record with id that will hash to a different bucket
            // We'll use a value that hashes to a different bucket
            ts.insert();
            ts.setInt("id", 1000); // Different bucket
            ts.setString("name", "thousand");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 60000.0);
            RID rid1000 = ts.getRid();
            index.insert(new Datum(1000), rid1000);
            ts.close();

            // Search for value in first bucket - this creates indexScan
            index.beforeFirst(new Datum(0));
            index.next();

            // Search for value in different bucket - should close old scan and create new
            // one
            index.beforeFirst(new Datum(1000));
            assertTrue(index.next());
            assertEquals(rid1000, index.getRID());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Using same bucket (reuses scan)")
        void usingSameBucket() {
            TxMgrBase txMgr = createTxMgr("usingSameBucket", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "SameBucketTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "SameBucketTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert records that will hash to same bucket
            TableScanBase ts = new TableScan(tx, "SameBucketTable", layout);

            ts.insert();
            ts.setInt("id", 0);
            ts.setString("name", "zero");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID rid0 = ts.getRid();
            index.insert(new Datum(0), rid0);

            // Use a value that hashes to same bucket (same modulo result)
            // If bucketCount is, say, 10, then 0 and 10 hash to same bucket
            int bucketCount = DBConfiguration.INSTANCE.nStaticHashBuckets();
            int sameBucketValue = bucketCount; // Will hash to same bucket as 0

            ts.insert();
            ts.setInt("id", sameBucketValue);
            ts.setString("name", "sameBucket");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 60000.0);
            RID ridSame = ts.getRid();
            index.insert(new Datum(sameBucketValue), ridSame);
            ts.close();

            // Search for first value - creates indexScan
            index.beforeFirst(new Datum(0));
            index.next();

            // Search for value in same bucket - should reuse scan
            index.beforeFirst(new Datum(sameBucketValue));
            assertTrue(index.next());
            assertEquals(ridSame, index.getRID());

            index.close();
            tx.commit();
        }

        @Test
        @DisplayName("Initial scan creation")
        void initialScanCreation() {
            TxMgrBase txMgr = createTxMgr("initialScanCreation", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            LayoutBase layout = createTestTable(tableMgr, "InitialScanTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "InitialScanTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert record
            TableScanBase ts = new TableScan(tx, "InitialScanTable", layout);
            ts.insert();
            ts.setInt("id", 42);
            ts.setString("name", "test");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);
            RID rid = ts.getRid();
            index.insert(new Datum(42), rid);
            ts.close();

            // First beforeFirst should create indexScan (it's null initially)
            index.beforeFirst(new Datum(42));
            assertTrue(index.next());
            assertEquals(rid, index.getRID());

            index.close();
            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 7: Index.close() Edge Cases
    // ========================================================================

    @Nested
    @DisplayName("Index.close() Edge Cases")
    class IndexCloseEdgeCasesTests {

        @Test
        @DisplayName("close when indexScan is null")
        void closeWhenIndexScanIsNull() {
            TxMgrBase txMgr = createTxMgr("closeWhenIndexScanIsNull", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "CloseTestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "CloseTestTable", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Close without calling beforeFirst (indexScan is null)
            // Should not throw
            index.close();

            tx.commit();
        }

        @Test
        @DisplayName("close when tableScan is null")
        void closeWhenTableScanIsNull() {
            TxMgrBase txMgr = createTxMgr("closeWhenTableScanIsNull", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "CloseTestTable2", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "CloseTestTable2", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Call beforeFirst but not next (tableScan is null)
            index.beforeFirst(new Datum(42));

            // Close - tableScan is null, should not throw
            index.close();

            tx.commit();
        }

        @Test
        @DisplayName("close when both are null")
        void closeWhenBothAreNull() {
            TxMgrBase txMgr = createTxMgr("closeWhenBothAreNull", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "CloseTestTable3", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "CloseTestTable3", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Close immediately (both indexScan and tableScan are null)
            index.close();

            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 8: IndexDescriptor Coverage
    // ========================================================================

    @Nested
    @DisplayName("IndexDescriptor Coverage")
    class IndexDescriptorCoverageTests {

        @Test
        @DisplayName("toString() with various values")
        void toStringWithVariousValues() {
            TxMgrBase txMgr = createTxMgr("toStringWithVariousValues", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "ToStringTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "ToStringTable", "name", IndexType.STATIC_HASH);
            IndexDescriptorBase descriptor = indexMgr.get(tx, indexId);

            String str = descriptor.toString();
            assertNotNull(str);
            assertTrue(str.contains("ToStringTable"));
            assertTrue(str.contains("name"));
            assertTrue(str.contains("STATIC_HASH"));

            tx.commit();
        }

        @Test
        @DisplayName("equals() with different scenarios")
        void equalsWithDifferentScenarios() {
            TxMgrBase txMgr = createTxMgr("equalsWithDifferentScenarios", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "EqualsTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId1 = indexMgr.persistIndexDescriptor(tx, "EqualsTable", "id", IndexType.STATIC_HASH);
            int indexId2 = indexMgr.persistIndexDescriptor(tx, "EqualsTable", "name", IndexType.STATIC_HASH);

            IndexDescriptorBase desc1 = indexMgr.get(tx, indexId1);
            IndexDescriptorBase desc2 = indexMgr.get(tx, indexId2);
            IndexDescriptorBase desc1Again = indexMgr.get(tx, indexId1);

            // Same descriptor should be equal
            assertEquals(desc1, desc1Again);

            // Different descriptors should not be equal
            assertNotEquals(desc1, desc2);

            // Should not equal null
            assertNotEquals(desc1, null);

            // Should not equal different type
            assertNotEquals(desc1, "not a descriptor");

            tx.commit();
        }

        @Test
        @DisplayName("hashCode() consistency")
        void hashCodeConsistency() {
            TxMgrBase txMgr = createTxMgr("hashCodeConsistency", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "HashCodeTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "HashCodeTable", "id", IndexType.STATIC_HASH);
            IndexDescriptorBase descriptor = indexMgr.get(tx, indexId);

            int hashCode1 = descriptor.hashCode();
            int hashCode2 = descriptor.hashCode();

            // Hash code should be consistent
            assertEquals(hashCode1, hashCode2);

            // Equal objects should have same hash code
            IndexDescriptorBase descriptor2 = indexMgr.get(tx, indexId);
            assertEquals(descriptor, descriptor2);
            assertEquals(descriptor.hashCode(), descriptor2.hashCode());

            tx.commit();
        }

        @Test
        @DisplayName("getIndexedTableSchema() returns correct schema")
        void getIndexedTableSchemaReturnsCorrectSchema() {
            TxMgrBase txMgr = createTxMgr("getIndexedTableSchemaReturnsCorrectSchema", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "SchemaTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "SchemaTable", "id", IndexType.STATIC_HASH);
            IndexDescriptorBase descriptor = indexMgr.get(tx, indexId);

            SchemaBase indexedSchema = descriptor.getIndexedTableSchema();
            assertNotNull(indexedSchema);

            // Verify schema contains all expected fields
            assertTrue(indexedSchema.hasField("id"));
            assertTrue(indexedSchema.hasField("name"));
            assertTrue(indexedSchema.hasField("active"));
            assertTrue(indexedSchema.hasField("salary"));

            // Verify schema matches the original table schema
            LayoutBase tableLayout = tableMgr.getLayout("SchemaTable", tx);
            SchemaBase tableSchema = tableLayout.schema();
            assertEquals(tableSchema, indexedSchema);

            tx.commit();
        }

        @Test
        @DisplayName("getIndexedTableSchema() with different field types")
        void getIndexedTableSchemaWithDifferentFieldTypes() {
            TxMgrBase txMgr = createTxMgr("getIndexedTableSchemaWithDifferentFieldTypes", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            Schema schema = new Schema();
            schema.addIntField("intField");
            schema.addStringField("strField", 32);
            schema.addBooleanField("boolField");
            schema.addDoubleField("doubleField");
            createTestTable(tableMgr, "TypesTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "TypesTable", "intField", IndexType.STATIC_HASH);
            IndexDescriptorBase descriptor = indexMgr.get(tx, indexId);

            SchemaBase indexedSchema = descriptor.getIndexedTableSchema();
            assertNotNull(indexedSchema);

            // Verify all field types are preserved
            assertTrue(indexedSchema.hasField("intField"));
            assertTrue(indexedSchema.hasField("strField"));
            assertTrue(indexedSchema.hasField("boolField"));
            assertTrue(indexedSchema.hasField("doubleField"));

            // Verify field types match
            LayoutBase tableLayout = tableMgr.getLayout("TypesTable", tx);
            SchemaBase tableSchema = tableLayout.schema();
            assertEquals(tableSchema.type("intField"), indexedSchema.type("intField"));
            assertEquals(tableSchema.type("strField"), indexedSchema.type("strField"));
            assertEquals(tableSchema.type("boolField"), indexedSchema.type("boolField"));
            assertEquals(tableSchema.type("doubleField"), indexedSchema.type("doubleField"));

            tx.commit();
        }

        @Test
        @DisplayName("getIndexedTableSchema() persists correctly across transactions")
        void getIndexedTableSchemaPersistsCorrectly() {
            String testName = "getIndexedTableSchemaPersistsCorrectly";

            // First transaction: create table and index
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgrBase tableMgr1 = new TableMgr(tx1);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr1, "PersistSchemaTbl", schema, tx1);

            IndexMgrBase indexMgr1 = new IndexMgr(tx1, tableMgr1);
            int indexId = indexMgr1.persistIndexDescriptor(tx1, "PersistSchemaTbl", "name", IndexType.STATIC_HASH);
            tx1.commit();

            // Second transaction: verify schema persists correctly
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();

            TableMgrBase tableMgr2 = new TableMgr(tx2);
            IndexMgrBase indexMgr2 = new IndexMgr(tx2, tableMgr2);
            IndexDescriptorBase descriptor = indexMgr2.get(tx2, indexId);

            SchemaBase indexedSchema = descriptor.getIndexedTableSchema();
            assertNotNull(indexedSchema);

            // Verify schema matches the table schema
            LayoutBase tableLayout = tableMgr2.getLayout("PersistSchemaTbl", tx2);
            SchemaBase tableSchema = tableLayout.schema();
            assertEquals(tableSchema, indexedSchema);

            // Verify all fields are present
            assertTrue(indexedSchema.hasField("id"));
            assertTrue(indexedSchema.hasField("name"));
            assertTrue(indexedSchema.hasField("active"));
            assertTrue(indexedSchema.hasField("salary"));

            tx2.commit();
        }

        @Test
        @DisplayName("getIndexedTableSchema() returns same schema for multiple indexes on same table")
        void getIndexedTableSchemaSameForMultipleIndexes() {
            TxMgrBase txMgr = createTxMgr("getIndexedTableSchemaSameForMultipleIndexes", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "MultiIdxSchemaT", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId1 = indexMgr.persistIndexDescriptor(tx, "MultiIdxSchemaT", "id", IndexType.STATIC_HASH);
            int indexId2 = indexMgr.persistIndexDescriptor(tx, "MultiIdxSchemaT", "name", IndexType.STATIC_HASH);
            int indexId3 = indexMgr.persistIndexDescriptor(tx, "MultiIdxSchemaT", "salary", IndexType.STATIC_HASH);

            IndexDescriptorBase desc1 = indexMgr.get(tx, indexId1);
            IndexDescriptorBase desc2 = indexMgr.get(tx, indexId2);
            IndexDescriptorBase desc3 = indexMgr.get(tx, indexId3);

            SchemaBase schema1 = desc1.getIndexedTableSchema();
            SchemaBase schema2 = desc2.getIndexedTableSchema();
            SchemaBase schema3 = desc3.getIndexedTableSchema();

            // All indexes on the same table should return the same schema
            assertEquals(schema1, schema2);
            assertEquals(schema2, schema3);
            assertEquals(schema1, schema3);

            // Verify schema matches the table schema
            LayoutBase tableLayout = tableMgr.getLayout("MultiIdxSchemaT", tx);
            SchemaBase tableSchema = tableLayout.schema();
            assertEquals(tableSchema, schema1);
            assertEquals(tableSchema, schema2);
            assertEquals(tableSchema, schema3);

            tx.commit();
        }

        @Test
        @DisplayName("getIndexedTableSchema() with custom schema fields")
        void getIndexedTableSchemaWithCustomSchema() {
            TxMgrBase txMgr = createTxMgr("getIndexedTableSchemaWithCustomSchema", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            Schema customSchema = new Schema();
            customSchema.addIntField("employeeId");
            customSchema.addStringField("fullName", 50);
            customSchema.addDoubleField("hourlyRate");
            customSchema.addBooleanField("isManager");
            createTestTable(tableMgr, "CustomSchemaTbl", customSchema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "CustomSchemaTbl", "employeeId", IndexType.STATIC_HASH);
            IndexDescriptorBase descriptor = indexMgr.get(tx, indexId);

            SchemaBase indexedSchema = descriptor.getIndexedTableSchema();
            assertNotNull(indexedSchema);

            // Verify custom fields are present
            assertTrue(indexedSchema.hasField("employeeId"));
            assertTrue(indexedSchema.hasField("fullName"));
            assertTrue(indexedSchema.hasField("hourlyRate"));
            assertTrue(indexedSchema.hasField("isManager"));

            // Verify schema matches original
            LayoutBase tableLayout = tableMgr.getLayout("CustomSchemaTbl", tx);
            SchemaBase tableSchema = tableLayout.schema();
            assertEquals(tableSchema, indexedSchema);

            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 9: IndexMgr Error Checking Tests
    // ========================================================================

    @Nested
    @DisplayName("IndexMgr Error Checking Tests")
    class IndexMgrErrorCheckingTests {

        // ========================================================================
        // Constructor Error Checks
        // ========================================================================

        @Test
        @DisplayName("Constructor throws IAE for null tx")
        void constructorThrowsIAEForNullTx() {
            TxMgrBase txMgr = createTxMgr("constructorThrowsIAEForNullTx", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);

            assertThrows(IllegalArgumentException.class, () -> {
                new IndexMgr(null, tableMgr);
            });

            tx.rollback();
        }

        @Test
        @DisplayName("Constructor throws IAE for null tableMgr")
        void constructorThrowsIAEForNullTableMgr() {
            TxMgrBase txMgr = createTxMgr("constructorThrowsIAEForNullTableMgr", true);
            TxBase tx = txMgr.newTx();

            assertThrows(IllegalArgumentException.class, () -> {
                new IndexMgr(tx, null);
            });

            tx.rollback();
        }

        @Test
        @DisplayName("Constructor throws IllegalStateException when DBConfiguration.isDBStartup() throws")
        void constructorThrowsWhenDBConfigurationThrows() {
            // First set proper properties to create TxMgr and TableMgr
            Properties properProps = new Properties();
            properProps.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
            DBConfiguration.INSTANCE.get().setConfiguration(properProps);

            // Create TxMgr and TableMgr first (they need proper config)
            Path testDir = Path.of(TEST_BASE_DIR, "constructorExceptionTest");
            try {
                if (Files.exists(testDir)) {
                    deleteDirectory(testDir);
                }
            } catch (Exception e) {
                logger.warn("Could not clean up test directory: {}", testDir);
            }

            FileMgrBase fileMgr = new FileMgr(testDir.toFile(), BLOCK_SIZE);
            LogMgrBase logMgr = new LogMgr(fileMgr, LOG_FILE);
            BufferMgrBase bufferMgr = new BufferMgr(fileMgr, logMgr, BUFFER_SIZE, MAX_WAIT_TIME);
            TxMgrBase txMgr = new TxMgr(fileMgr, logMgr, bufferMgr, MAX_WAIT_TIME);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);

            // Now set empty properties (no DB_STARTUP property) to trigger exception in
            // IndexMgr constructor
            Properties emptyProps = new Properties();
            DBConfiguration.INSTANCE.get().setConfiguration(emptyProps);

            // IndexMgr constructor should throw IllegalStateException
            assertThrows(IllegalStateException.class, () -> {
                new IndexMgr(tx, tableMgr);
            });

            tx.rollback();
        }

        // ========================================================================
        // persistIndexDescriptor Error Checks and Functionality
        // ========================================================================

        @Test
        @DisplayName("persistIndexDescriptor throws IAE for null tx")
        void persistIndexDescriptorThrowsIAEForNullTx() {
            TxMgrBase txMgr = createTxMgr("persistIndexDescriptorThrowsIAEForNullTx", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "ErrorTestTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            assertThrows(IllegalArgumentException.class, () -> {
                indexMgr.persistIndexDescriptor(null, "ErrorTestTable", "id", IndexType.STATIC_HASH);
            });

            tx.rollback();
        }

        @Test
        @DisplayName("persistIndexDescriptor returns same ID for already existing index")
        void persistIndexDescriptorReturnsSameIdForExistingIndex() {
            TxMgrBase txMgr = createTxMgr("persistIndexDescriptorReturnsSameIdForExistingIndex", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "ExistingIdxTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            // Create index for the first time
            int firstId = indexMgr.persistIndexDescriptor(tx, "ExistingIdxTable", "id", IndexType.STATIC_HASH);

            // Call persistIndexDescriptor again with same table and field
            int secondId = indexMgr.persistIndexDescriptor(tx, "ExistingIdxTable", "id", IndexType.STATIC_HASH);

            // Should return the same ID (idempotent behavior)
            assertEquals(firstId, secondId, "Calling persistIndexDescriptor on existing index should return same ID");

            // Verify only one index exists for this table/field combination
            Set<Integer> ids = indexMgr.indexIds(tx, "ExistingIdxTable");
            assertEquals(1, ids.size(), "Should only have one index for this field");
            assertTrue(ids.contains(firstId));

            // Verify the descriptor is the same
            IndexDescriptorBase desc1 = indexMgr.get(tx, firstId);
            IndexDescriptorBase desc2 = indexMgr.get(tx, secondId);
            assertEquals(desc1, desc2);

            tx.commit();
        }

        @Test
        @DisplayName("persistIndexDescriptor on existing index when table has multiple indexes")
        void persistIndexDescriptorOnExistingIndexWithMultipleIndexes() {
            TxMgrBase txMgr = createTxMgr("persistIndexDescriptorOnExistingIndexWithMultipleIndexes", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "MultiIdxTable3", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            // Create multiple indexes
            int idIndexId = indexMgr.persistIndexDescriptor(tx, "MultiIdxTable3", "id", IndexType.STATIC_HASH);
            int nameIndexId = indexMgr.persistIndexDescriptor(tx, "MultiIdxTable3", "name", IndexType.STATIC_HASH);
            int salaryIndexId = indexMgr.persistIndexDescriptor(tx, "MultiIdxTable3", "salary", IndexType.STATIC_HASH);

            // Verify we have 3 indexes
            Set<Integer> idsBefore = indexMgr.indexIds(tx, "MultiIdxTable3");
            assertEquals(3, idsBefore.size());

            // Call persistIndexDescriptor again on the "id" field (should return existing
            // ID)
            int idIndexIdAgain = indexMgr.persistIndexDescriptor(tx, "MultiIdxTable3", "id", IndexType.STATIC_HASH);

            // Should return the same ID
            assertEquals(idIndexId, idIndexIdAgain, "Should return same ID for existing index");

            // Verify we still have only 3 indexes (no duplicate created)
            Set<Integer> idsAfter = indexMgr.indexIds(tx, "MultiIdxTable3");
            assertEquals(3, idsAfter.size(), "Should still have only 3 indexes");
            assertTrue(idsAfter.contains(idIndexId));
            assertTrue(idsAfter.contains(nameIndexId));
            assertTrue(idsAfter.contains(salaryIndexId));

            // Verify descriptors are unchanged
            IndexDescriptorBase desc1 = indexMgr.get(tx, idIndexId);
            IndexDescriptorBase desc2 = indexMgr.get(tx, idIndexIdAgain);
            assertEquals(desc1, desc2);

            tx.commit();
        }

        @Test
        @DisplayName("persistIndexDescriptor on existing index persists correctly across transactions")
        void persistIndexDescriptorOnExistingIndexPersistsCorrectly() {
            String testName = "persistIndexDescriptorOnExistingIndexPersistsCorrectly";

            // First transaction: create index
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgrBase tableMgr1 = new TableMgr(tx1);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr1, "PersistExistT", schema, tx1);

            IndexMgrBase indexMgr1 = new IndexMgr(tx1, tableMgr1);
            int originalId = indexMgr1.persistIndexDescriptor(tx1, "PersistExistT", "id", IndexType.STATIC_HASH);
            tx1.commit();

            // Second transaction: call persistIndexDescriptor again (should return same ID)
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();

            TableMgrBase tableMgr2 = new TableMgr(tx2);
            IndexMgrBase indexMgr2 = new IndexMgr(tx2, tableMgr2);

            // Call persistIndexDescriptor on the existing index
            int persistedId = indexMgr2.persistIndexDescriptor(tx2, "PersistExistT", "id", IndexType.STATIC_HASH);

            // Should return the same ID
            assertEquals(originalId, persistedId, "Should return same ID after database restart");

            // Verify only one index exists
            Set<Integer> ids = indexMgr2.indexIds(tx2, "PersistExistT");
            assertEquals(1, ids.size());
            assertTrue(ids.contains(originalId));

            tx2.commit();
        }

        // ========================================================================
        // indexIds Error Checks and Functionality
        // ========================================================================

        @Test
        @DisplayName("indexIds throws IAE for null tx")
        void indexIdsThrowsIAEForNullTx() {
            TxMgrBase txMgr = createTxMgr("indexIdsThrowsIAEForNullTx", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "ErrorTestTable2", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            assertThrows(IllegalArgumentException.class, () -> {
                indexMgr.indexIds(null, "ErrorTestTable2");
            });

            tx.rollback();
        }

        @Test
        @DisplayName("indexIds throws IAE for non-existent table (checkCatalog path)")
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
        @DisplayName("indexIds returns empty set for table with no indexes (getOrDefault path)")
        void indexIdsReturnsEmptySetForTableWithNoIndexes() {
            TxMgrBase txMgr = createTxMgr("indexIdsReturnsEmptySetForTableWithNoIndexes", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "NoIndexTable2", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            // Table exists but has no indexes - should return empty set
            Set<Integer> ids = indexMgr.indexIds(tx, "NoIndexTable2");
            assertNotNull(ids);
            assertTrue(ids.isEmpty());

            tx.commit();
        }

        @Test
        @DisplayName("indexIds returns correct set for table with single index")
        void indexIdsReturnsCorrectSetForSingleIndex() {
            TxMgrBase txMgr = createTxMgr("indexIdsReturnsCorrectSetForSingleIndex", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "SingleIdxTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "SingleIdxTable", "id", IndexType.STATIC_HASH);

            Set<Integer> ids = indexMgr.indexIds(tx, "SingleIdxTable");
            assertEquals(1, ids.size());
            assertTrue(ids.contains(indexId));

            tx.commit();
        }

        @Test
        @DisplayName("indexIds returns correct set for table with multiple indexes")
        void indexIdsReturnsCorrectSetForMultipleIndexes() {
            TxMgrBase txMgr = createTxMgr("indexIdsReturnsCorrectSetForMultipleIndexes", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "MultiIdxTable2", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int id1 = indexMgr.persistIndexDescriptor(tx, "MultiIdxTable2", "id", IndexType.STATIC_HASH);
            int id2 = indexMgr.persistIndexDescriptor(tx, "MultiIdxTable2", "name", IndexType.STATIC_HASH);
            int id3 = indexMgr.persistIndexDescriptor(tx, "MultiIdxTable2", "salary", IndexType.STATIC_HASH);

            Set<Integer> ids = indexMgr.indexIds(tx, "MultiIdxTable2");
            assertEquals(3, ids.size());
            assertTrue(ids.contains(id1));
            assertTrue(ids.contains(id2));
            assertTrue(ids.contains(id3));

            tx.commit();
        }

        @Test
        @DisplayName("indexIds returns correct set after persistence and restart")
        void indexIdsReturnsCorrectSetAfterPersistence() {
            String testName = "indexIdsReturnsCorrectSetAfterPersistence";

            // First transaction: create table and indexes
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgrBase tableMgr1 = new TableMgr(tx1);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr1, "PersistIdxTable", schema, tx1);

            IndexMgrBase indexMgr1 = new IndexMgr(tx1, tableMgr1);
            int id1 = indexMgr1.persistIndexDescriptor(tx1, "PersistIdxTable", "id", IndexType.STATIC_HASH);
            int id2 = indexMgr1.persistIndexDescriptor(tx1, "PersistIdxTable", "name", IndexType.STATIC_HASH);
            tx1.commit();

            // Second transaction: verify indexIds persists correctly
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();

            TableMgrBase tableMgr2 = new TableMgr(tx2);
            IndexMgrBase indexMgr2 = new IndexMgr(tx2, tableMgr2);

            Set<Integer> ids = indexMgr2.indexIds(tx2, "PersistIdxTable");
            assertEquals(2, ids.size());
            assertTrue(ids.contains(id1));
            assertTrue(ids.contains(id2));

            tx2.commit();
        }

        // ========================================================================
        // checkCatalog Error Checks (tested indirectly)
        // ========================================================================

        @Test
        @DisplayName("checkCatalog(String, TxBase) throws IAE for non-existent table via indexIds")
        void checkCatalogThrowsIAEForNonExistentTableViaIndexIds() {
            TxMgrBase txMgr = createTxMgr("checkCatalogThrowsIAEForNonExistentTableViaIndexIds", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            // This tests checkCatalog(String tableName, TxBase tx) indirectly
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
                indexMgr.indexIds(tx, "NonExistentTableName");
            });

            assertTrue(exception.getMessage().contains("Table must have a defined layout"));

            tx.rollback();
        }

        @Test
        @DisplayName("checkCatalog(String, String, TxBase) throws IAE for non-existent table via persistIndexDescriptor")
        void checkCatalogThrowsIAEForNonExistentTableViaPersist() {
            TxMgrBase txMgr = createTxMgr("checkCatalogThrowsIAEForNonExistentTableViaPersist", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            // This tests checkCatalog(String tableName, String fieldName, TxBase tx)
            // with null layout path
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
                indexMgr.persistIndexDescriptor(tx, "NonExistentTable", "id", IndexType.STATIC_HASH);
            });

            assertTrue(exception.getMessage().contains("Table must have a defined layout"));

            tx.rollback();
        }

        @Test
        @DisplayName("checkCatalog(String, String, TxBase) throws IAE for non-existent field via persistIndexDescriptor")
        void checkCatalogThrowsIAEForNonExistentFieldViaPersist() {
            TxMgrBase txMgr = createTxMgr("checkCatalogThrowsIAEForNonExistentFieldViaPersist", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "FieldCheckTable", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

            // This tests checkCatalog(String tableName, String fieldName, TxBase tx)
            // with non-existent field path
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
                indexMgr.persistIndexDescriptor(tx, "FieldCheckTable", "nonExistentField", IndexType.STATIC_HASH);
            });

            assertTrue(exception.getMessage().contains("Field must be defined in table layout"));

            tx.rollback();
        }

        // ========================================================================
        // get Error Checks
        // ========================================================================

        @Test
        @DisplayName("get throws IAE for null tx")
        void getThrowsIAEForNullTx() {
            TxMgrBase txMgr = createTxMgr("getThrowsIAEForNullTx", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "ErrorTestTable3", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "ErrorTestTable3", "id", IndexType.STATIC_HASH);

            assertThrows(IllegalArgumentException.class, () -> {
                indexMgr.get(null, indexId);
            });

            tx.rollback();
        }

        // ========================================================================
        // instantiate Error Checks
        // ========================================================================

        @Test
        @DisplayName("instantiate throws IAE for null tx")
        void instantiateThrowsIAEForNullTx() {
            TxMgrBase txMgr = createTxMgr("instantiateThrowsIAEForNullTx", true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();
            createTestTable(tableMgr, "ErrorTestTable4", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "ErrorTestTable4", "id", IndexType.STATIC_HASH);

            assertThrows(IllegalArgumentException.class, () -> {
                indexMgr.instantiate(null, indexId);
            });

            tx.rollback();
        }
    }
}
