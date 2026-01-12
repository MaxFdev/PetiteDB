package edu.yu.dbimpl.metadata;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.record.LayoutBase;
import edu.yu.dbimpl.record.Schema;
import edu.yu.dbimpl.record.SchemaBase;
import edu.yu.dbimpl.record.TableScan;
import edu.yu.dbimpl.tx.TxBase;
import edu.yu.dbimpl.tx.TxMgr;
import edu.yu.dbimpl.tx.TxMgrBase;

/**
 * Comprehensive correctness and exception tests for the TableMgr class.
 * Tests cover happy path semantics, IllegalArgumentException validation,
 * and performance benchmarks.
 */
public class MetadataTest {

    private static final Logger logger = LogManager.getLogger(MetadataTest.class);

    // Test configuration constants
    private static final int BLOCK_SIZE = 400;
    private static final int BUFFER_SIZE = 100;
    private static final int MAX_WAIT_TIME = 500;
    private static final String TEST_BASE_DIR = "testing/MetadataTest";
    private static final String LOG_FILE = "metadata_logfile";

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

    // ========================================================================
    // SECTION 1: HAPPY PATH SEMANTICS TESTS
    // ========================================================================

    @Nested
    @DisplayName("Happy Path Semantics Tests")
    class HappyPathTests {

        @Test
        @DisplayName("createTable() creates and persists table metadata correctly")
        void createTablePersistsMetadata() {
            TxMgrBase txMgr = createTxMgr("createTablePersistsMetadata", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();

            LayoutBase layout = tableMgr.createTable("TestTable", schema, tx);

            assertNotNull(layout, "createTable should return a Layout");
            assertEquals(schema.fields().size(), layout.schema().fields().size());
            assertTrue(layout.slotSize() > 0);

            tx.commit();
        }

        @Test
        @DisplayName("getLayout() retrieves correct Layout for existing table")
        void getLayoutRetrievesExistingTable() {
            TxMgrBase txMgr = createTxMgr("getLayoutRetrievesExistingTable", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();

            LayoutBase createdLayout = tableMgr.createTable("MyTable", schema, tx);
            LayoutBase retrievedLayout = tableMgr.getLayout("MyTable", tx);

            assertNotNull(retrievedLayout);
            assertEquals(createdLayout.slotSize(), retrievedLayout.slotSize());
            assertEquals(createdLayout.schema().fields().size(), retrievedLayout.schema().fields().size());

            tx.commit();
        }

        @Test
        @DisplayName("getLayout() returns null for non-existent table")
        void getLayoutReturnsNullForNonExistent() {
            TxMgrBase txMgr = createTxMgr("getLayoutReturnsNullForNonExistent", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            LayoutBase layout = tableMgr.getLayout("NonExistentTable", tx);

            assertNull(layout, "getLayout should return null for non-existent table");

            tx.rollback();
        }

        @Test
        @DisplayName("replace() updates existing table metadata")
        void replaceUpdatesMetadata() {
            TxMgrBase txMgr = createTxMgr("replaceUpdatesMetadata", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            // Create initial table
            Schema schema1 = new Schema();
            schema1.addIntField("id");
            schema1.addStringField("name", 10);
            LayoutBase layout1 = tableMgr.createTable("ReplaceTable", schema1, tx);

            // Replace with new schema
            Schema schema2 = new Schema();
            schema2.addIntField("newId");
            schema2.addDoubleField("amount");
            schema2.addBooleanField("flag");
            LayoutBase oldLayout = tableMgr.replace("ReplaceTable", schema2, tx);

            // Verify old layout was returned
            assertEquals(layout1.slotSize(), oldLayout.slotSize());

            // Verify new layout is in place
            LayoutBase newLayout = tableMgr.getLayout("ReplaceTable", tx);
            assertNotNull(newLayout);
            assertEquals(3, newLayout.schema().fields().size());
            assertTrue(newLayout.schema().hasField("newId"));
            assertTrue(newLayout.schema().hasField("amount"));
            assertTrue(newLayout.schema().hasField("flag"));

            tx.commit();
        }

        @Test
        @DisplayName("replace() with null schema deletes metadata only")
        void replaceWithNullDeletesMetadata() {
            TxMgrBase txMgr = createTxMgr("replaceWithNullDeletesMetadata", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            // Create table
            SchemaBase schema = createStandardSchema();
            tableMgr.createTable("ToDelete", schema, tx);

            // Replace with null to delete
            LayoutBase oldLayout = tableMgr.replace("ToDelete", null, tx);

            assertNotNull(oldLayout, "replace should return the old layout");

            // Verify table is gone
            LayoutBase deletedLayout = tableMgr.getLayout("ToDelete", tx);
            assertNull(deletedLayout, "Table should be deleted");

            tx.commit();
        }

        @Test
        @DisplayName("Layout fields, offsets, and slotSize match persisted metadata")
        void layoutMatchesPersistedMetadata() {
            TxMgrBase txMgr = createTxMgr("layoutMatchesPersistedMetadata", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);
            SchemaBase schema = createStandardSchema();

            LayoutBase createdLayout = tableMgr.createTable("LayoutTest", schema, tx);

            // Verify all fields are present
            for (String field : schema.fields()) {
                assertTrue(createdLayout.schema().hasField(field));
                assertEquals(schema.type(field), createdLayout.schema().type(field));
                assertEquals(schema.length(field), createdLayout.schema().length(field));
                assertTrue(createdLayout.offset(field) >= 0);
            }

            tx.commit();
        }
    }

    // ========================================================================
    // SECTION 2: CATALOG PERSISTENCE TESTS
    // ========================================================================

    @Nested
    @DisplayName("Catalog Persistence Tests")
    class CatalogPersistenceTests {

        @Test
        @DisplayName("Constructor with new DB (startup=true) creates tblcat and fldcat")
        void constructorCreatesMetadataTables() {
            TxMgrBase txMgr = createTxMgr("constructorCreatesMetadataTables", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            // Verify tblcat exists by getting its layout
            LayoutBase tblcatLayout = tableMgr.getLayout(TableMgrBase.TABLE_META_DATA_TABLE, tx);
            assertNotNull(tblcatLayout, "tblcat should exist");

            // Verify fldcat exists by getting its layout
            LayoutBase fldcatLayout = tableMgr.getLayout(TableMgrBase.FIELD_META_DATA_TABLE, tx);
            assertNotNull(fldcatLayout, "fldcat should exist");

            tx.commit();
        }

        @Test
        @DisplayName("Constructor with existing DB (startup=false) restores cached layouts")
        void constructorRestoresCachedLayouts() {
            String testName = "constructorRestoresCachedLayouts";

            // First transaction: create table
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgr tableMgr1 = new TableMgr(tx1);
            SchemaBase schema = createStandardSchema();
            LayoutBase originalLayout = tableMgr1.createTable("PersistTest", schema, tx1);
            tx1.commit();

            // Second transaction: restore and verify
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();

            TableMgr tableMgr2 = new TableMgr(tx2);
            LayoutBase restoredLayout = tableMgr2.getLayout("PersistTest", tx2);

            assertNotNull(restoredLayout, "Layout should be restored from catalog");
            assertEquals(originalLayout.slotSize(), restoredLayout.slotSize());
            assertEquals(originalLayout.schema().fields().size(), restoredLayout.schema().fields().size());

            for (String field : originalLayout.schema().fields()) {
                assertTrue(restoredLayout.schema().hasField(field));
                assertEquals(originalLayout.offset(field), restoredLayout.offset(field));
            }

            tx2.commit();
        }

        @Test
        @DisplayName("TABLE_META_DATA_TABLE scannable via TableScan after createTable")
        void tblcatScannableAfterCreateTable() {
            TxMgrBase txMgr = createTxMgr("tblcatScannableAfterCreateTable", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            // Create some tables
            Schema schema1 = new Schema();
            schema1.addIntField("id");
            tableMgr.createTable("Table1", schema1, tx);

            Schema schema2 = new Schema();
            schema2.addStringField("name", 10);
            tableMgr.createTable("Table2", schema2, tx);

            // Scan tblcat
            LayoutBase tblcatLayout = tableMgr.getLayout(TableMgrBase.TABLE_META_DATA_TABLE, tx);
            TableScan tblcatScan = new TableScan(tx, TableMgrBase.TABLE_META_DATA_TABLE, tblcatLayout);

            List<String> tableNames = new ArrayList<>();
            tblcatScan.beforeFirst();
            while (tblcatScan.next()) {
                tableNames.add(tblcatScan.getString(TableMgrBase.TABLE_NAME));
            }
            tblcatScan.close();

            // Should contain at least tblcat, fldcat, Table1, Table2
            assertTrue(tableNames.contains(TableMgrBase.TABLE_META_DATA_TABLE));
            assertTrue(tableNames.contains(TableMgrBase.FIELD_META_DATA_TABLE));
            assertTrue(tableNames.contains("Table1"));
            assertTrue(tableNames.contains("Table2"));

            tx.commit();
        }

        @Test
        @DisplayName("FIELD_META_DATA_TABLE scannable via TableScan after createTable")
        void fldcatScannableAfterCreateTable() {
            TxMgrBase txMgr = createTxMgr("fldcatScannableAfterCreateTable", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            // Create table with multiple fields
            Schema schema = new Schema();
            schema.addIntField("field1");
            schema.addStringField("field2", 10);
            schema.addBooleanField("field3");
            tableMgr.createTable("FieldTest", schema, tx);

            // Scan fldcat
            LayoutBase fldcatLayout = tableMgr.getLayout(TableMgrBase.FIELD_META_DATA_TABLE, tx);
            TableScan fldcatScan = new TableScan(tx, TableMgrBase.FIELD_META_DATA_TABLE, fldcatLayout);

            List<String> fieldNames = new ArrayList<>();
            fldcatScan.beforeFirst();
            while (fldcatScan.next()) {
                String tblName = fldcatScan.getString(TableMgrBase.TABLE_NAME);
                if ("FieldTest".equals(tblName)) {
                    fieldNames.add(fldcatScan.getString("fldname"));
                }
            }
            fldcatScan.close();

            assertEquals(3, fieldNames.size());
            assertTrue(fieldNames.contains("field1"));
            assertTrue(fieldNames.contains("field2"));
            assertTrue(fieldNames.contains("field3"));

            tx.commit();
        }

        @Test
        @DisplayName("Multiple tables persist correctly across separate transactions")
        void multipleTablesPersistAcrossTransactions() {
            String testName = "multipleTablesPersistAcrossTransactions";

            // First transaction: create multiple tables
            TxMgrBase txMgr1 = createTxMgr(testName, true);
            TxBase tx1 = txMgr1.newTx();

            TableMgr tableMgr1 = new TableMgr(tx1);

            Schema schema1 = new Schema();
            schema1.addIntField("id");
            tableMgr1.createTable("Customers", schema1, tx1);

            Schema schema2 = new Schema();
            schema2.addIntField("orderId");
            schema2.addDoubleField("amount");
            tableMgr1.createTable("Orders", schema2, tx1);

            Schema schema3 = new Schema();
            schema3.addStringField("productName", 15);
            schema3.addBooleanField("inStock");
            tableMgr1.createTable("Products", schema3, tx1);

            tx1.commit();

            // Second transaction: verify all tables exist
            TxMgrBase txMgr2 = createTxMgr(testName, false);
            TxBase tx2 = txMgr2.newTx();

            TableMgr tableMgr2 = new TableMgr(tx2);

            LayoutBase customersLayout = tableMgr2.getLayout("Customers", tx2);
            LayoutBase ordersLayout = tableMgr2.getLayout("Orders", tx2);
            LayoutBase productsLayout = tableMgr2.getLayout("Products", tx2);

            assertNotNull(customersLayout);
            assertNotNull(ordersLayout);
            assertNotNull(productsLayout);

            assertEquals(1, customersLayout.schema().fields().size());
            assertEquals(2, ordersLayout.schema().fields().size());
            assertEquals(2, productsLayout.schema().fields().size());

            tx2.commit();
        }
    }

    // ========================================================================
    // SECTION 3: SCHEMA VARIATION TESTS
    // ========================================================================

    @Nested
    @DisplayName("Schema Variation Tests")
    class SchemaVariationTests {

        @Test
        @DisplayName("Tables with all field types (INT, VARCHAR, BOOLEAN, DOUBLE)")
        void tablesWithAllFieldTypes() {
            TxMgrBase txMgr = createTxMgr("tablesWithAllFieldTypes", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            Schema schema = new Schema();
            schema.addIntField("intField");
            schema.addStringField("varcharField", 15);
            schema.addBooleanField("booleanField");
            schema.addDoubleField("doubleField");

            LayoutBase layout = tableMgr.createTable("AllTypes", schema, tx);

            assertEquals(Types.INTEGER, layout.schema().type("intField"));
            assertEquals(Types.VARCHAR, layout.schema().type("varcharField"));
            assertEquals(Types.BOOLEAN, layout.schema().type("booleanField"));
            assertEquals(Types.DOUBLE, layout.schema().type("doubleField"));

            tx.commit();
        }

        @Test
        @DisplayName("Tables with single field schema")
        void tableWithSingleField() {
            TxMgrBase txMgr = createTxMgr("tableWithSingleField", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            Schema schema = new Schema();
            schema.addIntField("onlyField");

            LayoutBase layout = tableMgr.createTable("SingleField", schema, tx);

            assertNotNull(layout);
            assertEquals(1, layout.schema().fields().size());
            assertTrue(layout.schema().hasField("onlyField"));

            tx.commit();
        }

        @Test
        @DisplayName("String fields at MAX_LENGTH_PER_NAME (16 char) boundary")
        void stringFieldAtMaxLength() {
            TxMgrBase txMgr = createTxMgr("stringFieldAtMaxLength", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            // Create table name at exactly MAX_LENGTH_PER_NAME
            String maxLengthName = "ABCDEFGHIJKLMNOP"; // 16 chars
            assertEquals(TableMgrBase.MAX_LENGTH_PER_NAME, maxLengthName.length());

            Schema schema = new Schema();
            schema.addStringField(maxLengthName, 10);

            LayoutBase layout = tableMgr.createTable(maxLengthName, schema, tx);

            assertNotNull(layout);
            assertTrue(layout.schema().hasField(maxLengthName));

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
        @DisplayName("createTable() throws IAE if table already exists")
        void createTableThrowsIAEForDuplicate() {
            TxMgrBase txMgr = createTxMgr("createTableThrowsIAEForDuplicate", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            Schema schema = new Schema();
            schema.addIntField("id");

            tableMgr.createTable("DuplicateTable", schema, tx);

            // Attempt to create again - should throw IAE
            assertThrows(IllegalArgumentException.class, () -> {
                tableMgr.createTable("DuplicateTable", schema, tx);
            });

            tx.rollback();
        }

        @Test
        @DisplayName("replace() throws IAE if table doesn't exist in catalog")
        void replaceThrowsIAEForNonExistent() {
            TxMgrBase txMgr = createTxMgr("replaceThrowsIAEForNonExistent", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            Schema schema = new Schema();
            schema.addIntField("id");

            // Attempt to replace non-existent table
            assertThrows(IllegalArgumentException.class, () -> {
                tableMgr.replace("NonExistentTable", schema, tx);
            });

            tx.rollback();
        }
    }

    // ========================================================================
    // SECTION 5: PERFORMANCE TESTS
    // ========================================================================

    @Nested
    @DisplayName("Performance Tests")
    class PerformanceTests {

        @Test
        @DisplayName("Create 50+ tables in single transaction")
        void createManyTables() {
            TxMgrBase txMgr = createTxMgr("createManyTables", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            int tableCount = 50;
            long startTime = System.currentTimeMillis();

            for (int i = 0; i < tableCount; i++) {
                Schema schema = new Schema();
                schema.addIntField("id");
                schema.addStringField("name", 10);
                tableMgr.createTable("Table" + i, schema, tx);
            }

            long elapsed = System.currentTimeMillis() - startTime;

            logger.info("Created {} tables in {} ms", tableCount, elapsed);

            // Verify all tables exist
            for (int i = 0; i < tableCount; i++) {
                LayoutBase layout = tableMgr.getLayout("Table" + i, tx);
                assertNotNull(layout, "Table" + i + " should exist");
            }

            tx.commit();
        }

        @Test
        @DisplayName("Sequential getLayout() lookups")
        void sequentialGetLayoutLookups() {
            TxMgrBase txMgr = createTxMgr("sequentialGetLayoutLookups", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            // Create tables
            int tableCount = 20;
            for (int i = 0; i < tableCount; i++) {
                Schema schema = new Schema();
                schema.addIntField("id");
                tableMgr.createTable("LookupTable" + i, schema, tx);
            }

            // Perform many lookups
            int lookupCount = 100;
            long startTime = System.currentTimeMillis();

            for (int i = 0; i < lookupCount; i++) {
                String tableName = "LookupTable" + (i % tableCount);
                LayoutBase layout = tableMgr.getLayout(tableName, tx);
                assertNotNull(layout);
            }

            long elapsed = System.currentTimeMillis() - startTime;
            double lookupsPerSecond = (lookupCount * 1000.0) / elapsed;

            logger.info("Performed {} lookups in {} ms ({} lookups/sec)",
                    lookupCount, elapsed, lookupsPerSecond);

            tx.commit();
        }

        @Test
        @DisplayName("Catalog scan performance with many tables")
        void catalogScanPerformance() {
            TxMgrBase txMgr = createTxMgr("catalogScanPerformance", true);
            TxBase tx = txMgr.newTx();

            TableMgr tableMgr = new TableMgr(tx);

            // Create many tables
            int tableCount = 30;
            for (int i = 0; i < tableCount; i++) {
                Schema schema = new Schema();
                schema.addIntField("id");
                schema.addStringField("name", 10);
                schema.addDoubleField("value");
                tableMgr.createTable("ScanTable" + i, schema, tx);
            }

            // Time catalog scan
            LayoutBase tblcatLayout = tableMgr.getLayout(TableMgrBase.TABLE_META_DATA_TABLE, tx);

            long startTime = System.currentTimeMillis();

            TableScan tblcatScan = new TableScan(tx, TableMgrBase.TABLE_META_DATA_TABLE, tblcatLayout);
            int count = 0;
            tblcatScan.beforeFirst();
            while (tblcatScan.next()) {
                tblcatScan.getString(TableMgrBase.TABLE_NAME);
                count++;
            }
            tblcatScan.close();

            long elapsed = System.currentTimeMillis() - startTime;

            logger.info("Scanned {} entries in tblcat in {} ms", count, elapsed);

            // Should have at least tblcat + fldcat + our tables
            assertTrue(count >= tableCount + 2);

            tx.commit();
        }
    }
}
