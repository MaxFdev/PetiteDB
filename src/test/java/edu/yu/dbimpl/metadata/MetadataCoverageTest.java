package edu.yu.dbimpl.metadata;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.DisplayName;
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
 * Exhaustive method-by-method coverage tests for the TableMgr class.
 * Tests cover every code path including constructor variants, getLayout,
 * createTable, replace, gap reuse, and catalog scanability.
 */
public class MetadataCoverageTest {

    private static final Logger logger = LogManager.getLogger(MetadataCoverageTest.class);

    private static final int BLOCK_SIZE = 400;
    private static final int BUFFER_SIZE = 100;
    private static final int MAX_WAIT_TIME = 500;
    private static final String TEST_BASE_DIR = "testing/MetadataCoverageTest";
    private static final String LOG_FILE = "coverage_logfile";

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

    private SchemaBase createTestSchema() {
        Schema schema = new Schema();
        schema.addIntField("id");
        schema.addStringField("name", 16);
        schema.addBooleanField("active");
        schema.addDoubleField("value");
        return schema;
    }

    // ========== Constructor Coverage ==========

    @Test
    @DisplayName("Constructor with startup=true: creates tblcat and fldcat tables")
    void constructorStartupTrueCreatesCatalogTables() {
        TxMgrBase txMgr = createTxMgr("constructorStartupTrue", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        // Verify tblcat layout exists
        LayoutBase tblcatLayout = tableMgr.getLayout(TableMgrBase.TABLE_META_DATA_TABLE, tx);
        assertNotNull(tblcatLayout, "tblcat should be created on startup");
        assertTrue(tblcatLayout.schema().hasField(TableMgrBase.TABLE_NAME));

        // Verify fldcat layout exists
        LayoutBase fldcatLayout = tableMgr.getLayout(TableMgrBase.FIELD_META_DATA_TABLE, tx);
        assertNotNull(fldcatLayout, "fldcat should be created on startup");
        assertTrue(fldcatLayout.schema().hasField(TableMgrBase.TABLE_NAME));
        assertTrue(fldcatLayout.schema().hasField("fldname"));

        tx.commit();
    }

    @Test
    @DisplayName("Constructor with startup=false: loads layouts from existing catalog")
    void constructorStartupFalseLoadsLayouts() {
        String testName = "constructorStartupFalseLoadsLayouts";

        // Create and populate
        TxMgrBase txMgr1 = createTxMgr(testName, true);
        TxBase tx1 = txMgr1.newTx();
        TableMgr tableMgr1 = new TableMgr(tx1);

        Schema schema = new Schema();
        schema.addIntField("pk");
        schema.addDoubleField("amount");
        tableMgr1.createTable("LoadTest", schema, tx1);
        tx1.commit();

        // Reload
        TxMgrBase txMgr2 = createTxMgr(testName, false);
        TxBase tx2 = txMgr2.newTx();
        TableMgr tableMgr2 = new TableMgr(tx2);

        LayoutBase layout = tableMgr2.getLayout("LoadTest", tx2);
        assertNotNull(layout);
        assertTrue(layout.schema().hasField("pk"));
        assertTrue(layout.schema().hasField("amount"));
        assertEquals(Types.INTEGER, layout.schema().type("pk"));
        assertEquals(Types.DOUBLE, layout.schema().type("amount"));

        tx2.commit();
    }

    @Test
    @DisplayName("Constructor with startup=false: restores tableCatalogLocation map")
    void constructorRestoresTableCatalogLocation() {
        String testName = "constructorRestoresTableCatalogLocation";

        // Create tables
        TxMgrBase txMgr1 = createTxMgr(testName, true);
        TxBase tx1 = txMgr1.newTx();
        TableMgr tableMgr1 = new TableMgr(tx1);

        Schema schema1 = new Schema();
        schema1.addIntField("id");
        tableMgr1.createTable("CatalogLocTable1", schema1, tx1);

        Schema schema2 = new Schema();
        schema2.addStringField("data", 10);
        tableMgr1.createTable("CatalogLocTable2", schema2, tx1);

        tx1.commit();

        // Reload and verify replace works (which uses tableCatalogLocation)
        TxMgrBase txMgr2 = createTxMgr(testName, false);
        TxBase tx2 = txMgr2.newTx();
        TableMgr tableMgr2 = new TableMgr(tx2);

        // Replace should work, meaning tableCatalogLocation was restored
        Schema newSchema = new Schema();
        newSchema.addBooleanField("flag");
        LayoutBase oldLayout = tableMgr2.replace("CatalogLocTable1", newSchema, tx2);

        assertNotNull(oldLayout);
        assertEquals(1, oldLayout.schema().fields().size());

        LayoutBase updatedLayout = tableMgr2.getLayout("CatalogLocTable1", tx2);
        assertTrue(updatedLayout.schema().hasField("flag"));

        tx2.commit();
    }

    @Test
    @DisplayName("Constructor with startup=false: restores fieldCatalogInsertMarker position")
    void constructorRestoresFieldCatalogInsertMarker() {
        String testName = "constructorRestoresFieldCatalogInsertMarker";

        // Create tables
        TxMgrBase txMgr1 = createTxMgr(testName, true);
        TxBase tx1 = txMgr1.newTx();
        TableMgr tableMgr1 = new TableMgr(tx1);

        Schema schema = new Schema();
        schema.addIntField("f1");
        schema.addIntField("f2");
        schema.addIntField("f3");
        tableMgr1.createTable("MarkerTest", schema, tx1);
        tx1.commit();

        // Reload and create another table
        TxMgrBase txMgr2 = createTxMgr(testName, false);
        TxBase tx2 = txMgr2.newTx();
        TableMgr tableMgr2 = new TableMgr(tx2);

        Schema schema2 = new Schema();
        schema2.addDoubleField("newField");
        tableMgr2.createTable("MarkerTest2", schema2, tx2);

        // Verify both tables exist
        assertNotNull(tableMgr2.getLayout("MarkerTest", tx2));
        assertNotNull(tableMgr2.getLayout("MarkerTest2", tx2));

        tx2.commit();
    }

    // ========== getLayout() Coverage ==========

    @Test
    @DisplayName("getLayout() existing table returns correct Layout")
    void getLayoutExistingTableReturnsLayout() {
        TxMgrBase txMgr = createTxMgr("getLayoutExistingTable", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);
        SchemaBase schema = createTestSchema();
        tableMgr.createTable("GetLayoutTest", schema, tx);

        LayoutBase layout = tableMgr.getLayout("GetLayoutTest", tx);

        assertNotNull(layout);
        assertEquals(4, layout.schema().fields().size());

        tx.commit();
    }

    @Test
    @DisplayName("getLayout() non-existent table returns null")
    void getLayoutNonExistentReturnsNull() {
        TxMgrBase txMgr = createTxMgr("getLayoutNonExistent", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        LayoutBase layout = tableMgr.getLayout("DoesNotExist", tx);

        assertNull(layout);

        tx.rollback();
    }

    @Test
    @DisplayName("getLayout() Layout contains correct schema fields")
    void getLayoutContainsCorrectSchemaFields() {
        TxMgrBase txMgr = createTxMgr("getLayoutCorrectFields", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("intCol");
        schema.addStringField("strCol", 12);
        schema.addBooleanField("boolCol");
        tableMgr.createTable("FieldsTest", schema, tx);

        LayoutBase layout = tableMgr.getLayout("FieldsTest", tx);

        assertTrue(layout.schema().hasField("intCol"));
        assertTrue(layout.schema().hasField("strCol"));
        assertTrue(layout.schema().hasField("boolCol"));
        assertFalse(layout.schema().hasField("nonExistent"));

        tx.commit();
    }

    @Test
    @DisplayName("getLayout() Layout contains correct field offsets")
    void getLayoutContainsCorrectFieldOffsets() {
        TxMgrBase txMgr = createTxMgr("getLayoutCorrectOffsets", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("first");
        schema.addBooleanField("second");
        tableMgr.createTable("OffsetsTest", schema, tx);

        LayoutBase layout = tableMgr.getLayout("OffsetsTest", tx);

        // First field at offset 1 (after in-use byte)
        assertEquals(1, layout.offset("first"));
        // Second field at offset 1 + 4 = 5
        assertEquals(5, layout.offset("second"));

        tx.commit();
    }

    @Test
    @DisplayName("getLayout() Layout contains correct slotSize")
    void getLayoutContainsCorrectSlotSize() {
        TxMgrBase txMgr = createTxMgr("getLayoutCorrectSlotSize", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("id"); // 4 bytes
        schema.addBooleanField("flag"); // 1 byte
        tableMgr.createTable("SlotSizeTest", schema, tx);

        LayoutBase layout = tableMgr.getLayout("SlotSizeTest", tx);

        // Expected: 1 (in-use) + 4 (int) + 1 (boolean) = 6
        assertEquals(6, layout.slotSize());

        tx.commit();
    }

    @Test
    @DisplayName("getLayout() after createTable returns cached Layout")
    void getLayoutAfterCreateTableReturnsCached() {
        TxMgrBase txMgr = createTxMgr("getLayoutAfterCreate", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);
        SchemaBase schema = createTestSchema();

        LayoutBase createdLayout = tableMgr.createTable("CacheTest", schema, tx);
        LayoutBase retrievedLayout = tableMgr.getLayout("CacheTest", tx);

        // Should be the same cached instance
        assertSame(createdLayout, retrievedLayout);

        tx.commit();
    }

    @Test
    @DisplayName("getLayout() after replace returns updated Layout")
    void getLayoutAfterReplaceReturnsUpdated() {
        TxMgrBase txMgr = createTxMgr("getLayoutAfterReplace", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema1 = new Schema();
        schema1.addIntField("oldField");
        tableMgr.createTable("ReplaceTest", schema1, tx);

        Schema schema2 = new Schema();
        schema2.addDoubleField("newField");
        tableMgr.replace("ReplaceTest", schema2, tx);

        LayoutBase layout = tableMgr.getLayout("ReplaceTest", tx);

        assertFalse(layout.schema().hasField("oldField"));
        assertTrue(layout.schema().hasField("newField"));

        tx.commit();
    }

    @Test
    @DisplayName("getLayout() after replace(null) returns null")
    void getLayoutAfterReplaceNullReturnsNull() {
        TxMgrBase txMgr = createTxMgr("getLayoutAfterReplaceNull", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("id");
        tableMgr.createTable("DeleteTest", schema, tx);

        tableMgr.replace("DeleteTest", null, tx);

        LayoutBase layout = tableMgr.getLayout("DeleteTest", tx);
        assertNull(layout);

        tx.commit();
    }

    // ========== createTable() Coverage ==========

    @Test
    @DisplayName("createTable() valid schema returns new Layout")
    void createTableValidSchemaReturnsLayout() {
        TxMgrBase txMgr = createTxMgr("createTableValidSchema", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);
        SchemaBase schema = createTestSchema();

        LayoutBase layout = tableMgr.createTable("ValidSchema", schema, tx);

        assertNotNull(layout);
        assertNotNull(layout.schema());
        assertTrue(layout.slotSize() > 0);

        tx.commit();
    }

    @Test
    @DisplayName("createTable() persists entry to TABLE_META_DATA_TABLE")
    void createTablePersistsToTblcat() {
        TxMgrBase txMgr = createTxMgr("createTablePersistsToTblcat", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("id");
        tableMgr.createTable("TblcatEntry", schema, tx);

        // Scan tblcat to verify
        LayoutBase tblcatLayout = tableMgr.getLayout(TableMgrBase.TABLE_META_DATA_TABLE, tx);
        TableScan scan = new TableScan(tx, TableMgrBase.TABLE_META_DATA_TABLE, tblcatLayout);

        boolean found = false;
        scan.beforeFirst();
        while (scan.next()) {
            if ("TblcatEntry".equals(scan.getString(TableMgrBase.TABLE_NAME))) {
                found = true;
                break;
            }
        }
        scan.close();

        assertTrue(found, "Table should be in tblcat");

        tx.commit();
    }

    @Test
    @DisplayName("createTable() persists all fields to FIELD_META_DATA_TABLE")
    void createTablePersistsAllFieldsToFldcat() {
        TxMgrBase txMgr = createTxMgr("createTablePersistsToFldcat", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("f1");
        schema.addStringField("f2", 10);
        schema.addBooleanField("f3");
        tableMgr.createTable("FldcatEntry", schema, tx);

        // Scan fldcat to verify
        LayoutBase fldcatLayout = tableMgr.getLayout(TableMgrBase.FIELD_META_DATA_TABLE, tx);
        TableScan scan = new TableScan(tx, TableMgrBase.FIELD_META_DATA_TABLE, fldcatLayout);

        Set<String> foundFields = new HashSet<>();
        scan.beforeFirst();
        while (scan.next()) {
            if ("FldcatEntry".equals(scan.getString(TableMgrBase.TABLE_NAME))) {
                foundFields.add(scan.getString("fldname"));
            }
        }
        scan.close();

        assertEquals(3, foundFields.size());
        assertTrue(foundFields.contains("f1"));
        assertTrue(foundFields.contains("f2"));
        assertTrue(foundFields.contains("f3"));

        tx.commit();
    }

    @Test
    @DisplayName("createTable() with INT field stores correct type/length")
    void createTableIntFieldStoresCorrectMetadata() {
        TxMgrBase txMgr = createTxMgr("createTableIntField", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("intField");
        tableMgr.createTable("IntFieldTest", schema, tx);

        LayoutBase layout = tableMgr.getLayout("IntFieldTest", tx);
        assertEquals(Types.INTEGER, layout.schema().type("intField"));
        assertEquals(Integer.BYTES, layout.schema().length("intField"));

        tx.commit();
    }

    @Test
    @DisplayName("createTable() with VARCHAR field stores correct type/length")
    void createTableVarcharFieldStoresCorrectMetadata() {
        TxMgrBase txMgr = createTxMgr("createTableVarcharField", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addStringField("varcharField", 15);
        tableMgr.createTable("VarcharFieldTest", schema, tx);

        LayoutBase layout = tableMgr.getLayout("VarcharFieldTest", tx);
        assertEquals(Types.VARCHAR, layout.schema().type("varcharField"));
        assertEquals(15, layout.schema().length("varcharField"));

        tx.commit();
    }

    @Test
    @DisplayName("createTable() with BOOLEAN field stores correct type/length")
    void createTableBooleanFieldStoresCorrectMetadata() {
        TxMgrBase txMgr = createTxMgr("createTableBooleanField", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addBooleanField("boolField");
        tableMgr.createTable("BoolFieldTest", schema, tx);

        LayoutBase layout = tableMgr.getLayout("BoolFieldTest", tx);
        assertEquals(Types.BOOLEAN, layout.schema().type("boolField"));

        tx.commit();
    }

    @Test
    @DisplayName("createTable() with DOUBLE field stores correct type/length")
    void createTableDoubleFieldStoresCorrectMetadata() {
        TxMgrBase txMgr = createTxMgr("createTableDoubleField", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addDoubleField("doubleField");
        tableMgr.createTable("DoubleFieldTest", schema, tx);

        LayoutBase layout = tableMgr.getLayout("DoubleFieldTest", tx);
        assertEquals(Types.DOUBLE, layout.schema().type("doubleField"));
        assertEquals(Double.BYTES, layout.schema().length("doubleField"));

        tx.commit();
    }

    @Test
    @DisplayName("createTable() with multiple fields preserves field order")
    void createTableMultipleFieldsPreservesOrder() {
        TxMgrBase txMgr = createTxMgr("createTableFieldOrder", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("first");
        schema.addStringField("second", 10);
        schema.addBooleanField("third");
        schema.addDoubleField("fourth");
        tableMgr.createTable("FieldOrderTest", schema, tx);

        LayoutBase layout = tableMgr.getLayout("FieldOrderTest", tx);
        List<String> fields = layout.schema().fields();

        assertEquals(4, fields.size());
        assertEquals("first", fields.get(0));
        assertEquals("second", fields.get(1));
        assertEquals("third", fields.get(2));
        assertEquals("fourth", fields.get(3));

        tx.commit();
    }

    @Test
    @DisplayName("createTable() with single field works correctly")
    void createTableSingleFieldWorks() {
        TxMgrBase txMgr = createTxMgr("createTableSingleField", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("only");
        tableMgr.createTable("SingleFieldTest", schema, tx);

        LayoutBase layout = tableMgr.getLayout("SingleFieldTest", tx);

        assertEquals(1, layout.schema().fields().size());
        assertTrue(layout.schema().hasField("only"));

        tx.commit();
    }

    @Test
    @DisplayName("createTable() duplicate table name throws IAE")
    void createTableDuplicateThrowsIAE() {
        TxMgrBase txMgr = createTxMgr("createTableDuplicate", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("id");
        tableMgr.createTable("Duplicate", schema, tx);

        assertThrows(IllegalArgumentException.class, () -> {
            tableMgr.createTable("Duplicate", schema, tx);
        });

        tx.rollback();
    }

    // ========== replace() Coverage ==========

    @Test
    @DisplayName("replace() with new schema updates TABLE_META_DATA_TABLE entry")
    void replaceUpdatesTableMetadata() {
        TxMgrBase txMgr = createTxMgr("replaceUpdatesTableMetadata", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema1 = new Schema();
        schema1.addIntField("old");
        LayoutBase layout1 = tableMgr.createTable("ReplaceTable", schema1, tx);
        int oldSlotSize = layout1.slotSize();

        Schema schema2 = new Schema();
        schema2.addDoubleField("new");
        schema2.addBooleanField("flag");
        tableMgr.replace("ReplaceTable", schema2, tx);

        LayoutBase layout2 = tableMgr.getLayout("ReplaceTable", tx);
        assertNotEquals(oldSlotSize, layout2.slotSize());

        tx.commit();
    }

    @Test
    @DisplayName("replace() with new schema replaces FIELD_META_DATA_TABLE entries")
    void replaceUpdatesFieldMetadata() {
        TxMgrBase txMgr = createTxMgr("replaceUpdatesFieldMetadata", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema1 = new Schema();
        schema1.addIntField("oldField1");
        schema1.addIntField("oldField2");
        tableMgr.createTable("FieldReplaceTbl", schema1, tx);

        Schema schema2 = new Schema();
        schema2.addStringField("newField", 10);
        tableMgr.replace("FieldReplaceTbl", schema2, tx);

        // Scan fldcat to verify old fields are gone
        LayoutBase fldcatLayout = tableMgr.getLayout(TableMgrBase.FIELD_META_DATA_TABLE, tx);
        TableScan scan = new TableScan(tx, TableMgrBase.FIELD_META_DATA_TABLE, fldcatLayout);

        Set<String> foundFields = new HashSet<>();
        scan.beforeFirst();
        while (scan.next()) {
            if ("FieldReplaceTbl".equals(scan.getString(TableMgrBase.TABLE_NAME))) {
                foundFields.add(scan.getString("fldname"));
            }
        }
        scan.close();

        assertEquals(1, foundFields.size());
        assertTrue(foundFields.contains("newField"));
        assertFalse(foundFields.contains("oldField1"));
        assertFalse(foundFields.contains("oldField2"));

        tx.commit();
    }

    @Test
    @DisplayName("replace() returns previous Layout before replacement")
    void replaceReturnsPreviousLayout() {
        TxMgrBase txMgr = createTxMgr("replaceReturnsPrevious", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema1 = new Schema();
        schema1.addIntField("original");
        LayoutBase originalLayout = tableMgr.createTable("PreviousLayout", schema1, tx);

        Schema schema2 = new Schema();
        schema2.addDoubleField("replacement");
        LayoutBase returnedLayout = tableMgr.replace("PreviousLayout", schema2, tx);

        assertEquals(originalLayout.slotSize(), returnedLayout.slotSize());
        assertTrue(returnedLayout.schema().hasField("original"));

        tx.commit();
    }

    @Test
    @DisplayName("replace() with null schema deletes TABLE_META_DATA_TABLE entry")
    void replaceNullDeletesTableEntry() {
        TxMgrBase txMgr = createTxMgr("replaceNullDeletesTable", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("id");
        tableMgr.createTable("ToBeDeleted", schema, tx);

        tableMgr.replace("ToBeDeleted", null, tx);

        // Scan tblcat to verify entry is gone
        LayoutBase tblcatLayout = tableMgr.getLayout(TableMgrBase.TABLE_META_DATA_TABLE, tx);
        TableScan scan = new TableScan(tx, TableMgrBase.TABLE_META_DATA_TABLE, tblcatLayout);

        boolean found = false;
        scan.beforeFirst();
        while (scan.next()) {
            if ("ToBeDeleted".equals(scan.getString(TableMgrBase.TABLE_NAME))) {
                found = true;
                break;
            }
        }
        scan.close();

        assertFalse(found, "Deleted table should not be in tblcat");

        tx.commit();
    }

    @Test
    @DisplayName("replace() with null schema deletes FIELD_META_DATA_TABLE entries")
    void replaceNullDeletesFieldEntries() {
        TxMgrBase txMgr = createTxMgr("replaceNullDeletesFields", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("f1");
        schema.addIntField("f2");
        tableMgr.createTable("FieldsToDelete", schema, tx);

        tableMgr.replace("FieldsToDelete", null, tx);

        // Scan fldcat to verify fields are gone
        LayoutBase fldcatLayout = tableMgr.getLayout(TableMgrBase.FIELD_META_DATA_TABLE, tx);
        TableScan scan = new TableScan(tx, TableMgrBase.FIELD_META_DATA_TABLE, fldcatLayout);

        int fieldCount = 0;
        scan.beforeFirst();
        while (scan.next()) {
            if ("FieldsToDelete".equals(scan.getString(TableMgrBase.TABLE_NAME))) {
                fieldCount++;
            }
        }
        scan.close();

        assertEquals(0, fieldCount, "No fields should remain for deleted table");

        tx.commit();
    }

    @Test
    @DisplayName("replace() with null schema removes from tableLayoutMap cache")
    void replaceNullRemovesFromCache() {
        TxMgrBase txMgr = createTxMgr("replaceNullRemovesCache", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("id");
        tableMgr.createTable("CacheRemove", schema, tx);

        tableMgr.replace("CacheRemove", null, tx);

        LayoutBase layout = tableMgr.getLayout("CacheRemove", tx);
        assertNull(layout, "Deleted table should not be in cache");

        tx.commit();
    }

    @Test
    @DisplayName("replace() non-existent table throws IAE")
    void replaceNonExistentThrowsIAE() {
        TxMgrBase txMgr = createTxMgr("replaceNonExistent", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("id");

        assertThrows(IllegalArgumentException.class, () -> {
            tableMgr.replace("DoesNotExist", schema, tx);
        });

        tx.rollback();
    }

    // ========== Gap Reuse Coverage ==========

    @Test
    @DisplayName("Gap reuse: delete table then create new table reuses gap slots")
    void gapReuseAfterDelete() {
        TxMgrBase txMgr = createTxMgr("gapReuseAfterDelete", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        // Create and delete a table with 3 fields
        Schema schema1 = new Schema();
        schema1.addIntField("f1");
        schema1.addIntField("f2");
        schema1.addIntField("f3");
        tableMgr.createTable("ToDelete", schema1, tx);
        tableMgr.replace("ToDelete", null, tx);

        // Create new table with same number of fields
        Schema schema2 = new Schema();
        schema2.addDoubleField("g1");
        schema2.addDoubleField("g2");
        schema2.addDoubleField("g3");
        tableMgr.createTable("NewTable", schema2, tx);

        // Verify new table exists
        LayoutBase layout = tableMgr.getLayout("NewTable", tx);
        assertNotNull(layout);
        assertEquals(3, layout.schema().fields().size());

        tx.commit();
    }

    @Test
    @DisplayName("Gap reuse: smaller table fits in larger gap")
    void gapReuseSmallerTableInLargerGap() {
        TxMgrBase txMgr = createTxMgr("gapReuseSmallerInLarger", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        // Create table with 5 fields, then delete
        Schema schema1 = new Schema();
        schema1.addIntField("a");
        schema1.addIntField("b");
        schema1.addIntField("c");
        schema1.addIntField("d");
        schema1.addIntField("e");
        tableMgr.createTable("LargeTable", schema1, tx);
        tableMgr.replace("LargeTable", null, tx);

        // Create table with 2 fields (should fit in gap)
        Schema schema2 = new Schema();
        schema2.addIntField("x");
        schema2.addIntField("y");
        tableMgr.createTable("SmallTable", schema2, tx);

        assertNotNull(tableMgr.getLayout("SmallTable", tx));

        tx.commit();
    }

    @Test
    @DisplayName("Gap reuse: exact size match fully consumes gap")
    void gapReuseExactMatch() {
        TxMgrBase txMgr = createTxMgr("gapReuseExactMatch", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        // Create and delete table with 2 fields
        Schema schema1 = new Schema();
        schema1.addIntField("old1");
        schema1.addIntField("old2");
        tableMgr.createTable("ExactMatch", schema1, tx);
        tableMgr.replace("ExactMatch", null, tx);

        // Create table with exactly 2 fields
        Schema schema2 = new Schema();
        schema2.addBooleanField("new1");
        schema2.addBooleanField("new2");
        tableMgr.createTable("ExactFit", schema2, tx);

        assertNotNull(tableMgr.getLayout("ExactFit", tx));

        tx.commit();
    }

    @Test
    @DisplayName("Gap reuse: no suitable gap causes append at end")
    void gapReuseNoSuitableGapAppends() {
        TxMgrBase txMgr = createTxMgr("gapReuseNoSuitableGap", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        // Create and delete table with 1 field
        Schema schema1 = new Schema();
        schema1.addIntField("tiny");
        tableMgr.createTable("TinyTable", schema1, tx);
        tableMgr.replace("TinyTable", null, tx);

        // Create table with 5 fields (won't fit in 1-slot gap)
        Schema schema2 = new Schema();
        schema2.addIntField("f1");
        schema2.addIntField("f2");
        schema2.addIntField("f3");
        schema2.addIntField("f4");
        schema2.addIntField("f5");
        tableMgr.createTable("LargeNewTable", schema2, tx);

        assertNotNull(tableMgr.getLayout("LargeNewTable", tx));

        tx.commit();
    }

    @Test
    @DisplayName("Gap reuse across block boundaries: detects gaps spanning block end during reload")
    void gapDetectionAcrossBlockBoundaryOnReload() {
        // This test covers the code path in loadCatalogCache where:
        // if (startSlot >= this.slotsPerBlock) { startBlock += 1; startSlot = 0; }
        //
        // We need to create a gap where the previous RID is at the last slot of a block
        // so that the gap starts at slot 0 of the next block.

        String testName = "gapDetectBlockBoundary";

        // Phase 1: Create many tables to fill multiple blocks, then delete one in the middle
        TxMgrBase txMgr1 = createTxMgr(testName, true);
        TxBase tx1 = txMgr1.newTx();

        TableMgr tableMgr1 = new TableMgr(tx1);

        // Create many tables with multiple fields each to fill up fldcat blocks.
        // With BLOCK_SIZE=400 and FIELD_CATALOG_LAYOUT having ~50 bytes per slot,
        // we get roughly 8 slots per block. We need to create enough fields to span
        // multiple blocks and position a deletion at a block boundary.

        // Create initial tables to fill first block (tblcat and fldcat are already there)
        // fldcat has 5 fields, tblcat has 2 fields - that's 7 slots used already

        // Create enough tables to span multiple blocks in fldcat
        int tablesNeeded = 15; // Each with 3 fields = 45 total field entries
        for (int i = 0; i < tablesNeeded; i++) {
            Schema schema = new Schema();
            schema.addIntField("field1");
            schema.addIntField("field2");
            schema.addIntField("field3");
            tableMgr1.createTable("SpanTable" + i, schema, tx1);
        }

        // Delete a table from somewhere in the middle to create a gap
        // This gap may end up at a block boundary depending on exact positioning
        tableMgr1.replace("SpanTable7", null, tx1);
        tableMgr1.replace("SpanTable8", null, tx1);

        tx1.commit();

        // Phase 2: Restart with startup=false to trigger loadCatalogCache
        TxMgrBase txMgr2 = createTxMgr(testName, false);
        TxBase tx2 = txMgr2.newTx();

        // This constructor triggers loadCatalogCache which should detect gaps
        TableMgr tableMgr2 = new TableMgr(tx2);

        // Verify the remaining tables still exist
        assertNotNull(tableMgr2.getLayout("SpanTable6", tx2));
        assertNotNull(tableMgr2.getLayout("SpanTable9", tx2));
        assertNull(tableMgr2.getLayout("SpanTable7", tx2)); // Deleted
        assertNull(tableMgr2.getLayout("SpanTable8", tx2)); // Deleted

        // Create a new table that should reuse the detected gap
        Schema newSchema = new Schema();
        newSchema.addIntField("reused1");
        newSchema.addIntField("reused2");
        tableMgr2.createTable("GapReused", newSchema, tx2);

        assertNotNull(tableMgr2.getLayout("GapReused", tx2));

        tx2.commit();
    }

    @Test
    @DisplayName("Gap recalculation when remainder spans to next block")
    void gapRecalculationSpansNextBlock() {
        // This test covers the code path in writeSchemaToCatalog where:
        // if (newSlot >= this.slotsPerBlock) {
        //     newBlock += newSlot / this.slotsPerBlock;
        //     newSlot = newSlot % this.slotsPerBlock;
        // }
        //
        // We need to create a large gap that spans across a block boundary,
        // then insert a smaller table so that the remaining gap wraps to the next block.

        TxMgrBase txMgr = createTxMgr("gapRecalcNextBlock", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        // Create a table with many fields (to create a large gap when deleted)
        // This should span close to or across a block boundary
        Schema largeSchema = new Schema();
        for (int i = 0; i < 20; i++) {
            largeSchema.addIntField("bigField" + i);
        }
        tableMgr.createTable("BigTable", largeSchema, tx);

        // Create another table after to "anchor" the end position
        Schema anchorSchema = new Schema();
        anchorSchema.addIntField("anchor");
        tableMgr.createTable("AnchorTable", anchorSchema, tx);

        // Delete the big table to create a large gap
        tableMgr.replace("BigTable", null, tx);

        // Now create a smaller table (e.g. 3 fields) that uses part of the gap
        // The remaining gap (20 - 3 = 17 slots) needs to be recalculated
        // If the gap started near slot 5 and we use 3, newSlot = 5 + 3 = 8
        // If slotsPerBlock is ~8, then newSlot >= slotsPerBlock triggers the wrap
        Schema smallSchema = new Schema();
        smallSchema.addIntField("small1");
        smallSchema.addIntField("small2");
        smallSchema.addIntField("small3");
        tableMgr.createTable("SmallInGap", smallSchema, tx);

        assertNotNull(tableMgr.getLayout("SmallInGap", tx));
        assertNotNull(tableMgr.getLayout("AnchorTable", tx));

        // Create another table that should reuse the remaining gap
        Schema anotherSchema = new Schema();
        anotherSchema.addIntField("another1");
        anotherSchema.addIntField("another2");
        tableMgr.createTable("AnotherTable", anotherSchema, tx);

        assertNotNull(tableMgr.getLayout("AnotherTable", tx));

        tx.commit();
    }

    @Test
    @DisplayName("Gap recalculation: newSlot >= slotsPerBlock triggers block wrap")
    void gapRecalculationNewSlotExceedsSlotsPerBlock() {
        // This test SPECIFICALLY targets the condition:
        //   if (newSlot >= this.slotsPerBlock)
        // where newSlot = startRid.slot() + fieldCount
        //
        // To trigger this:
        // 1. Create a gap that starts at a HIGH slot number within a block
        // 2. Insert a table with enough fields so that startSlot + fieldCount >= slotsPerBlock
        //
        // With BLOCK_SIZE=400 and FIELD_CATALOG_LAYOUT slot size ~57 bytes,
        // slotsPerBlock ≈ 7. If gap starts at slot 4 and we insert 4 fields,
        // newSlot = 4 + 4 = 8 >= 7, triggering the wrap.

        TxMgrBase txMgr = createTxMgr("gapNewSlotWrap", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        // Phase 1: Position fldcat entries to set up a gap at a high slot number
        // Initial state: fldcat has 5 fields (slots 0-4), tblcat has 2 fields (slots 5-6)
        // Total: 7 slots used in block 0

        // Create a "filler" table to use up remaining slots in block 0
        // and push subsequent entries into block 1
        Schema fillerSchema = new Schema();
        fillerSchema.addIntField("filler1");
        fillerSchema.addIntField("filler2");
        tableMgr.createTable("FillerTable", fillerSchema, tx);
        // Now we have 9 slots used: 7 (catalog) + 2 (filler) 
        // If slotsPerBlock=7: block 0 full, 2 slots in block 1 (slots 0-1)

        // Create a table with many fields that will span from a mid-block position
        // This table will be deleted to create a gap
        Schema targetSchema = new Schema();
        for (int i = 0; i < 10; i++) {
            targetSchema.addIntField("target" + i);
        }
        tableMgr.createTable("TargetForGap", targetSchema, tx);
        // 10 more fields added. If block 1 had slots 0-1 used, now 0-11 used
        // That spans blocks: block 1 slots 2-6 (5 fields), block 2 slots 0-4 (5 fields)

        // Create anchor table to mark end position
        Schema anchorSchema = new Schema();
        anchorSchema.addIntField("anchor");
        tableMgr.createTable("EndAnchor", anchorSchema, tx);

        // Delete the target table to create a 10-slot gap starting at a mid-block slot
        tableMgr.replace("TargetForGap", null, tx);

        // Phase 2: Insert a table that uses only PART of the gap
        // If gap starts at slot 2 and we insert 6 fields, newSlot = 2 + 6 = 8 >= 7
        // This MUST trigger the wrap logic
        Schema partialSchema = new Schema();
        partialSchema.addIntField("p1");
        partialSchema.addIntField("p2");
        partialSchema.addIntField("p3");
        partialSchema.addIntField("p4");
        partialSchema.addIntField("p5");
        partialSchema.addIntField("p6");
        tableMgr.createTable("PartialFill", partialSchema, tx);

        assertNotNull(tableMgr.getLayout("PartialFill", tx));
        assertEquals(6, tableMgr.getLayout("PartialFill", tx).schema().fields().size());

        // Verify the remaining 4-slot gap can still be used
        Schema remainingSchema = new Schema();
        remainingSchema.addIntField("r1");
        remainingSchema.addIntField("r2");
        remainingSchema.addIntField("r3");
        tableMgr.createTable("UseRemainingGap", remainingSchema, tx);

        assertNotNull(tableMgr.getLayout("UseRemainingGap", tx));
        assertNotNull(tableMgr.getLayout("EndAnchor", tx));

        tx.commit();
    }

    @Test
    @DisplayName("Gap recalculation: force newSlot to wrap multiple blocks")
    void gapRecalculationMultiBlockWrap() {
        // This test ensures the division/modulo logic works for larger wraps
        // newBlock += newSlot / this.slotsPerBlock;
        // newSlot = newSlot % this.slotsPerBlock;
        //
        // If slotsPerBlock = 7, startSlot = 5, fieldCount = 12:
        // newSlot = 5 + 12 = 17
        // newBlock += 17 / 7 = 2 (wraps 2 blocks)
        // newSlot = 17 % 7 = 3

        TxMgrBase txMgr = createTxMgr("gapMultiBlockWrap", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        // Create a very large table that will create a big gap when deleted
        Schema hugeSchema = new Schema();
        for (int i = 0; i < 25; i++) {
            hugeSchema.addIntField("huge" + i);
        }
        tableMgr.createTable("HugeTable", hugeSchema, tx);

        // Anchor table
        Schema anchorSchema = new Schema();
        anchorSchema.addIntField("anch");
        tableMgr.createTable("HugeAnchor", anchorSchema, tx);

        // Delete to create 25-slot gap
        tableMgr.replace("HugeTable", null, tx);

        // Insert table with 12 fields - should trigger multi-block wrap in gap recalculation
        Schema mediumSchema = new Schema();
        for (int i = 0; i < 12; i++) {
            mediumSchema.addIntField("med" + i);
        }
        tableMgr.createTable("MediumInGap", mediumSchema, tx);

        assertNotNull(tableMgr.getLayout("MediumInGap", tx));
        assertEquals(12, tableMgr.getLayout("MediumInGap", tx).schema().fields().size());

        // The remaining 13-slot gap should still be usable
        Schema useRemaining = new Schema();
        for (int i = 0; i < 10; i++) {
            useRemaining.addIntField("use" + i);
        }
        tableMgr.createTable("UseRemaining", useRemaining, tx);

        assertNotNull(tableMgr.getLayout("UseRemaining", tx));
        assertNotNull(tableMgr.getLayout("HugeAnchor", tx));

        tx.commit();
    }

    @Test
    @DisplayName("Gap at exact block boundary with slot at last position")
    void gapAtExactBlockBoundary() {
        // This specifically tests when a gap starts exactly at the last slot of a block
        // requiring the startBlock adjustment in loadCatalogCache

        String testName = "gapExactBlockBoundary";

        // Phase 1: Create tables with precise field counts to position at block boundary
        TxMgrBase txMgr1 = createTxMgr(testName, true);
        TxBase tx1 = txMgr1.newTx();

        TableMgr tableMgr1 = new TableMgr(tx1);

        // Calculate approximate slots: tblcat (2 fields) + fldcat (5 fields) = 7 slots
        // With ~8 slots per block, we need 1 more field to complete first block

        // Create first table with 1 field to complete block 0
        Schema s1 = new Schema();
        s1.addIntField("complete");
        tableMgr1.createTable("CompleteBlock", s1, tx1);

        // Now block 0 is full (8 slots used). Create more tables to fill block 1.
        for (int i = 0; i < 8; i++) {
            Schema s = new Schema();
            s.addIntField("f");
            tableMgr1.createTable("Block1_" + i, s, tx1);
        }

        // Block 1 should now also be full. Delete a table at the end of block 1
        // to create a gap that starts at the beginning of block 2
        tableMgr1.replace("Block1_7", null, tx1);

        tx1.commit();

        // Phase 2: Restart to trigger gap detection during loadCatalogCache
        TxMgrBase txMgr2 = createTxMgr(testName, false);
        TxBase tx2 = txMgr2.newTx();

        TableMgr tableMgr2 = new TableMgr(tx2);

        // Verify the remaining tables
        assertNotNull(tableMgr2.getLayout("Block1_6", tx2));
        assertNull(tableMgr2.getLayout("Block1_7", tx2)); // Deleted

        // Create a table that should reuse the gap
        Schema newSchema = new Schema();
        newSchema.addIntField("reuse");
        tableMgr2.createTable("ReuseGap", newSchema, tx2);

        assertNotNull(tableMgr2.getLayout("ReuseGap", tx2));

        tx2.commit();
    }

    @Test
    @DisplayName("Multiple block-spanning gaps detected and reused correctly on reload")
    void multipleBlockSpanningGapsOnReload() {
        // This test creates multiple gaps that span block boundaries to ensure
        // the gap detection logic handles all of them correctly

        String testName = "multiBlockGaps";

        // Phase 1: Create many tables spanning multiple blocks
        TxMgrBase txMgr1 = createTxMgr(testName, true);
        TxBase tx1 = txMgr1.newTx();

        TableMgr tableMgr1 = new TableMgr(tx1);

        // Create 30 tables with 2 fields each = 60 field entries
        // Should span ~8 blocks (60 / 8 = 7.5 blocks)
        for (int i = 0; i < 30; i++) {
            Schema s = new Schema();
            s.addIntField("a");
            s.addIntField("b");
            tableMgr1.createTable("MultiGap" + i, s, tx1);
        }

        // Delete tables at various positions to create gaps at different block positions
        tableMgr1.replace("MultiGap5", null, tx1);  // Gap in first half
        tableMgr1.replace("MultiGap15", null, tx1); // Gap in middle
        tableMgr1.replace("MultiGap25", null, tx1); // Gap near end

        tx1.commit();

        // Phase 2: Restart to detect all gaps
        TxMgrBase txMgr2 = createTxMgr(testName, false);
        TxBase tx2 = txMgr2.newTx();

        TableMgr tableMgr2 = new TableMgr(tx2);

        // Verify gaps exist
        assertNull(tableMgr2.getLayout("MultiGap5", tx2));
        assertNull(tableMgr2.getLayout("MultiGap15", tx2));
        assertNull(tableMgr2.getLayout("MultiGap25", tx2));

        // Verify neighbors still exist
        assertNotNull(tableMgr2.getLayout("MultiGap4", tx2));
        assertNotNull(tableMgr2.getLayout("MultiGap6", tx2));
        assertNotNull(tableMgr2.getLayout("MultiGap14", tx2));
        assertNotNull(tableMgr2.getLayout("MultiGap16", tx2));

        // Create new tables to reuse the detected gaps
        for (int i = 0; i < 3; i++) {
            Schema s = new Schema();
            s.addIntField("reused");
            tableMgr2.createTable("Reused" + i, s, tx2);
        }

        assertNotNull(tableMgr2.getLayout("Reused0", tx2));
        assertNotNull(tableMgr2.getLayout("Reused1", tx2));
        assertNotNull(tableMgr2.getLayout("Reused2", tx2));

        tx2.commit();
    }

    // ========== Catalog Scanability Coverage ==========

    @Test
    @DisplayName("tblcat TABLE_NAME field contains correct table names")
    void tblcatContainsCorrectTableNames() {
        TxMgrBase txMgr = createTxMgr("tblcatTableNames", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema = new Schema();
        schema.addIntField("id");
        tableMgr.createTable("CustomTable1", schema, tx);
        tableMgr.createTable("CustomTable2", schema, tx);

        LayoutBase tblcatLayout = tableMgr.getLayout(TableMgrBase.TABLE_META_DATA_TABLE, tx);
        TableScan scan = new TableScan(tx, TableMgrBase.TABLE_META_DATA_TABLE, tblcatLayout);

        List<String> tableNames = new ArrayList<>();
        scan.beforeFirst();
        while (scan.next()) {
            tableNames.add(scan.getString(TableMgrBase.TABLE_NAME));
        }
        scan.close();

        assertTrue(tableNames.contains("CustomTable1"));
        assertTrue(tableNames.contains("CustomTable2"));
        assertTrue(tableNames.contains(TableMgrBase.TABLE_META_DATA_TABLE));
        assertTrue(tableNames.contains(TableMgrBase.FIELD_META_DATA_TABLE));

        tx.commit();
    }

    @Test
    @DisplayName("fldcat contains tblname, fldname, type, length, offset fields")
    void fldcatContainsRequiredFields() {
        TxMgrBase txMgr = createTxMgr("fldcatRequiredFields", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        LayoutBase fldcatLayout = tableMgr.getLayout(TableMgrBase.FIELD_META_DATA_TABLE, tx);

        assertTrue(fldcatLayout.schema().hasField(TableMgrBase.TABLE_NAME));
        assertTrue(fldcatLayout.schema().hasField("fldname"));
        assertTrue(fldcatLayout.schema().hasField("type"));
        assertTrue(fldcatLayout.schema().hasField("length"));
        assertTrue(fldcatLayout.schema().hasField("offset"));

        tx.commit();
    }

    @Test
    @DisplayName("Scanning tblcat after multiple createTable calls shows all tables")
    void scanTblcatShowsAllTables() {
        TxMgrBase txMgr = createTxMgr("scanTblcatShowsAll", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        int tableCount = 10;
        for (int i = 0; i < tableCount; i++) {
            Schema schema = new Schema();
            schema.addIntField("id");
            tableMgr.createTable("ScanTest" + i, schema, tx);
        }

        LayoutBase tblcatLayout = tableMgr.getLayout(TableMgrBase.TABLE_META_DATA_TABLE, tx);
        TableScan scan = new TableScan(tx, TableMgrBase.TABLE_META_DATA_TABLE, tblcatLayout);

        Set<String> foundTables = new HashSet<>();
        scan.beforeFirst();
        while (scan.next()) {
            foundTables.add(scan.getString(TableMgrBase.TABLE_NAME));
        }
        scan.close();

        for (int i = 0; i < tableCount; i++) {
            assertTrue(foundTables.contains("ScanTest" + i));
        }

        tx.commit();
    }

    @Test
    @DisplayName("Scanning fldcat shows all fields for all tables")
    void scanFldcatShowsAllFields() {
        TxMgrBase txMgr = createTxMgr("scanFldcatShowsAll", true);
        TxBase tx = txMgr.newTx();

        TableMgr tableMgr = new TableMgr(tx);

        Schema schema1 = new Schema();
        schema1.addIntField("a");
        schema1.addIntField("b");
        tableMgr.createTable("FldcatTest1", schema1, tx);

        Schema schema2 = new Schema();
        schema2.addDoubleField("x");
        schema2.addDoubleField("y");
        schema2.addDoubleField("z");
        tableMgr.createTable("FldcatTest2", schema2, tx);

        LayoutBase fldcatLayout = tableMgr.getLayout(TableMgrBase.FIELD_META_DATA_TABLE, tx);
        TableScan scan = new TableScan(tx, TableMgrBase.FIELD_META_DATA_TABLE, fldcatLayout);

        int table1FieldCount = 0;
        int table2FieldCount = 0;
        scan.beforeFirst();
        while (scan.next()) {
            String tblName = scan.getString(TableMgrBase.TABLE_NAME);
            if ("FldcatTest1".equals(tblName)) {
                table1FieldCount++;
            } else if ("FldcatTest2".equals(tblName)) {
                table2FieldCount++;
            }
        }
        scan.close();

        assertEquals(2, table1FieldCount);
        assertEquals(3, table2FieldCount);

        tx.commit();
    }
}
