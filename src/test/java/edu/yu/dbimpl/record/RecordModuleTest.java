package edu.yu.dbimpl.record;

import static org.junit.jupiter.api.Assertions.*;
import static edu.yu.dbimpl.record.RecordPageBase.BEFORE_FIRST_SLOT;

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
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.query.Datum;
import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.tx.TxBase;
import edu.yu.dbimpl.tx.TxMgr;
import edu.yu.dbimpl.tx.TxMgrBase;

/**
 * Comprehensive correctness tests for the Record module.
 * Tests cover Schema, Layout, RecordPage, TableScan individually and together.
 */
public class RecordModuleTest {

    private static final Logger logger = LogManager.getLogger(RecordModuleTest.class);

    // Test configuration constants
    private static final int BLOCK_SIZE = 400;
    private static final int BUFFER_SIZE = 10;
    private static final int MAX_WAIT_TIME = 500;
    private static final String TEST_BASE_DIR = "testing/RecordModuleTest";
    private static final String LOG_FILE = "test_logfile";

    /**
     * Helper method to create a fresh TxMgr for each test.
     * Sets up DB configuration and creates required infrastructure.
     */
    private TxMgrBase createTxMgr(String testName) {
        Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        Path testDir = Path.of(TEST_BASE_DIR, testName);
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
        schema.addStringField("name", 20);
        schema.addBooleanField("active");
        schema.addDoubleField("salary");
        return schema;
    }

    // ========================================================================
    // SECTION 1: SCHEMA CORRECTNESS TESTS
    // ========================================================================

    @Nested
    @DisplayName("Schema Correctness Tests")
    class SchemaTests {

        @Test
        @DisplayName("Field ordering is preserved when fields are added")
        void fieldOrderingPreserved() {
            Schema schema = new Schema();
            schema.addIntField("first");
            schema.addStringField("second", 10);
            schema.addBooleanField("third");
            schema.addDoubleField("fourth");

            List<String> fields = schema.fields();
            assertEquals(4, fields.size());
            assertEquals("first", fields.get(0));
            assertEquals("second", fields.get(1));
            assertEquals("third", fields.get(2));
            assertEquals("fourth", fields.get(3));
        }

        @Test
        @DisplayName("addIntField sets correct type and length")
        void addIntFieldCorrectness() {
            Schema schema = new Schema();
            schema.addIntField("intField");

            assertTrue(schema.hasField("intField"));
            assertEquals(Types.INTEGER, schema.type("intField"));
            assertEquals(Integer.BYTES, schema.length("intField"));
        }

        @Test
        @DisplayName("addStringField sets correct type and length")
        void addStringFieldCorrectness() {
            Schema schema = new Schema();
            int logicalLength = 50;
            schema.addStringField("stringField", logicalLength);

            assertTrue(schema.hasField("stringField"));
            assertEquals(Types.VARCHAR, schema.type("stringField"));
            assertEquals(logicalLength, schema.length("stringField"));
        }

        @Test
        @DisplayName("addBooleanField sets correct type")
        void addBooleanFieldCorrectness() {
            Schema schema = new Schema();
            schema.addBooleanField("boolField");

            assertTrue(schema.hasField("boolField"));
            assertEquals(Types.BOOLEAN, schema.type("boolField"));
        }

        @Test
        @DisplayName("addDoubleField sets correct type and length")
        void addDoubleFieldCorrectness() {
            Schema schema = new Schema();
            schema.addDoubleField("doubleField");

            assertTrue(schema.hasField("doubleField"));
            assertEquals(Types.DOUBLE, schema.type("doubleField"));
            assertEquals(Double.BYTES, schema.length("doubleField"));
        }

        @Test
        @DisplayName("Schema composition with add() copies field correctly")
        void schemaCompositionWithAdd() {
            Schema source = new Schema();
            source.addIntField("id");
            source.addStringField("name", 30);

            Schema target = new Schema();
            target.add("id", source);

            assertTrue(target.hasField("id"));
            assertEquals(Types.INTEGER, target.type("id"));
            assertFalse(target.hasField("name")); // Only "id" was added
        }

        @Test
        @DisplayName("Schema composition with addAll() copies all fields")
        void schemaCompositionWithAddAll() {
            Schema source = new Schema();
            source.addIntField("id");
            source.addStringField("name", 30);
            source.addBooleanField("active");

            Schema target = new Schema();
            target.addAll(source);

            assertEquals(3, target.fields().size());
            assertTrue(target.hasField("id"));
            assertTrue(target.hasField("name"));
            assertTrue(target.hasField("active"));
            assertEquals(Types.INTEGER, target.type("id"));
            assertEquals(Types.VARCHAR, target.type("name"));
            assertEquals(Types.BOOLEAN, target.type("active"));
        }

        @Test
        @DisplayName("Duplicate field updates overwrite previous values")
        void duplicateFieldUpdates() {
            Schema schema = new Schema();
            schema.addIntField("field");
            assertEquals(Types.INTEGER, schema.type("field"));

            // Update the field to a different type
            schema.addDoubleField("field");
            assertEquals(Types.DOUBLE, schema.type("field"));
            assertEquals(Double.BYTES, schema.length("field"));

            // Field list should still have only one entry
            assertEquals(1, schema.fields().size());
        }

        @Test
        @DisplayName("Empty schema operations work correctly")
        void emptySchemaOperations() {
            Schema schema = new Schema();

            assertTrue(schema.fields().isEmpty());
            assertFalse(schema.hasField("anyField"));
        }

        @Test
        @DisplayName("hasField returns false for non-existent field")
        void hasFieldNonExistent() {
            Schema schema = new Schema();
            schema.addIntField("exists");

            assertTrue(schema.hasField("exists"));
            assertFalse(schema.hasField("doesNotExist"));
        }
    }

    // ========================================================================
    // SECTION 2: LAYOUT CORRECTNESS TESTS
    // ========================================================================

    @Nested
    @DisplayName("Layout Correctness Tests")
    class LayoutTests {

        @Test
        @DisplayName("Offsets are calculated correctly for all field types")
        void offsetCalculationCorrectness() {
            Schema schema = new Schema();
            schema.addIntField("intField"); // 4 bytes
            schema.addBooleanField("boolField"); // 1 byte
            schema.addDoubleField("doubleField"); // 8 bytes
            schema.addStringField("stringField", 10); // 10 + 4 (length prefix) = 14 bytes

            Layout layout = new Layout(schema);

            // First byte is in-use flag
            // intField starts at offset 1
            assertEquals(1, layout.offset("intField"));
            // boolField starts at offset 1 + 4 = 5
            assertEquals(5, layout.offset("boolField"));
            // doubleField starts at offset 5 + 1 = 6
            assertEquals(6, layout.offset("doubleField"));
            // stringField starts at offset 6 + 8 = 14
            assertEquals(14, layout.offset("stringField"));
        }

        @Test
        @DisplayName("Offsets are sequential with no gaps")
        void offsetsSequentialNoGaps() {
            Schema schema = new Schema();
            schema.addIntField("f1");
            schema.addIntField("f2");
            schema.addIntField("f3");

            Layout layout = new Layout(schema);

            // Offset of f1 should be 1 (after in-use byte)
            assertEquals(1, layout.offset("f1"));
            // Offset of f2 should be 1 + 4 = 5
            assertEquals(5, layout.offset("f2"));
            // Offset of f3 should be 5 + 4 = 9
            assertEquals(9, layout.offset("f3"));
        }

        @Test
        @DisplayName("Slot size is correctly calculated")
        void slotSizeCalculation() {
            Schema schema = new Schema();
            schema.addIntField("id"); // 4 bytes
            schema.addBooleanField("active"); // 1 byte
            schema.addDoubleField("amount"); // 8 bytes
            schema.addStringField("name", 10); // 10 + 4 = 14 bytes

            Layout layout = new Layout(schema);

            // Expected: 1 (in-use) + 4 + 1 + 8 + 14 = 28
            int expectedSlotSize = 1 + 4 + 1 + 8 + (10 + Integer.BYTES);
            assertEquals(expectedSlotSize, layout.slotSize());
        }

        @Test
        @DisplayName("Schema reference integrity is maintained")
        void schemaReferenceIntegrity() {
            Schema schema = new Schema();
            schema.addIntField("id");
            schema.addStringField("name", 20);

            Layout layout = new Layout(schema);

            assertSame(schema, layout.schema());
            assertEquals(2, layout.schema().fields().size());
            assertTrue(layout.schema().hasField("id"));
            assertTrue(layout.schema().hasField("name"));
        }

        @Test
        @DisplayName("Layout with single field has correct slot size")
        void singleFieldSlotSize() {
            Schema schema = new Schema();
            schema.addIntField("onlyField");

            Layout layout = new Layout(schema);

            // 1 (in-use) + 4 (int) = 5
            assertEquals(5, layout.slotSize());
            assertEquals(1, layout.offset("onlyField"));
        }

        @Test
        @DisplayName("Offset throws IAE for unknown field")
        void offsetThrowsForUnknownField() {
            Schema schema = new Schema();
            schema.addIntField("known");

            Layout layout = new Layout(schema);

            assertThrows(IllegalArgumentException.class, () -> layout.offset("unknown"));
        }
    }

    // ========================================================================
    // SECTION 3: RECORDPAGE CORRECTNESS TESTS
    // ========================================================================

    @Nested
    @DisplayName("RecordPage Correctness Tests")
    class RecordPageTests {

        @Test
        @DisplayName("After format(), all slots are empty (nextAfter returns -1)")
        void formatCorrectness() {
            TxMgrBase txMgr = createTxMgr("formatCorrectness");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("id");
            Layout layout = new Layout(schema);

            BlockIdBase blk = tx.append("test");
            tx.pin(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);
            rp.format();

            // All slots should be empty after format
            assertEquals(-1, rp.nextAfter(BEFORE_FIRST_SLOT));

            tx.unpin(blk);
            tx.rollback();
        }

        @Test
        @DisplayName("Insert and get roundtrip for INTEGER type")
        void insertGetRoundtripInteger() {
            TxMgrBase txMgr = createTxMgr("insertGetRoundtripInteger");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("value");
            Layout layout = new Layout(schema);

            BlockIdBase blk = tx.append("test");
            tx.pin(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);
            rp.format();

            int slot = rp.insertAfter(BEFORE_FIRST_SLOT);
            rp.setInt(slot, "value", 12345);
            assertEquals(12345, rp.getInt(slot, "value"));

            tx.unpin(blk);
            tx.rollback();
        }

        @Test
        @DisplayName("Insert and get roundtrip for VARCHAR type")
        void insertGetRoundtripVarchar() {
            TxMgrBase txMgr = createTxMgr("insertGetRoundtripVarchar");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addStringField("text", 50);
            Layout layout = new Layout(schema);

            BlockIdBase blk = tx.append("test");
            tx.pin(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);
            rp.format();

            int slot = rp.insertAfter(BEFORE_FIRST_SLOT);
            rp.setString(slot, "text", "Hello World!");
            assertEquals("Hello World!", rp.getString(slot, "text"));

            tx.unpin(blk);
            tx.rollback();
        }

        @Test
        @DisplayName("Insert and get roundtrip for BOOLEAN type")
        void insertGetRoundtripBoolean() {
            TxMgrBase txMgr = createTxMgr("insertGetRoundtripBoolean");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addBooleanField("flag");
            Layout layout = new Layout(schema);

            BlockIdBase blk = tx.append("test");
            tx.pin(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);
            rp.format();

            int slot = rp.insertAfter(BEFORE_FIRST_SLOT);
            rp.setBoolean(slot, "flag", true);
            assertTrue(rp.getBoolean(slot, "flag"));

            rp.setBoolean(slot, "flag", false);
            assertFalse(rp.getBoolean(slot, "flag"));

            tx.unpin(blk);
            tx.rollback();
        }

        @Test
        @DisplayName("Insert and get roundtrip for DOUBLE type")
        void insertGetRoundtripDouble() {
            TxMgrBase txMgr = createTxMgr("insertGetRoundtripDouble");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addDoubleField("amount");
            Layout layout = new Layout(schema);

            BlockIdBase blk = tx.append("test");
            tx.pin(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);
            rp.format();

            int slot = rp.insertAfter(BEFORE_FIRST_SLOT);
            rp.setDouble(slot, "amount", 3.14159);
            assertEquals(3.14159, rp.getDouble(slot, "amount"), 0.00001);

            tx.unpin(blk);
            tx.rollback();
        }

        @Test
        @DisplayName("Slot lifecycle: insert -> set -> read -> delete -> verify ISE")
        void slotLifecycle() {
            TxMgrBase txMgr = createTxMgr("slotLifecycle");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("id");
            Layout layout = new Layout(schema);

            BlockIdBase blk = tx.append("test");
            tx.pin(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);
            rp.format();

            // Insert and set
            int slot = rp.insertAfter(BEFORE_FIRST_SLOT);
            rp.setInt(slot, "id", 42);

            // Read
            assertEquals(42, rp.getInt(slot, "id"));

            // Delete
            rp.delete(slot);

            // Verify ISE on access after delete
            assertThrows(IllegalStateException.class, () -> rp.getInt(slot, "id"));

            tx.unpin(blk);
            tx.rollback();
        }

        @Test
        @DisplayName("Fill entire page, insertAfter returns -1 when full")
        void boundaryConditionPageFull() {
            TxMgrBase txMgr = createTxMgr("boundaryConditionPageFull");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("id");
            Layout layout = new Layout(schema);

            BlockIdBase blk = tx.append("test");
            tx.pin(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);
            rp.format();

            // Calculate expected slots
            int expectedSlots = BLOCK_SIZE / layout.slotSize();

            // Fill all slots
            int insertedCount = 0;
            int slot = BEFORE_FIRST_SLOT;
            while (true) {
                int nextSlot = rp.insertAfter(slot);
                if (nextSlot == -1)
                    break;
                rp.setInt(nextSlot, "id", insertedCount);
                slot = nextSlot;
                insertedCount++;
            }

            assertEquals(expectedSlots, insertedCount);

            // Verify insertAfter returns -1 when page is full
            assertEquals(-1, rp.insertAfter(BEFORE_FIRST_SLOT));

            tx.unpin(blk);
            tx.rollback();
        }

        @Test
        @DisplayName("Multi-slot correctness: insert multiple records, verify each independently")
        void multiSlotCorrectness() {
            TxMgrBase txMgr = createTxMgr("multiSlotCorrectness");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("id");
            schema.addStringField("name", 10);
            Layout layout = new Layout(schema);

            BlockIdBase blk = tx.append("test");
            tx.pin(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);
            rp.format();

            // Insert multiple records
            int slot1 = rp.insertAfter(BEFORE_FIRST_SLOT);
            rp.setInt(slot1, "id", 100);
            rp.setString(slot1, "name", "Alice");

            int slot2 = rp.insertAfter(slot1);
            rp.setInt(slot2, "id", 200);
            rp.setString(slot2, "name", "Bob");

            int slot3 = rp.insertAfter(slot2);
            rp.setInt(slot3, "id", 300);
            rp.setString(slot3, "name", "Charlie");

            // Verify each record independently
            assertEquals(100, rp.getInt(slot1, "id"));
            assertEquals("Alice", rp.getString(slot1, "name"));

            assertEquals(200, rp.getInt(slot2, "id"));
            assertEquals("Bob", rp.getString(slot2, "name"));

            assertEquals(300, rp.getInt(slot3, "id"));
            assertEquals("Charlie", rp.getString(slot3, "name"));

            tx.unpin(blk);
            tx.rollback();
        }

        @Test
        @DisplayName("Field type enforcement: IAE thrown when accessing wrong type")
        void fieldTypeEnforcement() {
            TxMgrBase txMgr = createTxMgr("fieldTypeEnforcement");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("intField");
            schema.addStringField("stringField", 20);
            Layout layout = new Layout(schema);

            BlockIdBase blk = tx.append("test");
            tx.pin(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);
            rp.format();

            int slot = rp.insertAfter(BEFORE_FIRST_SLOT);
            rp.setInt(slot, "intField", 42);
            rp.setString(slot, "stringField", "test");

            // Try to access int field as string - should throw IAE
            assertThrows(IllegalArgumentException.class, () -> rp.getString(slot, "intField"));

            // Try to access string field as int - should throw IAE
            assertThrows(IllegalArgumentException.class, () -> rp.getInt(slot, "stringField"));

            tx.unpin(blk);
            tx.rollback();
        }

        @Test
        @DisplayName("nextAfter finds in-use slots correctly")
        void nextAfterFindsInUseSlots() {
            TxMgrBase txMgr = createTxMgr("nextAfterFindsInUseSlots");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("id");
            Layout layout = new Layout(schema);

            BlockIdBase blk = tx.append("test");
            tx.pin(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);
            rp.format();

            // Insert 3 records
            int slot1 = rp.insertAfter(BEFORE_FIRST_SLOT);
            rp.setInt(slot1, "id", 1);
            int slot2 = rp.insertAfter(slot1);
            rp.setInt(slot2, "id", 2);
            int slot3 = rp.insertAfter(slot2);
            rp.setInt(slot3, "id", 3);

            // Delete the middle one
            rp.delete(slot2);

            // nextAfter should skip the deleted slot
            int found1 = rp.nextAfter(BEFORE_FIRST_SLOT);
            assertEquals(slot1, found1);

            int found2 = rp.nextAfter(found1);
            assertEquals(slot3, found2); // Skip deleted slot2

            int found3 = rp.nextAfter(found2);
            assertEquals(-1, found3); // No more slots

            tx.unpin(blk);
            tx.rollback();
        }

        @Test
        @DisplayName("block() returns the encapsulated block")
        void blockReturnsEncapsulatedBlock() {
            TxMgrBase txMgr = createTxMgr("blockReturnsEncapsulatedBlock");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("id");
            Layout layout = new Layout(schema);

            BlockIdBase blk = tx.append("test");
            tx.pin(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);

            assertEquals(blk, rp.block());

            tx.unpin(blk);
            tx.rollback();
        }
    }

    // ========================================================================
    // SECTION 4: TABLESCAN CORRECTNESS TESTS
    // ========================================================================

    @Nested
    @DisplayName("TableScan Correctness Tests")
    class TableScanTests {

        @Test
        @DisplayName("Empty table scan: next() returns false on new/empty table")
        void emptyTableScan() {
            TxMgrBase txMgr = createTxMgr("emptyTableScan");
            TxBase tx = txMgr.newTx();

            SchemaBase schema = createStandardSchema();
            Layout layout = new Layout(schema);

            TableScan ts = new TableScan(tx, "emptyTable", layout);

            assertFalse(ts.next());

            ts.close();
            tx.rollback();
        }

        @Test
        @DisplayName("Single record CRUD operations")
        void singleRecordCrud() {
            TxMgrBase txMgr = createTxMgr("singleRecordCrud");
            TxBase tx = txMgr.newTx();

            SchemaBase schema = createStandardSchema();
            Layout layout = new Layout(schema);

            TableScan ts = new TableScan(tx, "crudTable", layout);

            // Create
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "Test User");
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0);

            // Read immediately after insert
            assertEquals(1, ts.getInt("id"));
            assertEquals("Test User", ts.getString("name"));
            assertTrue(ts.getBoolean("active"));
            assertEquals(50000.0, ts.getDouble("salary"), 0.001);

            // Update
            ts.setInt("id", 2);
            ts.setString("name", "Updated User");
            ts.setBoolean("active", false);
            ts.setDouble("salary", 60000.0);

            // Read after update
            assertEquals(2, ts.getInt("id"));
            assertEquals("Updated User", ts.getString("name"));
            assertFalse(ts.getBoolean("active"));
            assertEquals(60000.0, ts.getDouble("salary"), 0.001);

            // Delete
            ts.delete();

            // Verify table is empty after delete
            ts.beforeFirst();
            assertFalse(ts.next());

            ts.close();
            tx.rollback();
        }

        @Test
        @DisplayName("Multi-block traversal: insert enough records to span multiple blocks")
        void multiBlockTraversal() {
            TxMgrBase txMgr = createTxMgr("multiBlockTraversal");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("id");
            schema.addStringField("data", 10);
            Layout layout = new Layout(schema);

            TableScan ts = new TableScan(tx, "multiBlockTable", layout);

            // Calculate how many records fit in one block
            int slotsPerBlock = BLOCK_SIZE / layout.slotSize();
            // Insert enough to span at least 3 blocks
            int totalRecords = slotsPerBlock * 3;

            for (int i = 0; i < totalRecords; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("data", "rec" + i);
            }

            // Verify all records can be read back
            ts.beforeFirst();
            int count = 0;
            while (ts.next()) {
                assertEquals("rec" + ts.getInt("id"), ts.getString("data"));
                count++;
            }

            assertEquals(totalRecords, count);

            ts.close();
            tx.rollback();
        }

        @Test
        @DisplayName("RID navigation: insert records, save RIDs, use moveToRid() to navigate")
        void ridNavigation() {
            TxMgrBase txMgr = createTxMgr("ridNavigation");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("id");
            Layout layout = new Layout(schema);

            TableScan ts = new TableScan(tx, "ridTable", layout);

            // Insert records and save their RIDs
            List<RID> rids = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                ts.insert();
                ts.setInt("id", i * 100);
                rids.add(ts.getRid());
            }

            // Navigate to each RID and verify the value
            for (int i = 0; i < 10; i++) {
                ts.moveToRid(rids.get(i));
                assertEquals(i * 100, ts.getInt("id"));
            }

            // Navigate in reverse order
            for (int i = 9; i >= 0; i--) {
                ts.moveToRid(rids.get(i));
                assertEquals(i * 100, ts.getInt("id"));
            }

            ts.close();
            tx.rollback();
        }

        @Test
        @DisplayName("Delete mid-scan: insert records, scan and delete some, verify remaining count")
        void deleteMidScan() {
            TxMgrBase txMgr = createTxMgr("deleteMidScan");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("value");
            Layout layout = new Layout(schema);

            TableScan ts = new TableScan(tx, "deleteTable", layout);

            // Insert 20 records with values 0-19
            for (int i = 0; i < 20; i++) {
                ts.insert();
                ts.setInt("value", i);
            }

            // Delete all even-valued records
            ts.beforeFirst();
            int deletedCount = 0;
            while (ts.next()) {
                int val = ts.getInt("value");
                if (val % 2 == 0) {
                    ts.delete();
                    deletedCount++;
                }
            }
            assertEquals(10, deletedCount);

            // Verify remaining records are all odd
            ts.beforeFirst();
            int remainingCount = 0;
            while (ts.next()) {
                int val = ts.getInt("value");
                assertTrue(val % 2 == 1, "Remaining value should be odd: " + val);
                remainingCount++;
            }
            assertEquals(10, remainingCount);

            ts.close();
            tx.rollback();
        }

        @Test
        @DisplayName("Insert after delete: verify reuse of deleted slots")
        void insertAfterDelete() {
            TxMgrBase txMgr = createTxMgr("insertAfterDelete");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("id");
            Layout layout = new Layout(schema);

            // Calculate slots per block
            int slotsPerBlock = BLOCK_SIZE / layout.slotSize();

            TableScan ts = new TableScan(tx, "reuseTable", layout);

            // Fill exactly one block
            for (int i = 0; i < slotsPerBlock; i++) {
                ts.insert();
                ts.setInt("id", i);
            }

            // Delete half the records
            ts.beforeFirst();
            while (ts.next()) {
                if (ts.getInt("id") % 2 == 0) {
                    ts.delete();
                }
            }

            // Insert new records - they should reuse deleted slots
            for (int i = 0; i < slotsPerBlock / 2; i++) {
                ts.insert();
                ts.setInt("id", 1000 + i);
            }

            // Count total records - should still be slotsPerBlock
            ts.beforeFirst();
            int count = 0;
            while (ts.next()) {
                count++;
            }
            assertEquals(slotsPerBlock, count);

            ts.close();
            tx.rollback();
        }

        @Test
        @DisplayName("getVal/setVal roundtrip for all types using Datum")
        void getValSetValRoundtrip() {
            TxMgrBase txMgr = createTxMgr("getValSetValRoundtrip");
            TxBase tx = txMgr.newTx();

            SchemaBase schema = createStandardSchema();
            Layout layout = new Layout(schema);

            TableScan ts = new TableScan(tx, "datumTable", layout);

            ts.insert();

            // Set values using Datum
            ts.setVal("id", new Datum(42));
            ts.setVal("name", new Datum("Datum Test"));
            ts.setVal("active", new Datum(true));
            ts.setVal("salary", new Datum(75000.50));

            // Get values and verify types and values
            DatumBase idVal = ts.getVal("id");
            assertEquals(Types.INTEGER, idVal.getSQLType());
            assertEquals(42, idVal.asInt());

            DatumBase nameVal = ts.getVal("name");
            assertEquals(Types.VARCHAR, nameVal.getSQLType());
            assertEquals("Datum Test", nameVal.asString());

            DatumBase activeVal = ts.getVal("active");
            assertEquals(Types.BOOLEAN, activeVal.getSQLType());
            assertTrue(activeVal.asBoolean());

            DatumBase salaryVal = ts.getVal("salary");
            assertEquals(Types.DOUBLE, salaryVal.getSQLType());
            assertEquals(75000.50, salaryVal.asDouble(), 0.001);

            ts.close();
            tx.rollback();
        }

        @Test
        @DisplayName("hasField returns correct value for schema fields")
        void hasFieldCorrectness() {
            TxMgrBase txMgr = createTxMgr("hasFieldCorrectness");
            TxBase tx = txMgr.newTx();

            SchemaBase schema = createStandardSchema();
            Layout layout = new Layout(schema);

            TableScan ts = new TableScan(tx, "hasFieldTable", layout);

            assertTrue(ts.hasField("id"));
            assertTrue(ts.hasField("name"));
            assertTrue(ts.hasField("active"));
            assertTrue(ts.hasField("salary"));
            assertFalse(ts.hasField("nonexistent"));

            ts.close();
            tx.rollback();
        }

        @Test
        @DisplayName("getType returns correct SQL type for each field")
        void getTypeCorrectness() {
            TxMgrBase txMgr = createTxMgr("getTypeCorrectness");
            TxBase tx = txMgr.newTx();

            SchemaBase schema = createStandardSchema();
            Layout layout = new Layout(schema);

            TableScan ts = new TableScan(tx, "getTypeTable", layout);

            assertEquals(Types.INTEGER, ts.getType("id"));
            assertEquals(Types.VARCHAR, ts.getType("name"));
            assertEquals(Types.BOOLEAN, ts.getType("active"));
            assertEquals(Types.DOUBLE, ts.getType("salary"));

            ts.close();
            tx.rollback();
        }

        @Test
        @DisplayName("getTableFileName returns the table name")
        void getTableFileName() {
            TxMgrBase txMgr = createTxMgr("getTableFileName");
            TxBase tx = txMgr.newTx();

            SchemaBase schema = createStandardSchema();
            Layout layout = new Layout(schema);

            String tableName = "myTestTable";
            TableScan ts = new TableScan(tx, tableName, layout);

            assertEquals(tableName, ts.getTableFileName());

            ts.close();
            tx.rollback();
        }

        @Test
        @DisplayName("beforeFirst resets cursor to beginning")
        void beforeFirstResetsCursor() {
            TxMgrBase txMgr = createTxMgr("beforeFirstResetsCursor");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("seq");
            Layout layout = new Layout(schema);

            TableScan ts = new TableScan(tx, "beforeFirstTable", layout);

            // Insert records
            for (int i = 1; i <= 5; i++) {
                ts.insert();
                ts.setInt("seq", i);
            }

            // First scan
            ts.beforeFirst();
            assertTrue(ts.next());
            int firstValue = ts.getInt("seq");

            // Scan through all
            while (ts.next()) {
            }

            // Reset and verify we get the same first value
            ts.beforeFirst();
            assertTrue(ts.next());
            assertEquals(firstValue, ts.getInt("seq"));

            ts.close();
            tx.rollback();
        }
    }

    // ========================================================================
    // SECTION 5: INTEGRATION/WORKFLOW TESTS (DEMO-STYLE)
    // ========================================================================

    @Nested
    @DisplayName("Integration/Workflow Tests")
    class IntegrationTests {

        @Test
        @DisplayName("Full workflow: Schema -> Layout -> RecordPage -> Insert -> Read -> Delete")
        void fullRecordPageWorkflow() {
            logger.info("Starting fullRecordPageWorkflow test");
            TxMgrBase txMgr = createTxMgr("fullRecordPageWorkflow");
            TxBase tx = txMgr.newTx();

            // Create schema
            logger.info("Creating schema with int and string fields");
            Schema schema = new Schema();
            schema.addIntField("empId");
            schema.addStringField("empName", 15);
            schema.addDoubleField("empSalary");

            // Create layout
            logger.info("Creating layout from schema");
            Layout layout = new Layout(schema);
            logger.info("Slot size: {}", layout.slotSize());

            // Create and format RecordPage
            BlockIdBase blk = tx.append("employeeFile");
            tx.pin(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);
            logger.info("Formatting block {}", blk);
            rp.format();

            // Insert records
            logger.info("Inserting records into RecordPage");
            int recordCount = 0;
            int slot = BEFORE_FIRST_SLOT;
            while (true) {
                int nextSlot = rp.insertAfter(slot);
                if (nextSlot == -1)
                    break;

                rp.setInt(nextSlot, "empId", recordCount + 1000);
                rp.setString(nextSlot, "empName", "Emp" + recordCount);
                rp.setDouble(nextSlot, "empSalary", 50000.0 + recordCount * 1000);

                slot = nextSlot;
                recordCount++;
            }
            logger.info("Inserted {} records", recordCount);
            assertTrue(recordCount > 0);

            // Read all records
            logger.info("Reading all records");
            int readCount = 0;
            slot = rp.nextAfter(BEFORE_FIRST_SLOT);
            while (slot >= 0) {
                int id = rp.getInt(slot, "empId");
                String name = rp.getString(slot, "empName");
                double salary = rp.getDouble(slot, "empSalary");

                assertEquals(readCount + 1000, id);
                assertEquals("Emp" + readCount, name);
                assertEquals(50000.0 + readCount * 1000, salary, 0.001);

                readCount++;
                slot = rp.nextAfter(slot);
            }
            assertEquals(recordCount, readCount);

            // Delete half the records
            logger.info("Deleting every other record");
            int deleteCount = 0;
            slot = rp.nextAfter(BEFORE_FIRST_SLOT);
            while (slot >= 0) {
                int nextSlot = rp.nextAfter(slot);
                if (deleteCount % 2 == 0) {
                    rp.delete(slot);
                }
                deleteCount++;
                slot = nextSlot;
            }

            // Verify remaining records
            int remainingCount = 0;
            slot = rp.nextAfter(BEFORE_FIRST_SLOT);
            while (slot >= 0) {
                remainingCount++;
                slot = rp.nextAfter(slot);
            }
            logger.info("Remaining records: {}", remainingCount);
            assertEquals(recordCount / 2, remainingCount);

            tx.unpin(blk);
            tx.commit();
            logger.info("Completed fullRecordPageWorkflow test");
        }

        @Test
        @DisplayName("TableScan abstraction workflow: same operations using TableScan")
        void tableScanAbstractionWorkflow() {
            logger.info("Starting tableScanAbstractionWorkflow test");
            TxMgrBase txMgr = createTxMgr("tableScanAbstractionWorkflow");
            TxBase tx = txMgr.newTx();

            // Create schema
            Schema schema = new Schema();
            schema.addIntField("productId");
            schema.addStringField("productName", 20);
            schema.addDoubleField("price");
            schema.addBooleanField("inStock");

            Layout layout = new Layout(schema);

            // Use TableScan instead of RecordPage directly
            TableScan ts = new TableScan(tx, "products", layout);

            // Insert records spanning multiple blocks
            int slotsPerBlock = BLOCK_SIZE / layout.slotSize();
            int totalRecords = slotsPerBlock * 2 + 5; // Ensure multiple blocks

            logger.info("Inserting {} records using TableScan", totalRecords);
            for (int i = 0; i < totalRecords; i++) {
                ts.insert();
                ts.setInt("productId", i);
                ts.setString("productName", "Product_" + i);
                ts.setDouble("price", 9.99 + i * 0.50);
                ts.setBoolean("inStock", i % 3 != 0);
            }

            // Verify all records
            logger.info("Verifying all records");
            ts.beforeFirst();
            int verifyCount = 0;
            while (ts.next()) {
                int id = ts.getInt("productId");
                assertEquals("Product_" + id, ts.getString("productName"));
                assertEquals(9.99 + id * 0.50, ts.getDouble("price"), 0.001);
                assertEquals(id % 3 != 0, ts.getBoolean("inStock"));
                verifyCount++;
            }
            assertEquals(totalRecords, verifyCount);

            // Delete out-of-stock products
            logger.info("Deleting out-of-stock products");
            ts.beforeFirst();
            int deletedCount = 0;
            while (ts.next()) {
                if (!ts.getBoolean("inStock")) {
                    ts.delete();
                    deletedCount++;
                }
            }

            // Verify remaining count
            ts.beforeFirst();
            int remainingCount = 0;
            while (ts.next()) {
                assertTrue(ts.getBoolean("inStock"));
                remainingCount++;
            }
            assertEquals(totalRecords - deletedCount, remainingCount);
            logger.info("Remaining in-stock products: {}", remainingCount);

            ts.close();
            tx.commit();
            logger.info("Completed tableScanAbstractionWorkflow test");
        }

        @Test
        @DisplayName("Data persistence: insert records, beforeFirst, and verify all are readable")
        void dataPersistenceWithinTransaction() {
            logger.info("Starting dataPersistenceWithinTransaction test");
            TxMgrBase txMgr = createTxMgr("dataPersistenceWithinTransaction");
            TxBase tx = txMgr.newTx();

            Schema schema = new Schema();
            schema.addIntField("id");
            schema.addStringField("data", 30);
            Layout layout = new Layout(schema);

            // Insert records using TableScan
            logger.info("Inserting records");
            TableScan ts = new TableScan(tx, "persistTable", layout);

            for (int i = 0; i < 10; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("data", "PersistentData_" + i);
            }

            // Reset cursor and read all records
            logger.info("Reading records after beforeFirst");
            ts.beforeFirst();
            int readCount = 0;
            while (ts.next()) {
                int id = ts.getInt("id");
                String data = ts.getString("data");
                assertEquals("PersistentData_" + id, data);
                readCount++;
            }
            assertEquals(10, readCount);

            ts.close();
            tx.commit();
            logger.info("Test completed - verified {} records", readCount);
        }

        @Test
        @DisplayName("Multi-table correctness: create two tables, insert into both, verify isolation")
        void multiTableCorrectness() {
            logger.info("Starting multiTableCorrectness test");
            TxMgrBase txMgr = createTxMgr("multiTableCorrectness");
            TxBase tx = txMgr.newTx();

            // Create two different schemas
            Schema customersSchema = new Schema();
            customersSchema.addIntField("customerId");
            customersSchema.addStringField("customerName", 25);
            Layout customersLayout = new Layout(customersSchema);

            Schema ordersSchema = new Schema();
            ordersSchema.addIntField("orderId");
            ordersSchema.addIntField("customerId");
            ordersSchema.addDoubleField("amount");
            Layout ordersLayout = new Layout(ordersSchema);

            // Create table scans
            TableScan customersTs = new TableScan(tx, "customers", customersLayout);
            TableScan ordersTs = new TableScan(tx, "orders", ordersLayout);

            // Insert customers
            logger.info("Inserting customers");
            for (int i = 0; i < 5; i++) {
                customersTs.insert();
                customersTs.setInt("customerId", i + 100);
                customersTs.setString("customerName", "Customer_" + i);
            }

            // Insert orders
            logger.info("Inserting orders");
            for (int i = 0; i < 15; i++) {
                ordersTs.insert();
                ordersTs.setInt("orderId", i + 1000);
                ordersTs.setInt("customerId", (i % 5) + 100);
                ordersTs.setDouble("amount", 50.0 + i * 10);
            }

            // Verify customers table has correct structure and count
            customersTs.beforeFirst();
            int customerCount = 0;
            while (customersTs.next()) {
                assertTrue(customersTs.hasField("customerId"));
                assertTrue(customersTs.hasField("customerName"));
                assertFalse(customersTs.hasField("orderId")); // This field belongs to orders
                assertFalse(customersTs.hasField("amount")); // This field belongs to orders
                customerCount++;
            }
            assertEquals(5, customerCount);

            // Verify orders table has correct structure and count
            ordersTs.beforeFirst();
            int orderCount = 0;
            while (ordersTs.next()) {
                assertTrue(ordersTs.hasField("orderId"));
                assertTrue(ordersTs.hasField("customerId"));
                assertTrue(ordersTs.hasField("amount"));
                assertFalse(ordersTs.hasField("customerName")); // This field belongs to customers
                orderCount++;
            }
            assertEquals(15, orderCount);

            customersTs.close();
            ordersTs.close();
            tx.commit();
            logger.info("Multi-table test completed - {} customers, {} orders", customerCount, orderCount);
        }

        @Test
        @DisplayName("Demo-style comprehensive test following RecordDemoTest pattern")
        void demoStyleComprehensiveTest() {
            logger.info("Starting demoStyleComprehensiveTest");
            TxMgrBase txMgr = createTxMgr("demoStyleComprehensiveTest");
            TxBase tx = txMgr.newTx();

            // Create schema with all field types
            logger.info("Creating comprehensive schema");
            Schema schema = new Schema();
            schema.addIntField("A");
            schema.addStringField("B", 12);
            schema.addBooleanField("C");
            schema.addDoubleField("D");

            // Verify schema
            logger.info("Verifying schema fields");
            assertEquals(4, schema.fields().size());
            assertTrue(schema.hasField("A"));
            assertTrue(schema.hasField("B"));
            assertTrue(schema.hasField("C"));
            assertTrue(schema.hasField("D"));
            assertEquals(Types.INTEGER, schema.type("A"));
            assertEquals(Types.VARCHAR, schema.type("B"));
            assertEquals(Types.BOOLEAN, schema.type("C"));
            assertEquals(Types.DOUBLE, schema.type("D"));

            // Create layout
            logger.info("Creating layout");
            Layout layout = new Layout(schema);
            logger.info("Slot size: {}", layout.slotSize());
            assertTrue(layout.slotSize() > 0);

            // Test RecordPage directly
            logger.info("Testing RecordPage operations");
            BlockIdBase blk = tx.append("testFile");
            tx.pin(blk);
            RecordPage rp = new RecordPage(tx, blk, layout);
            rp.format();

            int slot = rp.insertAfter(BEFORE_FIRST_SLOT);
            rp.setInt(slot, "A", 42);
            rp.setString(slot, "B", "test");
            rp.setBoolean(slot, "C", true);
            rp.setDouble(slot, "D", 3.14);

            assertEquals(42, rp.getInt(slot, "A"));
            assertEquals("test", rp.getString(slot, "B"));
            assertTrue(rp.getBoolean(slot, "C"));
            assertEquals(3.14, rp.getDouble(slot, "D"), 0.001);

            tx.unpin(blk);

            // Test TableScan
            logger.info("Testing TableScan operations");
            TableScan ts = new TableScan(tx, "T", layout);

            int nRecords = 50;
            RID rid30 = null;

            logger.info("Inserting {} records", nRecords);
            for (int i = 0; i < nRecords; i++) {
                ts.insert();
                ts.setInt("A", i);
                ts.setString("B", "record" + i);
                ts.setBoolean("C", i % 2 == 0);
                ts.setDouble("D", i * 1.5);

                assertEquals(i, ts.getInt("A"));
                assertEquals("record" + i, ts.getString("B"));
                assertEquals(i % 2 == 0, ts.getBoolean("C"));
                assertEquals(i * 1.5, ts.getDouble("D"), 0.001);

                if (i == 30) {
                    rid30 = ts.getRid();
                }
            }

            // Delete records where A < 30
            logger.info("Deleting records with A < 30");
            int threshold = 30;
            int deleteCount = 0;
            ts.beforeFirst();
            while (ts.next()) {
                if (ts.getInt("A") < threshold) {
                    ts.delete();
                    deleteCount++;
                }
            }
            assertEquals(threshold, deleteCount);

            // Verify remaining records
            logger.info("Verifying remaining records");
            int remainingCount = 0;
            ts.beforeFirst();
            while (ts.next()) {
                assertTrue(ts.getInt("A") >= threshold);
                remainingCount++;
            }
            assertEquals(nRecords - threshold, remainingCount);

            // Navigate to specific RID
            logger.info("Navigating to RID: {}", rid30);
            ts.moveToRid(rid30);
            assertEquals(30, ts.getInt("A"));
            assertEquals("record30", ts.getString("B"));
            assertTrue(ts.getBoolean("C")); // 30 % 2 == 0
            assertEquals(30 * 1.5, ts.getDouble("D"), 0.001);

            ts.close();
            tx.commit();
            logger.info("Completed demoStyleComprehensiveTest");
        }
    }
}
