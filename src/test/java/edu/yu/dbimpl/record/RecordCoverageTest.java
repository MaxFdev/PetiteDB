package edu.yu.dbimpl.record;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.DisplayName;
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
 * Comprehensive coverage tests for the Record module including Datum, Schema,
 * Layout, RecordPage, and TableScan classes.
 */
public class RecordCoverageTest {

    private static final int BLOCK_SIZE = 400;
    private static final int BUFFER_COUNT = 8;
    private static final int WAIT_MILLIS = 1000;

    private TxMgrBase newTxMgr(String testName) {
        Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);
        Path dir = Path.of("testing", "RecordCoverageTest", testName);
        try {
            if (Files.exists(dir)) {
                deleteDirectory(dir);
            }
        } catch (Exception e) {
            // Ignore
        }
        FileMgrBase fm = new FileMgr(dir.toFile(), BLOCK_SIZE);
        LogMgrBase lm = new LogMgr(fm, "coverage_log");
        BufferMgrBase bm = new BufferMgr(fm, lm, BUFFER_COUNT, WAIT_MILLIS);
        return new TxMgr(fm, lm, bm, WAIT_MILLIS);
    }

    private void deleteDirectory(Path path) throws Exception {
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (Exception e) {
                        }
                    });
        }
    }

    private SchemaBase createTestSchema() {
        Schema schema = new Schema();
        schema.addIntField("id");
        schema.addStringField("name", 20);
        schema.addBooleanField("active");
        schema.addDoubleField("salary");
        return schema;
    }

    // ========== Datum Class Coverage ==========

    @Test
    @DisplayName("Datum: Integer constructor and asInt")
    void datumIntegerConstructor() {
        Datum d = new Datum(42);
        assertEquals(42, d.asInt());
        assertEquals(Types.INTEGER, d.getSQLType());
    }

    @Test
    @DisplayName("Datum: String constructor and asString")
    void datumStringConstructor() {
        Datum d = new Datum("hello");
        assertEquals("hello", d.asString());
        assertEquals(Types.VARCHAR, d.getSQLType());
    }

    @Test
    @DisplayName("Datum: Boolean constructor and asBoolean")
    void datumBooleanConstructor() {
        Datum d = new Datum(true);
        assertTrue(d.asBoolean());
        assertEquals(Types.BOOLEAN, d.getSQLType());
    }

    @Test
    @DisplayName("Datum: Double constructor and asDouble")
    void datumDoubleConstructor() {
        Datum d = new Datum(3.14);
        assertEquals(3.14, d.asDouble(), 0.001);
        assertEquals(Types.DOUBLE, d.getSQLType());
    }

    @Test
    @DisplayName("Datum: byte[] constructor and asBinaryArray")
    void datumByteArrayConstructor() {
        byte[] arr = { 1, 2, 3 };
        Datum d = new Datum(arr);
        assertArrayEquals(arr, d.asBinaryArray());
        assertEquals(Types.VARBINARY, d.getSQLType());
    }

    @Test
    @DisplayName("Datum: asInt throws ClassCastException on wrong type")
    void datumAsIntWrongType() {
        Datum d = new Datum("string");
        assertThrows(ClassCastException.class, () -> d.asInt());
    }

    @Test
    @DisplayName("Datum: asString throws ClassCastException on wrong type")
    void datumAsStringWrongType() {
        Datum d = new Datum(42);
        assertThrows(ClassCastException.class, () -> d.asString());
    }

    @Test
    @DisplayName("Datum: asBoolean throws ClassCastException on wrong type")
    void datumAsBooleanWrongType() {
        Datum d = new Datum(42);
        assertThrows(ClassCastException.class, () -> d.asBoolean());
    }

    @Test
    @DisplayName("Datum: asBinaryArray throws ClassCastException on wrong type")
    void datumAsBinaryArrayWrongType() {
        Datum d = new Datum(42);
        assertThrows(ClassCastException.class, () -> d.asBinaryArray());
    }

    @Test
    @DisplayName("Datum: compareTo INTEGER")
    void datumCompareToInteger() {
        Datum d1 = new Datum(10);
        Datum d2 = new Datum(20);
        assertTrue(d1.compareTo(d2) < 0);
        assertTrue(d2.compareTo(d1) > 0);
        assertEquals(0, d1.compareTo(new Datum(10)));
    }

    @Test
    @DisplayName("Datum: compareTo VARCHAR")
    void datumCompareToVarchar() {
        Datum d1 = new Datum("apple");
        Datum d2 = new Datum("banana");
        assertTrue(d1.compareTo(d2) < 0);
        assertTrue(d2.compareTo(d1) > 0);
        assertEquals(0, d1.compareTo(new Datum("apple")));
    }

    @Test
    @DisplayName("Datum: compareTo BOOLEAN")
    void datumCompareToBoolean() {
        Datum d1 = new Datum(false);
        Datum d2 = new Datum(true);
        assertTrue(d1.compareTo(d2) < 0);
        assertTrue(d2.compareTo(d1) > 0);
        assertEquals(0, d1.compareTo(new Datum(false)));
    }

    @Test
    @DisplayName("Datum: compareTo DOUBLE")
    void datumCompareToDouble() {
        Datum d1 = new Datum(1.5);
        Datum d2 = new Datum(2.5);
        assertTrue(d1.compareTo(d2) < 0);
        assertTrue(d2.compareTo(d1) > 0);
        assertEquals(0, d1.compareTo(new Datum(1.5)));
    }

    @Test
    @DisplayName("Datum: compareTo VARBINARY")
    void datumCompareToVarbinary() {
        Datum d1 = new Datum(new byte[] { 1, 2 });
        Datum d2 = new Datum(new byte[] { 1, 3 });
        assertTrue(d1.compareTo(d2) < 0);
        assertTrue(d2.compareTo(d1) > 0);
        assertEquals(0, d1.compareTo(new Datum(new byte[] { 1, 2 })));
    }

    @Test
    @DisplayName("Datum: compareTo throws ClassCastException on type mismatch")
    void datumCompareToTypeMismatch() {
        Datum d1 = new Datum(42);
        Datum d2 = new Datum("string");
        assertThrows(ClassCastException.class, () -> d1.compareTo(d2));
    }

    @Test
    @DisplayName("Datum: equals with null returns false")
    void datumEqualsNull() {
        Datum d = new Datum(42);
        assertFalse(d.equals(null));
    }

    @Test
    @DisplayName("Datum: equals with non-Datum returns false")
    void datumEqualsNonDatum() {
        Datum d = new Datum(42);
        assertFalse(d.equals("not a datum"));
    }

    @Test
    @DisplayName("Datum: equals with different type returns false")
    void datumEqualsDifferentType() {
        Datum d1 = new Datum(42);
        Datum d2 = new Datum("42");
        assertFalse(d1.equals(d2));
    }

    @Test
    @DisplayName("Datum: equals with same value returns true")
    void datumEqualsSameValue() {
        Datum d1 = new Datum(42);
        Datum d2 = new Datum(42);
        assertTrue(d1.equals(d2));
    }

    @Test
    @DisplayName("Datum: equals with VARBINARY uses Arrays.equals")
    void datumEqualsVarbinary() {
        Datum d1 = new Datum(new byte[] { 1, 2, 3 });
        Datum d2 = new Datum(new byte[] { 1, 2, 3 });
        Datum d3 = new Datum(new byte[] { 1, 2, 4 });
        assertTrue(d1.equals(d2));
        assertFalse(d1.equals(d3));
    }

    @Test
    @DisplayName("Datum: hashCode for VARBINARY uses Arrays.hashCode")
    void datumHashCodeVarbinary() {
        byte[] arr = { 1, 2, 3 };
        Datum d = new Datum(arr);
        assertEquals(java.util.Arrays.hashCode(arr), d.hashCode());
    }

    @Test
    @DisplayName("Datum: hashCode for non-VARBINARY uses val.hashCode")
    void datumHashCodeNonVarbinary() {
        Datum d = new Datum(42);
        assertEquals(Integer.valueOf(42).hashCode(), d.hashCode());
    }

    @Test
    @DisplayName("Datum: toString returns formatted string")
    void datumToString() {
        Datum d = new Datum(42);
        String str = d.toString();
        assertTrue(str.contains("Datum"));
        assertTrue(str.contains("42"));
    }

    // ===== New comprehensive Datum conversion tests per DatumBase design notes =====

    @Test
    @DisplayName("Datum conversions: from INTEGER per design notes")
    void datumConversionsFromInteger() {
        Datum d = new Datum(42);
        // Allowed: asInt, asDouble
        assertEquals(42, d.asInt());
        assertEquals(42.0, d.asDouble(), 0.000001);
        // Not allowed: asBoolean, asString, asBinaryArray
        assertThrows(ClassCastException.class, d::asBoolean);
        assertThrows(ClassCastException.class, d::asString);
        assertThrows(ClassCastException.class, d::asBinaryArray);
    }

    @Test
    @DisplayName("Datum conversions: from DOUBLE per design notes")
    void datumConversionsFromDouble() {
        Datum d = new Datum(3.7);
        // Allowed: asDouble, asInt (Double.intValue semantics: truncation)
        assertEquals(3.7, d.asDouble(), 0.000001);
        assertEquals(3, d.asInt());
        // Not allowed: asBoolean, asString, asBinaryArray
        assertThrows(ClassCastException.class, d::asBoolean);
        assertThrows(ClassCastException.class, d::asString);
        assertThrows(ClassCastException.class, d::asBinaryArray);
    }

    @Test
    @DisplayName("Datum conversions: from BOOLEAN per design notes")
    void datumConversionsFromBoolean() {
        Datum d = new Datum(true);
        // Allowed: asBoolean
        assertTrue(d.asBoolean());
        // Not allowed: asInt, asDouble, asString, asBinaryArray
        assertThrows(ClassCastException.class, d::asInt);
        assertThrows(ClassCastException.class, d::asDouble);
        assertThrows(ClassCastException.class, d::asString);
        assertThrows(ClassCastException.class, d::asBinaryArray);
    }

    @Test
    @DisplayName("Datum conversions: from VARCHAR per design notes")
    void datumConversionsFromVarchar() {
        Datum d = new Datum("abc");
        // Allowed: asString, asBinaryArray (String.getBytes semantics)
        assertEquals("abc", d.asString());
        assertArrayEquals("abc".getBytes(), d.asBinaryArray());
        // Not allowed: asInt, asDouble, asBoolean
        assertThrows(ClassCastException.class, d::asInt);
        assertThrows(ClassCastException.class, d::asDouble);
        assertThrows(ClassCastException.class, d::asBoolean);
    }

    @Test
    @DisplayName("Datum conversions: from VARBINARY per design notes")
    void datumConversionsFromVarbinary() {
        byte[] bytes = new byte[] {65, 66, 67}; // "ABC"
        Datum d = new Datum(bytes);
        // Allowed: asBinaryArray, asString (new String(byte[]) semantics)
        assertArrayEquals(bytes, d.asBinaryArray());
        assertEquals(new String(bytes), d.asString());
        // Not allowed: asInt, asDouble, asBoolean
        assertThrows(ClassCastException.class, d::asInt);
        assertThrows(ClassCastException.class, d::asDouble);
        assertThrows(ClassCastException.class, d::asBoolean);
    }

    // ========== Schema Class Coverage ==========

    @Test
    @DisplayName("Schema: no-arg constructor creates empty schema")
    void schemaConstructor() {
        Schema s = new Schema();
        assertTrue(s.fields().isEmpty());
    }

    @Test
    @DisplayName("Schema: addField with null fieldname throws IAE")
    void schemaAddFieldNullName() {
        Schema s = new Schema();
        assertThrows(IllegalArgumentException.class, () -> s.addField(null, Types.INTEGER, 4));
    }

    @Test
    @DisplayName("Schema: addField with empty fieldname throws IAE")
    void schemaAddFieldEmptyName() {
        Schema s = new Schema();
        assertThrows(IllegalArgumentException.class, () -> s.addField("", Types.INTEGER, 4));
    }

    @Test
    @DisplayName("Schema: addField VARCHAR with length <= 0 throws IAE")
    void schemaAddFieldVarcharZeroLength() {
        Schema s = new Schema();
        assertThrows(IllegalArgumentException.class, () -> s.addField("name", Types.VARCHAR, 0));
        assertThrows(IllegalArgumentException.class, () -> s.addField("name", Types.VARCHAR, -1));
    }

    @Test
    @DisplayName("Schema: addField creates new field")
    void schemaAddFieldNew() {
        Schema s = new Schema();
        s.addField("age", Types.INTEGER, 4);
        assertTrue(s.hasField("age"));
        assertEquals(Types.INTEGER, s.type("age"));
    }

    @Test
    @DisplayName("Schema: addField updates existing field")
    void schemaAddFieldUpdate() {
        Schema s = new Schema();
        s.addField("field", Types.INTEGER, 4);
        s.addField("field", Types.DOUBLE, 8);
        assertEquals(Types.DOUBLE, s.type("field"));
        assertEquals(8, s.length("field"));
    }

    @Test
    @DisplayName("Schema: addIntField works correctly")
    void schemaAddIntField() {
        Schema s = new Schema();
        s.addIntField("id");
        assertTrue(s.hasField("id"));
        assertEquals(Types.INTEGER, s.type("id"));
    }

    @Test
    @DisplayName("Schema: addBooleanField works correctly")
    void schemaAddBooleanField() {
        Schema s = new Schema();
        s.addBooleanField("active");
        assertTrue(s.hasField("active"));
        assertEquals(Types.BOOLEAN, s.type("active"));
    }

    @Test
    @DisplayName("Schema: addDoubleField works correctly")
    void schemaAddDoubleField() {
        Schema s = new Schema();
        s.addDoubleField("salary");
        assertTrue(s.hasField("salary"));
        assertEquals(Types.DOUBLE, s.type("salary"));
    }

    @Test
    @DisplayName("Schema: addStringField works correctly")
    void schemaAddStringField() {
        Schema s = new Schema();
        s.addStringField("name", 50);
        assertTrue(s.hasField("name"));
        assertEquals(Types.VARCHAR, s.type("name"));
        assertEquals(50, s.length("name"));
    }

    @Test
    @DisplayName("Schema: add with non-Schema throws IAE")
    void schemaAddNonSchema() {
        Schema s = new Schema();
        assertThrows(IllegalArgumentException.class, () -> s.add("field", null));
    }

    @Test
    @DisplayName("Schema: add with field not in source throws IAE")
    void schemaAddFieldNotInSource() {
        Schema s1 = new Schema();
        Schema s2 = new Schema();
        s2.addIntField("other");
        assertThrows(IllegalArgumentException.class, () -> s1.add("missing", s2));
    }

    @Test
    @DisplayName("Schema: add copies field from source schema")
    void schemaAddFromSource() {
        Schema source = new Schema();
        source.addIntField("id");
        Schema target = new Schema();
        target.add("id", source);
        assertTrue(target.hasField("id"));
        assertEquals(Types.INTEGER, target.type("id"));
    }

    @Test
    @DisplayName("Schema: addAll with non-Schema throws IAE")
    void schemaAddAllNonSchema() {
        Schema s = new Schema();
        assertThrows(IllegalArgumentException.class, () -> s.addAll(null));
    }

    @Test
    @DisplayName("Schema: addAll copies all fields from source")
    void schemaAddAllFromSource() {
        Schema source = new Schema();
        source.addIntField("id");
        source.addStringField("name", 20);
        Schema target = new Schema();
        target.addAll(source);
        assertTrue(target.hasField("id"));
        assertTrue(target.hasField("name"));
    }

    @Test
    @DisplayName("Schema: fields returns field names")
    void schemaFields() {
        Schema s = new Schema();
        s.addIntField("a");
        s.addIntField("b");
        assertEquals(2, s.fields().size());
        assertTrue(s.fields().contains("a"));
        assertTrue(s.fields().contains("b"));
    }

    @Test
    @DisplayName("Schema: hasField returns correct value")
    void schemaHasField() {
        Schema s = new Schema();
        s.addIntField("exists");
        assertTrue(s.hasField("exists"));
        assertFalse(s.hasField("missing"));
    }

    @Test
    @DisplayName("Schema: type with null fieldname throws IAE")
    void schemaTypeNullName() {
        Schema s = new Schema();
        assertThrows(IllegalArgumentException.class, () -> s.type(null));
    }

    @Test
    @DisplayName("Schema: type with non-existent field throws IAE")
    void schemaTypeNonExistent() {
        Schema s = new Schema();
        assertThrows(IllegalArgumentException.class, () -> s.type("missing"));
    }

    @Test
    @DisplayName("Schema: length with null fieldname throws IAE")
    void schemaLengthNullName() {
        Schema s = new Schema();
        assertThrows(IllegalArgumentException.class, () -> s.length(null));
    }

    @Test
    @DisplayName("Schema: length with non-existent field throws IAE")
    void schemaLengthNonExistent() {
        Schema s = new Schema();
        assertThrows(IllegalArgumentException.class, () -> s.length("missing"));
    }

    @Test
    @DisplayName("Schema: toString returns formatted string")
    void schemaToString() {
        Schema s = new Schema();
        s.addIntField("id");
        String str = s.toString();
        assertTrue(str.contains("Schema"));
        assertTrue(str.contains("id"));
    }

    @Test
    @DisplayName("Schema: equals with non-Schema returns false")
    void schemaEqualsNonSchema() {
        Schema s = new Schema();
        assertFalse(s.equals("not a schema"));
    }

    @Test
    @DisplayName("Schema: equals with same fields returns true")
    void schemaEqualsSameFields() {
        Schema s1 = new Schema();
        s1.addIntField("id");
        Schema s2 = new Schema();
        s2.addIntField("id");
        assertTrue(s1.equals(s2));
    }

    @Test
    @DisplayName("Schema: equals with same object returns true")
    void schemaEqualsSameObject() {
        Schema s = new Schema();
        s.addIntField("id");
        assertTrue(s.equals(s));
    }

    @Test
    @DisplayName("Schema: equals with null returns false")
    void schemaEqualsNull() {
        Schema s = new Schema();
        s.addIntField("id");
        assertFalse(s.equals(null));
    }

    @Test
    @DisplayName("Schema: equals with different field count returns false")
    void schemaEqualsDifferentFieldCount() {
        Schema s1 = new Schema();
        s1.addIntField("id");
        Schema s2 = new Schema();
        s2.addIntField("id");
        s2.addStringField("name", 20);
        assertFalse(s1.equals(s2));
    }

    @Test
    @DisplayName("Schema: hashCode returns fieldnameList hashCode")
    void schemaHashCode() {
        Schema s1 = new Schema();
        s1.addIntField("id");
        Schema s2 = new Schema();
        s2.addIntField("id");
        assertEquals(s1.hashCode(), s2.hashCode());
    }

    @Test
    @DisplayName("Schema: hashCode consistent with equals")
    void schemaHashCodeConsistent() {
        Schema s1 = new Schema();
        s1.addIntField("id");
        s1.addStringField("name", 20);
        Schema s2 = new Schema();
        s2.addIntField("id");
        s2.addStringField("name", 20);
        assertEquals(s1.hashCode(), s2.hashCode());
        assertTrue(s1.equals(s2));
    }

    @Test
    @DisplayName("Schema: logically unequal schemas are not equal")
    void logicallyUnequalSchemaAreNotEqual() {
        // Schema1: A (INTEGER), B (VARCHAR, length 9)
        Schema schema1 = new Schema();
        schema1.addIntField("A");
        schema1.addStringField("B", 9);

        // Schema2: A (INTEGER), C (VARCHAR, length 9) - different field name
        Schema schema2 = new Schema();
        schema2.addIntField("A");
        schema2.addStringField("C", 9);

        // Schema3: A (INTEGER), B (VARCHAR, length 8) - different length
        Schema schema3 = new Schema();
        schema3.addIntField("A");
        schema3.addStringField("B", 8);

        // All three schemas should be unequal
        assertNotEquals(schema1, schema2); // Different field names
        assertNotEquals(schema1, schema3); // Different field lengths
        assertNotEquals(schema2, schema3); // Different field names

        // Verify toString output differs for all three
        String str1 = schema1.toString();
        String str2 = schema2.toString();
        String str3 = schema3.toString();
        assertNotEquals(str1, str2);
        assertNotEquals(str1, str3);
        assertNotEquals(str2, str3);

        // Verify toString contains expected field information
        assertTrue(str1.contains("A") && str1.contains("B"));
        assertTrue(str2.contains("A") && str2.contains("C"));
        assertTrue(str3.contains("A") && str3.contains("B"));
    }

    // ========== Layout Class Coverage ==========

    @Test
    @DisplayName("Layout: constructor calculates offsets for all types")
    void layoutConstructorAllTypes() {
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        // First field (id - INTEGER) should be at offset 1 (after in-use byte)
        assertEquals(1, layout.offset("id"));
        // Second field (name - VARCHAR(20)) at offset 1 + 4 = 5
        assertEquals(5, layout.offset("name"));
        // Third field (active - BOOLEAN) at offset 5 + 20 + 4 = 29
        assertEquals(29, layout.offset("active"));
        // Fourth field (salary - DOUBLE) at offset 29 + 1 = 30
        assertEquals(30, layout.offset("salary"));
    }

    @Test
    @DisplayName("Layout: slotSize calculated correctly")
    void layoutSlotSize() {
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);
        // 1 (in-use) + 4 (int) + 24 (varchar 20 + 4 for length) + 1 (boolean) + 8
        // (double) = 38
        assertEquals(38, layout.slotSize());
    }

    @Test
    @DisplayName("Layout: 3-arg constructor uses provided values")
    void layoutThreeArgConstructor() {
        SchemaBase schema = createTestSchema();
        Map<String, Integer> offsets = new HashMap<>();
        offsets.put("id", 10);
        offsets.put("name", 20);
        offsets.put("active", 30);
        offsets.put("salary", 40);
        Layout layout = new Layout(schema, offsets, 100);

        assertEquals(10, layout.offset("id"));
        assertEquals(100, layout.slotSize());
    }

    @Test
    @DisplayName("Layout: schema returns encapsulated schema")
    void layoutSchema() {
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);
        assertSame(schema, layout.schema());
    }

    @Test
    @DisplayName("Layout: offset with unknown field throws IAE")
    void layoutOffsetUnknownField() {
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);
        assertThrows(IllegalArgumentException.class, () -> layout.offset("unknown"));
    }

    @Test
    @DisplayName("Layout: toString returns formatted string")
    void layoutToString() {
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);
        String str = layout.toString();
        assertTrue(str.contains("Layout"));
    }

    @Test
    @DisplayName("Layout: equals with same schema and offsets returns true")
    void layoutEqualsSameSchemaAndOffsets() {
        SchemaBase schema1 = createTestSchema();
        SchemaBase schema2 = createTestSchema();
        Layout layout1 = new Layout(schema1);
        Layout layout2 = new Layout(schema2);
        assertTrue(layout1.equals(layout2));
    }

    @Test
    @DisplayName("Layout: equals with different schema returns false")
    void layoutEqualsDifferentSchema() {
        SchemaBase schema1 = createTestSchema();
        Schema schema2 = new Schema();
        schema2.addIntField("different");
        Layout layout1 = new Layout(schema1);
        Layout layout2 = new Layout(schema2);
        assertFalse(layout1.equals(layout2));
    }

    @Test
    @DisplayName("Layout: equals with same object returns true")
    void layoutEqualsSameObject() {
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);
        assertTrue(layout.equals(layout));
    }

    @Test
    @DisplayName("Layout: equals with null returns false")
    void layoutEqualsNull() {
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);
        assertFalse(layout.equals(null));
    }

    @Test
    @DisplayName("Layout: equals with non-Layout returns false")
    void layoutEqualsNonLayout() {
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);
        assertFalse(layout.equals("not a layout"));
    }

    @Test
    @DisplayName("Layout: hashCode consistent with equals")
    void layoutHashCode() {
        SchemaBase schema1 = createTestSchema();
        SchemaBase schema2 = createTestSchema();
        Layout layout1 = new Layout(schema1);
        Layout layout2 = new Layout(schema2);
        assertEquals(layout1.hashCode(), layout2.hashCode());
    }

    // ========== RecordPage Class Coverage ==========

    @Test
    @DisplayName("RecordPage: constructor throws IAE if block too small")
    void recordPageConstructorBlockTooSmall() {
        TxMgrBase txMgr = newTxMgr("recordPageBlockTooSmall");
        TxBase tx = txMgr.newTx();

        // Create a schema with a slot size larger than block size
        Schema schema = new Schema();
        schema.addStringField("bigfield", 500); // This will exceed 400 byte block
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        assertThrows(IllegalArgumentException.class, () -> new RecordPage(tx, blk, layout));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: getInt and setInt work correctly")
    void recordPageGetSetInt() {
        TxMgrBase txMgr = newTxMgr("recordPageGetSetInt");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int slot = rp.insertAfter(RecordPageBase.BEFORE_FIRST_SLOT);
        rp.setInt(slot, "id", 42);
        assertEquals(42, rp.getInt(slot, "id"));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: getString and setString work correctly")
    void recordPageGetSetString() {
        TxMgrBase txMgr = newTxMgr("recordPageGetSetString");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int slot = rp.insertAfter(RecordPageBase.BEFORE_FIRST_SLOT);
        rp.setString(slot, "name", "Alice");
        assertEquals("Alice", rp.getString(slot, "name"));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: getBoolean and setBoolean work correctly")
    void recordPageGetSetBoolean() {
        TxMgrBase txMgr = newTxMgr("recordPageGetSetBoolean");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int slot = rp.insertAfter(RecordPageBase.BEFORE_FIRST_SLOT);
        rp.setBoolean(slot, "active", true);
        assertTrue(rp.getBoolean(slot, "active"));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: getDouble and setDouble work correctly")
    void recordPageGetSetDouble() {
        TxMgrBase txMgr = newTxMgr("recordPageGetSetDouble");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int slot = rp.insertAfter(RecordPageBase.BEFORE_FIRST_SLOT);
        rp.setDouble(slot, "salary", 50000.0);
        assertEquals(50000.0, rp.getDouble(slot, "salary"), 0.001);
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: getInt with negative slot throws IAE")
    void recordPageGetIntNegativeSlot() {
        TxMgrBase txMgr = newTxMgr("recordPageGetIntNegativeSlot");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        assertThrows(IllegalArgumentException.class, () -> rp.getInt(-1, "id"));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: getInt with slot >= totalSlots throws IAE")
    void recordPageGetIntSlotTooLarge() {
        TxMgrBase txMgr = newTxMgr("recordPageGetIntSlotTooLarge");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int totalSlots = BLOCK_SIZE / layout.slotSize();
        assertThrows(IllegalArgumentException.class, () -> rp.getInt(totalSlots, "id"));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: getInt on non-in-use slot throws ISE")
    void recordPageGetIntNotInUse() {
        TxMgrBase txMgr = newTxMgr("recordPageGetIntNotInUse");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        // Slot 0 is not in use after format
        assertThrows(IllegalStateException.class, () -> rp.getInt(0, "id"));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: getInt with wrong field type throws IAE")
    void recordPageGetIntWrongType() {
        TxMgrBase txMgr = newTxMgr("recordPageGetIntWrongType");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int slot = rp.insertAfter(RecordPageBase.BEFORE_FIRST_SLOT);
        assertThrows(IllegalArgumentException.class, () -> rp.getInt(slot, "name")); // name is VARCHAR
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: getString with wrong field type throws IAE")
    void recordPageGetStringWrongType() {
        TxMgrBase txMgr = newTxMgr("recordPageGetStringWrongType");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int slot = rp.insertAfter(RecordPageBase.BEFORE_FIRST_SLOT);
        assertThrows(IllegalArgumentException.class, () -> rp.getString(slot, "id")); // id is INTEGER
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: getBoolean with wrong field type throws IAE")
    void recordPageGetBooleanWrongType() {
        TxMgrBase txMgr = newTxMgr("recordPageGetBooleanWrongType");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int slot = rp.insertAfter(RecordPageBase.BEFORE_FIRST_SLOT);
        assertThrows(IllegalArgumentException.class, () -> rp.getBoolean(slot, "id")); // id is INTEGER
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: getDouble with wrong field type throws IAE")
    void recordPageGetDoubleWrongType() {
        TxMgrBase txMgr = newTxMgr("recordPageGetDoubleWrongType");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int slot = rp.insertAfter(RecordPageBase.BEFORE_FIRST_SLOT);
        assertThrows(IllegalArgumentException.class, () -> rp.getDouble(slot, "id")); // id is INTEGER
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: setString exceeding length throws IAE")
    void recordPageSetStringTooLong() {
        TxMgrBase txMgr = newTxMgr("recordPageSetStringTooLong");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int slot = rp.insertAfter(RecordPageBase.BEFORE_FIRST_SLOT);
        String longString = "This string is way too long for the field";
        assertThrows(IllegalArgumentException.class, () -> rp.setString(slot, "name", longString));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: delete marks slot as not in use")
    void recordPageDelete() {
        TxMgrBase txMgr = newTxMgr("recordPageDelete");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int slot = rp.insertAfter(RecordPageBase.BEFORE_FIRST_SLOT);
        rp.setInt(slot, "id", 42);
        rp.delete(slot);

        // After delete, accessing the slot should throw ISE
        assertThrows(IllegalStateException.class, () -> rp.getInt(slot, "id"));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: format initializes all slots")
    void recordPageFormat() {
        TxMgrBase txMgr = newTxMgr("recordPageFormat");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        // All slots should be empty after format
        assertEquals(-1, rp.nextAfter(RecordPageBase.BEFORE_FIRST_SLOT));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: nextAfter with slot < -1 throws IAE")
    void recordPageNextAfterInvalidSlot() {
        TxMgrBase txMgr = newTxMgr("recordPageNextAfterInvalidSlot");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        assertThrows(IllegalArgumentException.class, () -> rp.nextAfter(-2));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: nextAfter returns -1 when no more slots")
    void recordPageNextAfterNoMore() {
        TxMgrBase txMgr = newTxMgr("recordPageNextAfterNoMore");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        // No records inserted, so nextAfter should return -1
        assertEquals(-1, rp.nextAfter(RecordPageBase.BEFORE_FIRST_SLOT));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: nextAfter finds in-use slot")
    void recordPageNextAfterFindsSlot() {
        TxMgrBase txMgr = newTxMgr("recordPageNextAfterFindsSlot");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        int slot = rp.insertAfter(RecordPageBase.BEFORE_FIRST_SLOT);
        assertEquals(slot, rp.nextAfter(RecordPageBase.BEFORE_FIRST_SLOT));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: insertAfter with slot < -1 throws IAE")
    void recordPageInsertAfterInvalidSlot() {
        TxMgrBase txMgr = newTxMgr("recordPageInsertAfterInvalidSlot");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        assertThrows(IllegalArgumentException.class, () -> rp.insertAfter(-2));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: insertAfter returns -1 when page is full")
    void recordPageInsertAfterPageFull() {
        TxMgrBase txMgr = newTxMgr("recordPageInsertAfterPageFull");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        // Fill all slots
        int totalSlots = BLOCK_SIZE / layout.slotSize();
        int slot = RecordPageBase.BEFORE_FIRST_SLOT;
        for (int i = 0; i < totalSlots; i++) {
            slot = rp.insertAfter(slot);
        }

        // Now page is full, insertAfter should return -1
        assertEquals(-1, rp.insertAfter(RecordPageBase.BEFORE_FIRST_SLOT));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: block returns encapsulated block")
    void recordPageBlock() {
        TxMgrBase txMgr = newTxMgr("recordPageBlock");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);

        assertEquals(blk, rp.block());
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: toString returns formatted string")
    void recordPageToString() {
        TxMgrBase txMgr = newTxMgr("recordPageToString");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);
        rp.format();

        String str = rp.toString();
        assertTrue(str.contains("RecordPage"));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: equals with same block and layout returns true")
    void recordPageEqualsSameBlockAndLayout() {
        TxMgrBase txMgr = newTxMgr("recordPageEqualsSameBlockAndLayout");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp1 = new RecordPage(tx, blk, layout);
        RecordPage rp2 = new RecordPage(tx, blk, layout);

        assertTrue(rp1.equals(rp2));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: equals with different block returns false")
    void recordPageEqualsDifferentBlock() {
        TxMgrBase txMgr = newTxMgr("recordPageEqualsDifferentBlock");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk1 = tx.append("test1");
        BlockIdBase blk2 = tx.append("test2");
        tx.pin(blk1);
        tx.pin(blk2);
        RecordPage rp1 = new RecordPage(tx, blk1, layout);
        RecordPage rp2 = new RecordPage(tx, blk2, layout);

        assertFalse(rp1.equals(rp2));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: equals with same object returns true")
    void recordPageEqualsSameObject() {
        TxMgrBase txMgr = newTxMgr("recordPageEqualsSameObject");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);

        assertTrue(rp.equals(rp));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: equals with null returns false")
    void recordPageEqualsNull() {
        TxMgrBase txMgr = newTxMgr("recordPageEqualsNull");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);

        assertFalse(rp.equals(null));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: equals with non-RecordPage returns false")
    void recordPageEqualsNonRecordPage() {
        TxMgrBase txMgr = newTxMgr("recordPageEqualsNonRecordPage");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp = new RecordPage(tx, blk, layout);

        assertFalse(rp.equals("not a record page"));
        tx.rollback();
    }

    @Test
    @DisplayName("RecordPage: hashCode consistent with equals")
    void recordPageHashCode() {
        TxMgrBase txMgr = newTxMgr("recordPageHashCode");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        BlockIdBase blk = tx.append("test");
        tx.pin(blk);
        RecordPage rp1 = new RecordPage(tx, blk, layout);
        RecordPage rp2 = new RecordPage(tx, blk, layout);

        assertEquals(rp1.hashCode(), rp2.hashCode());
        tx.rollback();
    }

    // ========== TableScan Class Coverage ==========

    @Test
    @DisplayName("TableScan: constructor on empty file creates and formats block")
    void tableScanConstructorEmptyFile() {
        TxMgrBase txMgr = newTxMgr("tableScanConstructorEmptyFile");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "newtable", layout);
        // Should be able to insert without error
        ts.insert();
        ts.setInt("id", 1);
        ts.close();
        tx.commit();
    }

    @Test
    @DisplayName("TableScan: constructor on non-empty file positions on first block")
    void tableScanConstructorNonEmptyFile() {
        TxMgrBase txMgr = newTxMgr("tableScanConstructorNonEmptyFile");
        TxBase tx1 = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        // Create a table and insert a record
        TableScan ts1 = new TableScan(tx1, "existingtable", layout);
        ts1.insert();
        ts1.setInt("id", 42);
        ts1.close();
        tx1.commit();

        // Open the table again - should position on first block
        TxBase tx2 = txMgr.newTx();
        TableScan ts2 = new TableScan(tx2, "existingtable", layout);
        assertTrue(ts2.next());
        assertEquals(42, ts2.getInt("id"));
        ts2.close();
        tx2.commit();
    }

    @Test
    @DisplayName("TableScan: setVal with INTEGER type")
    void tableScanSetValInteger() {
        TxMgrBase txMgr = newTxMgr("tableScanSetValInteger");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setVal("id", new Datum(42));
        assertEquals(42, ts.getInt("id"));
        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: setVal with VARCHAR type")
    void tableScanSetValVarchar() {
        TxMgrBase txMgr = newTxMgr("tableScanSetValVarchar");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setVal("name", new Datum("Alice"));
        assertEquals("Alice", ts.getString("name"));
        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: setVal with BOOLEAN type")
    void tableScanSetValBoolean() {
        TxMgrBase txMgr = newTxMgr("tableScanSetValBoolean");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setVal("active", new Datum(true));
        assertTrue(ts.getBoolean("active"));
        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: setVal with DOUBLE type")
    void tableScanSetValDouble() {
        TxMgrBase txMgr = newTxMgr("tableScanSetValDouble");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setVal("salary", new Datum(50000.0));
        assertEquals(50000.0, ts.getDouble("salary"), 0.001);
        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: setVal with VARBINARY throws IAE")
    void tableScanSetValVarbinary() {
        TxMgrBase txMgr = newTxMgr("tableScanSetValVarbinary");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        assertThrows(IllegalArgumentException.class, () -> ts.setVal("id", new Datum(new byte[] { 1, 2, 3 })));
        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: setInt works correctly")
    void tableScanSetInt() {
        TxMgrBase txMgr = newTxMgr("tableScanSetInt");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setInt("id", 100);
        assertEquals(100, ts.getInt("id"));
        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: setDouble works correctly")
    void tableScanSetDouble() {
        TxMgrBase txMgr = newTxMgr("tableScanSetDouble");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setDouble("salary", 75000.5);
        assertEquals(75000.5, ts.getDouble("salary"), 0.001);
        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: setBoolean works correctly")
    void tableScanSetBoolean() {
        TxMgrBase txMgr = newTxMgr("tableScanSetBoolean");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setBoolean("active", false);
        assertFalse(ts.getBoolean("active"));
        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: setString works correctly")
    void tableScanSetString() {
        TxMgrBase txMgr = newTxMgr("tableScanSetString");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setString("name", "Bob");
        assertEquals("Bob", ts.getString("name"));
        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: insert when current page full moves to next page")
    void tableScanInsertNextPage() {
        TxMgrBase txMgr = newTxMgr("tableScanInsertNextPage");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);

        // Fill the first page
        int slotsPerPage = BLOCK_SIZE / layout.slotSize();
        for (int i = 0; i < slotsPerPage; i++) {
            ts.insert();
            ts.setInt("id", i);
        }

        // Insert one more - should move to next page
        ts.insert();
        ts.setInt("id", slotsPerPage);

        // Verify we can read it back
        ts.beforeFirst();
        int count = 0;
        while (ts.next()) {
            count++;
        }
        assertEquals(slotsPerPage + 1, count);

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: insert appends new block when all pages full")
    void tableScanInsertAppendBlock() {
        TxMgrBase txMgr = newTxMgr("tableScanInsertAppendBlock");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);

        int slotsPerPage = BLOCK_SIZE / layout.slotSize();
        // Insert enough records to fill first page and some of second
        for (int i = 0; i < slotsPerPage + 5; i++) {
            ts.insert();
            ts.setInt("id", i);
        }

        // Verify all records exist
        ts.beforeFirst();
        int count = 0;
        while (ts.next()) {
            count++;
        }
        assertEquals(slotsPerPage + 5, count);

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: insert traverses to later block with available slot")
    void tableScanInsertTraversesToLaterBlock() {
        TxMgrBase txMgr = newTxMgr("tableScanInsertTraversesToLaterBlock");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);

        int slotsPerPage = BLOCK_SIZE / layout.slotSize();

        // Fill blocks 0 and 1 completely
        for (int i = 0; i < slotsPerPage * 2; i++) {
            ts.insert();
            ts.setInt("id", i);
        }

        // Insert one more to create block 2 with one record
        ts.insert();
        ts.setInt("id", 999);

        // Now position scan back to beginning of block 0
        ts.beforeFirst();

        // Delete a record from block 1 (traverse to middle of block 1)
        // First skip all of block 0
        for (int i = 0; i < slotsPerPage; i++) {
            ts.next();
        }
        // Now we're at first record of block 1, move to middle
        for (int i = 0; i < slotsPerPage / 2; i++) {
            ts.next();
        }
        // Delete this record in the middle of block 1
        ts.getInt("id");
        ts.delete();

        // Reset to beginning of scan
        ts.beforeFirst();

        // Insert a new record - should traverse from block 0 (full) to block 1 (has
        // hole)
        ts.insert();
        ts.setInt("id", 12345);

        // Verify the new record was inserted by checking total count is still the same
        // (we deleted one and inserted one)
        ts.beforeFirst();
        int count = 0;
        boolean foundNewRecord = false;
        while (ts.next()) {
            if (ts.getInt("id") == 12345) {
                foundNewRecord = true;
            }
            count++;
        }

        assertEquals(slotsPerPage * 2 + 1, count);
        assertTrue(foundNewRecord, "New record with id 12345 should be found");

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: delete removes current record")
    void tableScanDelete() {
        TxMgrBase txMgr = newTxMgr("tableScanDelete");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setInt("id", 1);
        ts.insert();
        ts.setInt("id", 2);

        // Delete the second record
        ts.delete();

        // Count remaining records
        ts.beforeFirst();
        int count = 0;
        while (ts.next()) {
            count++;
        }
        assertEquals(1, count);

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: getRid returns current position")
    void tableScanGetRid() {
        TxMgrBase txMgr = newTxMgr("tableScanGetRid");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setInt("id", 42);

        RID rid = ts.getRid();
        assertEquals(0, rid.blockNumber());
        assertTrue(rid.slot() >= 0);

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: moveToRid positions on specified record")
    void tableScanMoveToRid() {
        TxMgrBase txMgr = newTxMgr("tableScanMoveToRid");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setInt("id", 1);
        RID rid1 = ts.getRid();

        ts.insert();
        ts.setInt("id", 2);

        // Move back to first record
        ts.moveToRid(rid1);
        assertEquals(1, ts.getInt("id"));

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: beforeFirst resets to beginning")
    void tableScanBeforeFirst() {
        TxMgrBase txMgr = newTxMgr("tableScanBeforeFirst");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setInt("id", 1);
        ts.insert();
        ts.setInt("id", 2);

        ts.beforeFirst();
        assertTrue(ts.next());
        assertEquals(1, ts.getInt("id"));

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: next returns false on empty table")
    void tableScanNextEmptyTable() {
        TxMgrBase txMgr = newTxMgr("tableScanNextEmptyTable");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        assertFalse(ts.next());

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: next traverses multiple blocks")
    void tableScanNextMultipleBlocks() {
        TxMgrBase txMgr = newTxMgr("tableScanNextMultipleBlocks");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);

        int slotsPerPage = BLOCK_SIZE / layout.slotSize();
        // Insert enough records to span multiple pages
        for (int i = 0; i < slotsPerPage + 3; i++) {
            ts.insert();
            ts.setInt("id", i);
        }

        // Traverse all records
        ts.beforeFirst();
        int count = 0;
        while (ts.next()) {
            ts.getInt("id");
            count++;
        }
        assertEquals(slotsPerPage + 3, count);

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: getVal returns correct Datum for INTEGER")
    void tableScanGetValInteger() {
        TxMgrBase txMgr = newTxMgr("tableScanGetValInteger");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setInt("id", 42);

        DatumBase val = ts.getVal("id");
        assertEquals(Types.INTEGER, val.getSQLType());
        assertEquals(42, val.asInt());

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: getVal returns correct Datum for VARCHAR")
    void tableScanGetValVarchar() {
        TxMgrBase txMgr = newTxMgr("tableScanGetValVarchar");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setString("name", "Alice");

        DatumBase val = ts.getVal("name");
        assertEquals(Types.VARCHAR, val.getSQLType());
        assertEquals("Alice", val.asString());

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: getVal returns correct Datum for BOOLEAN")
    void tableScanGetValBoolean() {
        TxMgrBase txMgr = newTxMgr("tableScanGetValBoolean");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setBoolean("active", true);

        DatumBase val = ts.getVal("active");
        assertEquals(Types.BOOLEAN, val.getSQLType());
        assertTrue(val.asBoolean());

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: getVal returns correct Datum for DOUBLE")
    void tableScanGetValDouble() {
        TxMgrBase txMgr = newTxMgr("tableScanGetValDouble");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setDouble("salary", 50000.0);

        DatumBase val = ts.getVal("salary");
        assertEquals(Types.DOUBLE, val.getSQLType());
        assertEquals(50000.0, val.asDouble(), 0.001);

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: hasField returns correct value")
    void tableScanHasField() {
        TxMgrBase txMgr = newTxMgr("tableScanHasField");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        assertTrue(ts.hasField("id"));
        assertTrue(ts.hasField("name"));
        assertFalse(ts.hasField("nonexistent"));

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: getType returns correct type")
    void tableScanGetType() {
        TxMgrBase txMgr = newTxMgr("tableScanGetType");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        assertEquals(Types.INTEGER, ts.getType("id"));
        assertEquals(Types.VARCHAR, ts.getType("name"));
        assertEquals(Types.BOOLEAN, ts.getType("active"));
        assertEquals(Types.DOUBLE, ts.getType("salary"));

        ts.close();
        tx.rollback();
    }

    @Test
    @DisplayName("TableScan: close unpins blocks")
    void tableScanClose() {
        TxMgrBase txMgr = newTxMgr("tableScanClose");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "test", layout);
        ts.insert();
        ts.setInt("id", 1);
        ts.close();

        // Should not throw - tx should still work after scan close
        tx.commit();
    }

    @Test
    @DisplayName("TableScan: getTableFileName returns table name")
    void tableScanGetTableFileName() {
        TxMgrBase txMgr = newTxMgr("tableScanGetTableFileName");
        TxBase tx = txMgr.newTx();
        SchemaBase schema = createTestSchema();
        Layout layout = new Layout(schema);

        TableScan ts = new TableScan(tx, "mytable", layout);
        assertEquals("mytable", ts.getTableFileName());

        ts.close();
        tx.rollback();
    }

    // ========== RID Class Coverage (value class) ==========

    @Test
    @DisplayName("RID: constructor and getters")
    void ridConstructorAndGetters() {
        RID rid = new RID(5, 10);
        assertEquals(5, rid.blockNumber());
        assertEquals(10, rid.slot());
    }

    @Test
    @DisplayName("RID: equals with same object")
    void ridEqualsSameObject() {
        RID rid = new RID(1, 2);
        assertTrue(rid.equals(rid));
    }

    @Test
    @DisplayName("RID: equals with null returns false")
    void ridEqualsNull() {
        RID rid = new RID(1, 2);
        assertFalse(rid.equals(null));
    }

    @Test
    @DisplayName("RID: equals with non-RID returns false")
    void ridEqualsNonRid() {
        RID rid = new RID(1, 2);
        assertFalse(rid.equals("not a RID"));
    }

    @Test
    @DisplayName("RID: equals with same values returns true")
    void ridEqualsSameValues() {
        RID rid1 = new RID(1, 2);
        RID rid2 = new RID(1, 2);
        assertTrue(rid1.equals(rid2));
    }

    @Test
    @DisplayName("RID: equals with different values returns false")
    void ridEqualsDifferentValues() {
        RID rid1 = new RID(1, 2);
        RID rid2 = new RID(1, 3);
        RID rid3 = new RID(2, 2);
        assertFalse(rid1.equals(rid2));
        assertFalse(rid1.equals(rid3));
    }

    @Test
    @DisplayName("RID: hashCode consistent with equals")
    void ridHashCode() {
        RID rid1 = new RID(1, 2);
        RID rid2 = new RID(1, 2);
        assertEquals(rid1.hashCode(), rid2.hashCode());
    }

    @Test
    @DisplayName("RID: toString returns formatted string")
    void ridToString() {
        RID rid = new RID(5, 10);
        String str = rid.toString();
        assertTrue(str.contains("5"));
        assertTrue(str.contains("10"));
    }
}
