package edu.yu.dbimpl.record;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Layout extends LayoutBase {

    private final SchemaBase schema;
    private final Map<String, Integer> offsetMap;
    private final int slotSize;

    public Layout(SchemaBase schema) {
        super(schema);

        this.schema = schema;
        this.offsetMap = new HashMap<>();

        // start offset after in use boolean
        int offset = 1;

        // calculate offsets
        for (String fieldname : schema.fields()) {
            int type = schema.type(fieldname);
            switch (type) {
                case Types.VARCHAR -> {
                    this.offsetMap.put(fieldname, offset);
                    offset += schema.length(fieldname) + Integer.BYTES;
                }
                case Types.DOUBLE -> {
                    this.offsetMap.put(fieldname, offset);
                    offset += Double.BYTES;
                }
                case Types.INTEGER -> {
                    this.offsetMap.put(fieldname, offset);
                    offset += Integer.BYTES;
                }
                case Types.BOOLEAN -> {
                    this.offsetMap.put(fieldname, offset);
                    offset += 1;
                }
            }
        }

        this.slotSize = offset;
    }

    public Layout(SchemaBase schema, Map<String, Integer> offsets, int slotSize) {
        super(schema, offsets, slotSize);

        this.schema = schema;
        this.offsetMap = offsets;
        this.slotSize = slotSize;
    }

    @Override
    public SchemaBase schema() {
        return this.schema;
    }

    @Override
    public int offset(String fldname) {
        Integer offset = this.offsetMap.get(fldname);
        if (offset == null) {
            throw new IllegalArgumentException("Field name is undefined for this layout");
        }

        return offset;
    }

    @Override
    public int slotSize() {
        return this.slotSize;
    }

    @Override
    public String toString() {
        StringBuilder stb = new StringBuilder();
        stb.append("Layout: [schema: ")
                .append(this.schema)
                .append(", offsets: ")
                .append(this.offsetMap)
                .append(", slotSize: ")
                .append(this.slotSize)
                .append("]");
        return stb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Layout)) {
            return false;
        }
        Layout other = (Layout) o;
        return this.schema.equals(other.schema)
                && this.offsetMap.equals(other.offsetMap)
                && this.slotSize == other.slotSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.schema, this.offsetMap, this.slotSize);
    }

}
