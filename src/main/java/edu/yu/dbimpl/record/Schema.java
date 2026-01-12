package edu.yu.dbimpl.record;

import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Schema extends SchemaBase {

    private final List<String> fieldnameList;
    private final Map<String, Field> fieldMap;

    public Schema() {
        super();

        this.fieldnameList = new LinkedList<>();
        this.fieldMap = new HashMap<>();
    }

    @Override
    public void addField(String fldname, int type, int length) {
        screenFieldName(fldname);

        // set correct length based on type
        int actualLength;
        switch (type) {
            case Types.VARCHAR -> {
                if (length <= 0) {
                    throw new IllegalArgumentException("Field length must be > 0 for type VARCHAR");
                }
                actualLength = length;
            }
            case Types.INTEGER -> actualLength = Integer.BYTES;
            case Types.DOUBLE -> actualLength = Double.BYTES;
            case Types.BOOLEAN -> actualLength = 1;
            default -> throw new IllegalArgumentException("Unsupported type");
        }

        // create or update the field based on the fieldname
        this.fieldMap.compute(fldname, (key, val) -> {
            if (val == null) {
                // add new field to the schema
                val = new Field(fldname, type, actualLength);
                this.fieldnameList.add(fldname);
            } else {
                // update a preexisting field
                val.type = type;
                val.length = actualLength;
            }

            return val;
        });
    }

    @Override
    public void addIntField(String fldname) {
        addField(fldname, Types.INTEGER, Integer.BYTES);
    }

    @Override
    public void addBooleanField(String fldname) {
        addField(fldname, Types.BOOLEAN, 1);
    }

    @Override
    public void addDoubleField(String fldname) {
        addField(fldname, Types.DOUBLE, Double.BYTES);
    }

    @Override
    public void addStringField(String fldname, int length) {
        addField(fldname, Types.VARCHAR, length);
    }

    @Override
    public void add(String fldname, SchemaBase sch) {
        if (sch == null || fldname == null) {
            throw new IllegalArgumentException("Input can't be null");
        }

        if (!sch.hasField(fldname)) {
            throw new IllegalArgumentException("Schema provided does not contain field");
        }

        addField(fldname, sch.type(fldname), sch.length(fldname));
    }

    @Override
    public void addAll(SchemaBase sch) {
        if (sch == null) {
            throw new IllegalArgumentException("Input can't be null");
        }

        for (String fieldName : sch.fields()) {
            addField(fieldName, sch.type(fieldName), sch.length(fieldName));
        }
    }

    @Override
    public List<String> fields() {
        return this.fieldnameList;
    }

    @Override
    public boolean hasField(String fldname) {
        return this.fieldMap.containsKey(fldname);
    }

    @Override
    public int type(String fldname) {
        screenFieldName(fldname);

        Field field = this.fieldMap.get(fldname);
        if (field == null) {
            throw new IllegalArgumentException("Field does not exist");
        }

        return field.type;
    }

    @Override
    public int length(String fldname) {
        screenFieldName(fldname);

        Field field = this.fieldMap.get(fldname);
        if (field == null) {
            throw new IllegalArgumentException("Field does not exist");
        }

        return field.length;
    }

    /**
     * Check if the field name is valid.
     * 
     * @param fieldname the field name to screen
     */
    private void screenFieldName(String fieldname) {
        if (fieldname == null) {
            throw new IllegalArgumentException("Field name can't be null");
        }

        if (fieldname.isEmpty()) {
            throw new IllegalArgumentException("Field name can't be empty");
        }
    }

    @Override
    public String toString() {
        StringBuilder stb = new StringBuilder();
        stb.append("Schema: [");
        Iterator<String> fieldnames = this.fieldnameList.iterator();
        while (fieldnames.hasNext()) {
            stb.append(this.fieldMap.get(fieldnames.next()));
            if (fieldnames.hasNext()) {
                stb.append(", ");
            }
        }
        stb.append("]");
        return stb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Schema)) {
            return false;
        }
        Schema other = (Schema) o;
        return this.fieldnameList.equals(other.fieldnameList)
                && this.fieldMap.equals(other.fieldMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.fieldnameList, this.fieldMap);
    }

    /**
     * Private class representing a single field.
     */
    private static class Field {
        private final String fieldname;
        private int type;
        private int length;

        private Field(String fieldname, int type, int length) {
            this.fieldname = fieldname;
            this.type = type;
            this.length = length;
        }

        @Override
        public String toString() {
            StringBuilder stb = new StringBuilder();
            stb.append("Field: [name: ")
                    .append(this.fieldname)
                    .append(", type: ")
                    .append(this.type)
                    .append(", length: ")
                    .append(this.length)
                    .append("]");
            return stb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Field)) {
                return false;
            }
            Field other = (Field) o;
            return this.fieldname.equals(other.fieldname)
                    && this.type == other.type
                    && this.length == other.length;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.fieldname, this.type, this.length);
        }
    }

}
