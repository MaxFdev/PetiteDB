package edu.yu.dbimpl.query;

import java.sql.Types;
import java.util.Arrays;

public class Datum extends DatumBase {

    private final Object val;
    private final int type;
    private Integer hashCode;

    public Datum(final Integer ival) {
        super(ival);

        this.val = ival;
        this.type = Types.INTEGER;
    }

    public Datum(final String sval) {
        super(sval);

        this.val = sval;
        this.type = Types.VARCHAR;
    }

    public Datum(final Boolean bval) {
        super(bval);

        this.val = bval;
        this.type = Types.BOOLEAN;
    }

    public Datum(final Double dval) {
        super(dval);

        this.val = dval;
        this.type = Types.DOUBLE;
    }

    public Datum(final byte[] array) {
        super(array);

        this.val = array;
        this.type = Types.VARBINARY;
    }

    @Override
    public int compareTo(DatumBase arg0) {
        if (this.type != arg0.getSQLType()) {
            throw new ClassCastException("Can't cast other datum val to same type as this datum");
        }

        switch (this.type) {
            case Types.INTEGER -> {
                return Integer.compare(asInt(), arg0.asInt());
            }
            case Types.VARCHAR -> {
                return asString().compareTo(arg0.asString());
            }
            case Types.BOOLEAN -> {
                return Boolean.compare(asBoolean(), arg0.asBoolean());
            }
            case Types.DOUBLE -> {
                return Double.compare(asDouble(), arg0.asDouble());
            }
            case Types.VARBINARY -> {
                return Arrays.compare(asBinaryArray(), arg0.asBinaryArray());
            }
            default -> {
                throw new IllegalStateException("Could not identify valid datum type");
            }
        }
    }

    @Override
    public int asInt() {
        if (this.type == Types.INTEGER) {
            return (int) this.val;
        } else if (this.type == Types.DOUBLE) {
            return (int) (double) this.val;
        } else {
            throw new ClassCastException("Can't cast datum val to incorrect type");
        }
    }

    @Override
    public boolean asBoolean() {
        if (this.type != Types.BOOLEAN) {
            throw new ClassCastException("Can't cast datum val to incorrect type");
        }
        return (boolean) this.val;
    }

    @Override
    public double asDouble() {
        if (this.type == Types.INTEGER) {
            return (double) (int) this.val;
        } else if (this.type == Types.DOUBLE) {
            return (double) this.val;
        } else {
            throw new ClassCastException("Can't cast datum val to incorrect type");
        }
    }

    @Override
    public String asString() {
        if (this.type == Types.VARBINARY) {
            return new String((byte[]) this.val);
        } else if (this.type == Types.VARCHAR) {
            return (String) this.val;
        } else {
            throw new ClassCastException("Can't cast datum val to incorrect type");
        }
    }

    @Override
    public byte[] asBinaryArray() {
        if (this.type == Types.VARBINARY) {
            return (byte[]) this.val;
        } else if (this.type == Types.VARCHAR) {
            return ((String) this.val).getBytes();
        } else {
            throw new ClassCastException("Can't cast datum val to incorrect type");
        }
    }

    @Override
    public int getSQLType() {
        return this.type;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof Datum)) {
            return false;
        }

        Datum otherDatum = (Datum) other;
        if (this.type != otherDatum.type) {
            return false;
        }

        if (this.type == Types.VARBINARY) {
            return Arrays.equals(asBinaryArray(), otherDatum.asBinaryArray());
        }

        return this.val.equals(otherDatum.val);
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Datum: [type: ")
                .append(this.type)
                .append(", val: ")
                .append(this.val)
                .append("]")
                .toString();
    }

    @Override
    public int hashCode() {
        if (this.hashCode != null) {
            return this.hashCode;
        }

        if (this.type == Types.VARBINARY) {
            this.hashCode = Arrays.hashCode(asBinaryArray());
        } else {
            this.hashCode = this.val.hashCode();
        }

        return this.hashCode;
    }

}
