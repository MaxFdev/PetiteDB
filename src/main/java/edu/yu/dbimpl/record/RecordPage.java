package edu.yu.dbimpl.record;

import java.sql.Types;
import java.util.Objects;

import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.tx.TxBase;

public class RecordPage extends RecordPageBase {

    private final TxBase tx;
    private final BlockIdBase block;
    private final LayoutBase layout;
    private final int totalSlots;
    private final int[] slotOffsets;

    public RecordPage(TxBase tx, BlockIdBase blk, LayoutBase layout) {
        super(tx, blk, layout);

        // check block and slot size compatibility
        if (tx.blockSize() < layout.slotSize()) {
            throw new IllegalArgumentException("Block size too small to fit even a single record");
        }

        this.tx = tx;
        this.block = blk;
        this.layout = layout;

        // calculate slot positions
        this.totalSlots = this.tx.blockSize() / this.layout.slotSize();
        this.slotOffsets = new int[this.totalSlots];
        for (int i = 0; i < this.slotOffsets.length; i++) {
            this.slotOffsets[i] = i * this.layout.slotSize();
        }
    }

    @Override
    public int getInt(int slot, String fldname) {
        checkSlotNumber(slot);
        checkFieldType(fldname, Types.INTEGER);

        // retrieve the field value
        int fieldOffset = this.slotOffsets[slot] + this.layout.offset(fldname);
        return this.tx.getInt(this.block, fieldOffset);
    }

    @Override
    public String getString(int slot, String fldname) {
        checkSlotNumber(slot);
        checkFieldType(fldname, Types.VARCHAR);

        // retrieve the field value
        int fieldOffset = this.slotOffsets[slot] + this.layout.offset(fldname);
        return this.tx.getString(this.block, fieldOffset);
    }

    @Override
    public boolean getBoolean(int slot, String fldname) {
        checkSlotNumber(slot);
        checkFieldType(fldname, Types.BOOLEAN);

        // retrieve the field value
        int fieldOffset = this.slotOffsets[slot] + this.layout.offset(fldname);
        return this.tx.getBoolean(this.block, fieldOffset);
    }

    @Override
    public double getDouble(int slot, String fldname) {
        checkSlotNumber(slot);
        checkFieldType(fldname, Types.DOUBLE);

        // retrieve the field value
        int fieldOffset = this.slotOffsets[slot] + this.layout.offset(fldname);
        return this.tx.getDouble(this.block, fieldOffset);
    }

    @Override
    public void setInt(int slot, String fldname, int val) {
        checkSlotNumber(slot);
        checkFieldType(fldname, Types.INTEGER);

        // retrieve the field value
        int fieldOffset = this.slotOffsets[slot] + this.layout.offset(fldname);
        this.tx.setInt(this.block, fieldOffset, val, true);
    }

    @Override
    public void setString(int slot, String fldname, String val) {
        checkSlotNumber(slot);
        checkFieldType(fldname, Types.VARCHAR);

        // check the string length
        if (val.length() > this.layout.schema().length(fldname)) {
            throw new IllegalArgumentException("String exceeds maximum logical length");
        }

        // retrieve the field value
        int fieldOffset = this.slotOffsets[slot] + this.layout.offset(fldname);
        this.tx.setString(this.block, fieldOffset, val, true);
    }

    @Override
    public void setBoolean(int slot, String fldname, boolean val) {
        checkSlotNumber(slot);
        checkFieldType(fldname, Types.BOOLEAN);

        // retrieve the field value
        int fieldOffset = this.slotOffsets[slot] + this.layout.offset(fldname);
        this.tx.setBoolean(this.block, fieldOffset, val, true);
    }

    @Override
    public void setDouble(int slot, String fldname, double val) {
        checkSlotNumber(slot);
        checkFieldType(fldname, Types.DOUBLE);

        // retrieve the field value
        int fieldOffset = this.slotOffsets[slot] + this.layout.offset(fldname);
        this.tx.setDouble(this.block, fieldOffset, val, true);
    }

    /**
     * Check if the field name correlates with the expected type.
     * 
     * @param fieldname field name
     * @param type      expected type
     * @throws IllegalArgumentException iff field type does not align
     */
    private void checkFieldType(String fieldname, int type) {
        if (this.layout.schema().type(fieldname) != type) {
            throw new IllegalArgumentException("Incorrect field type for requested field access");
        }
    }

    @Override
    public void delete(int slot) {
        checkSlotNumber(slot);

        // mark the slot as not in use
        this.tx.setBoolean(this.block, this.slotOffsets[slot], false, false);
    }

    /**
     * Check if the slot number is negative, too large, and "in-use". Before
     * checking if the slot is in use, the block is pinned.
     * 
     * @param slot
     * @throws IllegalArgumentException if slot is negative or greater than total
     *                                  slots per page
     * @throws IllegalStateException    iff slot is NOT "in-use"
     */
    private void checkSlotNumber(int slot) {
        // check if negative
        if (slot < 0) {
            throw new IllegalArgumentException("Slot must be positive");
        }

        // check if too large
        if (slot >= this.totalSlots) {
            throw new IllegalArgumentException("Slot can not exceed total slots per page");
        }

        // check if in-use
        if (!this.tx.getBoolean(this.block, this.slotOffsets[slot])) {
            throw new IllegalStateException("Can only operate on an 'in-use' slot");
        }
    }

    @Override
    public void format() {
        // initialize each slot
        for (int slotOffset : this.slotOffsets) {
            // set in-use to false
            this.tx.setBoolean(block, slotOffset, false, false);

            // initialize each field
            this.layout.schema().fields().forEach((fieldname) -> {
                // get the field type and offset
                int type = this.layout.schema().type(fieldname);
                int fieldOffset = slotOffset + this.layout.offset(fieldname);

                // set the initial values
                switch (type) {
                    case Types.VARCHAR -> {
                        this.tx.setString(this.block, fieldOffset, "", false);
                    }
                    case Types.DOUBLE -> {
                        this.tx.setDouble(this.block, fieldOffset, 0d, false);
                    }
                    case Types.INTEGER -> {
                        this.tx.setInt(this.block, fieldOffset, 0, false);
                    }
                    case Types.BOOLEAN -> {
                        this.tx.setBoolean(this.block, fieldOffset, false, false);
                    }
                }
            });
        }
    }

    @Override
    public int nextAfter(int slot) {
        if (slot < -1) {
            throw new IllegalArgumentException("Slot must be >= -1");
        }

        int slotCheck = slot + 1;
        while (slotCheck < this.totalSlots) {
            // check if the next slot is available
            if (this.tx.getBoolean(this.block, this.slotOffsets[slotCheck])) {
                return slotCheck;
            }

            slotCheck++;
        }

        return -1;
    }

    @Override
    public int insertAfter(int slot) {
        if (slot < -1) {
            throw new IllegalArgumentException("Slot must be >= -1");
        }

        int slotCheck = slot + 1;
        while (slotCheck < this.totalSlots) {
            // check if the next slot is empty
            if (!this.tx.getBoolean(this.block, this.slotOffsets[slotCheck])) {
                // set the slot to "in-use"
                this.tx.setBoolean(this.block, this.slotOffsets[slotCheck], true, false);
                return slotCheck;
            }

            slotCheck++;
        }

        return -1;
    }

    @Override
    public BlockIdBase block() {
        return this.block;
    }

    @Override
    public String toString() {
        StringBuilder stb = new StringBuilder();
        stb.append("RecordPage: [block: ")
                .append(this.block)
                .append(", layout: ")
                .append(this.layout)
                .append(", totalSlots: ")
                .append(this.totalSlots)
                .append("]");
        return stb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RecordPage)) {
            return false;
        }
        RecordPage other = (RecordPage) o;
        return this.block.equals(other.block)
                && this.layout.equals(other.layout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.block, this.layout);
    }

}
