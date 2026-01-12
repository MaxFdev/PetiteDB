package edu.yu.dbimpl.record;

import java.sql.Types;

import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.query.Datum;
import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.tx.TxBase;

public class TableScan extends TableScanBase {

    private final TxBase tx;
    private final String tblname;
    private final LayoutBase layout;
    private RecordPageBase currentRecordPage;
    private int currentRecordSlot;

    public TableScan(TxBase tx, String tblname, LayoutBase layout) {
        super(tx, tblname, layout);

        this.tx = tx;
        this.tblname = tblname;
        this.layout = layout;

        // check if the file exists/is empty
        if (tx.size(tblname) == 0) {
            // create and format a new block - tx.append doesn't pin, so we must
            BlockId newBlock = (BlockId) tx.append(tblname);
            tx.pin(newBlock);
            this.currentRecordPage = new RecordPage(tx, newBlock, layout);
            this.currentRecordPage.format();
        } else {
            // open existing table and pin the first block
            BlockId firstBlock = new BlockId(tblname, 0);
            tx.pin(firstBlock);
            this.currentRecordPage = new RecordPage(tx, firstBlock, layout);
        }

        this.currentRecordSlot = RecordPageBase.BEFORE_FIRST_SLOT;
    }

    @Override
    public void setVal(String fldname, DatumBase val) {
        switch (val.getSQLType()) {
            case Types.INTEGER -> {
                setInt(fldname, val.asInt());
            }
            case Types.VARCHAR -> {
                setString(fldname, val.asString());
            }
            case Types.BOOLEAN -> {
                setBoolean(fldname, val.asBoolean());
            }
            case Types.DOUBLE -> {
                setDouble(fldname, val.asDouble());
            }
            case Types.VARBINARY -> {
                throw new IllegalArgumentException("Unsupported datum value");
            }
        }

    }

    @Override
    public void setInt(String fldname, int val) {
        this.currentRecordPage.setInt(this.currentRecordSlot, fldname, val);
    }

    @Override
    public void setDouble(String fldname, double val) {
        this.currentRecordPage.setDouble(this.currentRecordSlot, fldname, val);
    }

    @Override
    public void setBoolean(String fldname, boolean val) {
        this.currentRecordPage.setBoolean(this.currentRecordSlot, fldname, val);
    }

    @Override
    public void setString(String fldname, String val) {
        this.currentRecordPage.setString(this.currentRecordSlot, fldname, val);
    }

    @Override
    public void insert() {
        // traverse the table looking for a place to insert a record
        int inserted = this.currentRecordPage.insertAfter(this.currentRecordSlot);
        while (inserted == -1 && this.currentRecordPage.block().number() < this.tx.size(this.tblname) - 1) {
            traverseTo(this.currentRecordPage.block().number() + 1);
            inserted = this.currentRecordPage.insertAfter(RecordPageBase.BEFORE_FIRST_SLOT);
        }

        // check if the insert was successful
        if (inserted != -1) {
            // set the record slot pointer
            this.currentRecordSlot = inserted;
        } else {
            // append a new block to insert the record into
            BlockIdBase blk = this.tx.append(this.tblname);
            traverseTo(blk.number(), RecordPageBase.BEFORE_FIRST_SLOT);
            this.currentRecordPage.format();
            this.currentRecordSlot = this.currentRecordPage.insertAfter(this.currentRecordSlot);
        }
    }

    @Override
    public void delete() {
        this.currentRecordPage.delete(this.currentRecordSlot);
    }

    @Override
    public RID getRid() {
        return new RID(this.currentRecordPage.block().number(), this.currentRecordSlot);
    }

    @Override
    public void moveToRid(RID rid) {
        traverseTo(rid.blockNumber(), rid.slot());
    }

    @Override
    public void beforeFirst() {
        traverseTo(0, RecordPageBase.BEFORE_FIRST_SLOT);
    }

    @Override
    public boolean next() {
        // keep track of starting position for potential rollback
        int startingBlock = this.currentRecordPage.block().number();
        int startingSlot = this.currentRecordSlot;

        // search through the table for the next available record
        int nextSlot = this.currentRecordPage.nextAfter(this.currentRecordSlot);
        while (nextSlot == -1 && this.currentRecordPage.block().number() < this.tx.size(this.tblname) - 1) {
            traverseTo(this.currentRecordPage.block().number() + 1);
            nextSlot = this.currentRecordPage.nextAfter(RecordPageBase.BEFORE_FIRST_SLOT);
        }

        // check if the next slot was found
        if (nextSlot != -1) {
            this.currentRecordSlot = nextSlot;
            return true;
        } else {
            // return to the starting position if there are no open records
            traverseTo(startingBlock, startingSlot);
            return false;
        }
    }

    /**
     * A method for traversing to a different record page, unpinning the current one
     * and pinning the new one. If already on the target block, does nothing to
     * avoid unnecessary unpin/pin cycles.
     * 
     * @param blknum
     */
    private void traverseTo(int blknum) {
        if (this.currentRecordPage.block().number() == blknum) {
            return;
        }

        this.tx.unpin(this.currentRecordPage.block());
        BlockId newBlock = new BlockId(this.tblname, blknum);
        this.tx.pin(newBlock);
        this.currentRecordPage = new RecordPage(this.tx, newBlock, this.layout);
    }

    /**
     * A method for traversing to a different record page, unpinning the current one
     * in the process. Additionally, this method sets the current slot to the
     * provided one.
     * 
     * @param blknum
     * @param slot
     */
    private void traverseTo(int blknum, int slot) {
        traverseTo(blknum);
        this.currentRecordSlot = slot;
    }

    @Override
    public int getInt(String fldname) {
        return this.currentRecordPage.getInt(this.currentRecordSlot, fldname);
    }

    @Override
    public boolean getBoolean(String fldname) {
        return this.currentRecordPage.getBoolean(this.currentRecordSlot, fldname);
    }

    @Override
    public double getDouble(String fldname) {
        return this.currentRecordPage.getDouble(this.currentRecordSlot, fldname);
    }

    @Override
    public String getString(String fldname) {
        return this.currentRecordPage.getString(this.currentRecordSlot, fldname);
    }

    @Override
    public DatumBase getVal(String fldname) {
        switch (this.layout.schema().type(fldname)) {
            case Types.INTEGER -> {
                return new Datum(getInt(fldname));
            }
            case Types.VARCHAR -> {
                return new Datum(getString(fldname));
            }
            case Types.BOOLEAN -> {
                return new Datum(getBoolean(fldname));
            }
            case Types.DOUBLE -> {
                return new Datum(getDouble(fldname));
            }
            default -> {
                throw new IllegalArgumentException("Unsupported field type");
            }
        }
    }

    @Override
    public boolean hasField(String fldname) {
        return this.layout.schema().hasField(fldname);
    }

    @Override
    public int getType(String fldname) {
        return this.layout.schema().type(fldname);
    }

    @Override
    public void close() {
        try {
            this.tx.unpin(this.currentRecordPage.block());
        } catch (IllegalArgumentException e) {
            // swallow exception in case the buffer was closed externally
        }
    }

    @Override
    public String getTableFileName() {
        return this.tblname;
    }

}
