package edu.yu.dbimpl.index;

import java.util.Objects;

import edu.yu.dbimpl.metadata.TableMgrBase;
import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.record.RID;
import edu.yu.dbimpl.record.TableScan;
import edu.yu.dbimpl.record.TableScanBase;
import edu.yu.dbimpl.tx.TxBase;

public class Index implements IndexBase {

    private final TxBase tx;
    private final TableMgrBase tableMgr;
    private final int id;
    private final IndexDescriptorBase descriptor;
    private final int bucketCount;
    private final int expectedValueType;
    private DatumBase searchKey;
    private TableScanBase indexScan;
    private TableScanBase tableScan;
    private RID tableRID;

    public Index(TxBase tx, TableMgrBase tableMgr, int id, IndexDescriptorBase descriptor, int bucketCount) {
        this.tx = tx;
        this.tableMgr = tableMgr;
        this.id = id;
        this.descriptor = descriptor;
        this.bucketCount = bucketCount;

        this.expectedValueType = this.tableMgr.getLayout(this.descriptor.getTableName(), this.tx)
                .schema()
                .type(this.descriptor.getFieldName());
    }

    @Override
    public void beforeFirst(DatumBase searchKey) {
        checkCompatibility(searchKey);

        // set the search key
        this.searchKey = searchKey;

        setIndexScan(searchKey);
    }

    @Override
    public boolean next() {
        if (this.indexScan == null) {
            throw new IllegalStateException("Index scan must be initialized via `beforeFirst`");
        }

        // check if the table scan needs to be initialized
        if (this.tableScan == null) {
            this.tableScan = new TableScan(
                    this.tx,
                    this.descriptor.getTableName(),
                    this.tableMgr.getLayout(this.descriptor.getTableName(), tx));
        }

        while (this.indexScan.next()) {
            // get the next val hash code
            int valueHashCode = this.indexScan.getInt("ValueHashCode");

            // check the hash code against the search key
            if (this.searchKey.hashCode() == valueHashCode) {
                // get the value location in the table
                RID tablePosition = new RID(
                        this.indexScan.getInt("RID_Block"),
                        this.indexScan.getInt("RID_Slot"));
                this.tableScan.moveToRid(tablePosition);

                try {
                    // check the actual value (in case of hash collision)
                    if (this.tableScan.getVal(this.descriptor.getFieldName()).equals(this.searchKey)) {
                        // set the current table rid if the value matches
                        this.tableRID = tablePosition;
                        return true;
                    }
                } catch (IllegalStateException e) {
                    // swallow any exceptions caused by misaligned data
                }
            }
        }

        return false;
    }

    @Override
    public RID getRID() {
        if (this.indexScan == null) {
            throw new IllegalStateException("Index scan must be initialized via `beforeFirst`");
        }

        if (this.tableRID == null) {
            throw new IllegalStateException("Index scan must be positioned over a valid RID via `next`");
        }

        return this.tableRID;
    }

    @Override
    public void insert(DatumBase value, RID rid) {
        checkCompatibility(value);
        if (rid == null) {
            throw new IllegalArgumentException("RID can't be null");
        }

        // get the search position if a search is going on
        RID tempRID = this.searchKey == null ? null : this.indexScan.getRid();

        setIndexScan(value);

        // insert the value into the index bucket
        this.indexScan.insert();
        this.indexScan.setInt("RID_Block", rid.blockNumber());
        this.indexScan.setInt("RID_Slot", rid.slot());
        this.indexScan.setInt("ValueHashCode", value.hashCode());

        // reset the search position
        if (tempRID != null) {
            setIndexScan(this.searchKey);
            this.indexScan.moveToRid(tempRID);
        }
    }

    @Override
    public void delete(DatumBase value, RID rid) {
        checkCompatibility(value);
        if (rid == null) {
            throw new IllegalArgumentException("RID can't be null");
        }

        // get the search position if a search is going on
        RID tempRID = this.searchKey == null ? null : this.indexScan.getRid();

        setIndexScan(value);

        while (this.indexScan.next()) {
            // get the rid
            RID tablePosition = new RID(
                    this.indexScan.getInt("RID_Block"),
                    this.indexScan.getInt("RID_Slot"));

            // get the hash code
            int valueHashCode = this.indexScan.getInt("ValueHashCode");

            // check the rid and the value hash code
            if (tablePosition.equals(rid) && valueHashCode == value.hashCode()) {
                // create a table to check the actual value
                TableScanBase tableProbe = new TableScan(
                        tx,
                        this.descriptor.getTableName(),
                        this.tableMgr.getLayout(this.descriptor.getTableName(), tx));
                tableProbe.moveToRid(rid);

                try {
                    // check the actual value
                    if (tableProbe.getVal(this.descriptor.getFieldName()).equals(value)) {
                        // delete the index record
                        this.indexScan.delete();
                    }
                } catch (IllegalStateException e) {
                    // swallow any exceptions caused by misaligned data
                }

                tableProbe.close();
            }
        }

        // reset the search position
        if (tempRID != null) {
            setIndexScan(this.searchKey);
            this.indexScan.moveToRid(tempRID);
        }
    }

    @Override
    public void deleteAll() {
        // cycle through each bucket
        for (int i = 0; i < this.bucketCount; i++) {
            // create a scan to linearly delete each record
            TableScanBase bucketDeleteScan = new TableScan(
                    tx,
                    IndexMgr.getBucketTableName(this.id, i),
                    IndexMgr.GENERIC_INDEX_BUCKET_LAYOUT);
            IndexMgr.delete(bucketDeleteScan);
            bucketDeleteScan.close();
        }
    }

    @Override
    public void close() {
        if (this.indexScan != null) {
            this.indexScan.close();
        }

        if (this.tableScan != null) {
            this.tableScan.close();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id, this.descriptor);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof Index)) {
            return false;
        }

        Index other = ((Index) o);
        return this.id == other.id
                && this.descriptor.equals(other.descriptor)
                && this.bucketCount == other.bucketCount;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Index: [id: ")
                .append(this.id)
                .append(", descriptor: ")
                .append(this.descriptor)
                .append(", bucketCount: ")
                .append(this.bucketCount)
                .append("]")
                .toString();
    }

    /**
     * Check the compatibility of the value based on the expected type.
     * 
     * @param value
     */
    private void checkCompatibility(DatumBase value) {
        if (value == null || this.expectedValueType != value.getSQLType()) {
            throw new IllegalArgumentException("Invalid datum value");
        }
    }

    /**
     * Set the index scan to the targeted bucket based on the datum. Also positions
     * the scan at the before first position.
     * 
     * @param datum
     */
    private void setIndexScan(DatumBase datum) {
        // get the hash code and bucket for the value key
        int searchHashCode = datum.hashCode();
        int bucket = Math.abs(searchHashCode) % this.bucketCount;
        String indexBucketTableName = IndexMgr.getBucketTableName(this.id, bucket);

        // check if a previous index scan can be used
        if (this.indexScan != null && !this.indexScan.getTableFileName().equals(indexBucketTableName)) {
            this.indexScan.close();
            this.indexScan = null;
        }

        // check if the index scan needs to be (re)created
        if (this.indexScan == null) {
            // position the scan on the correct bucket based on the hash
            this.indexScan = new TableScan(this.tx, indexBucketTableName, IndexMgr.GENERIC_INDEX_BUCKET_LAYOUT);
        }

        // ensure the scan is at the beginning
        this.indexScan.beforeFirst();
    }
}
