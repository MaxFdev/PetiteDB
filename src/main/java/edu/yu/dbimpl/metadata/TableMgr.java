package edu.yu.dbimpl.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.record.Layout;
import edu.yu.dbimpl.record.LayoutBase;
import edu.yu.dbimpl.record.RID;
import edu.yu.dbimpl.record.RecordPageBase;
import edu.yu.dbimpl.record.Schema;
import edu.yu.dbimpl.record.SchemaBase;
import edu.yu.dbimpl.record.TableScan;
import edu.yu.dbimpl.tx.TxBase;

public class TableMgr extends TableMgrBase {

    private static final Logger logger = LogManager.getLogger(TableMgr.class);

    /**
     * Table catalog schema:
     * [ tblname | slotsize ]
     * <p>
     * Key:
     * (tblname)
     */
    private static final LayoutBase TABLE_CATALOG_LAYOUT;
    static {
        SchemaBase tableCatalogSchema = new Schema();
        tableCatalogSchema.addStringField(TABLE_NAME, MAX_LENGTH_PER_NAME);
        tableCatalogSchema.addIntField("slotsize");
        TABLE_CATALOG_LAYOUT = new Layout(tableCatalogSchema);
    }

    /**
     * Field catalog schema:
     * [ tblname | fldname | type | length | offset]
     * <p>
     * Key:
     * (tblname, fldname)
     */
    private static final LayoutBase FIELD_CATALOG_LAYOUT;
    static {
        SchemaBase fieldCatalogSchema = new Schema();
        fieldCatalogSchema.addStringField(TABLE_NAME, MAX_LENGTH_PER_NAME);
        fieldCatalogSchema.addStringField("fldname", MAX_LENGTH_PER_NAME);
        fieldCatalogSchema.addIntField("type");
        fieldCatalogSchema.addIntField("length");
        fieldCatalogSchema.addIntField("offset");
        FIELD_CATALOG_LAYOUT = new Layout(fieldCatalogSchema);
    }

    private final Map<String, LayoutBase> tableLayoutMap;
    private final Map<String, CatalogLocation> tableCatalogLocation;
    private final TreeMap<Integer, List<FieldCatalogGap>> gapsByLength;
    private final AtomicReference<RID> fieldCatalogInsertMarker;
    private final int slotsPerBlock;

    public TableMgr(TxBase tx) {
        super(tx);

        logger.info("(Re)initialize database: {}", DBConfiguration.INSTANCE.isDBStartup());

        // check config to see if this is a new db or existing
        boolean startup;
        try {
            startup = DBConfiguration.INSTANCE.isDBStartup();
        } catch (Exception e) {
            // throw IllegalStateException if db config can't supply info
            throw new IllegalStateException("Config couldn't supply startup info", e);
        }

        // set up table catalog location map
        this.tableCatalogLocation = new ConcurrentHashMap<>();

        // set up the table layout map
        this.tableLayoutMap = new ConcurrentHashMap<>();

        this.gapsByLength = new TreeMap<>();
        this.fieldCatalogInsertMarker = new AtomicReference<>(new RID(0, RecordPageBase.BEFORE_FIRST_SLOT));
        this.slotsPerBlock = tx.blockSize() / FIELD_CATALOG_LAYOUT.slotSize();

        if (startup) {
            logger.info("{} creating new tblcat and fldcat tables", tx);

            // write meta data tables to the catalog
            writeSchemaToCatalog(FIELD_META_DATA_TABLE, FIELD_CATALOG_LAYOUT, tx);
            writeSchemaToCatalog(TABLE_META_DATA_TABLE, TABLE_CATALOG_LAYOUT, tx);

            // add meta data tables to cache
            this.tableLayoutMap.put(TABLE_META_DATA_TABLE, TABLE_CATALOG_LAYOUT);
            this.tableLayoutMap.put(FIELD_META_DATA_TABLE, FIELD_CATALOG_LAYOUT);
        } else {
            logger.info("{} restoring tblcat and fldcat tables", tx);

            loadCatalogCache(tx);
        }
    }

    /**
     * Load the catalog cache from the existing catalog tables. Reconstructs layout
     * objects and catalog locations for all tables found in the catalog.
     *
     * @param tx transaction to use for table scans
     */
    private void loadCatalogCache(TxBase tx) {
        TableScan tblCatScan = new TableScan(tx, TABLE_META_DATA_TABLE, TABLE_CATALOG_LAYOUT);
        TableScan fldCatScan = new TableScan(tx, FIELD_META_DATA_TABLE, FIELD_CATALOG_LAYOUT);

        // create maps for reconstructing the cache
        Map<String, Integer> slotSizeByTable = new HashMap<>();
        Map<String, RID> tableRidByTable = new HashMap<>();
        Map<String, Schema> schemaByTable = new HashMap<>();
        Map<String, RID> firstFieldRidByTable = new HashMap<>();
        Map<String, Map<String, Integer>> offsetsByTable = new HashMap<>();

        // read table catalog entries
        tblCatScan.beforeFirst();
        while (tblCatScan.next()) {
            String tableName = tblCatScan.getString(TABLE_NAME);
            slotSizeByTable.put(tableName, tblCatScan.getInt("slotsize"));
            tableRidByTable.put(tableName, tblCatScan.getRid());
        }

        // read field catalog entries
        RID previousFieldRid = null;
        fldCatScan.beforeFirst();
        while (fldCatScan.next()) {
            String tableName = fldCatScan.getString(TABLE_NAME);
            String fieldName = fldCatScan.getString("fldname");
            int type = fldCatScan.getInt("type");
            int length = fldCatScan.getInt("length");
            int offset = fldCatScan.getInt("offset");

            // detect gaps
            RID currentRid = fldCatScan.getRid();
            if (previousFieldRid != null) {
                int gapLength = (currentRid.blockNumber() - previousFieldRid.blockNumber()) * this.slotsPerBlock
                        + currentRid.slot() - previousFieldRid.slot() - 1;
                // record gaps between the previous and current RIDs
                if (gapLength > 0) {
                    int startBlock = previousFieldRid.blockNumber();
                    int startSlot = previousFieldRid.slot() + 1;
                    if (startSlot >= this.slotsPerBlock) {
                        startBlock += 1;
                        startSlot = 0;
                    }
                    recordGap(new RID(startBlock, startSlot), gapLength);
                }
            }

            // update the previous rid
            previousFieldRid = currentRid;

            // generate a new schema if one does not exist
            Schema schema = schemaByTable.computeIfAbsent(tableName, key -> new Schema());

            // add the field to the schema
            schema.addField(fieldName, type, length);

            // save the field offset
            offsetsByTable.computeIfAbsent(tableName, key -> new HashMap<>()).put(fieldName, offset);

            // cache the first field's RID for the table
            firstFieldRidByTable.computeIfAbsent(tableName, key -> fldCatScan.getRid());
        }

        // update the field catalog insert marker
        if (previousFieldRid != null) {
            updateInsertMarker(previousFieldRid);
        }

        // reconstruct layouts and catalog locations
        for (Map.Entry<String, Integer> entry : slotSizeByTable.entrySet()) {
            String tableName = entry.getKey();
            int slotSize = entry.getValue();
            Schema schema = schemaByTable.getOrDefault(tableName, new Schema());
            Map<String, Integer> offsets = offsetsByTable.getOrDefault(tableName, new HashMap<>());

            // create and cache the layout using the metadata
            LayoutBase layout = new Layout(schema, offsets, slotSize);
            this.tableLayoutMap.put(tableName, layout);

            // cache the catalog locations
            RID tableRid = tableRidByTable.get(tableName);
            RID fieldRid = firstFieldRidByTable.get(tableName);
            if (tableRid != null && fieldRid != null) {
                this.tableCatalogLocation.put(tableName, new CatalogLocation(tableRid, fieldRid));
            }
        }

        tblCatScan.close();
        fldCatScan.close();
    }

    /**
     * Write a schema (from a layout) to the table and field catalogs.
     * 
     * @param tableName the table name
     * @param layout    the layout containing the schema
     * @param tx        the transaction for the table scan
     */
    private void writeSchemaToCatalog(String tableName, LayoutBase layout, TxBase tx) {
        TableScan tblCatScan = new TableScan(tx, TABLE_META_DATA_TABLE, TABLE_CATALOG_LAYOUT);
        TableScan fldCatScan = new TableScan(tx, FIELD_META_DATA_TABLE, FIELD_CATALOG_LAYOUT);

        // get the fields count
        int fieldCount = layout.schema().fields().size();

        // attempt to retrieve a field catalog gap
        FieldCatalogGap selectedGap = null;
        Map.Entry<Integer, List<FieldCatalogGap>> entry = this.gapsByLength.ceilingEntry(fieldCount);
        if (entry != null) {
            selectedGap = entry.getValue().remove(0);
            if (entry.getValue().isEmpty()) {
                this.gapsByLength.remove(entry.getKey());
            }
        }

        // check if there is a gap
        if (selectedGap != null) {
            // position the table scan
            RID startRid = selectedGap.startRid;
            int slotBefore = startRid.slot() - 1;
            fldCatScan.moveToRid(new RID(startRid.blockNumber(), slotBefore));

            // recalculate the gap
            if (selectedGap.length > fieldCount) {
                int remaining = selectedGap.length - fieldCount;
                int newBlock = startRid.blockNumber();
                int newSlot = startRid.slot() + fieldCount;
                if (newSlot >= this.slotsPerBlock) {
                    newBlock += newSlot / this.slotsPerBlock;
                    newSlot = newSlot % this.slotsPerBlock;
                }
                recordGap(new RID(newBlock, newSlot), remaining);
            }
        } else {
            // position the table scan at the end
            fldCatScan.moveToRid(this.fieldCatalogInsertMarker.get());
        }

        // insert table catalog data
        tblCatScan.insert();
        tblCatScan.setString(TABLE_NAME, tableName);
        tblCatScan.setInt("slotsize", layout.slotSize());

        // insert field catalog data
        RID lastFieldRid = null;
        for (String fldname : layout.schema().fields()) {
            fldCatScan.insert();

            // create the catalog location first field slot
            this.tableCatalogLocation.computeIfAbsent(tableName, (key) -> {
                return new CatalogLocation(tblCatScan.getRid(), fldCatScan.getRid());
            });

            fldCatScan.setString(TABLE_NAME, tableName);
            fldCatScan.setString("fldname", fldname);
            fldCatScan.setInt("type", layout.schema().type(fldname));
            fldCatScan.setInt("length", layout.schema().length(fldname));
            fldCatScan.setInt("offset", layout.offset(fldname));
            lastFieldRid = fldCatScan.getRid();
        }

        // update the last insert marker
        if (lastFieldRid != null) {
            updateInsertMarker(lastFieldRid);
        }

        tblCatScan.close();
        fldCatScan.close();
    }

    @Override
    public LayoutBase getLayout(String tableName, TxBase tx) {
        return this.tableLayoutMap.get(tableName);
    }

    @Override
    public LayoutBase createTable(String tableName, SchemaBase schema, TxBase tx) {
        return this.tableLayoutMap.compute(tableName, (key, val) -> {
            // check if there is already an entry for this name
            if (val != null) {
                throw new IllegalArgumentException();
            }

            // create the layout and write it to the catalog
            LayoutBase layout = new Layout(schema);
            writeSchemaToCatalog(tableName, layout, tx);
            return layout;
        });
    }

    @Override
    public LayoutBase replace(String tableName, SchemaBase schema, TxBase tx) {
        AtomicReference<LayoutBase> lastLayoutReference = new AtomicReference<>(null);

        this.tableLayoutMap.compute(tableName, (key, val) -> {
            // check an entry for this name does not exist
            if (val == null) {
                throw new IllegalArgumentException("Can't replace nonexisting catalog entry");
            }

            // save the last layout
            lastLayoutReference.set(val);
            LayoutBase layout = schema == null ? null : new Layout(schema);
            editCatalogEntry(tableName, layout, tx);
            return layout;
        });

        return lastLayoutReference.get();
    }

    /**
     * Edits a catalogs entry using the new schema. The old metadata for the table
     * is deleted from the catalog and replaced with the new schema. If the new
     * schema is null, the metadata is simply deleted.
     * 
     * @param tableName the table name
     * @param layout    the layout containing the schema
     * @param tx        the transaction for the table scan
     */
    private void editCatalogEntry(String tableName, LayoutBase layout, TxBase tx) {
        // get the catalog locations for the table
        CatalogLocation catalogLocation = this.tableCatalogLocation.get(tableName);
        if (catalogLocation != null) {
            // delete from table catalog using the stored RID
            TableScan tblCatScan = new TableScan(tx, TABLE_META_DATA_TABLE, TABLE_CATALOG_LAYOUT);
            tblCatScan.moveToRid(catalogLocation.tableCatalogRID);
            tblCatScan.delete();
            tblCatScan.close();

            // delete from field catalog starting at the first field's RID
            TableScan fldCatScan = new TableScan(tx, FIELD_META_DATA_TABLE, FIELD_CATALOG_LAYOUT);
            fldCatScan.moveToRid(catalogLocation.fieldCatalogRID);

            // delete all field records for this table
            int deletedCount = 0;
            RID gapStartRid = catalogLocation.fieldCatalogRID;
            while (fldCatScan.getString(TABLE_NAME).equals(tableName)) {
                fldCatScan.delete();
                deletedCount++;
                if (!fldCatScan.next()) {
                    break;
                }
            }

            // record the new gap
            recordGap(gapStartRid, deletedCount);

            fldCatScan.close();
        }

        // Remove from the cache
        this.tableCatalogLocation.remove(tableName);

        // If new layout is provided, write it to the catalog
        if (layout != null) {
            writeSchemaToCatalog(tableName, layout, tx);
        }
    }

    /**
     * Updates the insert marker for the field catalog to track the most recent
     * insertion point.
     * 
     * @param rid the RID to potentially set as the new insert marker
     */
    private void updateInsertMarker(RID rid) {
        this.fieldCatalogInsertMarker.accumulateAndGet(rid, (existing, incoming) -> {
            if (existing == null) {
                return incoming;
            }
            if (incoming.blockNumber() > existing.blockNumber()) {
                return incoming;
            }
            if (incoming.blockNumber() == existing.blockNumber() && incoming.slot() > existing.slot()) {
                return incoming;
            }
            return existing;
        });
    }

    /**
     * Records a gap in the field catalog for potential reuse.
     * 
     * @param startRid the RID (Record ID) indicating the starting position of the
     *                 gap
     * @param length   the size of the gap in bytes; must be positive to be recorded
     */
    private void recordGap(RID startRid, int length) {
        if (length <= 0) {
            return;
        }

        FieldCatalogGap gap = new FieldCatalogGap(startRid, length);
        this.gapsByLength.computeIfAbsent(length, k -> new ArrayList<>()).add(gap);
    }

    /**
     * A class for caching the table and field catalog locations for a given table.
     */
    private final static class CatalogLocation {
        private final RID tableCatalogRID;
        private final RID fieldCatalogRID;

        private CatalogLocation(RID tableCatalogRID, RID fieldCatalogRID) {
            this.tableCatalogRID = tableCatalogRID;
            this.fieldCatalogRID = fieldCatalogRID;
        }
    }

    /**
     * A class for caching the gaps in the field catalog.
     */
    private static final class FieldCatalogGap {
        private final RID startRid;
        private final int length;

        private FieldCatalogGap(RID startRid, int length) {
            this.startRid = startRid;
            this.length = length;
        }
    }

}
