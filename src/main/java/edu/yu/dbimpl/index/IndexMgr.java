package edu.yu.dbimpl.index;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.metadata.TableMgrBase;
import edu.yu.dbimpl.record.Layout;
import edu.yu.dbimpl.record.LayoutBase;
import edu.yu.dbimpl.record.Schema;
import edu.yu.dbimpl.record.SchemaBase;
import edu.yu.dbimpl.record.TableScan;
import edu.yu.dbimpl.record.TableScanBase;
import edu.yu.dbimpl.tx.TxBase;

public class IndexMgr extends IndexMgrBase {

    /**
     * Index metadata table schema:
     * [ IndexID | TableName | FieldName ]
     * <p>
     * Key:
     * (IndexID) | (TableName, FieldName)
     */
    private static final SchemaBase INDEX_METADATA_SCHEMA;
    private static final String INDEX_METADATA_TABLE_NAME = "IndexMetadata";
    static {
        INDEX_METADATA_SCHEMA = new Schema();
        INDEX_METADATA_SCHEMA.addIntField("IndexID");
        INDEX_METADATA_SCHEMA.addStringField("TableName", TableMgrBase.MAX_LENGTH_PER_NAME);
        INDEX_METADATA_SCHEMA.addStringField("FieldName", TableMgrBase.MAX_LENGTH_PER_NAME);
    }

    /**
     * Index bucket table schema:
     * [ RID_Block | RID_Slot | ValueHashCode ]
     * <p>
     * Key:
     * (RID_Block, RID_Slot)
     * <p>
     * Note:
     * Buckets should be created with table name being {@code IndexID>BucketNumber},
     * ie: {@code 123456>1}.
     */
    private static final SchemaBase GENERIC_INDEX_BUCKET_SCHEMA;
    public static final LayoutBase GENERIC_INDEX_BUCKET_LAYOUT;
    static {
        GENERIC_INDEX_BUCKET_SCHEMA = new Schema();
        GENERIC_INDEX_BUCKET_SCHEMA.addIntField("RID_Block");
        GENERIC_INDEX_BUCKET_SCHEMA.addIntField("RID_Slot");
        GENERIC_INDEX_BUCKET_SCHEMA.addIntField("ValueHashCode");
        GENERIC_INDEX_BUCKET_LAYOUT = new Layout(GENERIC_INDEX_BUCKET_SCHEMA);
    }

    private final TableMgrBase tableMgr;
    private final int bucketCount;
    private final AtomicInteger IDCounter;
    private final Map<String, Set<Integer>> tableIndexIDMap;
    private final Map<Integer, IndexDescriptorBase> indexIDDescriptorMap;
    private final Map<String, Map<String, Integer>> tableFieldIndexIdMap;

    public IndexMgr(TxBase tx, TableMgrBase tableMgr) {
        super(tx, tableMgr);

        if (tx == null) {
            throw new IllegalArgumentException("Tx can't be null");
        }

        if (tableMgr == null) {
            throw new IllegalArgumentException("TableMgr can't be null");
        }

        // check config to see if this is a new db or existing
        boolean startup;
        try {
            startup = DBConfiguration.INSTANCE.isDBStartup();
        } catch (Exception e) {
            // throw IllegalStateException if db config can't supply info
            throw new IllegalStateException("Config couldn't supply startup info", e);
        }

        // check db config for number of buckets to use for hashing
        this.bucketCount = DBConfiguration.INSTANCE.nStaticHashBuckets();

        // create index relation maps
        this.tableIndexIDMap = new ConcurrentHashMap<>();
        this.indexIDDescriptorMap = new ConcurrentHashMap<>();
        this.tableFieldIndexIdMap = new ConcurrentHashMap<>();

        if (startup) {
            // create the index metadata table
            tableMgr.createTable(INDEX_METADATA_TABLE_NAME, INDEX_METADATA_SCHEMA, tx);
            this.IDCounter = new AtomicInteger();
        } else {
            // perform a scan on the persisted storage
            TableScanBase tableScan = new TableScan(
                    tx,
                    INDEX_METADATA_TABLE_NAME,
                    tableMgr.getLayout(INDEX_METADATA_TABLE_NAME, tx));

            // cycle through the saved indexes
            int highestID = 0;
            while (tableScan.next()) {
                int id = tableScan.getInt("IndexID");
                String tableName = tableScan.getString("TableName");
                String fieldName = tableScan.getString("FieldName");

                // reconstruct the index metadata maps
                IndexDescriptorBase descriptor = new IndexDescriptor(
                        tableName,
                        tableMgr.getLayout(tableName, tx).schema(),
                        fieldName,
                        fieldName,
                        IndexType.STATIC_HASH);
                this.indexIDDescriptorMap.put(id, descriptor);
                this.tableIndexIDMap.compute(tableName, (key, val) -> {
                    // create a new set if required
                    if (val == null) {
                        val = new HashSet<>();
                    }

                    // add the id to the set
                    val.add(id);
                    return val;
                });

                // add to quick lookup map
                this.tableFieldIndexIdMap
                        .computeIfAbsent(tableName, k -> new ConcurrentHashMap<>())
                        .put(fieldName, id);

                // track the highest id
                highestID = Math.max(highestID, id);
            }

            tableScan.close();
            this.IDCounter = new AtomicInteger(highestID + 1);
        }

        this.tableMgr = tableMgr;
    }

    @Override
    public int persistIndexDescriptor(TxBase tx, String tableName, String fieldName, IndexType indexType) {
        if (tx == null) {
            throw new IllegalArgumentException("Tx can't be null");
        }

        if (tableName == null || fieldName == null) {
            throw new IllegalArgumentException("Names can't be null");
        }

        if (indexType == null) {
            throw new IllegalArgumentException("Index type may not be null");
        }

        // the id to return
        AtomicInteger indexId = new AtomicInteger();

        // preform computations under lock
        this.tableIndexIDMap.compute(tableName, (key, val) -> {
            // check that the catalog contains the table and field name
            checkCatalog(tableName, fieldName, tx);

            // lookup for existing index on this field
            Map<String, Integer> fieldMap = this.tableFieldIndexIdMap.get(tableName);
            if (fieldMap != null) {
                Integer existingId = fieldMap.get(fieldName);
                if (existingId != null) {
                    indexId.set(existingId);
                    return val;
                }
            }

            // create the descriptor, using field name as index name
            IndexDescriptorBase descriptor = new IndexDescriptor(
                    tableName,
                    this.tableMgr.getLayout(tableName, tx).schema(),
                    fieldName,
                    fieldName,
                    indexType);

            // get the index id based on id counter
            int id = this.IDCounter.getAndIncrement();

            // persist the index
            TableScanBase tableScan = new TableScan(
                    tx,
                    INDEX_METADATA_TABLE_NAME,
                    this.tableMgr.getLayout(INDEX_METADATA_TABLE_NAME, tx));
            tableScan.insert();
            tableScan.setInt("IndexID", id);
            tableScan.setString("TableName", tableName);
            tableScan.setString("FieldName", fieldName);
            tableScan.close();

            // cache the index (in the maps)
            this.indexIDDescriptorMap.put(id, descriptor);

            // create a new set if required
            if (val == null) {
                val = new HashSet<>();
            }

            // add the id to the set
            val.add(id);

            // add to quick lookup map
            this.tableFieldIndexIdMap
                    .computeIfAbsent(tableName, k -> new ConcurrentHashMap<>())
                    .put(fieldName, id);

            // set the id to return
            indexId.set(id);

            return val;
        });

        return indexId.get();
    }

    @Override
    public Set<Integer> indexIds(TxBase tx, String tableName) {
        if (tx == null) {
            throw new IllegalArgumentException("Tx can't be null");
        }

        if (tableName == null) {
            throw new IllegalArgumentException("Table name can't be null");
        }

        checkCatalog(tableName, tx);
        return this.tableIndexIDMap.getOrDefault(tableName, new HashSet<>());
    }

    /**
     * Helper method for determining if a table has a defined layout.
     * 
     * @param tableName
     * @param tx
     */
    private void checkCatalog(String tableName, TxBase tx) {
        if (this.tableMgr.getLayout(tableName, tx) == null) {
            throw new IllegalArgumentException("Table must have a defined layout");
        }
    }

    /**
     * Helper method for determining if a table and field have a defined layout.
     * 
     * @param tableName
     * @param fieldName
     * @param tx
     */
    private void checkCatalog(String tableName, String fieldName, TxBase tx) {
        LayoutBase layout = this.tableMgr.getLayout(tableName, tx);
        if (layout == null) {
            throw new IllegalArgumentException("Table must have a defined layout");
        }

        if (!layout.schema().hasField(fieldName)) {
            throw new IllegalArgumentException("Field must be defined in table layout");
        }
    }

    @Override
    public IndexDescriptorBase get(TxBase tx, int indexId) {
        if (tx == null) {
            throw new IllegalArgumentException("Tx can't be null");
        }

        return this.indexIDDescriptorMap.get(indexId);
    }

    @Override
    public IndexBase instantiate(TxBase tx, int indexDescriptorId) {
        if (tx == null) {
            throw new IllegalArgumentException("Tx can't be null");
        }

        IndexDescriptorBase descriptor = this.indexIDDescriptorMap.get(indexDescriptorId);
        if (descriptor == null) {
            throw new IllegalArgumentException("ID does not correlate with a valid index descriptor");
        }

        // create and return the index
        return new Index(
                tx,
                this.tableMgr,
                indexDescriptorId,
                this.get(tx, indexDescriptorId),
                this.bucketCount);
    }

    @Override
    public void deleteAll(TxBase tx, String tableName) {
        if (tx == null || tableName == null) {
            throw new IllegalArgumentException("Input can't be null");
        }

        // remove the index table name id mappings under lock (via return null) [1]
        this.tableIndexIDMap.compute(tableName, (key, val) -> {
            if (val != null) {
                // delete each index
                for (Integer id : val) {
                    // remove the index descriptor mapping [2]
                    IndexDescriptorBase descriptor = this.indexIDDescriptorMap.remove(id);

                    // delete the index with all its buckets [3]
                    new Index(tx, this.tableMgr, id, descriptor, this.bucketCount).deleteAll();
                }

                // remove from quick lookup map [4]
                this.tableFieldIndexIdMap.remove(tableName);

                // delete the indexes from index metadata table
                TableScanBase tableScan = new TableScan(
                        tx,
                        INDEX_METADATA_TABLE_NAME,
                        this.tableMgr.getLayout(INDEX_METADATA_TABLE_NAME, tx));

                // cycle through the index metadata for the table to be deleted
                int deletedCount = 0;
                while (tableScan.next()) {
                    if (tableScan.getString("TableName").equals(tableName)) {
                        // perform delete on slot entry [5]
                        tableScan.delete();

                        // break if all entries for the table have been deleted
                        if (++deletedCount >= val.size()) {
                            break;
                        }
                    }
                }
                tableScan.close();
            }

            // delete the actual table (before metadata because metadata is required) [6]
            TableScanBase deleteTableScan = new TableScan(tx, tableName, this.tableMgr.getLayout(tableName, tx));
            delete(deleteTableScan);
            deleteTableScan.close();

            // delete table manager data (catalog data/layout) for the table [7]
            this.tableMgr.replace(tableName, null, tx);

            return null;
        });
    }

    /**
     * Get the table name for an index's bucket.
     * 
     * @param id
     * @param bucket
     * @return bucket table name
     */
    public static String getBucketTableName(int id, int bucket) {
        return new StringBuilder().append(id).append(">").append(bucket).toString();
    }

    /**
     * Preform a linear pruning (deletion of each record) on a table scan.
     * 
     * @param deleteScan
     * @apiNote This method does NOT close the table scan.
     */
    public static void delete(TableScanBase deleteScan) {
        while (deleteScan.next()) {
            deleteScan.delete();
        }
    }

}
