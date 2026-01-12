package edu.yu.dbimpl.index;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import edu.yu.dbimpl.buffer.BufferMgr;
import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.buffer.BufferMgrBase.EvictionPolicy;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.index.IndexMgrBase.IndexType;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.metadata.TableMgr;
import edu.yu.dbimpl.metadata.TableMgrBase;
import edu.yu.dbimpl.query.Datum;
import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.record.LayoutBase;
import edu.yu.dbimpl.record.RID;
import edu.yu.dbimpl.record.Schema;
import edu.yu.dbimpl.record.SchemaBase;
import edu.yu.dbimpl.record.TableScan;
import edu.yu.dbimpl.record.TableScanBase;
import edu.yu.dbimpl.tx.TxBase;
import edu.yu.dbimpl.tx.TxMgr;
import edu.yu.dbimpl.tx.TxMgrBase;

/**
 * Stress tests for the Index module.
 * Tests measure performance improvements from using indexes compared to table
 * scans.
 * All tests print performance metrics to System.out.
 */
public class IndexStressTest {

    private static final Logger logger = LogManager.getLogger(IndexStressTest.class);

    // Test configuration constants
    private static final int BLOCK_SIZE = 400;
    private static final int BUFFER_SIZE = 100;
    private static final int MAX_WAIT_TIME = 500;
    private static final String TEST_BASE_DIR = "testing/IndexStressTest";
    private static final String LOG_FILE = "index_stress_logfile";

    // ========================================================================
    // HELPER METHODS
    // ========================================================================

    /**
     * Helper method to create a fresh TxMgr for each test.
     */
    private TxMgrBase createTxMgr(String testName, boolean isStartup) {
        return createTxMgr(testName, isStartup, BLOCK_SIZE, BUFFER_SIZE);
    }

    /**
     * Helper method to create a fresh TxMgr with configurable block size and buffer
     * size.
     */
    private TxMgrBase createTxMgr(String testName, boolean isStartup, int blockSize, int bufferSize) {
        return createTxMgr(testName, isStartup, blockSize, bufferSize, EvictionPolicy.NAIVE);
    }

    /**
     * Helper method to create a fresh TxMgr with configurable block size, buffer
     * size, and eviction policy.
     */
    private TxMgrBase createTxMgr(String testName, boolean isStartup, int blockSize, int bufferSize,
            EvictionPolicy policy) {
        Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(isStartup));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        Path testDir = Path.of(TEST_BASE_DIR, testName);
        try {
            if (isStartup && Files.exists(testDir)) {
                deleteDirectory(testDir);
            }
        } catch (Exception e) {
            logger.warn("Could not clean up test directory: {}", testDir);
        }

        FileMgrBase fileMgr = new FileMgr(testDir.toFile(), blockSize);
        LogMgrBase logMgr = new LogMgr(fileMgr, LOG_FILE);
        BufferMgrBase bufferMgr = new BufferMgr(fileMgr, logMgr, bufferSize, MAX_WAIT_TIME, policy);
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
    private SchemaBase createTestSchema() {
        Schema schema = new Schema();
        schema.addIntField("id");
        schema.addStringField("name", 16);
        schema.addBooleanField("active");
        schema.addDoubleField("salary");
        return schema;
    }

    /**
     * Helper method to create a table and return its layout.
     */
    private LayoutBase createTestTable(TableMgrBase tableMgr, String tableName, SchemaBase schema, TxBase tx) {
        return tableMgr.createTable(tableName, schema, tx);
    }

    // ========================================================================
    // TEST 1: Large Dataset Lookup Performance
    // ========================================================================

    @Test
    @DisplayName("Large Dataset Lookup Comparison")
    void largeDatasetLookupComparison() {
        TxMgrBase txMgr = createTxMgr("largeDatasetLookup", true);
        TxBase tx = txMgr.newTx();

        TableMgrBase tableMgr = new TableMgr(tx);
        SchemaBase schema = createTestSchema();
        LayoutBase layout = createTestTable(tableMgr, "LargeDataTbl", schema, tx);

        IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
        int indexId = indexMgr.persistIndexDescriptor(tx, "LargeDataTbl", "id", IndexType.STATIC_HASH);
        IndexBase index = indexMgr.instantiate(tx, indexId);

        // Insert 7,500 records
        int recordCount = 7500;
        System.out.println("\n=== Large Dataset Lookup Comparison ===");
        System.out.println("Inserting " + recordCount + " records...");

        TableScanBase ts = new TableScan(tx, "LargeDataTbl", layout);
        List<RID> rids = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            ts.insert();
            ts.setInt("id", i);
            ts.setString("name", "name_" + i);
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0 + i);
            RID rid = ts.getRid();
            rids.add(rid);
            index.insert(new Datum(i), rid);
        }
        ts.close();

        // Generate 100 random search keys
        Random random = new Random(42); // Fixed seed for reproducibility
        List<Integer> searchKeys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            searchKeys.add(random.nextInt(recordCount));
        }

        // Measure index lookup
        long indexStart = System.nanoTime();
        int indexFound = 0;
        for (Integer key : searchKeys) {
            index.beforeFirst(new Datum(key));
            if (index.next()) {
                indexFound++;
            }
        }
        long indexEnd = System.nanoTime();
        long indexTime = (indexEnd - indexStart) / 1_000_000;

        // Close index before opening table scan (constraint)
        index.close();

        // Measure table scan
        TableScanBase scan = new TableScan(tx, "LargeDataTbl", layout);
        long scanStart = System.nanoTime();
        int scanFound = 0;
        for (Integer key : searchKeys) {
            scan.beforeFirst();
            while (scan.next()) {
                if (scan.getInt("id") == key) {
                    scanFound++;
                    break;
                }
            }
        }
        long scanEnd = System.nanoTime();
        long scanTime = (scanEnd - scanStart) / 1_000_000;
        scan.close();

        // Print metrics
        System.out.println("Index lookup time: " + indexTime + " ms");
        System.out.println("Table scan time: " + scanTime + " ms");
        double ratio = scanTime > 0 ? (double) scanTime / indexTime : 0;
        System.out.println("Performance ratio (scan/index): " + String.format("%.2f", ratio));
        System.out.println("Speedup factor: " + String.format("%.2fx", ratio));
        System.out.println("Index found: " + indexFound + " / 100");
        System.out.println("Table scan found: " + scanFound + " / 100");

        tx.commit();
    }

    // ========================================================================
    // TEST 2: Multiple Index Stress Test
    // ========================================================================

    @Test
    @DisplayName("Multiple Index Stress Test")
    void multipleIndexStressTest() {
        System.out.println("\n=== Multiple Index Stress Test ===");

        int recordCount = 2000;
        TxMgrBase txMgr = createTxMgr("multipleIndexStress", true);
        TxBase tx = txMgr.newTx();

        TableMgrBase tableMgr = new TableMgr(tx);
        SchemaBase schema = createTestSchema();
        IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

        List<Long> insertTimes = new ArrayList<>();
        List<Long> lookupTimes = new ArrayList<>();

        // Test with 0, 1, 2, 3, 4 indexes
        for (int numIndexes = 0; numIndexes <= 4; numIndexes++) {
            String tableName = "MultiIdxTbl" + numIndexes;
            LayoutBase layout = createTestTable(tableMgr, tableName, schema, tx);

            List<IndexBase> indexes = new ArrayList<>();
            List<String> fieldNames = new ArrayList<>();
            for (int i = 0; i < numIndexes; i++) {
                String fieldName = i == 0 ? "id" : (i == 1 ? "name" : (i == 2 ? "salary" : "active"));
                int indexId = indexMgr.persistIndexDescriptor(tx, tableName, fieldName, IndexType.STATIC_HASH);
                indexes.add(indexMgr.instantiate(tx, indexId));
                fieldNames.add(fieldName);
            }

            // Measure insert time
            long insertStart = System.nanoTime();
            TableScanBase ts = new TableScan(tx, tableName, layout);
            List<RID> rids = new ArrayList<>();
            for (int j = 0; j < recordCount; j++) {
                ts.insert();
                ts.setInt("id", j);
                ts.setString("name", "name_" + j);
                ts.setBoolean("active", j % 2 == 0);
                ts.setDouble("salary", 50000.0 + j);
                RID rid = ts.getRid();
                rids.add(rid);

                for (int k = 0; k < indexes.size(); k++) {
                    IndexBase idx = indexes.get(k);
                    String fldName = fieldNames.get(k);
                    DatumBase value;
                    if (fldName.equals("id")) {
                        value = new Datum(j);
                    } else if (fldName.equals("name")) {
                        value = new Datum("name_" + j);
                    } else if (fldName.equals("salary")) {
                        value = new Datum(50000.0 + j);
                    } else {
                        value = new Datum(j % 2 == 0);
                    }
                    idx.insert(value, rid);
                }
            }
            ts.close();
            long insertEnd = System.nanoTime();
            long insertTime = (insertEnd - insertStart) / 1_000_000;
            insertTimes.add(insertTime);

            // Measure lookup time (using first index if available)
            long lookupStart = System.nanoTime();
            if (numIndexes > 0) {
                IndexBase firstIndex = indexes.get(0);
                for (int j = 0; j < 100; j++) {
                    int searchKey = j * 20;
                    firstIndex.beforeFirst(new Datum(searchKey));
                    firstIndex.next();
                }
            } else {
                // Table scan for 0 indexes
                TableScanBase scan = new TableScan(tx, tableName, layout);
                for (int j = 0; j < 100; j++) {
                    int searchKey = j * 20;
                    scan.beforeFirst();
                    while (scan.next()) {
                        if (scan.getInt("id") == searchKey) {
                            break;
                        }
                    }
                }
                scan.close();
            }
            long lookupEnd = System.nanoTime();
            long lookupTime = (lookupEnd - lookupStart) / 1_000_000;
            lookupTimes.add(lookupTime);

            // Close indexes
            for (IndexBase idx : indexes) {
                idx.close();
            }
        }

        // Print metrics
        System.out.println("Insert times (ms) for 0-4 indexes:");
        for (int i = 0; i < insertTimes.size(); i++) {
            System.out.println("  " + i + " indexes: " + insertTimes.get(i) + " ms");
        }
        System.out.println("Cost ratios per index:");
        for (int i = 1; i < insertTimes.size(); i++) {
            double ratio = insertTimes.get(i) > 0 ? (double) insertTimes.get(i) / insertTimes.get(i - 1) : 0;
            System.out.println("  " + i + " indexes / " + (i - 1) + " indexes: " + String.format("%.2f", ratio));
        }
        System.out.println("Lookup times (ms) for 0-4 indexes:");
        for (int i = 0; i < lookupTimes.size(); i++) {
            System.out.println("  " + i + " indexes: " + lookupTimes.get(i) + " ms");
        }
        System.out.println("Operations per second (inserts):");
        for (int i = 0; i < insertTimes.size(); i++) {
            double opsPerSec = insertTimes.get(i) > 0 ? (recordCount * 1000.0) / insertTimes.get(i) : 0;
            System.out.println("  " + i + " indexes: " + String.format("%.2f", opsPerSec) + " ops/sec");

        }

        tx.commit();
    }

    // ========================================================================
    // TEST 3: Mixed Workload Stress Test - Read/Write Ratio Impact
    // ========================================================================

    @Test
    @DisplayName("Mixed Workload Stress Test - Read/Write Ratio Impact")
    void mixedWorkloadStressTest() {
        System.out.println("\n=== Mixed Workload Stress Test - Read/Write Ratio Impact ===");
        System.out.println("Testing: Greater read/write percentage favors indices");

        int initialRecords = 3000;
        int totalOperations = 1000;
        int[] readPercentages = { 10, 30, 50, 70, 90 };
        List<Double> improvements = new ArrayList<>();

        for (int readPct : readPercentages) {
            int readOps = (int) (totalOperations * readPct / 100.0);
            int writeOps = totalOperations - readOps;

            TxMgrBase txMgr = createTxMgr("mixedWorkload_" + readPct, true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createTestSchema();
            LayoutBase layout = createTestTable(tableMgr, "MixedWorkTbl", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "MixedWorkTbl", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert initial records
            TableScanBase ts = new TableScan(tx, "MixedWorkTbl", layout);
            List<RID> rids = new ArrayList<>();
            for (int i = 0; i < initialRecords; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("name", "name_" + i);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0 + i);
                RID rid = ts.getRid();
                rids.add(rid);
                index.insert(new Datum(i), rid);
            }
            ts.close();

            // Measure workload with index
            long startWithIndex = System.nanoTime();
            Random random = new Random(42);
            int nextId = initialRecords;

            // Reads
            for (int i = 0; i < readOps; i++) {
                int searchKey = random.nextInt(initialRecords);
                index.beforeFirst(new Datum(searchKey));
                index.next();
            }

            // Writes (inserts)
            for (int i = 0; i < writeOps; i++) {
                ts = new TableScan(tx, "MixedWorkTbl", layout);
                ts.insert();
                ts.setInt("id", nextId);
                ts.setString("name", "name_" + nextId);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0 + nextId);
                RID rid = ts.getRid();
                rids.add(rid);
                ts.close();
                index.insert(new Datum(nextId), rid);
                nextId++;
            }

            long endWithIndex = System.nanoTime();
            long timeWithIndex = (endWithIndex - startWithIndex) / 1_000_000;
            index.close();

            // Measure workload without index (table scans for reads)
            tx.commit();
            tx = txMgr.newTx();
            tableMgr = new TableMgr(tx);
            layout = tableMgr.getLayout("MixedWorkTbl", tx);
            if (layout == null) {
                // Table should exist, but if not, skip this iteration
                System.out.println("\nRead percentage: " + readPct + "%");
                System.out.println("  Warning: Could not retrieve table layout after commit");
                improvements.add(0.0);
                tx.commit();
                continue;
            }

            long startWithoutIndex = System.nanoTime();
            random = new Random(42);

            // Reads using table scan
            for (int i = 0; i < readOps; i++) {
                int searchKey = random.nextInt(initialRecords);
                TableScanBase scan = new TableScan(tx, "MixedWorkTbl", layout);
                scan.beforeFirst();
                while (scan.next()) {
                    if (scan.getInt("id") == searchKey) {
                        break;
                    }
                }
                scan.close();
            }
            long endWithoutIndex = System.nanoTime();
            long timeWithoutIndex = (endWithoutIndex - startWithoutIndex) / 1_000_000;

            double improvement = timeWithoutIndex > 0
                    ? ((double) (timeWithoutIndex - timeWithIndex) / timeWithoutIndex) * 100
                    : 0;
            improvements.add(improvement);

            System.out.println("\nRead percentage: " + readPct + "%");
            System.out.println("  Time with index: " + timeWithIndex + " ms");
            System.out.println("  Time without index (reads): " + timeWithoutIndex + " ms");
            System.out.println("  Performance improvement: " + String.format("%.2f", improvement) + "%");

            tx.commit();
        }

        // Print summary showing trend
        System.out.println("\nSummary - Performance improvement by read percentage:");
        for (int i = 0; i < readPercentages.length; i++) {
            System.out.println("  " + readPercentages[i] + "% reads: " + String.format("%.2f", improvements.get(i))
                    + "% improvement");
        }
        System.out.println("Conclusion: Higher read percentage shows greater advantage for indices");
    }

    // ========================================================================
    // TEST 4: Sequential vs Random Access Patterns
    // ========================================================================

    @Test
    @DisplayName("Sequential vs Random Access Patterns")
    void accessPatternComparison() {
        System.out.println("\n=== Sequential vs Random Access Patterns ===");

        int recordCount = 2000;
        int numLookups = 200;
        TxMgrBase txMgr = createTxMgr("accessPattern", true);
        TxBase tx = txMgr.newTx();

        TableMgrBase tableMgr = new TableMgr(tx);
        SchemaBase schema = createTestSchema();
        LayoutBase layout = createTestTable(tableMgr, "AccessPatTbl", schema, tx);

        IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
        int indexId = indexMgr.persistIndexDescriptor(tx, "AccessPatTbl", "id", IndexType.STATIC_HASH);
        IndexBase index = indexMgr.instantiate(tx, indexId);

        // Insert records
        TableScanBase ts = new TableScan(tx, "AccessPatTbl", layout);
        List<RID> rids = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            ts.insert();
            ts.setInt("id", i);
            ts.setString("name", "name_" + i);
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0 + i);
            RID rid = ts.getRid();
            rids.add(rid);
            index.insert(new Datum(i), rid);
        }
        ts.close();

        // Sequential access with index
        long seqIndexStart = System.nanoTime();
        for (int i = 0; i < numLookups; i++) {
            int key = i * 10;
            index.beforeFirst(new Datum(key));
            index.next();
        }
        long seqIndexEnd = System.nanoTime();
        long seqIndexTime = (seqIndexEnd - seqIndexStart) / 1_000_000;
        index.close(); // Close index before using table scan

        // Sequential access with table scan
        long seqScanStart = System.nanoTime();
        TableScanBase scan = new TableScan(tx, "AccessPatTbl", layout);
        for (int i = 0; i < numLookups; i++) {
            int key = i * 10;
            scan.beforeFirst();
            while (scan.next()) {
                if (scan.getInt("id") == key) {
                    break;
                }
            }
        }
        scan.close();
        long seqScanEnd = System.nanoTime();
        long seqScanTime = (seqScanEnd - seqScanStart) / 1_000_000;

        // Random access with index - need to recreate index
        index = indexMgr.instantiate(tx, indexId);
        Random random = new Random(42);
        List<Integer> randomKeys = new ArrayList<>();
        for (int i = 0; i < numLookups; i++) {
            randomKeys.add(random.nextInt(recordCount));
        }
        long randIndexStart = System.nanoTime();
        for (Integer key : randomKeys) {
            index.beforeFirst(new Datum(key));
            index.next();
        }
        long randIndexEnd = System.nanoTime();
        long randIndexTime = (randIndexEnd - randIndexStart) / 1_000_000;
        index.close(); // Close index before using table scan

        // Random access with table scan
        long randScanStart = System.nanoTime();
        scan = new TableScan(tx, "AccessPatTbl", layout);
        for (Integer key : randomKeys) {
            scan.beforeFirst();
            while (scan.next()) {
                if (scan.getInt("id") == key) {
                    break;
                }
            }
        }
        scan.close();
        long randScanEnd = System.nanoTime();
        long randScanTime = (randScanEnd - randScanStart) / 1_000_000;

        // Print metrics
        System.out.println("Sequential access:");
        System.out.println("  Index time: " + seqIndexTime + " ms");
        System.out.println("  Table scan time: " + seqScanTime + " ms");
        double seqRatio = seqScanTime > 0 ? (double) seqScanTime / seqIndexTime : 0;
        System.out.println("  Ratio (scan/index): " + String.format("%.2f", seqRatio));
        System.out.println("Random access:");
        System.out.println("  Index time: " + randIndexTime + " ms");
        System.out.println("  Table scan time: " + randScanTime + " ms");
        double randRatio = randScanTime > 0 ? (double) randScanTime / randIndexTime : 0;
        System.out.println("  Ratio (scan/index): " + String.format("%.2f", randRatio));
        System.out.println("Comparison:");
        System.out.println("  Sequential improvement: " + String.format("%.2fx", seqRatio));
        System.out.println("  Random improvement: " + String.format("%.2fx", randRatio));

        tx.commit();
    }

    // ========================================================================
    // TEST 5: Hash Collision Stress Test
    // ========================================================================

    @Test
    @DisplayName("Hash Collision Stress Test")
    void hashCollisionStressTest() {
        System.out.println("\n=== Hash Collision Stress Test ===");

        // Create values that might cause hash collisions
        // Using strings with same hash codes
        int recordCount = 1000;
        TxMgrBase txMgr = createTxMgr("hashCollision", true);
        TxBase tx = txMgr.newTx();

        TableMgrBase tableMgr = new TableMgr(tx);
        Schema schema = new Schema();
        schema.addIntField("id");
        schema.addStringField("name", 16);
        LayoutBase layout = createTestTable(tableMgr, "HashCollTbl", schema, tx);

        IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
        int indexId = indexMgr.persistIndexDescriptor(tx, "HashCollTbl", "name", IndexType.STATIC_HASH);
        IndexBase index = indexMgr.instantiate(tx, indexId);

        // Insert records with potential collisions
        TableScanBase ts = new TableScan(tx, "HashCollTbl", layout);
        List<RID> rids = new ArrayList<>();
        int collisionCount = 0;
        for (int i = 0; i < recordCount; i++) {
            ts.insert();
            ts.setInt("id", i);
            String name = "name_" + i;
            ts.setString("name", name);
            RID rid = ts.getRid();
            rids.add(rid);
            index.insert(new Datum(name), rid);

            // Check for hash collisions (simplified check)
            if (i > 0 && name.hashCode() == ("name_" + (i - 1)).hashCode()) {
                collisionCount++;
            }
        }
        ts.close();

        // Measure lookup time
        long lookupStart = System.nanoTime();
        int found = 0;
        for (int i = 0; i < 100; i++) {
            String searchName = "name_" + (i * 10);
            index.beforeFirst(new Datum(searchName));
            if (index.next()) {
                found++;
            }
        }
        long lookupEnd = System.nanoTime();
        long lookupTime = (lookupEnd - lookupStart) / 1_000_000;

        index.close();

        // Print metrics
        System.out.println("Records inserted: " + recordCount);
        System.out.println("Potential collisions detected: " + collisionCount);
        System.out.println("Lookup time (with potential collisions): " + lookupTime + " ms");
        System.out.println("Lookups performed: 100");
        System.out.println("Records found: " + found + " / 100");
        System.out.println("Average lookup time: " + String.format("%.2f", lookupTime / 100.0) + " ms per lookup");

        tx.commit();
    }

    // ========================================================================
    // TEST 6: Varying Record Size Impact
    // ========================================================================

    @Test
    @DisplayName("Varying Record Size Impact")
    void recordSizeImpactTest() {
        System.out.println("\n=== Varying Record Size Impact ===");

        int recordCount = 1000;
        TxMgrBase txMgr = createTxMgr("recordSize", true);
        TxBase tx = txMgr.newTx();

        TableMgrBase tableMgr = new TableMgr(tx);
        IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

        // Small records
        Schema smallSchema = new Schema();
        smallSchema.addIntField("id");
        smallSchema.addStringField("name", 4); // Small string
        LayoutBase smallLayout = createTestTable(tableMgr, "SmallRecTbl", smallSchema, tx);
        int smallIndexId = indexMgr.persistIndexDescriptor(tx, "SmallRecTbl", "id", IndexType.STATIC_HASH);
        IndexBase smallIndex = indexMgr.instantiate(tx, smallIndexId);

        TableScanBase ts = new TableScan(tx, "SmallRecTbl", smallLayout);
        List<RID> smallRids = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            ts.insert();
            ts.setInt("id", i);
            ts.setString("name", "n" + i);
            RID rid = ts.getRid();
            smallRids.add(rid);
            smallIndex.insert(new Datum(i), rid);
        }
        ts.close();

        long smallIndexStart = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            smallIndex.beforeFirst(new Datum(i * 10));
            smallIndex.next();
        }
        long smallIndexEnd = System.nanoTime();
        long smallIndexTime = (smallIndexEnd - smallIndexStart) / 1_000_000;

        TableScanBase scan = new TableScan(tx, "SmallRecTbl", smallLayout);
        long smallScanStart = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            int key = i * 10;
            scan.beforeFirst();
            while (scan.next()) {
                if (scan.getInt("id") == key) {
                    break;
                }
            }
        }
        scan.close();
        long smallScanEnd = System.nanoTime();
        long smallScanTime = (smallScanEnd - smallScanStart) / 1_000_000;
        smallIndex.close();

        // Large records
        Schema largeSchema = new Schema();
        largeSchema.addIntField("id");
        largeSchema.addStringField("name", 16); // Larger string
        largeSchema.addStringField("desc", 16); // Additional field
        largeSchema.addDoubleField("value1");
        largeSchema.addDoubleField("value2");
        largeSchema.addDoubleField("value3");
        LayoutBase largeLayout = createTestTable(tableMgr, "LargeRecTbl", largeSchema, tx);
        int largeIndexId = indexMgr.persistIndexDescriptor(tx, "LargeRecTbl", "id", IndexType.STATIC_HASH);
        IndexBase largeIndex = indexMgr.instantiate(tx, largeIndexId);

        ts = new TableScan(tx, "LargeRecTbl", largeLayout);
        List<RID> largeRids = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            ts.insert();
            ts.setInt("id", i);
            ts.setString("name", "name_" + i);
            ts.setString("desc", "desc_" + i);
            ts.setDouble("value1", i * 1.0);
            ts.setDouble("value2", i * 2.0);
            ts.setDouble("value3", i * 3.0);
            RID rid = ts.getRid();
            largeRids.add(rid);
            largeIndex.insert(new Datum(i), rid);
        }
        ts.close();

        long largeIndexStart = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            largeIndex.beforeFirst(new Datum(i * 10));
            largeIndex.next();
        }
        long largeIndexEnd = System.nanoTime();
        long largeIndexTime = (largeIndexEnd - largeIndexStart) / 1_000_000;

        scan = new TableScan(tx, "LargeRecTbl", largeLayout);
        long largeScanStart = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            int key = i * 10;
            scan.beforeFirst();
            while (scan.next()) {
                if (scan.getInt("id") == key) {
                    break;
                }
            }
        }
        scan.close();
        long largeScanEnd = System.nanoTime();
        long largeScanTime = (largeScanEnd - largeScanStart) / 1_000_000;
        largeIndex.close();

        // Print metrics
        System.out.println("Small record size:");
        System.out.println("  Index time: " + smallIndexTime + " ms");
        System.out.println("  Table scan time: " + smallScanTime + " ms");
        double smallRatio = smallScanTime > 0 ? (double) smallScanTime / smallIndexTime : 0;
        System.out.println("  Ratio (scan/index): " + String.format("%.2f", smallRatio));
        System.out.println("Large record size:");
        System.out.println("  Index time: " + largeIndexTime + " ms");
        System.out.println("  Table scan time: " + largeScanTime + " ms");
        double largeRatio = largeScanTime > 0 ? (double) largeScanTime / largeIndexTime : 0;
        System.out.println("  Ratio (scan/index): " + String.format("%.2f", largeRatio));
        double improvementDiff = largeRatio - smallRatio;
        System.out.println("Improvement difference (large - small): " + String.format("%.2f", improvementDiff));

        tx.commit();
    }

    // ========================================================================
    // TEST 7: High-Volume Insert Stress Test
    // ========================================================================

    @Test
    @DisplayName("High-Volume Insert Stress Test")
    void highVolumeInsertStressTest() {
        System.out.println("\n=== High-Volume Insert Stress Test ===");

        int recordCount = 5000;
        TxMgrBase txMgr = createTxMgr("highVolumeInsert", true);
        TxBase tx = txMgr.newTx();

        TableMgrBase tableMgr = new TableMgr(tx);
        SchemaBase schema = createTestSchema();
        IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);

        List<Long> insertTimes = new ArrayList<>();
        List<Double> recordsPerSec = new ArrayList<>();

        // Test with 0, 1, 2, 3 indexes
        for (int numIndexes = 0; numIndexes <= 3; numIndexes++) {
            String tableName = "HighVolTbl" + numIndexes;
            LayoutBase layout = createTestTable(tableMgr, tableName, schema, tx);

            List<IndexBase> indexes = new ArrayList<>();
            List<String> fieldNames = new ArrayList<>();
            for (int i = 0; i < numIndexes; i++) {
                String fieldName = i == 0 ? "id" : (i == 1 ? "name" : "salary");
                int indexId = indexMgr.persistIndexDescriptor(tx, tableName, fieldName, IndexType.STATIC_HASH);
                indexes.add(indexMgr.instantiate(tx, indexId));
                fieldNames.add(fieldName);
            }

            long insertStart = System.nanoTime();
            TableScanBase ts = new TableScan(tx, tableName, layout);
            List<RID> rids = new ArrayList<>();
            for (int j = 0; j < recordCount; j++) {
                ts.insert();
                ts.setInt("id", j);
                ts.setString("name", "name_" + j);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0 + j);
                RID rid = ts.getRid();
                rids.add(rid);

                for (int k = 0; k < indexes.size(); k++) {
                    IndexBase idx = indexes.get(k);
                    String fldName = fieldNames.get(k);
                    DatumBase value;
                    if (fldName.equals("id")) {
                        value = new Datum(j);
                    } else if (fldName.equals("name")) {
                        value = new Datum("name_" + j);
                    } else {
                        value = new Datum(50000.0 + j);
                    }
                    idx.insert(value, rid);
                }
            }
            ts.close();
            long insertEnd = System.nanoTime();
            long insertTime = (insertEnd - insertStart) / 1_000_000;
            insertTimes.add(insertTime);

            double recsPerSec = insertTime > 0 ? (recordCount * 1000.0) / insertTime : 0;
            recordsPerSec.add(recsPerSec);

            // Close indexes
            for (IndexBase idx : indexes) {
                idx.close();
            }
        }

        // Print metrics
        System.out.println("Insert time (ms) for each index count:");
        for (int i = 0; i < insertTimes.size(); i++) {
            System.out.println("  " + i + " indexes: " + insertTimes.get(i) + " ms");
        }
        System.out.println("Records per second for each configuration:");
        for (int i = 0; i < recordsPerSec.size(); i++) {
            System.out.println("  " + i + " indexes: " + String.format("%.2f", recordsPerSec.get(i)) + " recs/sec");
        }
        System.out.println("Cost per index (time ratio):");
        for (int i = 1; i < insertTimes.size(); i++) {
            double ratio = insertTimes.get(i) > 0 ? (double) insertTimes.get(i) / insertTimes.get(i - 1) : 0;
            System.out.println("  " + i + " indexes / " + (i - 1) + " indexes: " + String.format("%.2f", ratio));
        }
        System.out.println("Total overhead time per index:");
        for (int i = 1; i < insertTimes.size(); i++) {
            long overhead = insertTimes.get(i) - insertTimes.get(0);
            System.out.println("  " + i + " indexes overhead: " + overhead + " ms");

        }

        tx.commit();
    }

    // ========================================================================
    // TEST 8: Index Maintenance Overhead Test
    // ========================================================================

    @Test
    @DisplayName("Index Maintenance Overhead Test")
    void indexMaintenanceOverheadTest() {
        System.out.println("\n=== Index Maintenance Overhead Test ===");

        int recordCount = 1000;
        TxMgrBase txMgr = createTxMgr("maintenanceOverhead", true);
        TxBase tx = txMgr.newTx();

        TableMgrBase tableMgr = new TableMgr(tx);
        SchemaBase schema = createTestSchema();
        LayoutBase layout = createTestTable(tableMgr, "MaintOverTbl", schema, tx);

        IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
        int indexId = indexMgr.persistIndexDescriptor(tx, "MaintOverTbl", "id", IndexType.STATIC_HASH);
        IndexBase index = indexMgr.instantiate(tx, indexId);

        // Insert with index
        long insertWithIndexStart = System.nanoTime();
        TableScanBase ts = new TableScan(tx, "MaintOverTbl", layout);
        List<RID> rids = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            ts.insert();
            ts.setInt("id", i);
            ts.setString("name", "name_" + i);
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0 + i);
            RID rid = ts.getRid();
            rids.add(rid);
            index.insert(new Datum(i), rid);
        }
        ts.close();
        long insertWithIndexEnd = System.nanoTime();
        long insertWithIndexTime = (insertWithIndexEnd - insertWithIndexStart) / 1_000_000;
        index.close();

        // Insert without index (new table) - use same transaction
        LayoutBase layout2 = createTestTable(tableMgr, "MaintOverTbl2", schema, tx);

        long insertWithoutIndexStart = System.nanoTime();
        ts = new TableScan(tx, "MaintOverTbl2", layout2);
        for (int i = 0; i < recordCount; i++) {
            ts.insert();
            ts.setInt("id", i);
            ts.setString("name", "name_" + i);
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0 + i);
        }
        ts.close();
        long insertWithoutIndexEnd = System.nanoTime();
        long insertWithoutIndexTime = (insertWithoutIndexEnd - insertWithoutIndexStart) / 1_000_000;

        // Delete with index - recreate index in same transaction
        index = indexMgr.instantiate(tx, indexId);

        long deleteWithIndexStart = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            index.delete(new Datum(i), rids.get(i));
        }
        long deleteWithIndexEnd = System.nanoTime();
        long deleteWithIndexTime = (deleteWithIndexEnd - deleteWithIndexStart) / 1_000_000;
        index.close();

        // Delete without index (table scan)
        ts = new TableScan(tx, "MaintOverTbl2", layout2);
        long deleteWithoutIndexStart = System.nanoTime();
        ts.beforeFirst();
        int deleted = 0;
        while (ts.next() && deleted < 100) {
            if (ts.getInt("id") < 100) {
                ts.delete();
                deleted++;
            }
        }
        ts.close();
        long deleteWithoutIndexEnd = System.nanoTime();
        long deleteWithoutIndexTime = (deleteWithoutIndexEnd - deleteWithoutIndexStart) / 1_000_000;

        // Update simulation (delete + insert) with index
        index = indexMgr.instantiate(tx, indexId);
        long updateWithIndexStart = System.nanoTime();
        for (int i = 100; i < 200; i++) {
            index.delete(new Datum(i), rids.get(i));
            ts = new TableScan(tx, "MaintOverTbl", layout);
            ts.insert();
            ts.setInt("id", i);
            ts.setString("name", "name_" + i + "_upd");
            ts.setBoolean("active", false);
            ts.setDouble("salary", 60000.0 + i);
            RID newRid = ts.getRid();
            ts.close();
            index.insert(new Datum(i), newRid);
        }
        long updateWithIndexEnd = System.nanoTime();
        long updateWithIndexTime = (updateWithIndexEnd - updateWithIndexStart) / 1_000_000;
        index.close();

        // Update simulation without index
        ts = new TableScan(tx, "MaintOverTbl2", layout2);
        long updateWithoutIndexStart = System.nanoTime();
        ts.beforeFirst();
        int updated = 0;
        while (ts.next() && updated < 100) {
            if (ts.getInt("id") >= 100 && ts.getInt("id") < 200) {
                ts.delete();
                updated++;
            }
        }
        ts.beforeFirst();
        for (int i = 100; i < 200; i++) {
            ts.insert();
            ts.setInt("id", i);
            ts.setString("name", "name_" + i + "_upd");
            ts.setBoolean("active", false);
            ts.setDouble("salary", 60000.0 + i);
        }
        ts.close();
        long updateWithoutIndexEnd = System.nanoTime();
        long updateWithoutIndexTime = (updateWithoutIndexEnd - updateWithoutIndexStart) / 1_000_000;

        // Print metrics
        System.out.println("Insert operation:");
        System.out.println("  With index: " + insertWithIndexTime + " ms");
        System.out.println("  Without index: " + insertWithoutIndexTime + " ms");
        long insertOverhead = insertWithIndexTime - insertWithoutIndexTime;
        System.out.println("  Overhead: " + insertOverhead + " ms");
        double insertOverheadPct = insertWithoutIndexTime > 0 ? ((double) insertOverhead / insertWithoutIndexTime) * 100
                : 0;
        System.out.println("  Overhead percentage: " + String.format("%.2f", insertOverheadPct) + "%");
        System.out.println("Delete operation:");
        System.out.println("  With index: " + deleteWithIndexTime + " ms");
        System.out.println("  Without index: " + deleteWithoutIndexTime + " ms");
        long deleteOverhead = deleteWithIndexTime - deleteWithoutIndexTime;
        System.out.println("  Overhead: " + deleteOverhead + " ms");
        double deleteOverheadPct = deleteWithoutIndexTime > 0 ? ((double) deleteOverhead / deleteWithoutIndexTime) * 100
                : 0;
        System.out.println("  Overhead percentage: " + String.format("%.2f", deleteOverheadPct) + "%");
        System.out.println("Update operation (delete + insert):");
        System.out.println("  With index: " + updateWithIndexTime + " ms");
        System.out.println("  Without index: " + updateWithoutIndexTime + " ms");
        long updateOverhead = updateWithIndexTime - updateWithoutIndexTime;
        System.out.println("  Overhead: " + updateOverhead + " ms");
        double updateOverheadPct = updateWithoutIndexTime > 0 ? ((double) updateOverhead / updateWithoutIndexTime) * 100
                : 0;
        System.out.println("  Overhead percentage: " + String.format("%.2f", updateOverheadPct) + "%");

        tx.commit();
    }

    // ========================================================================
    // TEST 9: Block Size Impact Test
    // ========================================================================

    @Test
    @DisplayName("Block Size Impact Test")
    void blockSizeImpactTest() {
        System.out.println("\n=== Block Size Impact Test ===");
        System.out.println("Testing: When records spread over more blocks, indices are more beneficial");

        int recordCount = 2000; // More records to show block spreading effect
        int[] blockSizes = { 200, 400, 800 };
        List<Long> indexLookupTimes = new ArrayList<>();
        List<Long> scanLookupTimes = new ArrayList<>();
        List<Double> speedupRatios = new ArrayList<>();

        for (int blockSize : blockSizes) {
            String testName = "blockSize_" + blockSize;
            TxMgrBase txMgr = createTxMgr(testName, true, blockSize, BUFFER_SIZE);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createTestSchema();
            LayoutBase layout = createTestTable(tableMgr, "BlockSizeTbl", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "BlockSizeTbl", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert records
            TableScanBase ts = new TableScan(tx, "BlockSizeTbl", layout);
            List<RID> rids = new ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("name", "name_" + i);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0 + i);
                RID rid = ts.getRid();
                rids.add(rid);
                index.insert(new Datum(i), rid);
            }
            ts.close();

            // Measure index lookup time
            Random random = new Random(42);
            List<Integer> searchKeys = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                searchKeys.add(random.nextInt(recordCount));
            }

            long indexStart = System.nanoTime();
            for (Integer key : searchKeys) {
                index.beforeFirst(new Datum(key));
                index.next();
            }
            long indexEnd = System.nanoTime();
            long indexTime = (indexEnd - indexStart) / 1_000_000;
            indexLookupTimes.add(indexTime);
            index.close();

            // Measure table scan lookup time
            TableScanBase scan = new TableScan(tx, "BlockSizeTbl", layout);
            long scanStart = System.nanoTime();
            for (Integer key : searchKeys) {
                scan.beforeFirst();
                while (scan.next()) {
                    if (scan.getInt("id") == key) {
                        break;
                    }
                }
            }
            scan.close();
            long scanEnd = System.nanoTime();
            long scanTime = (scanEnd - scanStart) / 1_000_000;
            scanLookupTimes.add(scanTime);

            double speedup = indexTime > 0 ? (double) scanTime / indexTime : 0;
            speedupRatios.add(speedup);

            System.out.println("\nBlock size: " + blockSize + " bytes");
            System.out.println("  Index lookup time: " + indexTime + " ms");
            System.out.println("  Table scan time: " + scanTime + " ms");
            System.out.println("  Speedup ratio: " + String.format("%.2fx", speedup));

            tx.commit();
        }

        // Print summary
        System.out.println("\nSummary - Speedup ratio by block size:");
        for (int i = 0; i < blockSizes.length; i++) {
            System.out.println(
                    "  " + blockSizes[i] + " bytes: " + String.format("%.2fx", speedupRatios.get(i)) + " speedup");
        }
        System.out.println("Conclusion: Smaller blocks (more blocks) show greater index advantage");
    }

    // ========================================================================
    // TEST 10: Number of Records Impact Test
    // ========================================================================

    @Test
    @DisplayName("Number of Records Impact Test")
    void numberOfRecordsImpactTest() {
        System.out.println("\n=== Number of Records Impact Test ===");
        System.out.println("Testing: More records (more blocks) increases index advantage");

        int[] recordCounts = { 500, 1000, 2000, 5000 };
        List<Long> indexTimes = new ArrayList<>();
        List<Long> scanTimes = new ArrayList<>();
        List<Double> speedupRatios = new ArrayList<>();

        for (int recordCount : recordCounts) {
            String testName = "numRecords_" + recordCount;
            TxMgrBase txMgr = createTxMgr(testName, true);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createTestSchema();
            LayoutBase layout = createTestTable(tableMgr, "NumRecTbl", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "NumRecTbl", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert records
            TableScanBase ts = new TableScan(tx, "NumRecTbl", layout);
            List<RID> rids = new ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("name", "name_" + i);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0 + i);
                RID rid = ts.getRid();
                rids.add(rid);
                index.insert(new Datum(i), rid);
            }
            ts.close();

            // Generate random search keys
            Random random = new Random(42);
            int numLookups = Math.min(100, recordCount / 10);
            List<Integer> searchKeys = new ArrayList<>();
            for (int i = 0; i < numLookups; i++) {
                searchKeys.add(random.nextInt(recordCount));
            }

            // Measure index lookup
            long indexStart = System.nanoTime();
            for (Integer key : searchKeys) {
                index.beforeFirst(new Datum(key));
                index.next();
            }
            long indexEnd = System.nanoTime();
            long indexTime = (indexEnd - indexStart) / 1_000_000;
            indexTimes.add(indexTime);
            index.close();

            // Measure table scan
            TableScanBase scan = new TableScan(tx, "NumRecTbl", layout);
            long scanStart = System.nanoTime();
            for (Integer key : searchKeys) {
                scan.beforeFirst();
                while (scan.next()) {
                    if (scan.getInt("id") == key) {
                        break;
                    }
                }
            }
            scan.close();
            long scanEnd = System.nanoTime();
            long scanTime = (scanEnd - scanStart) / 1_000_000;
            scanTimes.add(scanTime);

            double speedup = indexTime > 0 ? (double) scanTime / indexTime : 0;
            speedupRatios.add(speedup);

            System.out.println("\nRecord count: " + recordCount);
            System.out.println("  Index lookup time: " + indexTime + " ms");
            System.out.println("  Table scan time: " + scanTime + " ms");
            System.out.println("  Speedup ratio: " + String.format("%.2fx", speedup));

            tx.commit();
        }

        // Print summary
        System.out.println("\nSummary - Speedup ratio by number of records:");
        for (int i = 0; i < recordCounts.length; i++) {
            System.out.println(
                    "  " + recordCounts[i] + " records: " + String.format("%.2fx", speedupRatios.get(i)) + " speedup");
        }
        System.out.println("Conclusion: More records (more blocks) show greater index advantage");
    }

    // ========================================================================
    // TEST 11: Eviction Policy Impact Test
    // ========================================================================

    @Test
    @DisplayName("Eviction Policy Impact Test")
    void evictionPolicyImpactTest() {
        System.out.println("\n=== Eviction Policy Impact Test ===");
        System.out.println("Testing: CLOCK policy favors index blocks being cached in memory");

        int recordCount = 2000;
        int numLookups = 200;
        EvictionPolicy[] policies = { EvictionPolicy.NAIVE, EvictionPolicy.CLOCK };
        List<Long> indexTimes = new ArrayList<>();
        List<Long> scanTimes = new ArrayList<>();

        for (EvictionPolicy policy : policies) {
            String testName = "evictionPolicy_" + policy.name();
            TxMgrBase txMgr = createTxMgr(testName, true, BLOCK_SIZE, BUFFER_SIZE, policy);
            TxBase tx = txMgr.newTx();

            TableMgrBase tableMgr = new TableMgr(tx);
            SchemaBase schema = createTestSchema();
            LayoutBase layout = createTestTable(tableMgr, "EvictPolTbl", schema, tx);

            IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
            int indexId = indexMgr.persistIndexDescriptor(tx, "EvictPolTbl", "id", IndexType.STATIC_HASH);
            IndexBase index = indexMgr.instantiate(tx, indexId);

            // Insert records
            TableScanBase ts = new TableScan(tx, "EvictPolTbl", layout);
            List<RID> rids = new ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("name", "name_" + i);
                ts.setBoolean("active", true);
                ts.setDouble("salary", 50000.0 + i);
                RID rid = ts.getRid();
                rids.add(rid);
                index.insert(new Datum(i), rid);
            }
            ts.close();

            // Generate random search keys
            Random random = new Random(42);
            List<Integer> searchKeys = new ArrayList<>();
            for (int i = 0; i < numLookups; i++) {
                searchKeys.add(random.nextInt(recordCount));
            }

            // Measure index lookup (multiple passes to show caching effect)
            long indexStart = System.nanoTime();
            for (int pass = 0; pass < 3; pass++) {
                for (Integer key : searchKeys) {
                    index.beforeFirst(new Datum(key));
                    index.next();
                }
            }
            long indexEnd = System.nanoTime();
            long indexTime = (indexEnd - indexStart) / 1_000_000;
            indexTimes.add(indexTime);
            index.close();

            // Measure table scan
            TableScanBase scan = new TableScan(tx, "EvictPolTbl", layout);
            long scanStart = System.nanoTime();
            for (int pass = 0; pass < 3; pass++) {
                for (Integer key : searchKeys) {
                    scan.beforeFirst();
                    while (scan.next()) {
                        if (scan.getInt("id") == key) {
                            break;
                        }
                    }
                }
            }
            scan.close();
            long scanEnd = System.nanoTime();
            long scanTime = (scanEnd - scanStart) / 1_000_000;
            scanTimes.add(scanTime);

            System.out.println("\nEviction policy: " + policy);
            System.out.println("  Index lookup time (3 passes): " + indexTime + " ms");
            System.out.println("  Table scan time (3 passes): " + scanTime + " ms");

            tx.commit();
        }

        // Print summary
        System.out.println("\nSummary - Index performance by eviction policy:");
        System.out.println("  NAIVE: " + indexTimes.get(0) + " ms");
        System.out.println("  CLOCK: " + indexTimes.get(1) + " ms");
        if (indexTimes.get(0) > 0) {
            double improvement = ((double) (indexTimes.get(0) - indexTimes.get(1)) / indexTimes.get(0)) * 100;
            System.out.println("  CLOCK improvement: " + String.format("%.2f", improvement) + "%");
        }
        System.out.println("Conclusion: CLOCK policy favors index blocks being cached");
    }

    // ========================================================================
    // TEST 12: Extreme Scale Test
    // ========================================================================

    @Test
    @DisplayName("Extreme Scale Test")
    void extremeScaleTest() {
        System.out.println("\n=== Extreme Scale Test ===");

        int recordCount = 10000;
        int numLookups = 1000;
        TxMgrBase txMgr = createTxMgr("extremeScale", true);
        TxBase tx = txMgr.newTx();

        TableMgrBase tableMgr = new TableMgr(tx);
        SchemaBase schema = createTestSchema();
        LayoutBase layout = createTestTable(tableMgr, "ExtremeSclTbl", schema, tx);

        IndexMgrBase indexMgr = new IndexMgr(tx, tableMgr);
        int indexId = indexMgr.persistIndexDescriptor(tx, "ExtremeSclTbl", "id", IndexType.STATIC_HASH);
        IndexBase index = indexMgr.instantiate(tx, indexId);

        // Insert records
        System.out.println("Inserting " + recordCount + " records...");
        TableScanBase ts = new TableScan(tx, "ExtremeSclTbl", layout);
        List<RID> rids = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            ts.insert();
            ts.setInt("id", i);
            ts.setString("name", "name_" + i);
            ts.setBoolean("active", true);
            ts.setDouble("salary", 50000.0 + i);
            RID rid = ts.getRid();
            rids.add(rid);
            index.insert(new Datum(i), rid);
        }
        ts.close();

        // Generate random search keys
        Random random = new Random(42);
        List<Integer> searchKeys = new ArrayList<>();
        for (int i = 0; i < numLookups; i++) {
            searchKeys.add(random.nextInt(recordCount));
        }

        // Measure index lookup
        long indexStart = System.nanoTime();
        int indexFound = 0;
        for (Integer key : searchKeys) {
            index.beforeFirst(new Datum(key));
            if (index.next()) {
                indexFound++;
            }
        }
        long indexEnd = System.nanoTime();
        long indexTime = (indexEnd - indexStart) / 1_000_000;
        index.close();

        // Measure table scan
        TableScanBase scan = new TableScan(tx, "ExtremeSclTbl", layout);
        long scanStart = System.nanoTime();
        int scanFound = 0;
        for (Integer key : searchKeys) {
            scan.beforeFirst();
            while (scan.next()) {
                if (scan.getInt("id") == key) {
                    scanFound++;
                    break;
                }
            }
        }
        long scanEnd = System.nanoTime();
        long scanTime = (scanEnd - scanStart) / 1_000_000;
        scan.close();

        // Print metrics
        System.out.println("Dataset size: " + recordCount + " records");
        System.out.println("Index lookup time: " + indexTime + " ms");
        System.out.println("Table scan time: " + scanTime + " ms");
        double ratio = scanTime > 0 ? (double) scanTime / indexTime : 0;
        System.out.println("Performance ratio (scan/index): " + String.format("%.2f", ratio));
        double lookupsPerSecIndex = indexTime > 0 ? (numLookups * 1000.0) / indexTime : 0;
        double lookupsPerSecScan = scanTime > 0 ? (numLookups * 1000.0) / scanTime : 0;
        System.out.println("Lookups per second (index): " + String.format("%.2f", lookupsPerSecIndex) + " lookups/sec");
        System.out.println(
                "Lookups per second (table scan): " + String.format("%.2f", lookupsPerSecScan) + " lookups/sec");
        System.out.println("Speedup factor: " + String.format("%.2fx", ratio));
        System.out.println("Index found: " + indexFound + " / " + numLookups);
        System.out.println("Table scan found: " + scanFound + " / " + numLookups);

        tx.commit();
    }
}
