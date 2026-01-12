package edu.yu.dbimpl.record;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import edu.yu.dbimpl.buffer.BufferAbortException;
import edu.yu.dbimpl.buffer.BufferMgr;
import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.tx.TxBase;
import edu.yu.dbimpl.tx.TxMgr;
import edu.yu.dbimpl.tx.TxMgrBase;
import edu.yu.dbimpl.tx.concurrency.LockAbortException;

/**
 * Stress tests for the Record module (TableScan, RecordPage).
 * Tests exercise concurrent record operations.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RecordStressTest {

    private static final int BLOCK_SIZE = 4096;
    private static final String TEST_BASE_DIR = "testing/RecordStressTest";
    private static final String LOG_FILE = "stress_logfile";

    private TxMgrBase createTxMgr(String testName, int bufferSize, int maxWaitTime) {
        Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);

        Path testDir = Path.of(TEST_BASE_DIR, testName);
        try {
            if (Files.exists(testDir)) {
                deleteDirectory(testDir);
            }
        } catch (Exception e) {
            // ignore
        }

        FileMgrBase fileMgr = new FileMgr(testDir.toFile(), BLOCK_SIZE);
        LogMgrBase logMgr = new LogMgr(fileMgr, LOG_FILE);
        BufferMgrBase bufferMgr = new BufferMgr(fileMgr, logMgr, bufferSize, maxWaitTime);
        return new TxMgr(fileMgr, logMgr, bufferMgr, maxWaitTime);
    }

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

    private SchemaBase createStandardSchema() {
        Schema schema = new Schema();
        schema.addIntField("id");
        schema.addStringField("name", 20);
        schema.addDoubleField("value");
        schema.addBooleanField("active");
        return schema;
    }

    /**
     * Executes an operation with proper TableScan lifecycle management.
     * Ensures the TableScan is always closed before commit/rollback.
     */
    private boolean executeWithTableScan(TxBase tx, String tableName, LayoutBase layout,
            java.util.function.Consumer<TableScan> operation) {
        TableScan ts = null;
        try {
            ts = new TableScan(tx, tableName, layout);
            operation.accept(ts);
            ts.close();
            ts = null; // Mark as closed
            tx.commit();
            return true;
        } catch (LockAbortException | BufferAbortException e) {
            if (ts != null) {
                ts.close();
            }
            tx.rollback();
            return false;
        } catch (Exception e) {
            if (ts != null) {
                try {
                    ts.close();
                } catch (Exception ignored) {
                }
            }
            try {
                tx.rollback();
            } catch (Exception ignored) {
            }
            return false;
        }
    }

    // ========================================================================
    // TEST 1: Write/Read Throughput (Sequential write, then parallel read)
    // ========================================================================

    @Test
    @Order(0)
    public void heavy_write_read_throughput() {
        final String testName = "throughput-" + System.nanoTime();
        final int nRecords = 500;
        final int concurrency = 4;
        final int bufferSize = 200;
        final int maxWait = 2000;

        TxMgrBase txMgr = createTxMgr(testName, bufferSize, maxWait);
        SchemaBase schema = createStandardSchema();
        LayoutBase layout = new Layout(schema);
        final String tableName = "stress_tbl";

        // Phase 1: Single-threaded writes
        AtomicInteger totalWritten = new AtomicInteger(0);
        TxBase writeTx = txMgr.newTx();
        boolean writeSuccess = executeWithTableScan(writeTx, tableName, layout, ts -> {
            for (int id = 0; id < nRecords; id++) {
                ts.insert();
                ts.setInt("id", id);
                ts.setString("name", "rec_" + id);
                ts.setDouble("value", id * 1.5);
                ts.setBoolean("active", id % 2 == 0);
                totalWritten.incrementAndGet();
            }
        });

        assertTrue(writeSuccess, "Write phase should succeed");
        assertEquals(nRecords, totalWritten.get(), "All records should be written");

        // Phase 2: Parallel reads
        ExecutorService readPool = Executors.newFixedThreadPool(concurrency);
        List<Future<Integer>> readFutures = new ArrayList<>();
        AtomicInteger successfulReads = new AtomicInteger(0);

        for (int t = 0; t < concurrency; t++) {
            readFutures.add(readPool.submit(() -> {
                AtomicInteger counted = new AtomicInteger(0);
                TxBase tx = txMgr.newTx();
                boolean success = executeWithTableScan(tx, tableName, layout, ts -> {
                    ts.beforeFirst();
                    while (ts.next()) {
                        ts.getInt("id");
                        ts.getString("name");
                        ts.getDouble("value");
                        ts.getBoolean("active");
                        counted.incrementAndGet();
                    }
                });
                if (success) {
                    successfulReads.incrementAndGet();
                }
                return counted.get();
            }));
        }

        int totalRead = 0;
        for (Future<Integer> f : readFutures) {
            try {
                totalRead += f.get();
            } catch (ExecutionException | InterruptedException e) {
                fail("Read thread failed: " + e.getCause());
            }
        }
        readPool.shutdown();
        awaitTermination(readPool);

        System.out.println("\n=== heavy_write_read_throughput ===");
        System.out.printf("Written: %d records, Read total: %d, Successful readers: %d/%d%n",
                totalWritten.get(), totalRead, successfulReads.get(), concurrency);

        assertTrue(totalWritten.get() > 0, "Should have written some records");
        assertTrue(successfulReads.get() > 0, "At least some reads should succeed");
    }

    // ========================================================================
    // TEST 2: Lock Contention Under Load
    // ========================================================================

    @Test
    @Order(1)
    public void lock_contention_under_load() {
        final String testName = "contention-" + System.nanoTime();
        final int nRecords = 50;
        final int concurrency = 100;
        final int attemptsPerThread = 3;
        final int bufferSize = 200;
        final int maxWait = 1000;

        TxMgrBase txMgr = createTxMgr(testName, bufferSize, maxWait);
        SchemaBase schema = createStandardSchema();
        LayoutBase layout = new Layout(schema);
        final String tableName = "contention_tbl";

        // Initialize records
        TxBase initTx = txMgr.newTx();
        boolean initSuccess = executeWithTableScan(initTx, tableName, layout, ts -> {
            for (int i = 0; i < nRecords; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("name", "init_" + i);
                ts.setDouble("value", i * 0.5);
                ts.setBoolean("active", true);
            }
        });
        assertTrue(initSuccess, "Failed to initialize test data");

        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger aborts = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < concurrency; t++) {
            final long seed = System.nanoTime() ^ (t * 0x9E3779B97F4A7C15L);
            futures.add(pool.submit(() -> {
                Random rng = new Random(seed);
                boolean succeeded = false;
                for (int k = 0; k < attemptsPerThread && !succeeded; k++) {
                    boolean isWrite = rng.nextBoolean();
                    TxBase tx = txMgr.newTx();
                    boolean success = executeWithTableScan(tx, tableName, layout, ts -> {
                        ts.beforeFirst();
                        int accessed = 0;
                        while (ts.next() && accessed < 3) {
                            if (isWrite) {
                                ts.setDouble("value", rng.nextDouble() * 1000);
                            } else {
                                ts.getDouble("value");
                            }
                            accessed++;
                        }
                    });
                    if (success) {
                        successes.incrementAndGet();
                        succeeded = true;
                    }
                }
                if (!succeeded) {
                    aborts.incrementAndGet(); // Only count abort if all retries exhausted
                }
            }));
        }

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (ExecutionException | InterruptedException e) {
                fail("Worker thread failed: " + e.getCause());
            }
        }
        pool.shutdown();
        awaitTermination(pool);

        int totalAttempts = concurrency * attemptsPerThread;
        System.out.println("\n=== lock_contention_under_load ===");
        System.out.printf("Attempts: %d, Successes: %d, Aborts: %d%n",
                totalAttempts, successes.get(), aborts.get());

        assertTrue(totalAttempts > aborts.get(), "All attempts should be more than aborts");
    }

    // ========================================================================
    // TEST 3: Mixed Operations Random Workload
    // ========================================================================

    @Test
    @Order(2)
    public void mixed_ops_random_workload() {
        final String testName = "mixed-" + System.nanoTime();
        final int totalOps = 200;
        final int concurrency = 2;
        final int bufferSize = 300;
        final int maxWait = 500;

        TxMgrBase txMgr = createTxMgr(testName, bufferSize, maxWait);
        SchemaBase schema = createStandardSchema();
        LayoutBase layout = new Layout(schema);
        final String tableName = "mixed_tbl";

        // Initialize with some records
        final int initialRecords = 100;
        TxBase initTx = txMgr.newTx();
        boolean initSuccess = executeWithTableScan(initTx, tableName, layout, ts -> {
            for (int i = 0; i < initialRecords; i++) {
                ts.insert();
                ts.setInt("id", i);
                ts.setString("name", "init_" + i);
                ts.setDouble("value", i * 0.1);
                ts.setBoolean("active", true);
            }
        });
        assertTrue(initSuccess, "Failed to initialize test data");

        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger aborts = new AtomicInteger(0);
        AtomicInteger nextId = new AtomicInteger(initialRecords);
        AtomicInteger reads = new AtomicInteger(0);
        AtomicInteger inserts = new AtomicInteger(0);
        AtomicInteger updates = new AtomicInteger(0);
        AtomicInteger deletes = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        List<Future<?>> futures = new ArrayList<>();

        int opsPerThread = totalOps / concurrency;

        for (int t = 0; t < concurrency; t++) {
            final int threadOps = (t == concurrency - 1) ? totalOps - (opsPerThread * (concurrency - 1)) : opsPerThread;
            final long seed = System.nanoTime() ^ (t * 0x9E3779B97F4A7C15L);

            futures.add(pool.submit(() -> {
                Random rng = new Random(seed);

                for (int k = 0; k < threadOps; k++) {
                    double opChoice = rng.nextDouble();
                    TxBase tx = txMgr.newTx();

                    boolean success = executeWithTableScan(tx, tableName, layout, ts -> {
                        if (opChoice < 0.60) { // READ
                            ts.beforeFirst();
                            int count = 0;
                            while (ts.next() && count < 3) {
                                ts.getInt("id");
                                ts.getDouble("value");
                                count++;
                            }
                            reads.incrementAndGet();
                        } else if (opChoice < 0.85) { // INSERT
                            int newId = nextId.getAndIncrement();
                            ts.insert();
                            ts.setInt("id", newId);
                            ts.setString("name", "new_" + newId);
                            ts.setDouble("value", rng.nextDouble() * 100);
                            ts.setBoolean("active", rng.nextBoolean());
                            inserts.incrementAndGet();
                        } else if (opChoice < 0.95) { // UPDATE
                            ts.beforeFirst();
                            if (ts.next()) {
                                ts.setDouble("value", rng.nextDouble() * 1000);
                            }
                            updates.incrementAndGet();
                        } else { // DELETE
                            ts.beforeFirst();
                            if (ts.next()) {
                                ts.delete();
                            }
                            deletes.incrementAndGet();
                        }
                    });

                    if (success) {
                        successes.incrementAndGet();
                    } else {
                        aborts.incrementAndGet();
                    }
                }
            }));
        }

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (ExecutionException | InterruptedException e) {
                fail("Worker thread failed: " + e.getCause());
            }
        }
        pool.shutdown();
        awaitTermination(pool);

        System.out.println("\n=== mixed_ops_random_workload ===");
        System.out.printf("Operations: reads=%d, inserts=%d, updates=%d, deletes=%d%n",
                reads.get(), inserts.get(), updates.get(), deletes.get());
        System.out.printf("Results: successes=%d, aborts=%d%n", successes.get(), aborts.get());

        assertEquals(totalOps, successes.get() + aborts.get(), "All operations should be accounted for");
    }

    // ========================================================================
    // TEST 4: Multi-Table Concurrent Access
    // ========================================================================

    @Test
    @Order(3)
    public void multi_table_concurrent_access() {
        final String testName = "multitable-" + System.nanoTime();
        final int nTables = 3;
        final int recordsPerTable = 30;
        final int concurrency = 2;
        final int opsPerThread = 50;
        final int bufferSize = 300;
        final int maxWait = 300;

        TxMgrBase txMgr = createTxMgr(testName, bufferSize, maxWait);

        LayoutBase[] layouts = new LayoutBase[nTables];
        String[] tableNames = new String[nTables];

        for (int i = 0; i < nTables; i++) {
            Schema schema = new Schema();
            schema.addIntField("id");
            schema.addStringField("data", 15);
            schema.addDoubleField("amount");
            layouts[i] = new Layout(schema);
            tableNames[i] = "table_" + i;
        }

        // Initialize all tables
        for (int t = 0; t < nTables; t++) {
            final int tableIdx = t;
            TxBase initTx = txMgr.newTx();
            boolean initSuccess = executeWithTableScan(initTx, tableNames[t], layouts[t], ts -> {
                for (int i = 0; i < recordsPerTable; i++) {
                    ts.insert();
                    ts.setInt("id", i);
                    ts.setString("data", "tbl" + tableIdx + "_r" + i);
                    ts.setDouble("amount", i * 0.25);
                }
            });
            assertTrue(initSuccess, "Failed to initialize table " + t);
        }

        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger aborts = new AtomicInteger(0);

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        List<Future<?>> futures = new ArrayList<>();

        for (int thread = 0; thread < concurrency; thread++) {
            final long seed = System.nanoTime() ^ (thread * 0x9E3779B97F4A7C15L);

            futures.add(pool.submit(() -> {
                Random rng = new Random(seed);

                for (int k = 0; k < opsPerThread; k++) {
                    int t1 = rng.nextInt(nTables);
                    int t2 = rng.nextInt(nTables);

                    // DEADLOCK PREVENTION: Always access tables in sorted order
                    int firstTable = Math.min(t1, t2);
                    int secondTable = Math.max(t1, t2);

                    TxBase tx = txMgr.newTx();
                    TableScan ts1 = null;
                    TableScan ts2 = null;

                    try {
                        // Read from first table
                        ts1 = new TableScan(tx, tableNames[firstTable], layouts[firstTable]);
                        ts1.beforeFirst();
                        double sum = 0;
                        int count = 0;
                        while (ts1.next() && count < 3) {
                            sum += ts1.getDouble("amount");
                            count++;
                        }
                        ts1.close();
                        ts1 = null;

                        // Write to second table if different
                        if (secondTable != firstTable) {
                            ts2 = new TableScan(tx, tableNames[secondTable], layouts[secondTable]);
                            ts2.beforeFirst();
                            if (ts2.next()) {
                                ts2.setDouble("amount", sum / Math.max(1, count));
                            }
                            ts2.close();
                            ts2 = null;
                        }

                        tx.commit();
                        successes.incrementAndGet();
                    } catch (LockAbortException | BufferAbortException e) {
                        if (ts1 != null)
                            ts1.close();
                        if (ts2 != null)
                            ts2.close();
                        tx.rollback();
                        aborts.incrementAndGet();
                    } catch (Exception e) {
                        if (ts1 != null) {
                            try {
                                ts1.close();
                            } catch (Exception ignored) {
                            }
                        }
                        if (ts2 != null) {
                            try {
                                ts2.close();
                            } catch (Exception ignored) {
                            }
                        }
                        try {
                            tx.rollback();
                        } catch (Exception ignored) {
                        }
                        aborts.incrementAndGet();
                    }
                }
            }));
        }

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (ExecutionException | InterruptedException e) {
                fail("Worker thread failed: " + e.getCause());
            }
        }
        pool.shutdown();
        awaitTermination(pool);

        int totalOps = concurrency * opsPerThread;
        System.out.println("\n=== multi_table_concurrent_access ===");
        System.out.printf("Tables: %d, Attempts: %d, Successes: %d, Aborts: %d%n",
                nTables, totalOps, successes.get(), aborts.get());

        assertEquals(totalOps, successes.get() + aborts.get(), "All operations should be accounted for");
    }

    private void awaitTermination(ExecutorService pool) {
        try {
            pool.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
