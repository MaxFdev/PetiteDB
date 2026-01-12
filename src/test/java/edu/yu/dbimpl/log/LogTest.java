package edu.yu.dbimpl.log;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.*;

/*
 * Prof tests:
 * • End-to-end verification that the log module implementation can be
 * used by higher-levels of the stack. This part focuses on the LogModule
 * to create log records and LogIterator to retrieve log records.
 * 
 * • Performance and thread-safety evaluation of the log module as a whole.
 * To give a sense of my expectations: on my Mac Pro (specifications in
 * the FileModule requirements doc), a test
 * – Creates 100, 000 (small) log records using a concurrency level of 10
 * – Verifies that all log records were created successfully takes 775
 * milliseconds.
 * 
 * • Some amount of testing that the implementation can handle edge-case
 * input to the API.
 */

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LogTest {
    private static final String BASE_DIR = "testing/logmgr_basic"; // reused for persistence test
    private static final String CONC_DIR = "testing/logmgr_concurrent"; // separate dir for concurrency test
    private static final String LOG_FILE = "myLog"; // name of the log file (without extension)
    private static final int BLOCK_SIZE = 256; // use modest block size for boundary testing

    /* Utility: recursively delete directory if present */
    private static void deleteDirectory(String root) {
        final Path rootPath = Paths.get(root);
        if (!Files.exists(rootPath))
            return;
        try (var walk = Files.walk(rootPath)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    /* ignore for test cleanup */ }
            });
        } catch (IOException e) {
            /* ignore */ }
    }

    /* Utility: create and return a fresh FileMgr configured for startup mode. */
    private FileMgrBase freshFileMgr(String dir, int blockSize) {
        deleteDirectory(dir); // ensure clean slate
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);
        return new FileMgr(new File(dir), blockSize);
    }

    /* Utility: create and return a FileMgr in restart (startup=false) mode. */
    private FileMgrBase restartFileMgr(String dir, int blockSize) {
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
        DBConfiguration.INSTANCE.get().setConfiguration(props);
        return new FileMgr(new File(dir), blockSize);
    }

    private static byte[] intToBytes(int v) {
        return new byte[] { (byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v };
    }

    /**
     * 0. Basic append semantics on fresh startup: first LSN == 0, sequential
     * increments
     */
    @Test
    @Order(0)
    public void testStartupAndBasicAppends() {
        FileMgrBase fm = freshFileMgr(BASE_DIR, BLOCK_SIZE);
        LogMgr lm = new LogMgr(fm, LOG_FILE);

        int lsn0 = lm.append(intToBytes(0));
        int lsn1 = lm.append(intToBytes(1));
        int lsn2 = lm.append(intToBytes(2));

        assertEquals(0, lsn0, "First append on fresh DB must return LSN 0");
        assertEquals(1, lsn1, "Second append must return previous LSN value (pre-incremented) 1");
        assertEquals(2, lsn2, "Third append must return LSN 2");
    }

    /**
     * 1. Iterator returns records in reverse order (most recent first) and only the
     * client's bytes (no internal metadata)
     */
    @Test
    @Order(1)
    public void testIteratorReverseOrder() {
        // Fresh environment to control content
        FileMgrBase fm = freshFileMgr("testing/logmgr_iter", BLOCK_SIZE);
        LogMgr lm = new LogMgr(fm, LOG_FILE);

        final int N = 10;
        List<byte[]> appended = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            byte[] rec = intToBytes(i);
            int lsn = lm.append(rec);
            assertEquals(i, lsn, "LSN should match number of previously appended records");
            appended.add(rec);
        }

        Iterator<byte[]> it = lm.iterator(); // per spec: implicitly flushes
        List<byte[]> iterated = new ArrayList<>();
        while (it.hasNext()) {
            iterated.add(it.next());
        }

        assertEquals(N, iterated.size(), "Iterator must return all appended records");
        for (int i = 0; i < N; i++) {
            // iterator order: most recent first
            byte[] expected = appended.get(N - 1 - i);
            assertArrayEquals(expected, iterated.get(i), "Iterator must yield records in reverse append order");
        }
    }

    /**
     * 2. flush(lsn) idempotence: calling flush on an already persisted LSN should
     * be a no-op
     * (We can't observe disk writes directly, but we can ensure no exceptions and
     * subsequent
     * appends keep correct sequencing.)
     */
    @Test
    @Order(2)
    public void testFlushIdempotent() {
        FileMgrBase fm = freshFileMgr("testing/logmgr_flush", BLOCK_SIZE);
        LogMgr lm = new LogMgr(fm, LOG_FILE);

        int lsn0 = lm.append(intToBytes(42));
        int lsn1 = lm.append(intToBytes(43));
        assertEquals(1, lsn1);

        // Flush the earlier record and then flush again (idempotent behavior expected)
        assertDoesNotThrow(() -> lm.flush(lsn0));
        assertDoesNotThrow(() -> lm.flush(lsn0));

        int lsn2 = lm.append(intToBytes(44));
        assertEquals(2, lsn2);

        // Iteration should still include all three in reverse order
        Iterator<byte[]> it = lm.iterator();
        List<Integer> vals = new ArrayList<>();
        while (it.hasNext()) {
            byte[] rec = it.next();
            int v = ((rec[0] & 0xFF) << 24) | ((rec[1] & 0xFF) << 16) | ((rec[2] & 0xFF) << 8) | (rec[3] & 0xFF);
            vals.add(v);
        }
        assertEquals(List.of(44, 43, 42), vals, "Reverse iterator order after flush operations incorrect");
    }

    /**
     * 3. Persistence across restart: LSN continues and prior records visible after
     * restart.
     */
    @Test
    @Order(3)
    public void testPersistenceAcrossRestart() {
        // Use an independent directory for this test and create seed records ourselves
        final String dir = "testing/logmgr_persist";

        // Startup phase: create three records (LSNs 0,1,2) and flush to disk
        FileMgrBase fmStartup = freshFileMgr(dir, BLOCK_SIZE);
        LogMgr lmStartup = new LogMgr(fmStartup, LOG_FILE);
        lmStartup.append(intToBytes(10));
        lmStartup.append(intToBytes(11));
        int lastLsn = lmStartup.append(intToBytes(12));
        lmStartup.flush(lastLsn);

        // Restart phase: verify the next append gets LSN 3 and iterator sees all 4
        FileMgrBase fmRestart = restartFileMgr(dir, BLOCK_SIZE);
        LogMgr lmRestart = new LogMgr(fmRestart, LOG_FILE);

        int next = lmRestart.append(intToBytes(999));
        assertEquals(3, next, "After restart, next LSN must continue from last persisted value");

        Iterator<byte[]> it = lmRestart.iterator();
        int count = 0;
        boolean saw999 = false;
        while (it.hasNext()) {
            byte[] rec = it.next();
            int v = ((rec[0] & 0xFF) << 24) | ((rec[1] & 0xFF) << 16) | ((rec[2] & 0xFF) << 8) | (rec[3] & 0xFF);
            if (v == 999)
                saw999 = true;
            count++;
        }
        assertTrue(saw999, "Iterator after restart must include newly appended record");
        assertEquals(4, count, "Total records after restart should equal prior + new");
    }

    /**
     * 4. Oversized record must be rejected (record larger than a page).
     */
    @Test
    @Order(4)
    public void testOversizedRecordRejected() {
        FileMgrBase fm = freshFileMgr("testing/logmgr_bigrec", BLOCK_SIZE);
        LogMgr lm = new LogMgr(fm, LOG_FILE);
        byte[] big = new byte[BLOCK_SIZE + 1]; // definitely larger than page
        assertThrows(IllegalArgumentException.class, () -> lm.append(big),
                "Append must reject record bigger than page");
    }

    /**
     * 5. Concurrency: many threads append; all LSNs unique and contiguous; iterator
     * sees all.
     */
    @Test
    @Order(5)
    public void testConcurrentAppends() throws Exception {
        final int threads = 8;
        final int perThread = 200; // total 1600 records
        FileMgrBase fm = freshFileMgr(CONC_DIR, BLOCK_SIZE);
        LogMgr lm = new LogMgr(fm, LOG_FILE);

        ExecutorService es = Executors.newFixedThreadPool(threads);
        Set<Integer> lsns = ConcurrentHashMap.newKeySet();

        Callable<Void> task = () -> {
            for (int i = 0; i < perThread; i++) {
                int lsn = lm.append(intToBytes(i));
                lsns.add(lsn);
            }
            return null;
        };

        List<Future<Void>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++)
            futures.add(es.submit(task));
        for (Future<Void> f : futures)
            f.get(30, TimeUnit.SECONDS);
        es.shutdownNow();

        int expected = threads * perThread;
        assertEquals(expected, lsns.size(), "All LSNs must be unique");
        for (int i = 0; i < expected; i++) {
            assertTrue(lsns.contains(i), "Missing LSN " + i);
        }

        // Iterator should see all records (in reverse order) once flushed implicitly
        int iterCount = 0;
        Iterator<byte[]> it = lm.iterator();
        while (it.hasNext()) {
            it.next();
            iterCount++;
        }
        assertEquals(expected, iterCount, "Iterator must traverse all concurrent records");
    }

    /**
     * 6. Append five string records (as UTF-8 bytes) and verify they are stored
     * and retrievable via the iterator in reverse order.
     */
    @Test
    @Order(6)
    public void testStringRecordsStored() {
        final String dir = "testing/logmgr_strings";
        FileMgrBase fm = freshFileMgr(dir, BLOCK_SIZE);
        LogMgr lm = new LogMgr(fm, LOG_FILE);

        String[] messages = { "alpha", "bravo", "charlie", "delta", "echo" };
        List<byte[]> appended = new ArrayList<>();
        for (String m : messages) {
            byte[] rec = m.getBytes(PageBase.CHARSET);
            appended.add(rec);
            lm.append(rec);
        }

        // Iterator must return exactly these five records in reverse append order
        Iterator<byte[]> it = lm.iterator();
        List<byte[]> iterated = new ArrayList<>();
        while (it.hasNext()) {
            iterated.add(it.next());
        }

        assertEquals(appended.size(), iterated.size(), "Iterator must return all string records");
        for (int i = 0; i < appended.size(); i++) {
            System.out.println(new String(iterated.get(i)));
            assertArrayEquals(appended.get(appended.size() - 1 - i), iterated.get(i),
                    "String log record mismatch at index " + i);
        }
    }

}
