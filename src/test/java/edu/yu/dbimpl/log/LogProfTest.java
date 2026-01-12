package edu.yu.dbimpl.log;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

import org.junit.jupiter.api.Test;

import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.*;

/**
 * Simulates the professor's end-to-end log module use cases:
 * - End-to-end verification that LogMgr can create records and iterator
 * retrieves them in reverse order.
 * - Concurrency and throughput check: 100,000 small records with concurrency
 * level 10, verify all present.
 * - Edge-case handling (oversized record, null input).
 *
 * Notes on performance: we record timing and print it for reference but do not
 * assert it,
 * since timing depends on the machine and environment.
 */
public class LogProfTest {

    private static final String BASE_DIR = "testing/logmgr_prof";
    private static final String CONC_DIR = "testing/logmgr_prof_conc";
    private static final String EDGE_DIR = "testing/logmgr_prof_edge";
    private static final String LOG_FILE = "myLog";
    private static final int BLOCK_SIZE = 256;

    private static void deleteDirectory(String root) {
        final Path rootPath = Paths.get(root);
        if (!Files.exists(rootPath))
            return;
        try (var walk = Files.walk(rootPath)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    /* ignore cleanup errors */ }
            });
        } catch (IOException e) {
            /* ignore */ }
    }

    private static FileMgrBase newFileMgr(String dir, int blockSize, boolean startup) {
        if (startup)
            deleteDirectory(dir);
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(startup));
        DBConfiguration.INSTANCE.get().setConfiguration(props);
        return new FileMgr(new File(dir), blockSize);
    }

    private static byte[] intToBytes(int v) {
        return new byte[] { (byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v };
    }

    @Test
    public void endToEndCreateAndIterateReverseOrder() {
        FileMgrBase fm = newFileMgr(BASE_DIR, BLOCK_SIZE, true);
        LogMgr lm = new LogMgr(fm, LOG_FILE);

        final int N = 12;
        List<byte[]> appended = new ArrayList<>(N);
        for (int i = 0; i < N; i++) {
            int lsn = lm.append(intToBytes(i));
            assertEquals(i, lsn, "LSN must be sequential starting at 0 on fresh startup");
            appended.add(intToBytes(i));
        }

        List<byte[]> iterated = new ArrayList<>();
        for (Iterator<byte[]> it = lm.iterator(); it.hasNext();) {
            iterated.add(it.next());
        }

        assertEquals(N, iterated.size(), "Iterator must return all records");
        for (int i = 0; i < N; i++) {
            assertArrayEquals(appended.get(N - 1 - i), iterated.get(i),
                    "Iterator must yield records in reverse append order");
        }
    }

    @Test
    public void concurrency100kRecordsLevel10AndVerify() throws Exception {
        final int threads = 10;
        final int perThread = 10_000; // total 100,000 records
        FileMgrBase fm = newFileMgr(CONC_DIR, BLOCK_SIZE, true);
        LogMgr lm = new LogMgr(fm, LOG_FILE);

        final ExecutorService es = Executors.newFixedThreadPool(threads);
        final Set<Integer> lsns = ConcurrentHashMap.newKeySet(threads * perThread);

        Callable<Void> task = () -> {
            for (int i = 0; i < perThread; i++) {
                int lsn = lm.append(intToBytes(i));
                lsns.add(lsn);
            }
            return null;
        };

        long start = System.nanoTime();
        List<Future<Void>> futures = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++) {
            futures.add(es.submit(task));
        }
        for (Future<Void> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        es.shutdown();
        es.awaitTermination(30, TimeUnit.SECONDS);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.out
                .println("LogUseCaseProfTest: appended " + (threads * perThread) + " records in " + elapsedMs + " ms");

        int expected = threads * perThread;
        assertEquals(expected, lsns.size(), "All LSNs must be unique and present");
        for (int i = 0; i < expected; i++) {
            assertTrue(lsns.contains(i), "Missing LSN " + i);
        }

        // iterator should traverse all records (reverse order)
        int iterCount = 0;
        Iterator<byte[]> it = lm.iterator();
        while (it.hasNext()) {
            it.next();
            iterCount++;
        }
        assertEquals(expected, iterCount, "Iterator must traverse all concurrent records");
    }

    @Test
    public void edgeCasesNullAndOversizedRecords() {
        FileMgrBase fm = newFileMgr(EDGE_DIR, BLOCK_SIZE, true);
        LogMgr lm = new LogMgr(fm, LOG_FILE);

        // null input should be rejected by the API (implementation may throw NPE or
        // IAE)
        assertThrows(RuntimeException.class, () -> lm.append(null),
                "Append(null) should be rejected");

        // oversized record (strictly larger than a page) must be rejected
        byte[] big = new byte[BLOCK_SIZE + 1];
        assertThrows(IllegalArgumentException.class, () -> lm.append(big),
                "Append must reject record bigger than page");
    }
}
