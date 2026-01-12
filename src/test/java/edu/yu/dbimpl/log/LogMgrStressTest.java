package edu.yu.dbimpl.log;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.Page;
import edu.yu.dbimpl.file.PageBase;

/**
 * Stress test for LogMgr: appends many records to force multiple blocks,
 * verifies iterator returns records in reverse order, and checks LSN
 * continuity across a simulated restart.
 */
public class LogMgrStressTest {

    private static final Logger logger = LogManager.getLogger(LogMgrStressTest.class);

    private File dbDirectory;
    private static final String LOGFILE = "stress_log";

    @BeforeEach
    public void setup() {
        final Properties dbProperties = new Properties();
        dbProperties.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(dbProperties);

        final String dirName = "testing/LogStressTest";
        this.dbDirectory = new File(dirName);
        if (!this.dbDirectory.exists()) {
            assertTrue(this.dbDirectory.mkdirs() || this.dbDirectory.exists(),
                    "Failed to create test directory");
        }
    }

    @AfterEach
    public void teardown() {
        // Best-effort cleanup: delete files in the directory (if any)
        if (this.dbDirectory != null && this.dbDirectory.exists()) {
            File[] children = this.dbDirectory.listFiles();
            if (children != null) {
                for (File f : children) {
                    // noinspection ResultOfMethodCallIgnored
                    f.delete();
                }
            }
            // noinspection ResultOfMethodCallIgnored
            this.dbDirectory.delete();
        }
    }

    @Test
    public void megaStressTest() throws Exception {
        logger.info("Starting megaStressTest");

        // Phase 1: serial stress append + iterate
        final int blockSize = 256; // force multiple blocks
        final int totalSerial = 3000;

        final FileMgrBase fileMgr = new FileMgr(dbDirectory, blockSize);
        final LogMgrBase logMgr = new LogMgr(fileMgr, LOGFILE);

        int firstLsn = -1;
        for (int i = 0; i < totalSerial; i++) {
            String s = makeDeterministicString(i);
            int intVal = i * 7 + 13;

            int spos = 0;
            int npos = spos + PageBase.maxLength(s.length());
            byte[] bytes = new byte[npos + Integer.BYTES];
            PageBase p = new Page(bytes);
            p.setString(spos, s);
            p.setInt(npos, intVal);

            int lsn = logMgr.append(bytes);
            if (i == 0)
                firstLsn = lsn;
            assertEquals(i, lsn, "LSN should equal the 0-based append index on fresh startup");
        }
        assertEquals(0, firstLsn, "First LSN must be 0");

        logMgr.flush(totalSerial - 1);

        int seenSerial = 0;
        for (byte[] rec : (Iterable<byte[]>) logMgr::iterator) {
            int idx = totalSerial - 1 - seenSerial;
            PageBase p = new Page(rec);
            String str = p.getString(0);
            int npos = PageBase.maxLength(str.length());
            int val = p.getInt(npos);
            assertEquals(makeDeterministicString(idx), str, "String payload mismatch at index " + idx);
            assertEquals(idx * 7 + 13, val, "Int payload mismatch at index " + idx);
            seenSerial++;
        }
        assertEquals(totalSerial, seenSerial, "Iterator should return all appended records");

        // Phase 2: restart check and newest-first verification
        Properties restartProps = new Properties();
        restartProps.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
        DBConfiguration.INSTANCE.get().setConfiguration(restartProps);

        final LogMgrBase logMgrAfterRestart = new LogMgr(new FileMgr(dbDirectory, blockSize), LOGFILE);
        int continuedLsn = logMgrAfterRestart.append(makeSimpleRecord("post-restart", 1234));
        assertEquals(totalSerial, continuedLsn, "After restart, next append should continue LSN sequence");

        List<String> firstTwo = new ArrayList<>();
        logMgrAfterRestart.iterator().forEachRemaining(bytes -> {
            if (firstTwo.size() < 2) {
                PageBase p = new Page(bytes);
                String str = p.getString(0);
                firstTwo.add(str);
            }
        });
        assertFalse(firstTwo.isEmpty(), "Iterator after restart should not be empty");
        assertEquals("post-restart", firstTwo.get(0), "Newest record should be returned first");

        // Phase 3: concurrent appends with random flushes
        final int totalConcurrent = 5000;
        final int threads = 6;
        final int perThread = totalConcurrent / threads;
        final int remainder = totalConcurrent % threads;

        final String[] expectedStr = new String[continuedLsn + 1 + totalConcurrent];
        final int[] expectedInt = new int[continuedLsn + 1 + totalConcurrent];
        final AtomicInteger highestLsn = new AtomicInteger(continuedLsn);
        final AtomicBoolean running = new AtomicBoolean(true);

        final ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        final List<Throwable> errors = new ArrayList<>();
        final Random rnd = new Random(42);

        // Flusher thread doing random flushes while appends are happening
        pool.submit(() -> {
            try {
                start.await();
                Random rf = new Random(1337);
                while (running.get()) {
                    int hi = highestLsn.get();
                    if (hi >= 0) {
                        int target = rf.nextInt(hi + 1);
                        try {
                            logMgrAfterRestart.flush(target);
                        } catch (Throwable t) {
                            synchronized (errors) {
                                errors.add(t);
                            }
                        }
                    }
                    TimeUnit.MILLISECONDS.sleep(rf.nextInt(3));
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });

        // Appender threads
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            final int count = perThread + (t < remainder ? 1 : 0);
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < count; i++) {
                        String s = "T" + tid + ":" + i + ":" + varyingString(rnd, tid, i);
                        int val = (tid + 1) * 100000 + i;

                        int spos = 0;
                        int npos = spos + PageBase.maxLength(s.length());
                        byte[] bytes = new byte[npos + Integer.BYTES];
                        PageBase p = new Page(bytes);
                        p.setString(spos, s);
                        p.setInt(npos, val);

                        int lsn = logMgrAfterRestart.append(bytes);
                        highestLsn.updateAndGet(prev -> Math.max(prev, lsn));
                        expectedStr[lsn] = s;
                        expectedInt[lsn] = val;

                        if ((lsn & 0x1FF) == 0) {
                            logMgrAfterRestart.flush(lsn);
                        }
                    }
                } catch (Throwable t1) {
                    synchronized (errors) {
                        errors.add(t1);
                    }
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(60, TimeUnit.SECONDS), "Appender threads did not finish in time");
        running.set(false);
        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        if (!errors.isEmpty()) {
            errors.forEach(Throwable::printStackTrace);
            fail("Errors occurred during concurrent append/flush: " + errors.get(0));
        }

        int hi = highestLsn.get();
        assertEquals(continuedLsn + totalConcurrent, hi, "Highest LSN should reflect all appends");

        logMgrAfterRestart.flush(hi);

        // Validate only the most recent totalConcurrent records (the concurrent phase)
        int validated = 0;
        for (byte[] rec : (Iterable<byte[]>) logMgrAfterRestart::iterator) {
            if (validated >= totalConcurrent)
                break;
            int expectedLsn = hi - validated;
            PageBase p = new Page(rec);
            String str = p.getString(0);
            int npos = PageBase.maxLength(str.length());
            int val = p.getInt(npos);

            assertEquals(expectedStr[expectedLsn], str, "String mismatch at LSN " + expectedLsn);
            assertEquals(expectedInt[expectedLsn], val, "Int mismatch at LSN " + expectedLsn);
            validated++;
        }
        assertEquals(totalConcurrent, validated, "Should validate all concurrently appended records");

        logger.info("Completed megaStressTest");
    }

    private static String varyingString(Random rnd, int tid, int i) {
        // Create a short but varying suffix to change record sizes
        int len = 1 + ((tid * 31 + i * 17 + rnd.nextInt(7)) % 48);
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < len; k++) {
            sb.append((char) ('a' + ((k + tid + i) % 26)));
        }
        return sb.toString();
    }

    private static String makeDeterministicString(int i) {
        // Create varying-length strings deterministically (lengths 1..24 repeating)
        int len = (i % 24) + 1;
        String base = "r" + i + "-";
        StringBuilder sb = new StringBuilder(base);
        while (sb.length() < len) {
            sb.append((char) ('a' + (sb.length() % 26)));
        }
        return sb.toString();
    }

    private static byte[] makeSimpleRecord(String s, int x) {
        int spos = 0;
        int npos = spos + PageBase.maxLength(s.length());
        byte[] bytes = new byte[npos + Integer.BYTES];
        PageBase p = new Page(bytes);
        p.setString(spos, s);
        p.setInt(npos, x);
        return bytes;
    }
}
