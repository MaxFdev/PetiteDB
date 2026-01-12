package edu.yu.dbimpl.file;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.List;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import edu.yu.dbimpl.config.DBConfiguration;

/**
 * Concurrent stress test:
 * 1. Initializes a fresh DB (blockSize = 4000)
 * 2. Concurrently creates 10 files, each with 3 blocks written with a string at offset 0
 * 3. Concurrently performs 50,000 total random block reads distributed evenly across files
 * 4. Verifies data integrity and file lengths, fails on any mismatch or exception
 */
public class ConcurrentStressTest {

    private static final int BLOCK_SIZE = 4000;
    private static final int N_FILES = 10;
    private static final int BLOCKS_PER_FILE = 3;
    // Total number of random read iterations across all files
    private static final int TOTAL_READ_ITERATIONS = 50_000;

    private static FileMgr newFm(File dir, int blockSize, boolean startup) {
        final Properties dbProps = new Properties();
        dbProps.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(startup));
        DBConfiguration.INSTANCE.get().setConfiguration(dbProps);
        return new FileMgr(dir, blockSize);
    }

    @Test
    public void concurrentMultiFileReadWriteStress() throws InterruptedException, ExecutionException {
        final File root = new File("testing/fm-concurrent-stress");
        final FileMgr fm = newFm(root, BLOCK_SIZE, true);
        assertEquals(BLOCK_SIZE, fm.blockSize(), "Block size mismatch");

        // Prepare expected strings
        final String[][] expected = new String[N_FILES][BLOCKS_PER_FILE];
        for (int f = 0; f < N_FILES; f++) {
            for (int b = 0; b < BLOCKS_PER_FILE; b++) {
                // modest sized payload to avoid excessive memory while being non-trivial
                expected[f][b] = "file-" + f + "-block-" + b + "|" + "X".repeat(64);
            }
        }

        // 1) Concurrently write all files/blocks
        ExecutorService writerPool = Executors.newFixedThreadPool(Math.min(16, N_FILES));
        List<Callable<Void>> writeTasks = new ArrayList<>();
        for (int f = 0; f < N_FILES; f++) {
            final int fileIdx = f;
            writeTasks.add(() -> {
                final String fname = "stress_file_" + fileIdx;
                for (int b = 0; b < BLOCKS_PER_FILE; b++) {
                    final Page p = new Page(BLOCK_SIZE);
                    p.setString(0, expected[fileIdx][b]);
                    fm.write(new BlockId(fname, b), p);
                }
                return null;
            });
        }
        List<Future<Void>> writeResults = writerPool.invokeAll(writeTasks);
        for (Future<Void> fut : writeResults) {
            fut.get();
        }
        writerPool.shutdown();

        // Validate lengths after writes
        for (int f = 0; f < N_FILES; f++) {
            final String fname = "stress_file_" + f;
            assertEquals(BLOCKS_PER_FILE, fm.length(fname), "Unexpected length for " + fname);
        }

    // 2) Concurrent random reads totaling 50,000 across all files
        ExecutorService readerPool = Executors.newFixedThreadPool(Math.min(32, N_FILES));
        List<Callable<Long>> readTasks = new ArrayList<>();
        for (int f = 0; f < N_FILES; f++) {
            final int fileIdx = f;
            readTasks.add(() -> {
                final ThreadLocalRandom rnd = ThreadLocalRandom.current();
                final String fname = "stress_file_" + fileIdx;
                final Page pageReuse = new Page(BLOCK_SIZE);
                long checksum = 0L; // simple aggregation to give the loop a side-effect
        final int iterations = TOTAL_READ_ITERATIONS / N_FILES; // evenly distributed (5000 each)
        for (int i = 0; i < iterations; i++) {
                    final int blkNum = rnd.nextInt(BLOCKS_PER_FILE);
                    fm.read(new BlockId(fname, blkNum), pageReuse);
                    final String got = pageReuse.getString(0);
                    final String exp = expected[fileIdx][blkNum];
                    if (!exp.equals(got)) {
                        fail("Data mismatch: file=" + fname + " block=" + blkNum + " expected='" + exp + "' got='" + got
                                + "'");
                    }
                    // accumulate a trivial checksum based on a few chars
                    checksum += got.charAt(0) + got.charAt(got.length() - 1);
                }
                return checksum;
            });
        }
        List<Future<Long>> readResults = readerPool.invokeAll(readTasks);
        long totalChecksum = 0L;
        for (Future<Long> fut : readResults) {
            totalChecksum += fut.get();
        }
        readerPool.shutdown();

        // Final sanity: lengths still correct & checksum non-zero
        for (int f = 0; f < N_FILES; f++) {
            final String fname = "stress_file_" + f;
            assertEquals(BLOCKS_PER_FILE, fm.length(fname), "Length changed unexpectedly for " + fname);
        }
        assertTrue(totalChecksum > 0, "Checksum should be > 0 indicating reads occurred");
    }
}
