package edu.yu.dbimpl.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.Test;

import edu.yu.dbimpl.config.DBConfiguration;

public class FileMgrResourceLifecycleTest {

    private static FileMgr newFm(File dir, int blockSize, boolean startup) {
        final Properties dbProps = new Properties();
        dbProps.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(startup));
        DBConfiguration.INSTANCE.get().setConfiguration(dbProps);
        return new FileMgr(dir, blockSize);
    }

    @Test
    public void manyFilesOneByteAppend() throws InterruptedException {
        final File root = new File("testing/fm-many-files");
        final int blockSize = 1;
        final int nFiles = 10000;

        FileMgr fm = newFm(root, blockSize, true);

        final List<Thread> threads = new ArrayList<>(nFiles);
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < nFiles; i++) {
            final String fname = "f" + i;
            Thread t = new Thread(() -> {
                try {
                    BlockIdBase blk = fm.append(fname);
                    if (blk.number() != 0) {
                        throw new AssertionError("first append must be block 0 for file " + fname + " but was " + blk.number());
                    }
                } catch (Throwable t1) {
                    errors.add(t1);
                }
            }, "append-" + fname);
            threads.add(t);
        }

        // Start all threads
        for (Thread t : threads) {
            t.start();
        }
        // Wait for all to finish
        for (Thread t : threads) {
            t.join();
        }

        assertTrue(errors.isEmpty(), "Errors encountered during threaded appends: " + (errors.peek() != null ? errors.peek().toString() : ""));

        // Spot-check some lengths
        assertEquals(1, fm.length("f0"));
        assertEquals(1, fm.length("f123"));
        assertEquals(1, fm.length("f9999"));

        // Ensure subsequent append works (resource should have been closed and reopened)
        final String checkFile = "f9990";
        BlockIdBase next = fm.append(checkFile);
        assertEquals(1, next.number());
        assertEquals(2, fm.length(checkFile));

        FileMgr fm2 = newFm(root, blockSize, true);
        assertEquals(0, root.list().length);
    }
}