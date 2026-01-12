package edu.yu.dbimpl.file;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import edu.yu.dbimpl.config.DBConfiguration;

/**
 * Comprehensive concurrent robustness tests for FileMgr/Page. Goals: 1) Verify
 * that client code cannot overflow Page/Block boundaries. 2) Exercise edge-case
 * inputs at boundaries and odd cases. 3) Verify persistence across restarts
 * (startup=false) and re-init behavior.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ComprehensiveConcurrentTest {

    private static FileMgr newFm(File dir, int blockSize, boolean startup) {
        final Properties dbProps = new Properties();
        dbProps.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(startup));
        DBConfiguration.INSTANCE.get().setConfiguration(dbProps);
        return new FileMgr(dir, blockSize);
    }

    private static void writeRecord(Page p, int blockSize, int idx) {
        // Chosen non-overlapping positions near boundaries
        final int posStr = 0;
        final int posInt = 128;
        final int posDbl = 256;
        final int posBool = blockSize - 1;
    final int bytesLen = 16;
    // Reserve space for 4-byte length header plus payload, and avoid last byte used by boolean
    final int posBytes = blockSize - 1 - Integer.BYTES - bytesLen; // header + payload end at blockSize-2

        final String s = "rec-" + idx;
        p.setString(posStr, s);
        p.setInt(posInt, 10_000 + idx);
        p.setDouble(posDbl, idx * Math.E);
        p.setBoolean(posBool, (idx % 2) == 0);
        byte[] blob = new byte[bytesLen];
        for (int i = 0; i < blob.length; i++)
            blob[i] = (byte) (idx + i);
        p.setBytes(posBytes, blob);
    }

    @Test
    @Order(0)
    public void concurrentWritesReadsWithBoundarySafetyAndEdgeCases() throws InterruptedException, ExecutionException {
        final int blockSize = 512;
        final int nBlocks = 40;
        final File root = new File("testing/fm-comprehensive-concurrent");
        final String fname = "ccfile";

        FileMgr fm = newFm(root, blockSize, true);
        assertEquals(blockSize, fm.blockSize());

        // 1) Concurrent writers: each writes a different block with values near boundaries
        ExecutorService exec = Executors.newFixedThreadPool(10);
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < nBlocks; i++) {
            final int idx = i;
            tasks.add(() -> {
                // Positive case write
                Page p = new Page(blockSize);
                writeRecord(p, blockSize, idx);
                fm.write(new BlockId(fname, idx), p);

                // 2) Negative/overflow attempts must throw
                Page bad = new Page(blockSize);
                // int overflow: start so only 3 bytes remain
                assertThrows(IllegalArgumentException.class, () -> bad.setInt(blockSize - (Integer.BYTES - 1), 7));
                // double overflow
                assertThrows(IllegalArgumentException.class,
                        () -> bad.setDouble(blockSize - (Double.BYTES - 2), Math.PI));
                // bytes overflow
                assertThrows(IllegalArgumentException.class, () -> bad.setBytes(blockSize - 10, new byte[32]));
                // string overflow: try payload that can't fit with length header
                final String tooLong = "X".repeat(10);
                assertThrows(IllegalArgumentException.class,
                        () -> bad.setString(blockSize - Integer.BYTES - 5, tooLong));
                // null edge cases
                assertThrows(IllegalArgumentException.class, () -> bad.setBytes(0, null));
                assertThrows(IllegalArgumentException.class, () -> bad.setString(0, null));
                // negative offsets
                assertThrows(IllegalArgumentException.class, () -> bad.setInt(-1, 1));
                assertThrows(IllegalArgumentException.class, () -> bad.setBytes(-1, new byte[1]));
                assertThrows(IllegalArgumentException.class, () -> bad.setString(-1, ""));

                // 3) Edge-case positive inputs at exact boundary
                Page edge = new Page(blockSize);
                // int at last valid starting position
                edge.setInt(blockSize - Integer.BYTES, 123);
                // double at last valid starting position
                edge.setDouble(blockSize - Double.BYTES, -7.5);
                // empty string exactly at the end (only length header stored)
                edge.setString(blockSize - Integer.BYTES, "");
                // persist edge block to a different file to avoid overlap
                fm.write(new BlockId(fname + "-edge", idx), edge);

                return null;
            });
        }

        List<Future<Void>> results = exec.invokeAll(tasks);
        for (Future<Void> f : results) {
            f.get();
        }
        exec.shutdownNow();

        // Verify length and content
        assertEquals(nBlocks, fm.length(fname));
        for (int i = 0; i < nBlocks; i++) {
            Page rp = new Page(blockSize);
            fm.read(new BlockId(fname, i), rp);

            final int posStr = 0;
            final int posInt = 128;
            final int posDbl = 256;
            final int posBool = blockSize - 1;
            final int bytesLen = 16;
            final int posBytes = blockSize - 1 - Integer.BYTES - bytesLen;

            assertEquals("rec-" + i, rp.getString(posStr));
            assertEquals(10_000 + i, rp.getInt(posInt));
            assertEquals(i * Math.E, rp.getDouble(posDbl), 1e-9);
            assertEquals((i % 2) == 0, rp.getBoolean(posBool));
            byte[] got = rp.getBytes(posBytes);
            assertEquals(bytesLen, got.length);
            for (int j = 0; j < bytesLen; j++) {
                assertEquals((byte) (i + j), got[j]);
            }
        }

        // 4) Persistence: restart without re-init, data must persist
        FileMgr fm2 = newFm(root, blockSize, false);
        assertEquals(nBlocks, fm2.length(fname));
        for (int i = 0; i < nBlocks; i += 7) { // sample a few blocks
            Page rp = new Page(blockSize);
            fm2.read(new BlockId(fname, i), rp);
            assertEquals("rec-" + i, rp.getString(0));
            assertEquals(10_000 + i, rp.getInt(128));
        }

        // Appending after restart should continue numbering
        BlockIdBase next = fm2.append(fname);
        assertEquals(nBlocks, next.number());
        assertEquals(nBlocks + 1, fm2.length(fname));

        // 5) Re-init: restart with startup=true clears DB
        FileMgr fm3 = newFm(root, blockSize, true);
        assertEquals(0, fm3.length(fname));
    }
}
