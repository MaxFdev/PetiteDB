package edu.yu.dbimpl.log;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;

/**
 * Additional edge and stress tests for LogMgr to increase branch and scenario coverage.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LogEdgeCasesTest {

    private static final int BLOCK_SIZE = 256; // keep consistent with other tests

    private static void rmrf(String root) {
        final Path p = Paths.get(root);
        if (!Files.exists(p)) return;
        try (var walk = Files.walk(p)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try { Files.deleteIfExists(path); } catch (IOException ignore) {}
            });
        } catch (IOException ignore) {}
    }

    private static FileMgrBase freshFileMgr(String dir) {
        rmrf(dir);
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);
        return new FileMgr(new File(dir), BLOCK_SIZE);
    }

    private static FileMgrBase restartFileMgr(String dir) {
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
        DBConfiguration.INSTANCE.get().setConfiguration(props);
        return new FileMgr(new File(dir), BLOCK_SIZE);
    }

    private static byte[] ints(int... vals) {
        byte[] out = new byte[4 * vals.length];
        int i = 0;
        for (int v : vals) {
            out[i++] = (byte) (v >>> 24);
            out[i++] = (byte) (v >>> 16);
            out[i++] = (byte) (v >>> 8);
            out[i++] = (byte) v;
        }
        return out;
    }

    // 1) Iterator on a freshly initialized DB with no records: should be empty and next() throws
    @Test
    @Order(1)
    public void iteratorOnFreshEmptyDb() {
        final String dir = "testing/logmgr_edge_empty_iter";
        FileMgrBase fm = freshFileMgr(dir);
        LogMgr lm = new LogMgr(fm, "logEmpty");

        Iterator<byte[]> it = lm.iterator();
        assertFalse(it.hasNext(), "Iterator should be empty on a fresh DB with no records");
        assertThrows(java.util.NoSuchElementException.class, it::next);
    }

    // 2) Zero-length record append: allowed, returned and preserved in reverse order
    @Test
    @Order(2)
    public void zeroLengthRecordsAppendAndIterate() {
        final String dir = "testing/logmgr_edge_zero_length";
        FileMgrBase fm = freshFileMgr(dir);
        LogMgr lm = new LogMgr(fm, "logZero");

        int l0 = lm.append(new byte[0]);
        int l1 = lm.append(new byte[0]);
        assertEquals(0, l0);
        assertEquals(1, l1);

        Iterator<byte[]> it = lm.iterator();
        assertTrue(it.hasNext());
        assertEquals(0, it.next().length, "Most recent zero-length record first");
        assertTrue(it.hasNext());
        assertEquals(0, it.next().length, "Older zero-length record second");
        assertFalse(it.hasNext());
    }

    // 3) Multi-block stress + restart: write many small records to span multiple blocks, restart, then iterate
    @Test
    @Order(3)
    public void multiBlockReverseIterationAfterRestart() {
        final String dir = "testing/logmgr_edge_multiblock_restart";
        // initial population
        FileMgrBase fmA = freshFileMgr(dir);
        LogMgr lmA = new LogMgr(fmA, "logMulti");
        final int N = 50; // with 4-byte payloads, this should span multiple 256B blocks
        for (int i = 0; i < N; i++) {
            lmA.append(ints(i));
        }
        // make sure everything is durable prior to restart
        lmA.flush(N - 1);

        // restart and iterate
        FileMgrBase fmB = restartFileMgr(dir);
        LogMgr lmB = new LogMgr(fmB, "logMulti");
        Iterator<byte[]> it = lmB.iterator();
        for (int i = N - 1; i >= 0; i--) {
            assertTrue(it.hasNext());
            byte[] rec = it.next();
            assertEquals(4, rec.length);
            int v = ((rec[0] & 0xFF) << 24) | ((rec[1] & 0xFF) << 16) | ((rec[2] & 0xFF) << 8) | (rec[3] & 0xFF);
            assertEquals(i, v, "Iterator should return exact reverse order across blocks after restart");
        }
        assertFalse(it.hasNext());
    }

    // 4) Iterator snapshot isolation: an iterator created before further appends should NOT see new records
    @Test
    @Order(4)
    public void iteratorSnapshotIsolationAcrossAppends() {
        final String dir = "testing/logmgr_edge_snapshot";
        FileMgrBase fm = freshFileMgr(dir);
        LogMgr lm = new LogMgr(fm, "logSnap");

        final int N = 10;
        for (int i = 0; i < N; i++) lm.append(ints(i));
        Iterator<byte[]> it1 = lm.iterator(); // snapshot here

        final int M = 7;
        for (int i = N; i < N + M; i++) lm.append(ints(i));
        Iterator<byte[]> it2 = lm.iterator(); // snapshot after more appends

        // it1 should see only N records: N-1..0
        for (int i = N - 1; i >= 0; i--) {
            assertTrue(it1.hasNext());
            byte[] rec = it1.next();
            int v = ((rec[0] & 0xFF) << 24) | ((rec[1] & 0xFF) << 16) | ((rec[2] & 0xFF) << 8) | (rec[3] & 0xFF);
            assertEquals(i, v);
        }
        assertFalse(it1.hasNext(), "First iterator must not see records appended later");

        // it2 should see N+M records: N+M-1..0
        for (int i = N + M - 1; i >= 0; i--) {
            assertTrue(it2.hasNext());
            byte[] rec = it2.next();
            int v = ((rec[0] & 0xFF) << 24) | ((rec[1] & 0xFF) << 16) | ((rec[2] & 0xFF) << 8) | (rec[3] & 0xFF);
            assertEquals(i, v);
        }
        assertFalse(it2.hasNext());
    }
}
