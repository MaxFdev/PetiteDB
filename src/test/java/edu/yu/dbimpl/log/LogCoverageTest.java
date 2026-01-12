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
 * Comprehensive branch-coverage tests for LogMgr.
 * Focus: exercise each logic branch across public API and iterator behavior.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LogCoverageTest {

    private static final int BLOCK_SIZE = 256; // small to easily force page-boundary cases

    private static void rmrf(String root) {
        final Path p = Paths.get(root);
        if (!Files.exists(p))
            return;
        try (var walk = Files.walk(p)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignore) {
                }
            });
        } catch (IOException ignore) {
        }
    }

    // Match LogTest semantics: fresh = delete dir + startup=true; restart = startup=false (no delete)
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

    private static byte[] bytesOfLen(int n, byte fill) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++)
            b[i] = fill;
        return b;
    }

    // 1) Constructor branch: startup=true fresh DB, basic append semantics
    @Test
    @Order(1)
    public void ctorStartupFreshAndBasicAppend() {
        final String dir = "testing/logmgr_branch_ctor_fresh";
    FileMgrBase fm = freshFileMgr(dir);
        LogMgr lm = new LogMgr(fm, "logA");

        int l0 = lm.append(ints(1));
        int l1 = lm.append(ints(2));
        assertEquals(0, l0);
        assertEquals(1, l1);
    }

    // 2) Constructor branch: startup=false with NO existing records (header==0 page
    // persisted)
    @Test
    @Order(2)
    public void ctorRestartWithoutRecordsInitializesCleanly() {
        final String dir = "testing/logmgr_branch_ctor_restart_empty";
        // First run: startup=true constructs and persists an empty block with header=0
    FileMgrBase fmA = freshFileMgr(dir);
        new LogMgr(fmA, "logB");
        // Second run: startup=false should scan, see no records, and re-init cleanly
    FileMgrBase fmB = restartFileMgr(dir);
        LogMgr lm = new LogMgr(fmB, "logB");
        // First append after restart-without-records should still be LSN 0
        int l0 = lm.append(ints(42));
        assertEquals(0, l0);
    }

    // 3) Constructor branch: startup=false with existing records found and
    // continued LSN
    @Test
    @Order(3)
    public void ctorRestartWithExistingRecordsContinuesLsn() {
        final String dir = "testing/logmgr_branch_ctor_restart_with_data";
        // Seed data in startup mode
    FileMgrBase fmA = freshFileMgr(dir);
        LogMgr lmA = new LogMgr(fmA, "logC");
        int last = -1;
        for (int i = 0; i < 3; i++)
            last = lmA.append(ints(i));
        lmA.flush(last);

        // Restart
    FileMgrBase fmB = restartFileMgr(dir);
        LogMgr lmB = new LogMgr(fmB, "logC");
        int next = lmB.append(ints(99));
        assertEquals(3, next, "LSN must continue from persisted value after restart");
    }

    // 4) flush(): invalid LSN branches: negative and >= current LSN
    @Test
    @Order(4)
    public void flushInvalidArgumentsThrow() {
        final String dir = "testing/logmgr_branch_flush_invalid";
    FileMgrBase fm = freshFileMgr(dir);
        LogMgr lm = new LogMgr(fm, "logD");
        // lsn >= current lsn (0) -> throws
        assertThrows(IllegalArgumentException.class, () -> lm.flush(0));
        int l0 = lm.append(ints(1));
        assertEquals(0, l0);
        // negative -> throws
        assertThrows(IllegalArgumentException.class, () -> lm.flush(-1));
        // equal to current lsn (1) -> throws
        assertThrows(IllegalArgumentException.class, () -> lm.flush(1));
    }

    // 5) flush(): normal and idempotent/early-return paths
    @Test
    @Order(5)
    public void flushNormalAndIdempotent() {
        final String dir = "testing/logmgr_branch_flush_normal";
    FileMgrBase fm = freshFileMgr(dir);
        LogMgr lm = new LogMgr(fm, "logE");
        int l0 = lm.append(ints(10));
        int l1 = lm.append(ints(11));
        int l2 = lm.append(ints(12));
        assertEquals(0, l0);
        assertEquals(1, l1);
        assertEquals(2, l2);

        // Normal flush to l2
        assertDoesNotThrow(() -> lm.flush(l2));
        // Early return: flushing an older LSN that's already persisted
        assertDoesNotThrow(() -> lm.flush(l1));
    }

    // 6) append(): null and oversize record validation
    @Test
    @Order(6)
    public void appendValidationNullAndOversize() {
        final String dir = "testing/logmgr_branch_append_validation";
    FileMgrBase fm = freshFileMgr(dir);
        LogMgr lm = new LogMgr(fm, "logF");

        assertThrows(IllegalArgumentException.class, () -> lm.append(null));

        // Max-legal payload: blocksize - header(4) - seq(4) - len(4)
        int maxPayload = BLOCK_SIZE - 12;
        int l0 = lm.append(bytesOfLen(maxPayload, (byte) 7));
        assertEquals(0, l0);

        // Oversize by 1 byte
        int tooBig = maxPayload + 1;
        assertThrows(IllegalArgumentException.class, () -> lm.append(bytesOfLen(tooBig, (byte) 1)));
    }

    // 7) append(): force new block path by making first record nearly fill the page
    @Test
    @Order(7)
    public void appendForcesNewBlockWhenPageHasNoSpace() {
        final String dir = "testing/logmgr_branch_append_newblock";
    FileMgrBase fm = freshFileMgr(dir);
        LogMgr lm = new LogMgr(fm, "logG");

        int maxPayload = BLOCK_SIZE - 12; // fills page leaving only header-space
        int l0 = lm.append(bytesOfLen(maxPayload, (byte) 9));
        assertEquals(0, l0);
        // Any additional record must go to a new block
        int l1 = lm.append(bytesOfLen(1, (byte) 1));
        assertEquals(1, l1);

        // Iterator should see both records (reverse order), across blocks
        Iterator<byte[]> it = lm.iterator();
        assertTrue(it.hasNext());
        byte[] first = it.next();
        assertEquals(1, first.length, "Iterator returns only client bytes: the newer 1-byte record first");
        assertTrue(it.hasNext());
        byte[] second = it.next();
        assertEquals(maxPayload, second.length, "Second record should be the large, older one");
        assertFalse(it.hasNext());
    }

    // 8) iterator(): reverse order, full traversal, and NoSuchElementException at
    // end
    @Test
    @Order(8)
    public void iteratorReverseAndEndException() {
        final String dir = "testing/logmgr_branch_iterator";
    FileMgrBase fm = freshFileMgr(dir);
        LogMgr lm = new LogMgr(fm, "logH");

        int N = 5;
        for (int i = 0; i < N; i++)
            lm.append(ints(i));
        Iterator<byte[]> it = lm.iterator();
        // Expect reverse order N-1..0
        for (int i = N - 1; i >= 0; i--) {
            assertTrue(it.hasNext());
            byte[] rec = it.next();
            assertEquals(4, rec.length);
            int v = ((rec[0] & 0xFF) << 24) | ((rec[1] & 0xFF) << 16) | ((rec[2] & 0xFF) << 8) | (rec[3] & 0xFF);
            assertEquals(i, v);
        }
        assertFalse(it.hasNext());
        assertThrows(java.util.NoSuchElementException.class, it::next);
    }
}
