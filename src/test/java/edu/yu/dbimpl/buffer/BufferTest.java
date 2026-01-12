package edu.yu.dbimpl.buffer;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.PageBase;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BufferTest {

    private static final int BLOCK_SIZE = 256;

    private static void deleteDirectoryIfExists(String rootDir) {
        try {
            Path p = Path.of(rootDir);
            if (Files.exists(p)) {
                Files.walk(p)
                        .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static FileMgrBase freshFileMgr(String dir, int blockSize) {
        deleteDirectoryIfExists(dir);
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(props);
        return new FileMgr(new File(dir), blockSize);
    }

    private static FileMgrBase restartFileMgr(String dir, int blockSize) {
        final Properties props = new Properties();
        props.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
        DBConfiguration.INSTANCE.get().setConfiguration(props);
        return new FileMgr(new File(dir), blockSize);
    }

    private static int appendDummyLSN(LogMgrBase lm) {
        // Return a valid LSN by appending a small record
        return ((LogMgr) lm).append(new byte[] { 1 });
    }

    @Test
    @Order(0)
    public void buffer_method_correctness() {
        final String dir = "testing/buffer/methods";
        FileMgrBase fm = freshFileMgr(dir, BLOCK_SIZE);
        LogMgrBase lm = new LogMgr(fm, "buffer_log");

        Buffer buf = new Buffer(fm, lm);

        // contents() non-null and sized
        assertNotNull(buf.contents());

        // block() initially null
        assertNull(buf.block());

        // setModified validations
        assertThrows(IllegalArgumentException.class, () -> buf.setModified(-1, -1));
        // No records yet: lsn=0 should be invalid
        assertThrows(IllegalArgumentException.class, () -> buf.setModified(1, 0));
        // Valid LSN after appending
        int lsn = appendDummyLSN(lm);
        assertDoesNotThrow(() -> buf.setModified(1, lsn));
        // -1 LSN is explicitly allowed (no log record case)
        assertDoesNotThrow(() -> buf.setModified(1, -1));

        // Pin modification behavior
        // Initially unpinned
        assertFalse(buf.isPinned());
        // Increment pins -> pinned
        assertTrue(buf.modifyPinAndCheckIfPinned(Buffer.PinModificationMode.INCREMENT));
        assertTrue(buf.isPinned());
        // Increment again -> still pinned
        assertTrue(buf.modifyPinAndCheckIfPinned(Buffer.PinModificationMode.INCREMENT));
        assertTrue(buf.isPinned());
        // Decrement -> still pinned (pin count now 1)
        assertTrue(buf.modifyPinAndCheckIfPinned(Buffer.PinModificationMode.DECREMENT));
        assertTrue(buf.isPinned());
        // Decrement to zero -> now unpinned and canAddPin set to false
        assertFalse(buf.modifyPinAndCheckIfPinned(Buffer.PinModificationMode.DECREMENT));
        assertFalse(buf.isPinned());
        assertFalse(buf.canAddPin());
        // Can't decrement when already at zero
        assertThrows(IllegalStateException.class,
                () -> buf.modifyPinAndCheckIfPinned(Buffer.PinModificationMode.DECREMENT));

        // updateBufferData should associate the block and load data (initially zeros)
        BlockIdBase blk = new BlockId("buffile", 0);
        buf.updateBufferData(blk);
        assertEquals(blk, buf.block());
        // Verify page is usable
        PageBase p = buf.contents();
        assertDoesNotThrow(() -> p.setInt(0, 1234));
    }

    @Test
    @Order(1)
    public void buffer_flush_and_persistence() {
        final String dir = "testing/buffer/flush";
        final int pos = 64;
        final int value = 4242;

        FileMgrBase fm = freshFileMgr(dir, BLOCK_SIZE);
        LogMgrBase lm = new LogMgr(fm, "buffer_log");
        Buffer buf = new Buffer(fm, lm);

        BlockId blk = new BlockId("data", 1);
        buf.updateBufferData(blk);
        PageBase page = buf.contents();

        // Write and flush
        page.setInt(pos, value);
        int lsn = appendDummyLSN(lm);
        buf.setModified(7, lsn);
        buf.flush(7);

        // Restart and verify persisted content
        FileMgrBase fm2 = restartFileMgr(dir, BLOCK_SIZE);
        PageBase check = new edu.yu.dbimpl.file.Page(BLOCK_SIZE);
        fm2.read(blk, check);
        assertEquals(value, check.getInt(pos));

        // Verify flush(tx) writes only matching tx
        // Modify in-memory to a new value but tag with different tx
        final int otherVal = 777;
        buf.updateBufferData(blk); // reload
        buf.contents().setInt(pos, otherVal);
        int lsn2 = appendDummyLSN(lm);
        buf.setModified(99, lsn2);
        // Flush for a non-matching tx (should be no-op)
        buf.flush(5);

        // Restart and verify value didn't change to otherVal
        FileMgrBase fm3 = restartFileMgr(dir, BLOCK_SIZE);
        PageBase check2 = new edu.yu.dbimpl.file.Page(BLOCK_SIZE);
        fm3.read(blk, check2);
        assertEquals(value, check2.getInt(pos));

        // Now flush with matching tx and verify persisted
        buf.flush(99);
        FileMgrBase fm4 = restartFileMgr(dir, BLOCK_SIZE);
        PageBase check3 = new edu.yu.dbimpl.file.Page(BLOCK_SIZE);
        fm4.read(blk, check3);
        assertEquals(otherVal, check3.getInt(pos));
    }

    @Test
    @Order(2)
    public void buffermgr_basic_pin_unpin_and_flushAll() {
        final String dir = "testing/buffermgr/basic";
        final int bufferSize = 2;
        final int maxWaitMs = 200;
        final int pos = 8;

        FileMgrBase fm = freshFileMgr(dir, BLOCK_SIZE);
        LogMgrBase lm = new LogMgr(fm, "buffer_log");
        BufferMgrBase bm = new BufferMgr(fm, lm, bufferSize, maxWaitMs);

        assertEquals(bufferSize, bm.available());

        BlockId b1 = new BlockId("tbl", 1);
        BlockId b2 = new BlockId("tbl", 2);

        BufferBase buf1 = bm.pin(b1);
        assertEquals(bufferSize - 1, bm.available());

        // Pin same block again: shouldn't consume additional permit
        BufferBase buf1b = bm.pin(b1);
        assertSame(buf1, buf1b);
        assertEquals(bufferSize - 1, bm.available());

        // Pin a second distinct block
        BufferBase buf2 = bm.pin(b2);
        assertEquals(0, bm.available());

        // Unpin once for b1: still pinned due to double-pin
        bm.unpin(buf1);
        assertEquals(0, bm.available());
        // Unpin again for b1: now released
        bm.unpin(buf1b);
        assertEquals(1, bm.available());

        // FlushAll only writes pages modified by given tx
        // Modify buf2 with tx=10 and a known value
        int lsn = appendDummyLSN(lm);
        int value = 13579;
        buf2.contents().setInt(pos, value);
        buf2.setModified(10, lsn);

        // Modify buf1 with tx=20 and different value
        BufferBase buf1c = bm.pin(b1);
        int lsn2 = appendDummyLSN(lm);
        int otherValue = 24680;
        buf1c.contents().setInt(pos, otherValue);
        buf1c.setModified(20, lsn2);

        // Flush only tx=10
        bm.flushAll(10);

        // Restart and verify: b2 persisted, b1 not yet
        FileMgrBase fm2 = restartFileMgr(dir, BLOCK_SIZE);
        PageBase check = new edu.yu.dbimpl.file.Page(BLOCK_SIZE);
        fm2.read(b2, check);
        assertEquals(value, check.getInt(pos));

        PageBase checkB1 = new edu.yu.dbimpl.file.Page(BLOCK_SIZE);
        fm2.read(b1, checkB1);
        // If not previously written, default may be 0
        // Ensure it is NOT equal to the otherValue we haven't flushed
        assertNotEquals(otherValue, checkB1.getInt(pos));

        // Now flush tx=20 and verify persisted
        bm.flushAll(20);
        FileMgrBase fm3 = restartFileMgr(dir, BLOCK_SIZE);
        PageBase checkB1b = new edu.yu.dbimpl.file.Page(BLOCK_SIZE);
        fm3.read(b1, checkB1b);
        assertEquals(otherValue, checkB1b.getInt(pos));
    }

    @Test
    @Order(3)
    public void buffermgr_timeout_abort_when_no_buffers_available() {
        final String dir = "testing/buffermgr/timeout";
        final int bufferSize = 1;
        final int maxWaitMs = 100; // keep small so test remains fast

        FileMgrBase fm = freshFileMgr(dir, BLOCK_SIZE);
        LogMgrBase lm = new LogMgr(fm, "buffer_log");
        BufferMgr bm = new BufferMgr(fm, lm, bufferSize, maxWaitMs);

        BlockId b1 = new BlockId("file", 1);
        BlockId b2 = new BlockId("file", 2);

        BufferBase first = bm.pin(b1);
        assertNotNull(first);
        assertEquals(0, bm.available());

        // Now try to pin a different block with pool exhausted: expect timeout/abort
        assertThrows(edu.yu.dbimpl.buffer.BufferAbortException.class, () -> bm.pin(b2));

        // Cleanup: unpin to release
        bm.unpin(first);
        assertEquals(1, bm.available());
    }

    @Test
    @Order(4)
    public void system_end_to_end_eviction_and_persistence() {
        final String dir = "testing/buffermgr/system";
        final int bufferSize = 3;
        final int maxWaitMs = 500;
        final String dbFile = "sysfile";
        final int pos = 80;

        FileMgrBase fm = freshFileMgr(dir, BLOCK_SIZE);
        LogMgrBase lm = new LogMgr(fm, "buffer_log");
        BufferMgrBase bm = new BufferMgr(fm, lm, bufferSize, maxWaitMs);

        BlockId block1 = new BlockId(dbFile, 1);
        BufferBase buffer1 = bm.pin(block1);
        int n0 = buffer1.contents().getInt(pos); // default 0 on fresh DB
        buffer1.contents().setInt(pos, n0 + 1);
        int lsn = appendDummyLSN(lm);
        buffer1.setModified(1, lsn);
        bm.unpin(buffer1); // allow eviction/flush when needed

        // Pin three new blocks to force eviction of block1 if needed
        BufferBase b2 = bm.pin(new BlockId(dbFile, 2));
        BufferBase b3 = bm.pin(new BlockId(dbFile, 3));
        BufferBase b4 = bm.pin(new BlockId(dbFile, 4));
        assertNotNull(b3);
        assertNotNull(b4);

        // Unpin one and repin block1, then verify value via a new read
        bm.unpin(b2);
        BufferBase buffer1Again = bm.pin(block1);
        assertEquals(n0 + 1, buffer1Again.contents().getInt(pos));

        // Restart the whole stack and verify value persisted on disk
        FileMgrBase fm2 = restartFileMgr(dir, BLOCK_SIZE);
        PageBase check = new edu.yu.dbimpl.file.Page(BLOCK_SIZE);
        fm2.read(block1, check);
        assertEquals(n0 + 1, check.getInt(pos));
    }

    @Test
    @Order(5)
    public void BufferCoverageTest() {
        final String dir = "testing/buffer/coverage";
        FileMgrBase fm = freshFileMgr(dir, BLOCK_SIZE);
        LogMgrBase lm = new LogMgr(fm, "buffer_log");

        Buffer buf = new Buffer(fm, lm);

        // Basic object methods: toString, hashCode, equals
        assertNotNull(buf.toString());
        int hc = buf.hashCode();
        assertNotEquals(0, hc); // page-based hashCode should be non-zero
        assertTrue(buf.equals(buf));
        assertFalse(buf.equals(null));
        assertFalse(buf.equals(new Object()));
        // Another buffer: just invoke equals for coverage without asserting specific
        // semantics
        Buffer otherBuf = new Buffer(fm, lm);
        buf.equals(otherBuf);

        // canAddPin setter/getter toggling
        assertTrue(buf.canAddPin());
        buf.setCanAddPin(false);
        assertFalse(buf.canAddPin());
        buf.setCanAddPin(true);
        assertTrue(buf.canAddPin());

        // Pin state transitions + isPinned
        assertFalse(buf.isPinned());
        assertTrue(buf.modifyPinAndCheckIfPinned(Buffer.PinModificationMode.INCREMENT));
        assertTrue(buf.isPinned());
        assertTrue(buf.modifyPinAndCheckIfPinned(Buffer.PinModificationMode.INCREMENT));
        assertTrue(buf.isPinned());
        assertTrue(buf.modifyPinAndCheckIfPinned(Buffer.PinModificationMode.DECREMENT));
        assertTrue(buf.isPinned());
        assertFalse(buf.modifyPinAndCheckIfPinned(Buffer.PinModificationMode.DECREMENT));
        assertFalse(buf.isPinned());
        assertFalse(buf.canAddPin());

        // contents() and block() exercises
        assertNotNull(buf.contents());
        assertNull(buf.block());

        // Load a block -> updateBufferData and check association
        BlockIdBase b1 = new BlockId("cov", 1);
        buf.updateBufferData(b1);
        assertEquals(b1, buf.block());

        // Trigger private flush() via updateBufferData path:
        // mark modified with a valid LSN, then load a different block
        int lsn1 = appendDummyLSN(lm);
        buf.setModified(42, lsn1);
        BlockIdBase b2 = new BlockId("cov", 2);
        buf.updateBufferData(b2); // should flush b1 before reading b2
        assertEquals(b2, buf.block());

        // Exercise flush(int) no-op path (tx mismatch)
        int lsn2 = appendDummyLSN(lm);
        buf.setModified(7, lsn2);
        // Mismatch
        buf.flush(99);
        // Matching -> actually flush
        buf.flush(7);
    }
}
