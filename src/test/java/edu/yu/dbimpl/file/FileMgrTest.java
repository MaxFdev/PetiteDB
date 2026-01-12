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

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FileMgrTest {

    private static FileMgr newFm(File dir, int blockSize, boolean startup) {
        final Properties dbProps = new Properties();
        dbProps.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(startup));
        DBConfiguration.INSTANCE.get().setConfiguration(dbProps);
        return new FileMgr(dir, blockSize);
    }

    private static void writeSampleRecord(Page p, int stringPos, String s, int intVal, double dblVal, int bytesPos,
            byte[] blob) {
        p.setString(stringPos, s);
        final int size = PageBase.maxLength(s.length());
        final int intPos = stringPos + size;
        p.setInt(intPos, intVal);
        final int dblPos = intPos + Integer.BYTES;
        p.setDouble(dblPos, dblVal);
        final int boolPos = dblPos + Double.BYTES;
        p.setBoolean(boolPos, true);
        p.setBytes(bytesPos, blob);
    }

    @Test
    @Order(0)
    public void constructorAndBlockSizeAndLength() {
        System.out.println("FileMgr constructor & basics:");

        final File root = new File("testing/fm-basics");
        final int blockSize = 256;
        // startup true should create (or re-create) the directory
        FileMgr fm = newFm(root, blockSize, true);
        assertTrue(root.exists() && root.isDirectory());
        assertEquals(blockSize, fm.blockSize());

        // length on non-existent file should be 0
        assertEquals(0, fm.length("no-file-yet"));

        // null arg validations
        Page p = new Page(blockSize);
        assertThrows(IllegalArgumentException.class, () -> fm.read(null, p));
        assertThrows(IllegalArgumentException.class, () -> fm.read(new BlockId("x", 0), null));
        assertThrows(IllegalArgumentException.class, () -> fm.write(null, p));
        assertThrows(IllegalArgumentException.class, () -> fm.write(new BlockId("x", 0), null));
        assertThrows(IllegalArgumentException.class, () -> fm.append(null));
        assertThrows(IllegalArgumentException.class, () -> fm.length(null));
    }

    @Test
    @Order(1)
    public void appendAndLengthGrowth() {
        System.out.println("FileMgr append & length:");

        final File root = new File("testing/fm-append");
        final int blockSize = 256;
        FileMgr fm = newFm(root, blockSize, true);

        final String fname = "fileA";
        assertEquals(0, fm.length(fname));

        // append three blocks, ensure numbering and length grow correctly
        BlockIdBase b0 = fm.append(fname);
        assertEquals(0, b0.number());
        assertEquals(1, fm.length(fname));

        BlockIdBase b1 = fm.append(fname);
        assertEquals(1, b1.number());
        assertEquals(2, fm.length(fname));

        BlockIdBase b2 = fm.append(fname);
        assertEquals(2, b2.number());
        assertEquals(3, fm.length(fname));
    }

    @Test
    @Order(2)
    public void readWriteRoundTripVariousTypes() {
        System.out.println("FileMgr read/write round-trip:");

        final File root = new File("testing/fm-rw");
        final int blockSize = 400; // roomy to store multiple fields
        FileMgr fm = newFm(root, blockSize, true);

        final String fname = "datafile";
        final BlockId blk = new BlockId(fname, 0);

        // prepare a page with a variety of types
        final String s = "hello world";
        final int stringPos = 32;
        final int intVal = 4242;
        final double dblVal = Math.PI;
        final byte[] blob = new byte[] { 9, 8, 7, 6, 5 };
        final int bytesPos = 200;

        final Page p1 = new Page(blockSize);
        writeSampleRecord(p1, stringPos, s, intVal, dblVal, bytesPos, blob);

        // write and then read into a fresh page
        fm.write(blk, p1);
        assertEquals(1, fm.length(fname));

        final Page p2 = new Page(blockSize);
        fm.read(blk, p2);

        assertEquals(s, p2.getString(stringPos));
        assertEquals(intVal, p2.getInt(stringPos + PageBase.maxLength(s.length())));
        assertEquals(dblVal, p2.getDouble(stringPos + PageBase.maxLength(s.length()) + Integer.BYTES), 1e-9);
        assertTrue(p2.getBoolean(stringPos + PageBase.maxLength(s.length()) + Integer.BYTES + Double.BYTES));
        assertArrayEquals(blob, p2.getBytes(bytesPos));
    }

    @Test
    @Order(3)
    public void persistenceAcrossRestartsAndReinit() {
        System.out.println("FileMgr persistence across restarts & reinit:");

        final File root = new File("testing/fm-persist");
        final int blockSize = 256;
        final String fname = "persistfile";
        final BlockId blk0 = new BlockId(fname, 0);

        // First startup: new DB, write data
        FileMgr fm1 = newFm(root, blockSize, true);
        Page p1 = new Page(blockSize);
        final String s = "persist";
        p1.setString(0, s);
        fm1.write(blk0, p1);
        assertEquals(1, fm1.length(fname));

        // Restart with startup=false: data should persist
        FileMgr fm2 = newFm(root, blockSize, false);
        assertEquals(1, fm2.length(fname));
        Page p2 = new Page(blockSize);
        fm2.read(blk0, p2);
        assertEquals(s, p2.getString(0));

        // Restart with startup=true: DB should be reinitialized (emptied)
        FileMgr fm3 = newFm(root, blockSize, true);
        assertEquals(0, fm3.length(fname));
    }

    @Test
    @Order(4)
    public void concurrentWritesAndReads() throws InterruptedException, ExecutionException {
        System.out.println("FileMgr concurrency smoke test:");

        final File root = new File("testing/fm-concurrent");
        final int blockSize = 256;
        final String fname = "cfile";
        final int nBlocks = 20;

        FileMgr fm = newFm(root, blockSize, true);

        ExecutorService exec = Executors.newFixedThreadPool(8);
        List<Callable<Void>> tasks = new ArrayList<>();

        // Prepare concurrent writers for different blocks
        for (int i = 0; i < nBlocks; i++) {
            final int idx = i;
            tasks.add(() -> {
                Page p = new Page(blockSize);
                int pos = 8;
                p.setInt(pos, idx);
                p.setDouble(pos + Integer.BYTES, idx * 1.5);
                p.setBoolean(pos + Integer.BYTES + Double.BYTES, (idx % 2) == 0);
                fm.write(new BlockId(fname, idx), p);
                return null;
            });
        }

        // execute and wait
        List<Future<Void>> results = exec.invokeAll(tasks);
        for (Future<Void> f : results) {
            f.get();
        }

        // Verify length and a few spot checks
        assertEquals(nBlocks, fm.length(fname));

        for (int i = 0; i < nBlocks; i += 5) {
            Page rp = new Page(blockSize);
            fm.read(new BlockId(fname, i), rp);
            int pos = 8;
            assertEquals(i, rp.getInt(pos));
            assertEquals(i * 1.5, rp.getDouble(pos + Integer.BYTES), 1e-9);
            assertEquals((i % 2) == 0, rp.getBoolean(pos + Integer.BYTES + Double.BYTES));
        }

        exec.shutdownNow();
    }

    @Test
    @Order(5)
    public void sparseWriteGrowsLengthAndIsReadable() {
        System.out.println("FileMgr sparse write grows length:");

        final File root = new File("testing/fm-sparse-write");
        final int blockSize = 256;
        final String fname = "sparsefile";

        FileMgr fm = newFm(root, blockSize, true);

        // Write to a block beyond current length (e.g., block #5)
        final int targetBlockNum = 5;
        final BlockId targetBlk = new BlockId(fname, targetBlockNum);

        Page p = new Page(blockSize);
        final int pos = 12;
        final String s = "sparse";
        p.setString(pos, s);
        p.setInt(pos + PageBase.maxLength(s.length()), 777);

        fm.write(targetBlk, p);

        // Length should reflect highest written block index + 1
        assertEquals(targetBlockNum + 1, fm.length(fname));

        // Read back and verify content
        Page rp = new Page(blockSize);
        fm.read(targetBlk, rp);
        assertEquals(s, rp.getString(pos));
        assertEquals(777, rp.getInt(pos + PageBase.maxLength(s.length())));
    }

    @Test
    @Order(6)
    public void appendAcrossRestartsRespectsExistingLength() {
        System.out.println("FileMgr append across restarts:");

        final File root = new File("testing/fm-append-restart");
        final int blockSize = 256;
        final String fname = "afile";

        FileMgr fm1 = newFm(root, blockSize, true);
        // append three blocks
        assertEquals(0, fm1.length(fname));
        assertEquals(0, fm1.append(fname).number());
        assertEquals(1, fm1.append(fname).number());
        assertEquals(2, fm1.append(fname).number());
        assertEquals(3, fm1.length(fname));

        // Restart without reinit; next append should continue numbering
        FileMgr fm2 = newFm(root, blockSize, false);
        BlockIdBase next = fm2.append(fname);
        assertEquals(3, next.number());
        assertEquals(4, fm2.length(fname));
    }

    @Test
    @Order(7)
    public void overwriteSameBlockKeepsLengthAndUpdatesData() {
        System.out.println("FileMgr overwrite block:");

        final File root = new File("testing/fm-overwrite");
        final int blockSize = 256;
        final String fname = "owfile";
        final BlockId blk0 = new BlockId(fname, 0);

        FileMgr fm = newFm(root, blockSize, true);

        Page p1 = new Page(blockSize);
        p1.setInt(4, 111);
        p1.setDouble(8 + Integer.BYTES, 2.5);
        fm.write(blk0, p1);
        assertEquals(1, fm.length(fname));

        // Overwrite with different values
        Page p2 = new Page(blockSize);
        p2.setInt(4, 222);
        p2.setDouble(8 + Integer.BYTES, 9.75);
        fm.write(blk0, p2);

        // Length unchanged
        assertEquals(1, fm.length(fname));

        // Verify updated data
        Page rp = new Page(blockSize);
        fm.read(blk0, rp);
        assertEquals(222, rp.getInt(4));
        assertEquals(9.75, rp.getDouble(8 + Integer.BYTES), 1e-9);
    }

    @Test
    @Order(8)
    public void multipleFilesAreIsolated() {
        System.out.println("FileMgr isolation across files:");

        final File root = new File("testing/fm-multifile");
        final int blockSize = 256;

        FileMgr fm = newFm(root, blockSize, true);

        final String fA = "fileA";
        final String fB = "fileB";

        Page a0 = new Page(blockSize);
        a0.setString(0, "A0");
        fm.write(new BlockId(fA, 0), a0);

        Page b0 = new Page(blockSize);
        b0.setString(0, "B0");
        fm.write(new BlockId(fB, 0), b0);

        Page b1 = new Page(blockSize);
        b1.setString(0, "B1");
        fm.write(new BlockId(fB, 1), b1);

        assertEquals(1, fm.length(fA));
        assertEquals(2, fm.length(fB));

        Page ar = new Page(blockSize);
        fm.read(new BlockId(fA, 0), ar);
        assertEquals("A0", ar.getString(0));

        Page br0 = new Page(blockSize);
        fm.read(new BlockId(fB, 0), br0);
        assertEquals("B0", br0.getString(0));

        Page br1 = new Page(blockSize);
        fm.read(new BlockId(fB, 1), br1);
        assertEquals("B1", br1.getString(0));
    }

    @Test
    @Order(9)
    public void boundaryPositionsWithinBlock() {
        System.out.println("FileMgr boundary writes:");

        final File root = new File("testing/fm-boundary");
        final int blockSize = 256;
        final String fname = "bfile";
        final BlockId blk = new BlockId(fname, 0);

        FileMgr fm = newFm(root, blockSize, true);

        Page p = new Page(blockSize);
        // Place values near the end of the block but not overlapping
        final int posBool = blockSize - 1; // boolean assumed 1 byte in Page
        final int posInt = blockSize - Integer.BYTES - 32; // some space before the end
        final int posDbl = blockSize - Double.BYTES - 16; // ensure no overlap

        p.setBoolean(posBool, true);
        p.setInt(posInt, 123456);
        p.setDouble(posDbl, -3.14159);

        fm.write(blk, p);

        Page r = new Page(blockSize);
        fm.read(blk, r);
        assertTrue(r.getBoolean(posBool));
        assertEquals(123456, r.getInt(posInt));
        assertEquals(-3.14159, r.getDouble(posDbl), 1e-9);
    }

    @Test
    @Order(10)
    public void concurrentAppendsProduceUniqueSequentialBlocks() throws InterruptedException, ExecutionException {
        System.out.println("FileMgr concurrent append:");

        final File root = new File("testing/fm-concurrent-append");
        final int blockSize = 8;
        final String fname = "cafile";
        final int nAppenders = 32;

        FileMgr fm = newFm(root, blockSize, true);

        ExecutorService exec = Executors.newFixedThreadPool(16);
        List<Callable<BlockIdBase>> tasks = new ArrayList<>();

        for (int i = 0; i < nAppenders; i++) {
            tasks.add(() -> {
                BlockIdBase blk = fm.append(fname);
                // Write the thread's id to its block
                Page p = new Page(blockSize);
                p.setInt(0, blk.number());
                fm.write(blk, p);
                return blk;
            });
        }

        List<Future<BlockIdBase>> results = exec.invokeAll(tasks);
        exec.shutdown();
        exec.shutdownNow();

        // Collect numbers and ensure uniqueness and correct length
        boolean[] seen = new boolean[nAppenders];
        for (Future<BlockIdBase> f : results) {
            BlockIdBase b = f.get();
            int n = b.number();
            assertTrue(n >= 0 && n < nAppenders, "unexpected block number: " + n);
            assertFalse(seen[n], "duplicate block number: " + n);
            seen[n] = true;
        }

        assertEquals(nAppenders, fm.length(fname));

        // Check blocks for correctness
        for (int i = 0; i < nAppenders; i++) {
            Page r = new Page(blockSize);
            fm.read(new BlockId(fname, i), r);
            assertEquals(i, r.getInt(0));
        }
    }
}

/**
 * Caching concept: private class RAFEntry {
 * 
 * private final ReentrantReadWriteLock useLock; private final AtomicInteger
 * requests; private final AtomicBoolean present; private volatile
 * RandomAccessFile raf;
 * 
 * private RAFEntry() { this.useLock = new ReentrantReadWriteLock();
 * this.requests = new AtomicInteger(); this.present = new AtomicBoolean(); }
 * 
 * private RandomAccessFile access(File file) throws FileNotFoundException {
 * this.requests.incrementAndGet(); if (this.present.compareAndSet(true, false))
 * { RandomAccessFile lend = this.raf; this.raf = null; return lend; } else {
 * return new RandomAccessFile(file, MODE); } }
 * 
 * private void lendOrClose(RandomAccessFile outerRaf) throws IOException { if
 * (requests.decrementAndGet() > 0) { this.raf = outerRaf; this.raf.seek(0);
 * this.present.set(true); } else { outerRaf.close(); } } }
 */
