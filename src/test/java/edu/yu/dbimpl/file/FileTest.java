package edu.yu.dbimpl.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import edu.yu.dbimpl.config.DBConfiguration;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FileTest {

    @Test
    @Order(0)
    public void BlockIdTests() {
        System.out.println("BlockId tests:");

        System.out.print("Testing illegal inits - ");
        assertThrows(IllegalArgumentException.class, () -> new BlockId(null, 0));
        assertThrows(IllegalArgumentException.class, () -> new BlockId("  ", 0));
        assertThrows(IllegalArgumentException.class, () -> new BlockId("test", -1));
        assertEquals("BlockID: [file: test, block: 0]", new BlockId("test", 0).toString());
        System.out.println("Passed");

        System.out.print("Testing valid init & use - ");
        BlockId blk = new BlockId("test", 0);
        assertTrue(blk.fileName().equals("test"));
        assertTrue(blk.number() == 0);
        assertEquals(blk, new BlockId("test", 0));
        assertEquals(blk.hashCode(), new BlockId("test", 0).hashCode());
        assertNotEquals(blk, new BlockId("test2", 1));
        assertNotEquals(blk, new Object());
        System.out.println("Passed");
    }

    @Test
    @Order(1)
    public void PageTest() {
        System.out.println("BlockId tests:");

        // Constructors
        System.out.print("Testing constructors - ");
        assertDoesNotThrow(() -> new Page(64));
        assertDoesNotThrow(() -> new Page(0));
        assertThrows(Exception.class, () -> new Page(-32));
        assertDoesNotThrow(() -> new Page(new byte[64]));
        assertDoesNotThrow(() -> new Page(new byte[0]));
        assertThrows(Exception.class, () -> new Page(null));
        assertThrows(Exception.class, () -> Page.logicalLength(null)); // test static method
        System.out.println("Passed");

        // int get/set
        System.out.print("Testing ints - ");
        Page pI = new Page(32);
        pI.setInt(0, 42);
        assertEquals(42, pI.getInt(0));
        assertThrows(IllegalArgumentException.class, () -> pI.getInt(-1));
        assertThrows(IllegalArgumentException.class, () -> pI.setInt(62, 7)); // 62..65 out of bounds
        System.out.println("Passed");

        // double get/set
        System.out.print("Testing double - ");
        Page pD = new Page(32);
        pD.setDouble(8, Math.PI);
        assertEquals(Math.PI, pD.getDouble(8), 1e-9);
        assertThrows(IllegalArgumentException.class, () -> pD.setDouble(-1, 1.0));
        assertThrows(IllegalArgumentException.class, () -> pD.setDouble(28, 1.0)); // 28..35 out of bounds
        System.out.println("Passed");

        // boolean get/set
        System.out.print("Testing boolean - ");
        Page pB = new Page(8);
        pB.setBoolean(0, true);
        pB.setBoolean(1, false);
        assertTrue(pB.getBoolean(0));
        assertTrue(!pB.getBoolean(1));
        assertThrows(IllegalArgumentException.class, () -> pB.getBoolean(8));
        assertThrows(IllegalArgumentException.class, () -> pB.setBoolean(-2, true));
        System.out.println("Passed");

        // bytes get/set
        System.out.print("Testing bytes - ");
        Page pBytes = new Page(64);
        byte[] data = new byte[] { 1, 2, 3, 4, 5 };
        pBytes.setBytes(10, data);
        assertArrayEquals(data, pBytes.getBytes(10));
        assertThrows(IllegalArgumentException.class, () -> pBytes.setBytes(-1, data));
        assertThrows(IllegalArgumentException.class, () -> pBytes.setBytes(0, null));
        assertThrows(IllegalArgumentException.class, () -> pBytes.setBytes(60, new byte[10])); // would exceed block
        System.out.println("Passed");

        // string get/set
        System.out.print("Testing string - ");
        Page pStr = new Page(128);
        String s = "hello world";
        pStr.setString(0, s);
        assertEquals(s, pStr.getString(0));
        assertThrows(IllegalArgumentException.class, () -> pStr.setString(-5, s));
        assertThrows(IllegalArgumentException.class, () -> pStr.setString(0, null));
        assertThrows(IllegalArgumentException.class, () -> pStr.setString(124, s)); // would exceed block
        System.out.println("Passed");

        // extractData returns a defensive copy
        System.out.print("Testing extract - ");
        Page pCopy = new Page(16);
        pCopy.setInt(0, 123);
        byte[] extracted = pCopy.extractData();
        assertEquals(16, extracted.length);
        extracted[0] ^= 0xFF; // edit the value in the data copy
        assertEquals(123, pCopy.getInt(0)); // ensure edits don't effect the page
        System.out.println("Passed");

        // propagateData clones from a direct ByteBuffer and replaces content
        System.out.print("Testing propagate (buffer) - ");
        ByteBuffer directBuf = ByteBuffer.allocateDirect(16);
        directBuf.putInt(0, 999);
        pCopy.propagateData(directBuf);
        assertEquals(999, pCopy.getInt(0));
        // mutate original buffer after propagation; page content must remain unchanged
        directBuf.putInt(0, 1000);
        assertEquals(999, pCopy.getInt(0));
        System.out.println("Passed");

        // propagateData clones the provided array and replaces content
        System.out.print("Testing propagate (array) - ");
        byte[] newData = new byte[16];
        // encode an int 999 at offset 0
        newData[0] = (byte) ((999 >>> 24) & 0xFF);
        newData[1] = (byte) ((999 >>> 16) & 0xFF);
        newData[2] = (byte) ((999 >>> 8) & 0xFF);
        newData[3] = (byte) (999 & 0xFF);
        pCopy.propagateData(newData);
        assertEquals(999, pCopy.getInt(0));
        // mutate original array after propagation; page content must remain unchanged
        newData[0] ^= 0xFF;
        assertEquals(999, pCopy.getInt(0));
        System.out.println("Passed");
    }

    @Test
    @Order(2)
    public void fileManagerTest() {
        System.out.println("FileMgr single comprehensive test:");

        final String rootDirName = "testing/filetest";
        final File root = new File(rootDirName);
        final int blockSize = 256;

        // No properties provided: should fail
        System.out.println("Initializing DB with NO properties should fail...");
        final Properties emptyProps = new Properties();
        DBConfiguration.INSTANCE.get().setConfiguration(emptyProps);
        assertThrows(IllegalStateException.class, () -> new FileMgr(root, blockSize));
        System.out.println("Passed");

        // Startup: initialize DB fresh
        System.out.println("Initializing DB (startup=true)...");
        final Properties dbProps1 = new Properties();
        dbProps1.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(dbProps1);
        FileMgr fm = new FileMgr(root, blockSize);
        assertEquals(blockSize, fm.blockSize());
        System.out.println("Passed");

        // Basic length on non-existent file
        System.out.println("Length on non-existent file is 0...");
        assertEquals(0, fm.length("nofile"));
        System.out.println("Passed");

        // Null arg validations
        System.out.println("Validating null args...");
        Page nullPage = new Page(blockSize);
        assertThrows(IllegalArgumentException.class, () -> fm.read(null, nullPage));
        assertThrows(IllegalArgumentException.class, () -> fm.read(new BlockId("x", 0), null));
        assertThrows(IllegalArgumentException.class, () -> fm.write(null, nullPage));
        assertThrows(IllegalArgumentException.class, () -> fm.write(new BlockId("x", 0), null));
        assertThrows(IllegalArgumentException.class, () -> fm.append(null));
        assertThrows(IllegalArgumentException.class, () -> fm.length(null));
        System.out.println("Passed");

        // Append and length growth
        System.out.println("Append grows length and numbers sequentially...");
        final String fname = "afile";
        assertEquals(0, fm.length(fname));
        BlockIdBase a0 = fm.append(fname);
        assertEquals(0, a0.number());
        assertEquals(1, fm.length(fname));
        BlockIdBase a1 = fm.append(fname);
        assertEquals(1, a1.number());
        assertEquals(2, fm.length(fname));
        BlockIdBase a2 = fm.append(fname);
        assertEquals(2, a2.number());
        assertEquals(3, fm.length(fname));
        System.out.println("Passed");

        // Write/read a variety of types
        System.out.println("Read/Write round-trip with multiple types...");
        final BlockId rwBlk = new BlockId(fname, 0);
        Page p1 = new Page(blockSize);
        final String s = "hello world";
        final int stringPos = 24;
        final int size = PageBase.maxLength(s.length());
        p1.setString(stringPos, s);
        final int intPos = stringPos + size;
        final int intVal = 4242;
        p1.setInt(intPos, intVal);
        final int dblPos = intPos + Integer.BYTES;
        final double dblVal = Math.E;
        p1.setDouble(dblPos, dblVal);
        final int boolPos = dblPos + Double.BYTES;
        p1.setBoolean(boolPos, true);
        fm.write(rwBlk, p1);

        Page p2 = new Page(blockSize);
        fm.read(rwBlk, p2);
        assertEquals(s, p2.getString(stringPos));
        assertEquals(intVal, p2.getInt(intPos));
        assertEquals(dblVal, p2.getDouble(dblPos), 1e-9);
        assertTrue(p2.getBoolean(boolPos));
        System.out.println("Passed");

        // Overwrite same block and verify length unchanged and data updated
        System.out.println("Overwrite block updates data without changing length...");
        final int beforeLen = fm.length(fname);
        Page pOverwrite = new Page(blockSize);
        pOverwrite.setInt(4, 999);
        pOverwrite.setDouble(8 + Integer.BYTES, -7.5);
        fm.write(rwBlk, pOverwrite);
        assertEquals(beforeLen, fm.length(fname));
        Page pOverRead = new Page(blockSize);
        fm.read(rwBlk, pOverRead);
        assertEquals(999, pOverRead.getInt(4));
        assertEquals(-7.5, pOverRead.getDouble(8 + Integer.BYTES), 1e-9);
        System.out.println("Passed");

        // Multiple files: isolation of lengths and data
        System.out.println("Multiple files are isolated...");
        final String fB = "bfile";
        Page b0 = new Page(blockSize);
        b0.setString(0, "B0");
        fm.write(new BlockId(fB, 0), b0);
        Page b1 = new Page(blockSize);
        b1.setString(0, "B1");
        fm.write(new BlockId(fB, 1), b1);
        assertEquals(3, fm.length(fname));
        assertEquals(2, fm.length(fB));
        Page b0r = new Page(blockSize);
        fm.read(new BlockId(fB, 0), b0r);
        assertEquals("B0", b0r.getString(0));
        System.out.println("Passed");

        // Sparse write grows length to highest index + 1
        System.out.println("Sparse write grows length...");
        final String sparse = "sfile";
        final BlockId sBlk = new BlockId(sparse, 5);
        Page sp = new Page(blockSize);
        sp.setString(12, "sparse");
        sp.setInt(12 + PageBase.maxLength("sparse".length()), 777);
        fm.write(sBlk, sp);
        assertEquals(6, fm.length(sparse));
        Page spr = new Page(blockSize);
        fm.read(sBlk, spr);
        assertEquals("sparse", spr.getString(12));
        assertEquals(777, spr.getInt(12 + PageBase.maxLength("sparse".length())));
        System.out.println("Passed");

        // Boundary writes within a block
        System.out.println("Boundary writes within block...");
        final String bfile = "boundary";
        final BlockId bBlk = new BlockId(bfile, 0);
        Page pb = new Page(blockSize);
        final int posBool = blockSize - 1;
        final int posInt = blockSize - Integer.BYTES - 32;
        final int posDbl = blockSize - Double.BYTES - 16;
        pb.setBoolean(posBool, true);
        pb.setInt(posInt, 123456);
        pb.setDouble(posDbl, -3.14159);
        fm.write(bBlk, pb);
        Page rb = new Page(blockSize);
        fm.read(bBlk, rb);
        assertTrue(rb.getBoolean(posBool));
        assertEquals(123456, rb.getInt(posInt));
        assertEquals(-3.14159, rb.getDouble(posDbl), 1e-9);
        System.out.println("Passed");

        // Restart without re-init: data should persist and appends continue numbering
        System.out.println("Restart (startup=false) preserves data & append numbering...");
        final Properties dbProps2 = new Properties();
        dbProps2.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(false));
        DBConfiguration.INSTANCE.get().setConfiguration(dbProps2);
        FileMgr fm2 = new FileMgr(root, blockSize);
        assertEquals(3, fm2.length(fname));
        BlockIdBase next = fm2.append(fname);
        assertEquals(3, next.number());
        assertEquals(4, fm2.length(fname));
        Page check = new Page(blockSize);
        fm2.read(new BlockId(fname, 0), check);
        assertEquals(999, check.getInt(4));
        System.out.println("Passed");

        // Restart with re-init: DB should be cleared
        System.out.println("Restart (startup=true) reinitializes DB...");
        final Properties dbProps3 = new Properties();
        dbProps3.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
        DBConfiguration.INSTANCE.get().setConfiguration(dbProps3);
        FileMgr fm3 = new FileMgr(root, blockSize);
        assertEquals(0, fm3.length(fname));
        assertEquals(0, fm3.length(fB));
        assertEquals(0, fm3.length(sparse));
        System.out.println("Passed");
    }

}
