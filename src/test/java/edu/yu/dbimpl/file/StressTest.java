package edu.yu.dbimpl.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import edu.yu.dbimpl.config.DBConfiguration;

public class StressTest {

    private static FileMgr newFm(File dir, int blockSize, boolean startup) {
        final Properties dbProps = new Properties();
        dbProps.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(startup));
        DBConfiguration.INSTANCE.get().setConfiguration(dbProps);
        return new FileMgr(dir, blockSize);
    }

    private static String makeMaxString(final int blockSize, final String prefix) {
        final int stringBytesLen = blockSize - Integer.BYTES; // bytes available for the string payload
        if (prefix == null) {
            throw new IllegalArgumentException("prefix is null");
        }
        if (prefix.length() > stringBytesLen) {
            throw new IllegalArgumentException("prefix too long to fit in block");
        }
        final int fillerLen = stringBytesLen - prefix.length();
        final StringBuilder sb = new StringBuilder(stringBytesLen);
        sb.append(prefix);
        for (int i = 0; i < fillerLen; i++) {
            sb.append('X');
        }
        // ASCII is 1 byte/char, so total chars == total bytes
        assert sb.length() == stringBytesLen;
        return sb.toString();
    }

    @Test
    public void write400BlocksAndReadBack_maxStringPerBlock() {
        final int blockSize = 4096;
        final int nBlocks = 400;
        final File root = new File("testing/fm-stress-4096");
        final String fname = "bigfile";

        // fresh DB init
        FileMgr fm = newFm(root, blockSize, true);
        assertEquals(blockSize, fm.blockSize());

        // Write 400 blocks, each filled to capacity with a String
        for (int i = 0; i < nBlocks; i++) {
            final String prefix = String.format("blk-%03d|", i);
            final String payload = makeMaxString(blockSize, prefix);
            final Page p = new Page(blockSize);
            p.setString(0, payload);
            fm.write(new BlockId(fname, i), p);
        }

        // Verify length
        assertEquals(nBlocks, fm.length(fname));

        // Read all blocks back and sanity-check
        for (int i = 0; i < nBlocks; i++) {
            final String expectedPrefix = String.format("blk-%03d|", i);
            final Page r = new Page(blockSize);
            fm.read(new BlockId(fname, i), r);
            final String got = r.getString(0);
            // should be full payload length (4096 - 4)
            assertEquals(blockSize - Integer.BYTES, got.length());
            assertTrue(got.startsWith(expectedPrefix));
        }
    }
}
