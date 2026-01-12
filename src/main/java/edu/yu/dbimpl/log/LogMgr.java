package edu.yu.dbimpl.log;

import java.util.Iterator;
import java.util.NoSuchElementException;

import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.Page;
import edu.yu.dbimpl.file.PageBase;

/*
 * logical log record structure:
 * - Page = [position of last record, {space left for records}, last record, ..., earliest record]
 * - record = [sequence number, length (automatically written by Page API), record data]
 */

public class LogMgr extends LogMgrBase {

    private final FileMgrBase fm;
    private final int blocksize;
    private final String logfile;
    private final Page page;
    private int currentBlock;
    private int pagePointer;
    private int persistedLsn;
    private int lsn;

    public LogMgr(FileMgrBase fm, String logfile) {
        super(fm, logfile);

        // check config to see if lsn already exists
        boolean startup;
        try {
            startup = DBConfiguration.INSTANCE.isDBStartup();
        } catch (Exception e) {
            // throw IllegalStateException if db config can't supply info
            throw new IllegalStateException("Config couldn't supply startup info", e);
        }

        this.fm = fm;
        this.blocksize = this.fm.blockSize();
        this.logfile = logfile;
        this.page = new Page(this.blocksize);

        if (!startup) {
            // check each page for the last record
            int block = this.fm.length(this.logfile) - 1;
            while (block >= 0) {
                // read page and get header
                this.fm.read(new BlockId(this.logfile, block), this.page);
                int lastRecordPointer = this.page.getInt(0);

                // verify the header
                if (lastRecordPointer > 0) {
                    // set the current block, pagePointer, persistedLsn, and lsn
                    this.currentBlock = block;
                    this.pagePointer = lastRecordPointer;
                    this.persistedLsn = this.page.getInt(lastRecordPointer);
                    this.lsn = this.persistedLsn + 1;
                    return;
                }

                block--;
            }
        }

        // initialize header pointer
        this.page.setInt(0, 0);
        this.pagePointer = this.blocksize;
        this.currentBlock = 0;
        this.fm.write(new BlockId(this.logfile, this.currentBlock), this.page);

        // set up lsn trackers
        this.persistedLsn = -1;
        this.lsn = 0;
    }

    @Override
    public synchronized void flush(int lsn) {
        // check if the record exists
        if (lsn < 0 || lsn >= this.lsn) {
            throw new IllegalArgumentException("LSN doesn't match any existing record");
        }

        // check if flush needs to be performed
        if (this.persistedLsn >= lsn) {
            return;
        }

        // get the highest lsn in the page
        int lastConfirmedLsn = this.page.getInt(this.page.getInt(0));

        // flush the page to disk
        this.fm.write(new BlockId(this.logfile, this.currentBlock), this.page);

        // increment flushed lsn
        this.persistedLsn = lastConfirmedLsn;
    }

    @Override
    public synchronized Iterator<byte[]> iterator() {
        // flush before retrieving records
        if (this.lsn > 0) {
            flush(this.lsn - 1);
        }

        // copy the current page
        Page page = new Page(this.blocksize);
        page.propagateData(this.page.extractData());
        return new LogRecordIterator(this.fm, this.logfile, page);
    }

    @Override
    public synchronized int append(byte[] logrec) {
        if (logrec == null) {
            throw new IllegalArgumentException("Log record was null");
        }

        // check the record size
        int logicalSize = logicalSize(logrec.length);
        if (logicalSize + Integer.BYTES > this.blocksize) {
            throw new IllegalArgumentException("Log record is too large to fit in a block");
        }

        // check if there is space for the record on this page
        if (this.pagePointer - logicalSize < Integer.BYTES) {
            flush(this.lsn - 1);
            this.currentBlock++;
            this.page.propagateData(new byte[this.blocksize]);
            this.pagePointer = this.blocksize;
        }

        // append the log record to the page
        int writePtr = this.pagePointer - logicalSize;
        int sn = this.lsn++;
        this.page.setInt(writePtr, sn);
        this.page.setBytes(writePtr + Integer.BYTES, logrec);
        this.page.setInt(0, writePtr);
        this.pagePointer = writePtr;
        return sn;
    }

    /**
     * Check if the lsn is valid. An lsn if valid iff it is less than the current
     * lsn (which doesn't have an associated log).
     * 
     * @param lsn
     * @return true iff lsn is less than current lsn
     */
    public boolean isValidLsn(int lsn) {
        return lsn < this.lsn;
    }

    /**
     * Calculate the logical disk size for a record. Size is based on: the length of
     * the record's byte array + an integer for the sequence number + an integer
     * for the size of the byte array.
     * 
     * @param logrecLength
     * @return logical size or the record (in bytes)
     */
    private static int logicalSize(int logrecLength) {
        return logrecLength + (2 * Integer.BYTES);
    }

    private class LogRecordIterator implements Iterator<byte[]> {

        private final FileMgrBase fm;
        private final String logfile;
        private final PageBase page;
        private final int blocksize;
        private int block;
        private int position;
        private int lsn;

        public LogRecordIterator(FileMgrBase fm, String logfile, PageBase page) {
            this.fm = fm;
            this.logfile = logfile;
            this.page = page;
            this.blocksize = this.fm.blockSize();
            this.block = this.fm.length(this.logfile) - 1;
            this.position = this.page.getInt(0);

            // check if the position is valid
            if (this.position == 0) {
                this.lsn = -1;
            } else {
                this.lsn = this.page.getInt(this.position) + 1;
            }
        }

        @Override
        public boolean hasNext() {
            return this.lsn > 0;
        }

        @Override
        public byte[] next() {
            // check if there is a next
            if (!hasNext()) {
                throw new NoSuchElementException("No more log records");
            }

            // check if the next page needs to be loaded
            if (this.position + Integer.BYTES > this.blocksize && this.block >= 0) {
                this.block--;
                this.fm.read(new BlockId(this.logfile, this.block), this.page);
                this.position = this.page.getInt(0);
            }

            // retrieve the next log record
            this.lsn = this.page.getInt(this.position);
            byte[] logrec = this.page.getBytes(this.position + Integer.BYTES);
            this.position += this.page.getInt(this.position + Integer.BYTES) + (2 * Integer.BYTES);
            return logrec;
        }

    }

}
