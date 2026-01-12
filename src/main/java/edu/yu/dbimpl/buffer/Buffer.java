package edu.yu.dbimpl.buffer;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.Page;
import edu.yu.dbimpl.file.PageBase;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.log.LogMgr;

public class Buffer extends BufferBase {

    private final FileMgrBase fileMgr;
    private final LogMgr logMgr;
    private final AtomicBoolean canAddPin;
    private final AtomicInteger txnum;
    private final AtomicInteger lsn;
    private final Page page;
    private volatile BlockIdBase block;
    private volatile int pins;

    public Buffer(FileMgrBase fileMgr, LogMgrBase logMgr) {
        super(fileMgr, logMgr);

        this.fileMgr = fileMgr;
        if (logMgr instanceof LogMgr) {
            this.logMgr = (LogMgr) logMgr;
        } else {
            throw new IllegalArgumentException("Buffer requires the log manager to be an instance of LogMgr");
        }

        this.canAddPin = new AtomicBoolean(true);
        this.txnum = new AtomicInteger(-1);
        this.lsn = new AtomicInteger(-1);
        this.page = new Page(fileMgr.blockSize());
    }

    @Override
    public PageBase contents() {
        return this.page;
    }

    @Override
    public BlockIdBase block() {
        return this.block;
    }

    @Override
    public void setModified(int txnum, int lsn) {
        if (txnum < 0) {
            throw new IllegalArgumentException("Tx number must be positive");
        }

        // check that the lsn is valid
        if (lsn >= 0 && !this.logMgr.isValidLsn(lsn)) {
            throw new IllegalArgumentException("Lsn must be a valid record");
        }

        synchronized (this.page) {
            // set the lsn
            this.lsn.set(lsn);

            // set the txnum
            this.txnum.set(txnum);
        }
    }

    /**
     * Synchronously read the blocks content into the buffer page. Before reading
     * the new blocks content, the buffer will be flushed if it was modified.
     * 
     * @param block
     */
    public void updateBufferData(BlockIdBase block) {
        synchronized (this.page) {
            // flush if the page is modified
            if (this.txnum.get() != -1 && this.block != null) {
                flush();
            }

            // read the data
            this.fileMgr.read(block, this.page);

            // store the block id
            this.block = block;
        }
    }

    /**
     * Flush the page to disk if the the buffer was last modified by the provided
     * txnum.
     * 
     * @param txnum
     */
    public void flush(int txnum) {
        synchronized (this.page) {
            if (this.txnum.get() == txnum) {
                flush();
            }
        }
    }

    /**
     * Flush the page to the disk.
     */
    private void flush() {
        // flush the modifying to disk
        int lsn = this.lsn.get();
        if (lsn >= 0) {
            this.logMgr.flush(lsn);
        }

        // flush the page to disk
        this.fileMgr.write(this.block, this.page);

        // set unmodified
        this.txnum.set(-1);
    }

    /**
     * Set whether or not this buffer may accept additional pins.
     * 
     * @param canAddPin
     */
    public void setCanAddPin(boolean canAddPin) {
        this.canAddPin.set(canAddPin);
    }

    /**
     * Check if this buffer is accepting additional pins (pin count must be >= 1).
     * 
     * @return true iff this buffer can accept additional pins
     */
    public boolean canAddPin() {
        return this.canAddPin.get();
    }

    @Override
    public boolean isPinned() {
        return this.pins > 0;
    }

    /**
     * An enum used to determine how the pin should be modified.
     */
    public enum PinModificationMode {
        INCREMENT, DECREMENT
    };

    /**
     * Synchronously increment/decrement the pin counter based on the pin
     * modification mode provided. The method will return false only when the buffer
     * is no longer pinned. If the pin is unpinned (pin count == 0) and the mode is
     * decrement, an ISE will be thrown.
     * 
     * @param mode
     * @return true iff the buffer is still pinned
     * @throws IllegalStateException if pin count <= 0
     */
    public synchronized boolean modifyPinAndCheckIfPinned(PinModificationMode mode) throws IllegalStateException {
        if (mode == PinModificationMode.INCREMENT) {
            this.pins++;
            return true;
        } else {
            if (this.pins <= 0) {
                throw new IllegalStateException("Can't decrement when pin count <= 0 (unpinned)");
            }

            this.pins--;
            if (this.pins == 0) {
                // stop additional pins if the buffer is unpinned
                setCanAddPin(false);
            }

            return this.pins > 0;
        }
    }

    @Override
    public int hashCode() {
        return this.page.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o != null && (o instanceof Buffer) && Objects.equals(((Buffer) o).page, this.page);
    }

    @Override
    public String toString() {
        StringBuilder stb = new StringBuilder();
        stb.append("Buffer: [block: ")
                .append(this.block)
                .append(", pins: ")
                .append(this.pins)
                .append(", txnum: ")
                .append(this.txnum.get())
                .append("]");
        return stb.toString();
    }
}
