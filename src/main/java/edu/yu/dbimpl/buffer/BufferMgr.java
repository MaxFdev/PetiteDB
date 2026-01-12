package edu.yu.dbimpl.buffer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.yu.dbimpl.buffer.Buffer.PinModificationMode;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.log.LogMgrBase;

public class BufferMgr extends BufferMgrBase {

    private static final Logger logger = LogManager.getLogger(BufferMgr.class);

    private final EvictionPolicy evictionPolicy;
    private final FileMgrBase fileMgr;
    private final LogMgrBase logMgr;
    private final int maxWaitTime;
    private final Semaphore semaphore;
    private final Buffer[] bufferPool;
    private final Map<BlockIdBase, Integer> blockIndexMap;
    private final Supplier<Integer> nextIndexSupplier;
    private int indexTracker;

    public BufferMgr(FileMgrBase fileMgr, LogMgrBase logMgr, int nBuffers, int maxWaitTime, EvictionPolicy policy) {
        super(fileMgr, logMgr, nBuffers, maxWaitTime, policy);

        if (maxWaitTime <= 0) {
            throw new IllegalArgumentException("Max wait time must be positive");
        }

        checkDatabaseInitializationStatus();

        this.evictionPolicy = policy;
        this.fileMgr = fileMgr;
        this.logMgr = logMgr;
        this.maxWaitTime = maxWaitTime;
        this.semaphore = new Semaphore(nBuffers);
        this.bufferPool = new Buffer[nBuffers];
        this.blockIndexMap = new ConcurrentHashMap<>();
        this.nextIndexSupplier = initializeNextIndexSupplier(policy);
        initializeBufferPool();
    }

    public BufferMgr(FileMgrBase fileMgr, LogMgrBase logMgr, int nBuffers, int maxWaitTime) {
        super(fileMgr, logMgr, nBuffers, maxWaitTime);

        if (maxWaitTime <= 0) {
            throw new IllegalArgumentException("Max wait time must be positive");
        }

        checkDatabaseInitializationStatus();

        this.evictionPolicy = EvictionPolicy.NAIVE;
        this.fileMgr = fileMgr;
        this.logMgr = logMgr;
        this.maxWaitTime = maxWaitTime;
        this.semaphore = new Semaphore(nBuffers);
        this.bufferPool = new Buffer[nBuffers];
        this.blockIndexMap = new ConcurrentHashMap<>();
        this.nextIndexSupplier = initializeNextIndexSupplier(EvictionPolicy.NAIVE);
        initializeBufferPool();
    }

    private static void checkDatabaseInitializationStatus() {
        logger.info("(Re)initialize database: {}", DBConfiguration.INSTANCE.isDBStartup());
    }

    /**
     * Initialize a supplier function to get the next unpinned buffer index based on
     * the eviction policy algorithm.
     * 
     * @param evictionPolicy
     * @return a supplier function to run the eviction policy algorithm
     */
    private Supplier<Integer> initializeNextIndexSupplier(EvictionPolicy evictionPolicy) {
        if (evictionPolicy == EvictionPolicy.CLOCK) {
            return () -> {
                // keep track of the start
                final int start = this.indexTracker;

                // check each index once, wrapping around if necessary
                for (int i = 0; i < this.bufferPool.length; i++) {
                    int index = (start + i) % this.bufferPool.length;
                    if (!this.bufferPool[index].isPinned()) {
                        this.indexTracker = (index + 1) % this.bufferPool.length;
                        return index;
                    }
                }

                return -1;
            };
        } else {
            return () -> {
                // start at the beginning and return the first unpinned index
                for (int i = 0; i < this.bufferPool.length; i++) {
                    if (!this.bufferPool[i].isPinned()) {
                        return i;
                    }
                }

                return -1;
            };
        }
    }

    /**
     * Initialize each position in the pool to a new buffer.
     */
    private void initializeBufferPool() {
        for (int i = 0; i < this.bufferPool.length; i++) {
            this.bufferPool[i] = new Buffer(this.fileMgr, this.logMgr);
        }
    }

    @Override
    public int available() {
        return this.semaphore.availablePermits();
    }

    @Override
    public void flushAll(int txnum) {
        if (txnum < 0) {
            throw new IllegalArgumentException("Tx number must be positive");
        }

        // flush each buffer with a matching "modified by" txnum
        for (Buffer buffer : this.bufferPool) {
            buffer.flush(txnum);
        }
    }

    @Override
    public void unpin(BufferBase buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer can't be null");
        }

        if (!(buffer instanceof Buffer)) {
            throw new IllegalArgumentException("BufferBase must be of type Buffer");
        }

        try {
            // decrement the pin count and check if the buffer is unpinned
            if (!((Buffer) buffer).modifyPinAndCheckIfPinned(PinModificationMode.DECREMENT)) {
                // release a semaphore lock if it is unpinned
                this.semaphore.release();
            }
        } catch (IllegalStateException e) {
            // the buffer was not pinned
            throw new IllegalArgumentException("Buffer must be pinned to unpin");
        }
    }

    @Override
    public BufferBase pin(BlockIdBase blk) {
        if (blk == null) {
            throw new IllegalArgumentException("BlockID can't be null");
        }

        // check if block is mapped to a buffer index
        Integer mappedIndex = this.blockIndexMap.get(blk);
        if (mappedIndex != null) {
            // check if the buffer is correct and able to accept additional pins
            Buffer buffer = this.bufferPool[mappedIndex];
            if (buffer.canAddPin() && blk.equals(buffer.block())) {
                buffer.modifyPinAndCheckIfPinned(PinModificationMode.INCREMENT);
                return buffer;
            }
        }

        // track possible stale mapping
        AtomicReference<BlockIdBase> staleBlockRef = new AtomicReference<>();

        // * lock point 1 (on blk)
        mappedIndex = this.blockIndexMap.compute(blk, (key, val) -> {
            // double check if the block was mapped to a buffer index
            if (val != null) {
                // check if a pin can be added to the buffer
                Buffer buffer = this.bufferPool[val];
                if (buffer.canAddPin() && blk.equals(buffer.block())) {
                    // add a pin to the buffer
                    // * lock point 2a (on method)
                    buffer.modifyPinAndCheckIfPinned(PinModificationMode.INCREMENT);
                    return val;
                }
            }

            try {
                // acquire a semaphore lock to pin a buffer
                // * lock point 2b (on semaphore)
                if (this.semaphore.tryAcquire(this.maxWaitTime, TimeUnit.MILLISECONDS)) {
                    // * lock point 3 (on method)
                    int bufferIndex = getUnpinnedBufferIndexFor(blk);

                    // check for errors
                    if (bufferIndex == -1) {
                        this.semaphore.release();
                        throw new RuntimeException("Error finding buffer");
                    }

                    Buffer buffer = this.bufferPool[bufferIndex];

                    // save current block for stale map checking
                    staleBlockRef.set(buffer.block());

                    // check if the buffer page needs to be flushed and updated
                    if (buffer.block() == null || !blk.equals(buffer.block())) {
                        // update the buffers data, flushing if modified
                        // * lock point 4 (on buffer page)
                        buffer.updateBufferData(blk);
                    }

                    // enable additional pinning
                    buffer.setCanAddPin(true);

                    return bufferIndex;
                } else {
                    throw new BufferAbortException("No buffers available at this time");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("Thread interrupted while pinning", e);
            }
        });

        // remove stale mapping if present
        BlockIdBase lastBlock = staleBlockRef.get();
        if (lastBlock != null && !blk.equals(lastBlock)) {
            this.blockIndexMap.remove(lastBlock, mappedIndex);
        }

        return this.bufferPool[mappedIndex];
    }

    /**
     * Synchronously find an unpinned buffer's index for a given block. If a buffer
     * already contains the block's content, then its index will be returned.
     * Otherwise, the eviction policy's algorithm will be used to find an unpinned
     * buffer.
     * 
     * @param blk
     * @return the index of the correct buffer to use or -1 if there were no
     *         unpinned buffers found
     */
    private synchronized int getUnpinnedBufferIndexFor(BlockIdBase blk) {
        // check if the block is already present in a buffer
        Integer index = this.blockIndexMap.get(blk);

        // if not, use the eviction policy algorithm to get the next unpinned buffer
        index = (index != null && blk.equals(this.bufferPool[index].block()))
                ? index
                : this.nextIndexSupplier.get();

        // process the buffer if there was no error (error == -1)
        if (index != -1) {
            // (re)pin this buffer to ensure another block can't reserve it
            this.bufferPool[index].modifyPinAndCheckIfPinned(PinModificationMode.INCREMENT);
        }

        return index;
    }

    @Override
    public EvictionPolicy getEvictionPolicy() {
        return this.evictionPolicy;
    }

}
