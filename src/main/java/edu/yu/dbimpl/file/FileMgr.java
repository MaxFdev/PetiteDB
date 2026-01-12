package edu.yu.dbimpl.file;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.yu.dbimpl.config.DBConfiguration;

public class FileMgr extends FileMgrBase {

    private static final Logger logger = LogManager.getLogger(FileMgr.class);
    private static final int CACHE_MAX_SIZE = 32;

    private final File dbDirectory;
    private final int blocksize;
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> lockMap;
    private final ConcurrentHashMap<String, FileHandler> fileHandlerMap;
    private final LinkedHashMap<String, FileHandler> channelCache;

    public FileMgr(File dbDirectory, int blocksize) {
        super(dbDirectory, blocksize);

        if (dbDirectory == null) {
            throw new IllegalArgumentException("Root dir can't be null");
        }
        if (blocksize < 0) {
            throw new IllegalArgumentException("Blocksize must be positive");
        }

        this.dbDirectory = new File(Paths.get("").toAbsolutePath().toString(), dbDirectory.getPath());
        this.blocksize = blocksize;
        this.lockMap = new ConcurrentHashMap<>();
        this.fileHandlerMap = new ConcurrentHashMap<>();
        this.channelCache = new LinkedHashMap<String, FileHandler>(CACHE_MAX_SIZE, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, FileHandler> eldest) {
                if (size() > CACHE_MAX_SIZE) {
                    FileHandler evictRef = eldest.getValue();
                    remove(eldest.getKey(), evictRef);
                    if (evictRef != null) {
                        evictRef.requestClose();
                    }
                    return true;
                }
                return false;
            }
        };

        // check config to see if this is a new db or existing
        boolean startup;
        try {
            startup = DBConfiguration.INSTANCE.isDBStartup();
        } catch (Exception e) {
            // throw IllegalStateException if db config can't supply info
            throw new IllegalStateException("Config couldn't supply startup info", e);
        }

        logger.info("Starting FileMgr | DBConfiguration startup = {}", startup);

        if (startup) {
            logger.info("(Re)Initializing DB");

            // check if the dir exists
            if (this.dbDirectory.exists()) {
                logger.info("Deleting prior DB instance");
                deleteDir(dbDirectory);
            }

            // initialize new db
            this.dbDirectory.mkdirs();
        }

        logger.info("DB initialized: {}", this.dbDirectory.getAbsolutePath());
    }

    /**
     * A helper method to recursively delete a directory.
     * 
     * @param dir
     */
    private static void deleteDir(File dir) {
        if (dir != null) {
            // get the files list
            File[] files = dir.listFiles();

            // cycle through files
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDir(f);
                } else {
                    f.delete();
                }
            }

            // delete root
            dir.delete();
        }
    }

    /**
     * Acquires the lock for the file, creating one if necessary.
     * 
     * @param filename
     * @return the lock pertaining to the named file
     */
    private ReentrantReadWriteLock acquireLock(String filename) {
        return this.lockMap.computeIfAbsent(filename, (k) -> new ReentrantReadWriteLock());
    }

    @Override
    public void read(BlockIdBase blk, PageBase p) {
        // check input
        if (blk == null || p == null) {
            throw new IllegalArgumentException("Input can't be null");
        }

        // convert page
        Page page = p instanceof Page ? (Page) p : null;
        if (page == null) {
            throw new IllegalArgumentException("`PageBase p` is not instance of Page");
        }

        // get read lock
        ReadWriteLock rwl = acquireLock(blk.fileName());
        rwl.readLock().lock();

        // read the block
        FileHandler ref = null;
        try {
            ref = acquireFileHandler(blk.fileName());
            ByteBuffer buffer = ByteBuffer.allocate(this.blocksize);
            ref.channel.read(buffer, ((long) blk.number()) * this.blocksize);
            page.propagateData(buffer);
        } catch (Exception e) {
            throw new RuntimeException("Error reading from block", e);
        } finally {
            // release lock
            rwl.readLock().unlock();
            releaseReference(ref);
        }
    }

    @Override
    public void write(BlockIdBase blk, PageBase p) {
        // check input
        if (blk == null || p == null) {
            throw new IllegalArgumentException("Input can't be null");
        }

        // convert page
        Page page = p instanceof Page ? (Page) p : null;
        if (page == null) {
            throw new IllegalArgumentException("`PageBase p` is not instance of Page");
        }

        // get write lock
        ReadWriteLock rwl = acquireLock(blk.fileName());
        rwl.writeLock().lock();

        // write the block
        FileHandler ref = null;
        try {
            ref = acquireFileHandler(blk.fileName());
            FileChannel channel = ref.channel;
            ByteBuffer buffer = ByteBuffer.wrap(page.extractData());
            channel.write(buffer, ((long) blk.number()) * this.blocksize);
        } catch (Exception e) {
            throw new RuntimeException("Error writing to block", e);
        } finally {
            // release lock
            rwl.writeLock().unlock();
            releaseReference(ref);
        }
    }

    @Override
    public BlockIdBase append(String filename) {
        if (filename == null) {
            throw new IllegalArgumentException("Filename can't be null");
        }

        // get write lock
        ReadWriteLock rwl = acquireLock(filename);
        rwl.writeLock().lock();

        FileHandler ref = null;
        int blockNumber;
        try {
            ref = acquireFileHandler(filename);
            long oldLength = ref.channel.size();
            blockNumber = (int) (oldLength / this.blocksize);
            ref.channel.write(ByteBuffer.allocate(this.blocksize), oldLength);
        } catch (Exception e) {
            throw new RuntimeException("Error appending block", e);
        } finally {
            rwl.writeLock().unlock();
            releaseReference(ref);
        }

        return new BlockId(filename, blockNumber);
    }

    @Override
    public int length(String filename) {
        // check filename
        if (filename == null) {
            throw new IllegalArgumentException("Filename can't be null");
        }

        // get read lock
        ReadWriteLock rwl = acquireLock(filename);
        rwl.readLock().lock();

        // get the length
        int length;
        FileHandler ref = null;
        try {
            ref = acquireFileHandler(filename);
            length = (int) (ref.channel.size() / this.blocksize);
        } catch (IOException e) {
            throw new RuntimeException("Error getting file length", e);
        } finally {
            // release lock
            rwl.readLock().unlock();
            releaseReference(ref);
        }

        return length;
    }

    @Override
    public int blockSize() {
        return this.blocksize;
    }

    /**
     * Acquires the file handler, creating it if necessary. Uses the LRU cache to
     * keep some open between calls.
     * 
     * @param filename
     * @return the file handler wrapper for the file chanel
     * @throws IOException problem opening the file channel
     */
    private FileHandler acquireFileHandler(String filename) throws IOException {
        // look up file handler
        FileHandler ref = this.fileHandlerMap.get(filename);
        if (ref != null && ref.channel.isOpen()) {
            ref.retain();
        } else {
            // create or retain
            ref = this.fileHandlerMap.compute(filename, (k, oldRef) -> {
                if (oldRef == null || !oldRef.channel.isOpen()) {
                    try {
                        return new FileHandler(
                                FileChannel.open(new File(this.dbDirectory, filename).toPath(),
                                        StandardOpenOption.READ,
                                        StandardOpenOption.WRITE,
                                        StandardOpenOption.CREATE));
                    } catch (IOException e) {
                        throw new RuntimeException("Problem creating file handler", e);
                    }
                } else {
                    oldRef.retain();
                    return oldRef;
                }
            });
        }

        // maintain LRU cache
        synchronized (this.channelCache) {
            // calls remove eldest if necessary
            this.channelCache.put(filename, ref);
        }

        return ref;
    }

    /**
     * Release the reference to the file handler.
     * 
     * @param ref
     */
    private void releaseReference(FileHandler ref) {
        if (ref != null) {
            ref.release();
        }
    }

    private static final class FileHandler {
        final FileChannel channel;
        private final AtomicInteger referenceCount;
        private final AtomicBoolean closeRequested;

        FileHandler(FileChannel channel) {
            this.channel = channel;
            this.referenceCount = new AtomicInteger(1);
            this.closeRequested = new AtomicBoolean(false);
        }

        void retain() {
            this.referenceCount.getAndIncrement();
        }

        void requestClose() {
            this.closeRequested.set(true);
            if (this.referenceCount.get() == 0) {
                tryClose();
            }
        }

        void release() {
            if (this.referenceCount.decrementAndGet() == 0 && this.closeRequested.get()) {
                tryClose();
            }
        }

        synchronized private void tryClose() {
            try {
                if (this.channel.isOpen()) {
                    this.channel.close();
                }
            } catch (IOException e) {
                logger.warn("Error closing channel for file", e);
            }
        }
    }

}
