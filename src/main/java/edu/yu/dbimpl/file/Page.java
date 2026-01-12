package edu.yu.dbimpl.file;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Page extends PageBase {

    private final ReadWriteLock lock;
    private final String toString;
    private final int blocksize;
    private ByteBuffer buffer;

    public Page(byte[] b) {
        super(b);

        this.lock = new ReentrantReadWriteLock();
        this.toString = "Page(byte[])";
        this.blocksize = b.length;
        this.buffer = ByteBuffer.wrap(b);
    }

    public Page(int blocksize) {
        super(blocksize);

        this.lock = new ReentrantReadWriteLock();
        this.toString = "Page(blocksize)";
        this.blocksize = blocksize;
        this.buffer = ByteBuffer.allocateDirect(blocksize);
    }

    @Override
    public int getInt(int offset) {
        // get read lock
        this.lock.readLock().lock();

        int val;
        try {
            // read int
            val = this.buffer.getInt(offset);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error getting int", e);
        } finally {
            // release lock
            this.lock.readLock().unlock();
        }

        return val;
    }

    @Override
    public void setInt(int offset, int n) {
        // get write lock
        this.lock.writeLock().lock();

        try {
            // write int
            this.buffer.putInt(offset, n);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error setting int", e);
        } finally {
            // release lock
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public double getDouble(int offset) {
        // get read lock
        this.lock.readLock().lock();

        double val;
        try {
            // read double
            val = this.buffer.getDouble(offset);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error getting double", e);
        } finally {
            // release lock
            this.lock.readLock().unlock();
        }

        return val;
    }

    @Override
    public void setDouble(int offset, double d) {
        // get write lock
        this.lock.writeLock().lock();

        try {
            // write double
            this.buffer.putDouble(offset, d);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error setting double", e);
        } finally {
            // release lock
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public boolean getBoolean(int offset) {
        // get read lock
        this.lock.readLock().lock();

        boolean val;
        try {
            // read boolean
            val = this.buffer.get(offset) != 0;
        } catch (Exception e) {
            throw new IllegalArgumentException("Error getting boolean", e);
        } finally {
            // release lock
            this.lock.readLock().unlock();
        }

        return val;
    }

    @Override
    public void setBoolean(int offset, boolean d) {
        // get write lock
        this.lock.writeLock().lock();

        try {
            // write boolean
            this.buffer.put(offset, (byte) (d ? 1 : 0));
        } catch (Exception e) {
            throw new IllegalArgumentException("Error setting boolean", e);
        } finally {
            // release lock
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public byte[] getBytes(int offset) {
        // get read lock
        this.lock.readLock().lock();

        byte[] val;
        try {
            // get length
            int len = this.buffer.getInt(offset);

            // read bytes
            val = new byte[len];
            this.buffer.get(offset + Integer.BYTES, val, 0, len);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error getting bytes", e);
        } finally {
            // release lock
            this.lock.readLock().unlock();
        }

        return val;
    }

    @Override
    public void setBytes(int offset, byte[] b) {
        // check null
        if (b == null) {
            throw new IllegalArgumentException("Byte array is null");
        }

        // check length
        if (Integer.BYTES + b.length + offset > this.blocksize) {
            throw new IllegalArgumentException("Byte array exceeds max length");
        }

        // get lock
        this.lock.writeLock().lock();

        try {
            // write length
            this.buffer.putInt(offset, b.length);

            // write bytes
            this.buffer.put(offset + Integer.BYTES, b, 0, b.length);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error setting bytes", e);
        } finally {
            // release lock
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public String getString(int offset) {
        // get read lock
        this.lock.readLock().lock();

        String val;
        try {
            // get length
            int len = this.buffer.getInt(offset);

            // read string
            byte[] bytes = new byte[len];
            this.buffer.get(offset + Integer.BYTES, bytes, 0, len);
            val = new String(bytes, CHARSET);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error getting string", e);
        } finally {
            // release lock
            this.lock.readLock().unlock();
        }

        return val;
    }

    @Override
    public void setString(int offset, String s) {
        // check if null
        if (s == null) {
            throw new IllegalArgumentException("String is null");
        }

        // check length
        if (maxLength(s.length()) + offset > this.blocksize) {
            throw new IllegalArgumentException("String exceeds max length");
        }

        // get write lock
        this.lock.writeLock().lock();

        try {
            // get length
            int len = logicalLength(s);

            // write length
            this.buffer.putInt(offset, len);

            // write string
            this.buffer.put(offset + Integer.BYTES, s.getBytes(CHARSET), 0, len);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error setting string", e);
        } finally {
            // release lock
            this.lock.writeLock().unlock();
        }
    }

    /**
     * Set the underlying data in the buffer.
     * 
     * @param data
     */
    public void propagateData(byte[] data) {
        // get write lock
        this.lock.writeLock().lock();

        try {
            // set data
            this.buffer.put(0, data, 0, this.blocksize);
        } finally {
            // release lock
            this.lock.writeLock().unlock();
        }
    }

    /**
     * Set the underlying data in the buffer.
     * 
     * @param data
     */
    public void propagateData(ByteBuffer data) {
        // get write lock
        this.lock.writeLock().lock();

        try {
            // set data
            this.buffer.put(0, data, 0, this.blocksize);
        } finally {
            // release lock
            this.lock.writeLock().unlock();
        }
    }

    /**
     * Retrieve the data from the underlying buffer.
     * 
     * @return byte[] data
     */
    public byte[] extractData() {
        // get read lock
        this.lock.readLock().lock();

        byte[] data;
        try {
            // copy the data
            data = new byte[this.blocksize];
            this.buffer.get(0, data, 0, this.blocksize);
        } finally {
            // release lock
            this.lock.readLock().unlock();
        }

        return data;
    }

    @Override
    public String toString() {
        return this.toString;
    }

    @Override
    public int hashCode() {
        return this.buffer.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o != null && (o instanceof Page) && ((Page) o).buffer.equals(this.buffer);
    }

}
