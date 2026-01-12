package edu.yu.dbimpl.tx.concurrency;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.tx.TxMgr;
import edu.yu.dbimpl.tx.TxMgrBase;

public class ConcurrencyMgr extends ConcurrencyMgrBase {

    private final long waitTime;
    private final Map<BlockIdBase, TxLock> globalLockMap;
    private final Set<TxLock> txLockSet;
    private int txnum;

    public ConcurrencyMgr(TxMgrBase txMgr, int txnum) {
        super(txMgr);

        if (!(txMgr instanceof TxMgr)) {
            throw new IllegalArgumentException("txMgr must be an instance of TxMgr");
        }

        this.waitTime = txMgr.getMaxWaitTimeInMillis();
        this.globalLockMap = ((TxMgr) txMgr).getGlobalLockMap();
        this.txLockSet = new HashSet<>();
        this.txnum = txnum;
    }

    @Override
    public void sLock(BlockIdBase blk) {
        // get the lock
        TxLock lock = getLock(blk);

        // check if any lock is already held
        if (this.txLockSet.contains(lock)) {
            return;
        }

        try {
            // attempt to get the sLock
            if (lock.trySharedLock(this.txnum, this.waitTime)) {
                // keep track of the acquired locks
                this.txLockSet.add(lock);
            } else {
                throw new LockAbortException("Thread timed out waiting for sLock");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Thread interrupted while waiting for sLock", e);
        }
    }

    @Override
    public void xLock(BlockIdBase blk) {
        // get the lock
        TxLock lock = getLock(blk);

        // check if the xLock is already held
        if (this.txLockSet.contains(lock) && lock.holdsExclusive(this.txnum)) {
            return;
        }

        try {
            // attempt to get the xLock
            if (lock.tryExclusiveLock(this.txnum, this.waitTime)) {
                // keep track of the acquired locks
                this.txLockSet.add(lock);
            } else {
                throw new LockAbortException("Thread timed out waiting for xLock");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Thread interrupted while waiting for xLock", e);
        }
    }

    /**
     * Retrieve the lock associated with the block. If one doesn't exist, it will be
     * created.
     * 
     * @param blk
     * @return the TxLock for the blk
     */
    private TxLock getLock(BlockIdBase blk) {
        // create a lock if one does not exist for the block
        return this.globalLockMap.compute(blk, (key, val) -> {
            return val != null ? val : new TxLock();
        });
    }

    @Override
    public void release() {
        // release all locks
        for (TxLock lock : this.txLockSet) {
            lock.release(this.txnum);
        }

        // clear the lock set
        this.txLockSet.clear();
    }

    /**
     * A transaction-based read-write lock that uses txnum instead of thread
     * identity. Implements FIFO fairness queue for lock acquisition to prevent
     * starvation and ensure predictable ordering.
     */
    public static class TxLock {
        private final Set<Integer> sharedHolders = new HashSet<>();
        private Integer exclusiveHolder = null;
        private final Object monitor = new Object();
        private final Deque<LockRequest> waitQueue = new ArrayDeque<>();

        /**
         * Represents a lock request in the wait queue.
         * The request object itself is used as a monitor for wait/notify.
         */
        private static class LockRequest {
            final int txnum;
            final LockType lockType;
            final long deadlineNanos;
            boolean granted;

            LockRequest(int txnum, LockType lockType, long deadlineNanos) {
                this.txnum = txnum;
                this.lockType = lockType;
                this.deadlineNanos = deadlineNanos;
                this.granted = false;
            }
        }

        /**
         * Types of locks that can be requested.
         */
        private enum LockType {
            SHARED, EXCLUSIVE
        }

        /**
         * Try to acquire a shared (read) lock with timeout using FIFO queue.
         * 
         * @param txnum         the transaction number requesting the lock
         * @param timeoutMillis the maximum time to wait in milliseconds
         * @return true if lock acquired, false if timeout
         * @throws InterruptedException if interrupted while waiting
         */
        public boolean trySharedLock(int txnum, long timeoutMillis) throws InterruptedException {
            long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            LockRequest request = null;

            synchronized (monitor) {
                // Fast path: if lock is immediately available and queue is empty, grant
                // directly
                if (waitQueue.isEmpty() && canAcquireShared(txnum)) {
                    sharedHolders.add(txnum);
                    return true;
                }

                // Must wait - create request and add to queue
                request = new LockRequest(txnum, LockType.SHARED, deadlineNanos);
                waitQueue.addLast(request);
            }

            // Wait on the request object itself for the lock to be granted
            synchronized (request) {
                while (!request.granted) {
                    long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0) {
                        // Timeout: leave request in queue for passive cleanup
                        return false;
                    }

                    long remainingMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
                    if (remainingMillis > 0) {
                        request.wait(remainingMillis);
                    }
                }
                return true;
            }
        }

        /**
         * Try to acquire an exclusive (write) lock with timeout using FIFO queue.
         * Treats upgrades as new requests for pure FIFO fairness.
         * 
         * @param txnum         the transaction number requesting the lock
         * @param timeoutMillis the maximum time to wait in milliseconds
         * @return true if lock acquired, false if timeout
         * @throws InterruptedException if interrupted while waiting
         */
        public boolean tryExclusiveLock(int txnum, long timeoutMillis) throws InterruptedException {
            long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            LockRequest request = null;

            synchronized (monitor) {
                // Fast path: if lock is immediately available and queue is empty, grant
                // directly
                if (waitQueue.isEmpty() && canAcquireExclusive(txnum)) {
                    // Remove from shared holders if upgrading
                    sharedHolders.remove(txnum);
                    exclusiveHolder = txnum;
                    return true;
                }

                // Must wait - create request and add to queue (even for upgrades - pure FIFO)
                request = new LockRequest(txnum, LockType.EXCLUSIVE, deadlineNanos);
                waitQueue.addLast(request);
            }

            // Wait on the request object itself for the lock to be granted
            synchronized (request) {
                while (!request.granted) {
                    long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0) {
                        // Timeout: leave request in queue for passive cleanup
                        return false;
                    }

                    long remainingMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
                    if (remainingMillis > 0) {
                        request.wait(remainingMillis);
                    }
                }
                return true;
            }
        }

        /**
         * Release whichever lock (shared or exclusive) is held by this transaction.
         * Then process the wait queue to grant locks to waiting transactions.
         * 
         * @param txnum the transaction number releasing the lock
         */
        public void release(int txnum) {
            synchronized (monitor) {
                boolean released = false;

                // Release exclusive lock if held
                if (exclusiveHolder != null && exclusiveHolder == txnum) {
                    exclusiveHolder = null;
                    released = true;
                }

                // Release shared lock if held
                if (sharedHolders.remove(txnum)) {
                    released = true;
                }

                // Process wait queue if any lock was released
                if (released) {
                    processWaitQueue();
                }
            }
        }

        /**
         * Process the wait queue to grant locks to waiting transactions.
         * Must be called while holding the monitor lock.
         * 
         * Granting rules:
         * - Skip expired (timed-out) requests
         * - Grant consecutive SHARED requests from the head while compatible
         * - Grant single EXCLUSIVE request if compatible, then stop
         */
        private void processWaitQueue() {
            while (!waitQueue.isEmpty()) {
                LockRequest request = waitQueue.peekFirst();

                // Check if request has expired (timed out)
                if (System.nanoTime() > request.deadlineNanos) {
                    // Passive cleanup: remove expired request and continue
                    waitQueue.removeFirst();
                    continue;
                }

                if (request.lockType == LockType.SHARED) {
                    if (canAcquireShared(request.txnum)) {
                        // Grant shared lock
                        waitQueue.removeFirst();
                        sharedHolders.add(request.txnum);
                        request.granted = true;
                        // Notify the waiting thread on its request object
                        synchronized (request) {
                            request.notify();
                        }
                        // Continue to potentially grant more consecutive shared locks
                    } else {
                        // Cannot grant, stop processing
                        break;
                    }
                } else { // EXCLUSIVE
                    if (canAcquireExclusive(request.txnum)) {
                        // Grant exclusive lock
                        waitQueue.removeFirst();
                        // Remove from shared holders if upgrading
                        sharedHolders.remove(request.txnum);
                        exclusiveHolder = request.txnum;
                        request.granted = true;
                        // Notify the waiting thread on its request object
                        synchronized (request) {
                            request.notify();
                        }
                        // Stop after granting exclusive lock
                        break;
                    } else {
                        // Cannot grant, stop processing
                        break;
                    }
                }
            }
        }

        /**
         * Check if this transaction already holds a shared lock.
         * 
         * @param txnum the transaction number to check
         * @return true if txnum holds a shared lock
         */
        public boolean holdsShared(int txnum) {
            synchronized (monitor) {
                return sharedHolders.contains(txnum);
            }
        }

        /**
         * Check if this transaction already holds an exclusive lock.
         * 
         * @param txnum the transaction number to check
         * @return true if txnum holds an exclusive lock
         */
        public boolean holdsExclusive(int txnum) {
            synchronized (monitor) {
                return exclusiveHolder != null && exclusiveHolder == txnum;
            }
        }

        /**
         * Check if a shared lock can be acquired by this transaction.
         * Shared locks are compatible with other shared locks but not with exclusive
         * locks.
         * 
         * @param txnum the transaction number requesting the lock
         * @return true if the lock can be acquired
         */
        private boolean canAcquireShared(int txnum) {
            // Can acquire if no exclusive holder, or we are the exclusive holder
            return exclusiveHolder == null || exclusiveHolder == txnum;
        }

        /**
         * Check if an exclusive lock can be acquired by this transaction.
         * Exclusive locks are incompatible with all other locks except when upgrading.
         * 
         * @param txnum the transaction number requesting the lock
         * @return true if the lock can be acquired
         */
        private boolean canAcquireExclusive(int txnum) {
            // Can acquire if:
            // 1. No exclusive holder (or we are the exclusive holder), AND
            // 2. No shared holders (or we are the only shared holder - lock upgrade case)
            boolean noExclusiveConflict = (exclusiveHolder == null || exclusiveHolder == txnum);
            boolean noSharedConflict = (sharedHolders.isEmpty() ||
                    (sharedHolders.size() == 1 && sharedHolders.contains(txnum)));
            return noExclusiveConflict && noSharedConflict;
        }
    }

}
