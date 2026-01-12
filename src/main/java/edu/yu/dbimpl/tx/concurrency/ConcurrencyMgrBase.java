package edu.yu.dbimpl.tx.concurrency;

/** Specifies the public API for the ConcurrencyMgr implementation by requiring
 * all ConcurrencyMgr implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * In this design, every transaction is associated with its own concurrency
 * manager.  This concurrency manager design assumes a lock-based approach to
 * concurrency control: the concurrency manager keeps track of which locks the
 * transaction currently has, and interacts with the global lock table as
 * needed.
 *
 * Design note: the resetAllLockState() method implicitly assumes that the DBMS
 * is using a lock-based concurrency control implementation.  This restriction
 * should be cleaned up in subsequent iterations.
 *
 */

import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.tx.TxMgrBase;

public abstract class ConcurrencyMgrBase {

  /** Create a concurrency manager.
   *
   */
  public ConcurrencyMgrBase(TxMgrBase txMgr) {
    // fill me in in your implementation class!
  }

  /** Obtain an SLock on the block, if necessary.  The method will ask the lock
   * table for an SLock if the transaction currently has no locks on that
   * block.
   *
   * @param blk a reference to the disk block
   */
  public abstract void sLock(BlockIdBase blk);

  /** Obtain an XLock on the block, if necessary.  If the transaction does not
   * have an XLock on that block, then the method first gets an SLock on that
   * block (if necessary), and then upgrades it to an XLock.
   *
   * @param blk a reference to the disk block
   */
  public abstract void xLock(BlockIdBase blk);

  /** Release all locks held by the concurrency manager's tx by asking the lock
   * table to unlock each one.
   */
  public abstract void release();

}
