package edu.yu.dbimpl.buffer;

/** Specifies the public API for the Buffer implementation by requiring all
 * Buffer implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must support EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * An single buffer in BufferMgr's buffer pool: it encapsulates a Page, adding
 * additional information such as whether the Page's contents have been
 * modified since it was fetched from disk.
 *
 * Design note: Buffers should be manipulated through the BufferMgr API to the
 * greatest extent possible.  For example, difficult to see how a client can
 * usefully create a Buffer through its constructor since it will not be
 * associated with a disk block.
 *
 * @author Avraham Leff
 */

import edu.yu.dbimpl.file.*;
import edu.yu.dbimpl.log.LogMgrBase;

public abstract class BufferBase {

  public BufferBase(FileMgrBase fileMgr, LogMgrBase logMgr) {
    // fill me in in your implementation class!
  }
   

  /** Returns the Page encapsulated by this Buffer instance.
   *
   * @return the encapsulated Page.
   */
  public abstract PageBase contents();

  /** Returns a reference to the disk block allocated to the buffer.
   *
   * @return a reference to a disk block
   */
  public abstract BlockIdBase block();

  /** Sets the buffer's "modified" bit.  This method enables performance
   * enhancements since buffers that have not been modified by the client need
   * not be flushed to disk (since the disk block represents the current
   * state).
   *
   * @param txnum identifies the transaction that modified the Buffer.
   * @param lsn The LSN of the most recent log record, set to a negative number
   * to indicate that the client didn't generate a log record when modifying
   * the Buffer.
   * @throws IllegalArgumentException if txnum is negative
   */
  public abstract void setModified(int txnum, int lsn);

  /** Return true iff the buffer is currently pinned, defined as "has a pin
   * count that is greater than 0".
   *
   * @return true iff the buffer is pinned.
   */
  public abstract boolean isPinned();
}
