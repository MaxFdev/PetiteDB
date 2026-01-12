package edu.yu.dbimpl.tx.recovery;

/** Specifies the public API for the RecoveryMgr implementation by requiring all
 * RecoveryMgr implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * In this design, every transaction is associated with its own recovery
 * manager.
 */

import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.buffer.BufferBase;
import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.tx.TxBase;

public abstract class RecoveryMgrBase {

  /** Create a recovery manager for the specified transaction.
   *
   * @param tx the transaction associated with this recovery manager instance
   * @param logMgr the singleton logMgr for the DBMS
   * @param bufferMgr the singleton bufferMgr for the DBMS
   */
  public RecoveryMgrBase(TxBase tx, LogMgrBase logMgr, BufferMgrBase bufferMgr) {
    // fill me in in your implementation class!
  }

  /** Write a commit record to the log, and flushes it to disk, and do whatever
   * concommitant processing is required by your implementation.
   *
   */
  public abstract void commit();

  /** Write a rollback record to the log and flush it to disk, and do whatever
   * concommitant processing is required by your implementation.
   */
  public abstract void rollback();

  /** Recover uncompleted transactions from the log and then write a quiescent
   * checkpoint record to the log and flush it.
   */
  public abstract void recover();

  /** Write a setInt record to the log and return its lsn.
   *
   * @param buff the buffer containing the page
   * @param offset the offset of the value in the page
   * @param newval the value to be written
   * @return the LSN after the record has been written to the log
   */
  public abstract int setInt(BufferBase buff, int offset, int newval);

  /** Write a setBoolean record to the log and return its lsn.
   *
   * @param buff the buffer containing the page
   * @param offset the offset of the value in the page
   * @param newval the value to be written
   * @return the LSN after the record has been written to the log
   */
  public abstract int setBoolean(BufferBase buff, int offset, boolean newval);

  /** Write a setDouble record to the log and return its lsn.
   *
   * @param buff the buffer containing the page
   * @param offset the offset of the value in the page
   * @param newval the value to be written
   * @return the LSN after the record has been written to the log
   */
  public abstract int setDouble(BufferBase buff, int offset, double newval);
  
  /** Write a setString record to the log and return its lsn.
   *
   * @param buff the buffer containing the page
   * @param offset the offset of the value in the page
   * @param newval the value to be written
   * @return the LSN after the record has been written to the log
   */
  public abstract int setString(BufferBase buff, int offset, String newval);

  /** Write a setBytes record to the log and return its lsn.
   *
   * @param buff the buffer containing the page
   * @param offset the offset of the value in the page
   * @param newval the value to be written
   * @return the LSN after the record has been written to the log
   */
  public abstract int setBytes(BufferBase buff, int offset, byte[] newval);
}
