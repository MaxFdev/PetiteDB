package edu.yu.dbimpl.record;

import edu.yu.dbimpl.query.*;
import edu.yu.dbimpl.tx.TxBase;

/** Specifies the public API for the TableScan implementation by requiring all
 * TableScan implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * A TableScan is an UpdateScan implementation over records stored in a file.
 *
 * Because the TableScan encapsulates (possibly many) blocks, it is responsible
 * for formatting any new blocks that it appends to the file.
 * 
 * Design note: given the PetiteDB assumptions about record layout (per lecture
 * discussion), there should be no need to persist the RID information.  Should
 * you choose to persist it, the implementation must not change the offset or
 * the block state (i.e., such meta-data must be persisted in a way that is
 * transparent to the client).
 *
 * Design note: a given instance of a TableScan need not be thread-safe.
 *
 * Design note: All get/set methods MUST throw an IllegalStateException (NOT
 * IAE) if the TableScan is not positioned on an "in-use" RecordPage slot.
 *
 * Reminder: per interface semantics and per relational database semantics,
 * TableScan.next() on an empty table will return false.
 */
public abstract class TableScanBase implements UpdateScan {

  /** Constructor: if the file for the specified table is currently empty, the
   * Scan will append a block; otherwise, the Scan will be positioned on the
   * first block of the file.  
   *
   * @param tx Defines the transactional scope under which the scan operations
   * will take place 
   * @param tblname Specifies the prefix of the table over which the scan will
   * be performed.  The implementation can add a suffix to the prefix to
   * generate the full name of the file that will store the data.
   * @param layout Defines the logical and physical schema of the
   * table/relation
   * @see e.g., java.nio.Files#createTempFile for meaning of "prefix" and "suffix".
   * @see getTableName
   */
  public TableScanBase(TxBase tx, String tblname, LayoutBase layout) {
    // fill me in in the implementation class!
  }

  /** Returns the file name (relative to the dbDirectory parameter supplied to
   * the FileMgr) that the implementation used to name the file storing the
   * table's data.
   *
   * @return name of the file used by the implementation to store the table's
   * data.
   * @see java.io.File#getName
   */
  public abstract String getTableFileName();
  
} // class
