package edu.yu.dbimpl.tx.recovery;

/** Interface supported by all Log Record types.
 *
 * NOTE: implementing this interface is optional because LogRecords are an
 * implementation detail of the recovery manager.  The interface specification
 * is only a design suggestion.  See lecture for more explanation.
 *
 * @author Avraham Leff
 */

import edu.yu.dbimpl.tx.TxBase;

interface LogRecord {
  /** Returns the log record's type. 
   *
   * @return the log record's type
   */
  int op();

  /** Returns the transaction id stored with the log record.
   *
   * @return the log record's transaction id
   */
  int txNumber();

  /** Undoes the operation encoded by this log record.  The "undo" semantics
   * may not apply to all LogRecord types, and they are free to provide a no-op
   * implementation.
   *
   * @param tx the transaction that is performing the undo operation.
   */
  void undo(TxBase tx);
}
