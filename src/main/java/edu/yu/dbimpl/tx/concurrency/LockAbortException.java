package edu.yu.dbimpl.tx.concurrency;

/** An unchecked exception that indicates that the dbms aborted a transaction
 * because it could not acquire all locks required by that transaction.
 *
 * @author Avraham Leff
 */

@SuppressWarnings("serial")
public class LockAbortException extends RuntimeException {
  public LockAbortException() {
    super();
  }

  public LockAbortException(final String msg) {
    super(msg);
  }

  public LockAbortException(String msg, Throwable cause) {
    super(msg, cause);
  }

}
