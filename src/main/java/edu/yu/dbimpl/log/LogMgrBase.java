package edu.yu.dbimpl.log;

/** Specifies the public API for the LogMgr implementation by requiring all
 * LogMgr implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must support EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * The DBMS has exactly one LogMgr instance (singleton pattern), which is
 * created during system startup.
 *
 * The LogMgr is responsible for writing log records into its log file. (The
 * log file isn't exposed to any database client, and its data is accessible
 * only via the log file iterator()).  A LogMgr is responsible for writing and
 * returning log records but has no knowledge of the structure of these
 * records: as far as its concerned, it persists "a sequence of bytes".
 *
 * A LogMgr is free to persist additional meta-data to the block as long as it
 * only returns the client's bytes to the client.  However, for reasonably
 * sized blocks and log records, it's unreasonable to throw an
 * IllegalArgumentException because the meta-data causes log records to exceed
 * the size of a block.  In other words: efficiency counts, or at least
 * "excessive inefficiency" will cost you.
 *
 * When the database is started for the first time, the LogMgr
 * latest-sequence-number (LSN) MUST be initialized (to facilitate my testing)
 * to 0.  A successful call to append() returns the current LSN to the client
 * and then increments the value.  For subsequent instantiations of the
 * database, the LogMgr latest-sequence-number MUST be initialized to the
 * latest LSN value of the previously instantiated DBMS.  Similarly, all
 * previously persisted log records must be available to the LogMgr upon
 * database re-instantiation.
 *
 * Design note: it is recommended, but not required, for the log manager to
 * delegate all read/write function of its log file to the file manager.
 *
 * @author Avraham Leff
 */

import java.util.Iterator;
import edu.yu.dbimpl.file.FileMgrBase;

public abstract class LogMgrBase {

  /**
   * Creates the manager for the specified log file. If the log file does
   * not yet exist, it is created with an empty first block.
   *
   * The log manager MUST access the DBConfiguration singleton to determine if
   * implementation specific actions must be taken to (re)initialize the
   * necessary state. The client is responsible for creating the file manager
   * before invoking the log manager constructor. The log manager may
   * therefore rely on the file manager to do "file manager initialization".
   *
   * @param FileMgr the file manager
   * @param logfile the name of the log file.
   */
  public LogMgrBase(FileMgrBase fm, String logfile) {
    // fill me in your implementation class!
  }

  /**
   * Ensures that the log record corresponding to the specified LSN has been
   * written to disk. All log records in the same in-memory page as the
   * specified log record will also be written to disk.
   *
   * IMPORTANT: the implementation should use the LSN value to determine
   * whether the specified log record has already been written to disk. If it
   * HAS been written to disk, the method should be a no-op, potentially
   * improving performance by avoiding unnecessary disk-writes.
   * 
   * @param lsn the LSN of a log record
   */
  public abstract void flush(int lsn);

  /**
   * First flushes the log to disk, then return an Iterator over the contents
   * of the persisted log.
   *
   * The iterator iterates over log records in REVERSE order (most recently
   * created to earliest created). This is the ONLY architected use case for
   * the iterator, and is needed to support transaction recovery. At this
   * level of the module hierarchy, you do NOT need to understand why such
   * iteration is necessary: focus on providing this function.
   *
   * Design note: given that log iteration is done on startup by the recovery
   * manager as a single-threaded process, or on a per-tx basis for rollback,
   * the iterator need not be thread-safe. (Note to self: if these assumptions
   * are incorrect, then the iterator must be thread-safe.)
   * 
   * When this method returns (i.e., the iterator is instantiated), the
   * iterator is positioned after the LAST log record. Thus if there is even a
   * single log record: hasNext() will return true, and next() will return that
   * record. The hasNext() method returns false iff the current log record is
   * the first record in the log file (log iteration proceeds from latest
   * record to earliest record), otherwise returns true. The next() method
   * returns the "next earliest" log record. That is, it moves to the "next"
   * log record in the block (moving backwards). If there are no more log
   * records in the current block, then move to the previous block and return
   * the last log record from that block. Lather, rinse, and repeat.
   *
   * @return a LogIterator (typed as an Iterator<byte[]>)
   */
  public abstract Iterator<byte[]> iterator();

  /**
   * Appends a log record (as an arbitrary byte array), and return the current
   * ("pre-incremented") LSN to the client. If successful, the LSN is
   * incremented internally.
   *
   * Note: because performance matters, appending a record to the log MUST NOT
   * write to disk UNLESS the current log page/block doesn't have sufficient
   * room for the incoming record. In THAT case only, the LogRecord MUST write
   * the current log page/block to disk and append the record to a new page.
   *
   * Implication: to guarantee that a log record is immediately written to
   * disk, clients must invoke flush() after the append().
   *
   * Suggested implementation (non-dispositive, as long as the other API
   * semantics (including the Iterator) are provided) follows. Log records are
   * written from "right to left" order in a given Page (i.e., at decreasing
   * byte offsets in the main-memory page). Storing the records backwards
   * makes it easy to read them in reverse order.
   *
   * @param logrec a byte buffer containing the bytes. The only constraint is
   *               that the array must fits inside a single Page.
   * @return the LSN at the time before it was incremented as a side-effect of
   *         this method.
   * @throws IllegalArgumentException if the log record is too large to fit
   *                                  into a single page. The implementation can
   *                                  include "reasonable" amounts
   *                                  of meta-data when determining that a log
   *                                  record is too big.
   * @see #flush
   */
  public abstract int append(byte[] logrec);
}
