package edu.yu.dbimpl.tx;

/** Specifies the public API for the Transaction implementation by requiring
 * all Transaction implementations to implement this interface.
 *
 * Students MAY NOT modify this interface in any way.
 *
 * Transactions provide clients with the classic "ACID" properties, interfacing
 * with concurrency and recovery managers as necessary.
 *
 * Tx lifespan methods: the constructor begins a new transaction,
 * commit/rollback end that transaction.  "recover" rolls back ALL uncommitted
 * txs.
 *
 * Tx buffer management methods: all state written to, and read from, a buffer
 * is mediated through the appropriate setX/getX method.  All setX/getX methods
 * must acquire the appropriate locks before proceeding.  The method must throw
 * a LockAbortException if the DBMS is unable to acquire the lock within the
 * specified timeout period.  Before invoking these methods, clients must
 * invoke "pin" indicating that the tx should take control of the specified
 * block and the main-memory buffer that encapsulates that disk block.  The tx
 * maintains control until the client invokes "unpin".  The getX/setX methods
 * provide the hooks for Tx implementation to provide concurrency and recovery
 * function.
 *
 * Design note: a value MAY NOT be persisted across blocks!  Implication: the
 * length of a value's persisted bytes (obviously) cannot exceed block size nor
 * can the "offset + length of a value's persisted bytes" exceed block size.
 * Any attempt to "set" or "get" a value whose semantics imply "setting" or
 * "getting" a value that exceeds a single block size MUST RESULT in an
 * IllegalArgumentException.
 *
 * NOTE: Transaction instances are created by invoking TxMgrBase.newTx().
 *
 * @author Avraham Leff
 */

import edu.yu.dbimpl.file.BlockIdBase;

public interface TxBase {
   
  /** A Tx enters the ACTIVE status as soon as it's instantiated.  It remains
   * in that state until the client invokes "commit()", at which point it
   * enters the COMMITTING state, and (if commit succeeds), enters the
   * COMMITTED state from which it never exits.  Alternatively, if the client
   * invokes "rollback()" on an ACTIVE tx, the tx enters the ROLLING_BACK
   * state, and (if rollback succeeds), enters the ROLLED_BACK state from which
   * it next exits.
   *
   * A client can only invoke "recover()" when tx status is ACTIVE.  The tx
   * then enters the RECOVERING state, transitioning to RECOVERED when the
   * recovery process is complete, and doesn't exit from that state.
   *
   * All "getter" and "setter" methods MUST throw IllegalStateException if the
   * tx isn't in the ACTIVE state.  For the rest of the API, see the per-method
   * Javadoc to see when IllegalStateException MUST be thrown if the tx is not
   * in the prerequisite state.
   */
  public enum Status { ACTIVE, COMMITTING, COMMITTED, ROLLING_BACK, ROLLED_BACK,
                       RECOVERING, RECOVERED
  };

  /** Returns the current status of the transaction.  May be invoked regardless
   * of the tx's status.
   *
   * NOTE: the value returned may be only a point in time value since the
   * transaction may change status immediately after the value is returned.
   *
   * @return the current status
   */
  public Status getStatus();

  /** Every tx is associated with a unique non-negative integer id: this method
   * returns the tx's id.  May be invoked regardless of tx's status.
   *
   * Suggestion: if only to facilitate debugging, make these values increment
   * monotonically.
   * 
   * @return the unique tx id
   */
  public int txnum();

  /** Return the status of the tx.
   *
   * @return the tx status
   */

  /** Commits the current transaction: first flush all modified buffers (and
   * their log records); then write and flush a commit record to the log; then
   * release all locks, and unpin any pinned buffers.
   *
   * @throws IllegalStateException if tx isn't in the ACTIVE state.
   */
  public void commit();
   
  /** Roll the current transaction back: first undoes any modified values; then
   * flushes those buffers; then write and flush a rollback record to the log;
   * then releases all locks, and unpins any pinned buffers.
   *
   * @throws IllegalStateException if tx isn't in the ACTIVE state.
   */
  public void rollback();
   
  /** Flushes all modified buffers, then traverse the log, rolling back all
   * uncommitted transactions.  Finally, writes a quiescent "checkpoint record"
   * to the log.  This method MUST be called by the DBMS during system startup,
   * before processing user transactions so as to set the system to a
   * consistent state.  The method MAY be called by a client at any time, but
   * the method may then block until the system is deemed quiescent by the
   * DBMS.
   *
   * @throws IllegalStateException if tx isn't in the ACTIVE state.
   */
  public void recover();
   
  /** Pins the specified block to a page buffer.  Going forward, the
   * transaction will manage the buffer on behalf of the client (until "unpin"
   * is invoked)
   *
   * @param blk a reference to the disk block
   * @throws IllegalArgumentException if BlockId is null.  
   * @throws IllegalStateException if tx isn't in the ACTIVE state.
   * @see #unpin
   */
  public void pin(BlockIdBase blk);
   
  /** Unpins the specified block.
   *
   * @param blk a reference to the disk block
   * @throws IllegalStateException if tx isn't in the ACTIVE state.
   * @throws IllegalArgumentException if block isn't pinned by this tx
   * @see #pin
   */
  public void unpin(BlockIdBase blk);
   
  /** Returns the integer value stored at the specified offset of the specified
   * block.  The transaction acquires an "s-lock" on behalf of the client
   * before returning the value.
   *
   * @param blk a reference to a disk block
   * @param offset the byte offset within the block
   * @return the integer stored at that offset
   * @throws IllegalStateException if specified block isn't currently pinned by
   * this tx
   */
  public int getInt(BlockIdBase blk, int offset);

  /** Returns the boolean value stored at the specified offset of the specified
   * block.  The transaction acquires an "s-lock" on behalf of the client
   * before returning the value.
   *
   * @param blk a reference to a disk block
   * @param offset the byte offset within the block
   * @return the boolean stored at that offset
   * @throws IllegalStateException if specified block isn't currently pinned by this tx
   */
  public boolean getBoolean(BlockIdBase blk, int offset);

  /** Returns the double value stored at the specified offset of the specified
   * block.  The transaction acquires an "s-lock" on behalf of the client
   * before returning the value.
   *
   * @param blk a reference to a disk block
   * @param offset the byte offset within the block
   * @return the boolean stored at that offset
   * @throws IllegalStateException if specified block isn't currently pinned by this tx
   */
  public double getDouble(BlockIdBase blk, int offset);
    
  /** Returns the string value stored at the specified offset of the specified
   * block.  The transaction acquires an "s-lock" on behalf of the client
   * before returning the value.
   *
   * @param blk a reference to a disk block
   * @param offset the byte offset within the block
   * @return the string stored at that offset
   * @throws IllegalStateException if specified block isn't currently pinned by this tx
   */
  public String getString(BlockIdBase blk, int offset);

  /** Returns the byte value stored at the specified offset of the specified
   * block.  The transaction acquires an "s-lock" on behalf of the client
   * before returning the value.
   *
   * @param blk a reference to a disk block
   * @param offset the byte offset within the block
   * @return the byte stored at that offset
   * @throws IllegalStateException if specified block isn't currently pinned by this tx
   */
  public byte[] getBytes(BlockIdBase blk, int offset);
  
  /** Stores an integer at the specified offset of the specified block.  The
   * transaction acquires an "x-lock" on behalf of the client before reading
   * the value, creating the appropriate "update" log record and adding the
   * record to the log file.  Finally, the modified value is written to the
   * buffer.  The transaction is responsible for invoking the buffer
   * setModified() method, passing in the appropriate parameter values.
   *
   * @param blk a reference to the disk block
   * @param offset a byte offset within that block
   * @param val the value to be stored
   * @param okToLog true iff the client wants the operation to be logged, false
   * otherwise.
   * @throws IllegalStateException if specified block isn't currently pinned by this tx
   */
  public void
    setInt(BlockIdBase blk, int offset, int val, boolean okToLog);

  /** Stores an boolean at the specified offset of the specified block.  The
   * transaction acquires an "x-lock" on behalf of the client before reading
   * the value, creating the appropriate "update" log record and adding the
   * record to the log file.  Finally, the modified value is written to the
   * buffer.  The transaction is responsible for invoking the buffer
   * setModified() method, passing in the appropriate parameter values.
   *
   * @param blk a reference to the disk block
   * @param offset a byte offset within that block
   * @param val the value to be stored
   * @param okToLog true iff the client wants the operation to be logged, false
   * otherwise.
   * @throws IllegalStateException if specified block isn't currently pinned by this tx
   */
  public void
    setBoolean(BlockIdBase blk, int offset, boolean val, boolean okToLog);

  /** Stores a double at the specified offset of the specified block.  The
   * transaction acquires an "x-lock" on behalf of the client before reading
   * the value, creating the appropriate "update" log record and adding the
   * record to the log file.  Finally, the modified value is written to the
   * buffer.  The transaction is responsible for invoking the buffer
   * setModified() method, passing in the appropriate parameter values.
   *
   * @param blk a reference to the disk block
   * @param offset a byte offset within that block
   * @param val the value to be stored
   * @param okToLog true iff the client wants the operation to be logged, false
   * otherwise.
   * @throws IllegalStateException if specified block isn't currently pinned by this tx   */
  public void
    setDouble(BlockIdBase blk, int offset, double val, boolean okToLog);
  
  /** Stores a string at the specified offset of the specified block. The
   * transaction acquires an "x-lock" on behalf of the client before reading
   * the value, creating the appropriate "update" log record and adding the
   * record to the log file.  Finally, the modified value is written to the
   * buffer.  The transaction is responsible for invoking the buffer
   * setModified() method, passing in the appropriate parameter values.
   *
   * @param blk a reference to the disk block
   * @param offset a byte offset within that block
   * @param val the value to be stored
   * @param okToLog true iff the client wants the operation to be logged, false
   * otherwise.
   * @throws IllegalStateException if specified block isn't currently pinned by this tx
   */
  public void
    setString(BlockIdBase blk, int offset, String val, boolean okToLog);

  /** Stores a byte[] at the specified offset of the specified block. The
   * transaction acquires an "x-lock" on behalf of the client before reading
   * the value, creating the appropriate "update" log record and adding the
   * record to the log file.  Finally, the modified value is written to the
   * buffer.  The transaction is responsible for invoking the buffer
   * setModified() method, passing in the appropriate parameter values.
   *
   * @param blk a reference to the disk block
   * @param offset a byte offset within that block
   * @param val the value to be stored
   * @param okToLog true iff the client wants the operation to be logged, false
   * otherwise.
   * @throws IllegalStateException if specified block isn't currently pinned by this tx
   */
  public void
    setBytes(BlockIdBase blk, int offset, byte[] val, boolean okToLog);

  
  /** Returns the number of blocks in the specified file.
   *
   * Note: be sure to provide transactional semantics for this method.
   *
   * @param filename the name of the file
   * @return the number of blocks in the file
   * @throws IllegalStateException if tx isn't in the ACTIVE state.
   */
  public int size(String filename);
   
  /** Appends a new block to the end of the specified file and returns a
   * reference to it.
   *
   * Note: be sure to provide transactional semantics for this method.
   *
   * @param filename the name of the file
   * @return a reference to the newly-created disk block
   * @throws IllegalStateException if tx isn't in the ACTIVE state.   
   */
  public BlockIdBase append(String filename);
   
  /** Returns the size of blocks, uniform across all disk blocks managed by the
   * DBMS.
   *
   * @return the uniform size  of all disk blocks
   */
  public int blockSize();
   
  /** Returns the number of available (i.e. unpinned) buffers.
   *
   * @return the number of available buffers
   * @throws IllegalStateException if tx isn't in the ACTIVE state.      
   */
  public int availableBuffs();
} // interface
