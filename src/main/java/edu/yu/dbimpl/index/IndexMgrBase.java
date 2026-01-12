package edu.yu.dbimpl.index;

/** Specifies the public API for the IndexMgr implementation by requiring all
 * IndexMgr implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 * 
 * An IndexMgr manages and persists index meta-data (allowing clients to create
 * and retrieve such meta-data).  An IndexMgr also instantiates corresponding
 * index "runtime index" instances, allowing clients to iterate over an index's
 * records and access the corresponding data records.
 *
 * The DBMS has exactly one IndexMgr object (singleton pattern), which is
 * (conceptually) created during system startup, and in practice by a single
 * invocation of the constructor.
 *
 * Note to implementors: while not adequate for production deployments,
 * worst-case linear performance for these APIs is sufficient.
 *
 * @author Avraham Leff
 */

import java.util.Set;
import edu.yu.dbimpl.metadata.TableMgrBase;
import edu.yu.dbimpl.tx.TxBase;

public abstract class IndexMgrBase {

  /** Defines the set of index types that a client can use when creating an index.
   */
  public enum IndexType { STATIC_HASH };

  /** Constructor creates a new index manager.
   *
   * An index manager MUST access the DBConfiguration singleton to determine if
   * it is required to manage a brand-new database or to use an existing
   * database.  If the latter, the index manager is responsible for loading
   * previously persisted state before servicing client requests.
   *
   * An index manager MUST access the DBConfiguration singleton to determine
   * the number of buckets to use in a static hashing scheme.  It's the DBMS
   * client's responsibility to ensure that the value doesn't change after it's
   * initial ("startup") state.
   *
   * @param tx supplies the transactional scope for database operations used in
   * the constructor implementation, cannot be null.  It's the client's
   * responsibility to manage the lifecycle of this transaction, and to ensure
   * that the tx is active when passed to the constructor.
   * @param tableMgr to be used to persist index meta-data, cannot be null.
   * @throws IllegalArgumentException if arguments don't meet the
   * pre-conditions.
   */
  public IndexMgrBase(TxBase tx, TableMgrBase tableMgr) {
    // use the implementation class to provide function
  } // constructor


  /** Persists information about the specified index and returns the unique id
   * that the IndexMgr associates with this index.  If the index already
   * exists, no exception is thrown (the operation is a no-op), and the
   * implementation returns the id of the previously persisted index.
   *
   * @param tx supplies the transactional scope for database operations,
   * client's responsibility to ensure that not null.  It's the client's
   * responsibility to manage the lifecycle of this transaction.
   * @param tableName the name of the table-to-be-indexed, catalog information
   * for this table must already exist.
   * @param fieldName the name of the field-to-be-indexed, must match a field
   * in the table's catalog information.  The name of the index is identical to
   * the field name.
   * @param indexType type of the index (e.g., static hashing, B-Tree), must be
   * non-null.
   * @return the persisted id that is associated with the index information.
   * @throws IllegalArgumentException if arguments don't meet the
   * pre-conditions.
   * @see #get
   * @see #indexIds
   * @see #instantiate
   */
  public abstract int
    persistIndexDescriptor(TxBase tx, String tableName, String fieldName,
                           IndexType indexType);  

  /** Returns the unique index ids associated with the specified table name.
   *
   * @param tx supplies the transactional scope for database operations used in
   * the implementation, cannot be null.  It's the client's responsibility to
   * manage the lifecycle of this transaction.
   * @param tableName the table about which index information is being requested
   * @return Set containing the ids, empty set if no indices are defined for the table.
   * @throws IllegalArgumentException if IndexMgr.persistIndexDescriptor()
   * hasn't been invoked for this table name.
   */
  public abstract Set<Integer> indexIds(TxBase tx, String tableName);

  /** Given a unique index id, returns the associated IndexDescriptor.
   *
   * @param tx supplies the transactional scope for database operations used in
   * the implementation, cannot be null.  It's the client's responsibility to
   * manage the lifecycle of this transaction.
   * @param id identifies the index
   * @return corresponding IndexDescriptor, null if id is not associated with
   * with an index.
   */
  public abstract IndexDescriptorBase get(TxBase tx, int indexId);

  /** Returns an Index instance based on the persisted information associated
   * with the index descriptor id.  The Index instance isn't positioned
   * internally on any index record: use Index.beforeFirst() and next() to set
   * the index's internal state correctly.
   *
   * @param tx supplies the transactional scope for database operations used in
   * the implementation, cannot be null.  It's the client's responsibility to
   * manage the lifecycle of this transaction.
   * @param indexDescriptorId specifies a previously persisted IndexDescriptor
   * @return Index corresponding gto the indexDescriptorId.
   * @throws IllegalArgumentException if no information is associated with the
   * index descriptor id
   * @see persistIndexDescriptor
   */
  public abstract IndexBase instantiate(TxBase tx, int indexDescriptorId);

  /** Deletes all data, catalog metadata, and index metadata associated with
   * the specified table.
   *
   * @param tx supplies the transactional scope for database operations used in
   * the implementation, cannot be null.  It's the client's responsibility to
   * manage the lifecycle of this transaction.
   * @param tableName the table whose information is to be deleted.
   */
  public abstract void deleteAll(TxBase tx, String tableName);
  
} // abstract base class
