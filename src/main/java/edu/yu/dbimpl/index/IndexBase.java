package edu.yu.dbimpl.index;

import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.record.RID;

/** Specifies the interface for all Index implementations.
 *
 * Students MAY NOT modify this interface in any way.
 * 
 * An index is a file of index records such that an (index) record exists in
 * the index file for each record in the "real" table that's being indexed.
 * Each index record contains a RID (the record identifier of the "real record"
 * that's being indexed).  An index record also contains a "value": this is the
 * value of the field being indexed in the "real" record.
 *
 * The Index API resembles a customized TableScan in the sense that clients can
 * position the index at the beginning of the index file and iterate
 * sequentially through its records.  Unlike a TableScan, the client specifies
 * that she is only interested in index records whose "value" field matches a
 * specified "search key".  Iteration only proceeds through records whose
 * "value" field matches the "search key".  When positioned on an index record,
 * clients can read the index record's RID and pass that value to a Scan
 * instance on the "real" table to access the corresponding "real" record.
 * Clients can also use an Index to insert new index records or to delete index
 * reccords.
 * 
 * NOTE: this design precludes support of composite indices.
 */

public interface IndexBase {
   
  /** Positions the index before the first record (if any) whose "value"
   * matches the value of the specified search key.  Clients must invoke a
   * "next" that returns true in order to read an index record that matches the
   * search semantics.
   *
   * NOTE: "match" semantics are ".equals" semantics
   *
   * NOTE: client navigational use of an index requires that they first invoke
   * beforeFirst().  Otherwise, the index has no idea as to what it should look
   * for (i.e., what it must navigate to).
   *
   * @param searchKey the search key value.
   * @throws IllegalArgumentException if the IndexDescriptor associated with this
   * index implies that the searchKey parameter is incompatible with the index
   * definition
   * @see IndexDescriptorBase
   * @see DatumBase#equals
   */
  public void beforeFirst(DatumBase searchKey);
   
  /**  Moves the index cursor to the next record whose value matches the value
   * of the search key supplied in the beforeFirst method.
   *
   * @return True if such a record exists, false otherwise
   * @throws IllegalStateException if beforeFirst has not previously supplied a
   * search key.
   */
  public boolean next();
   
  /** Returns the RID value stored in the current index record.
   *
   * @return the current index record's RID.
   * @throws IllegalStateException if the index hasn't been positioned on a
   * valid index record
   * @see next
   */
  public RID getRID();
   
  /** Inserts an index record having the specified value and RID.
   *
   * @param value the "search key" value in the new index record.
   * @param rid specifies the corresponding record in the "real" table
   * @throws IllegalArgumentException if the IndexDescriptor associated with
   * this index implies that the valued parameter is incompatible with the
   * index definition
   */
  public void insert(DatumBase value, RID rid);
   
  /** Deletes the index record having the specified value and RID.
   *
   * @param value the "search key" value of the record to be deleted.
   * @param rid specifies the corresponding record in the "real" table. 
   * @throws IllegalArgumentException if the IndexDescriptor associated with
   * this index implies that the valued parameter is incompatible with the
   * index definition
   */
  public void delete(DatumBase value, RID rid);
   
  /** Deletes all index records associated with this index.
   *
   */
  public void deleteAll();
    

  /** Closes all resources (if any) used by the index.
   */
  public void close();
}
