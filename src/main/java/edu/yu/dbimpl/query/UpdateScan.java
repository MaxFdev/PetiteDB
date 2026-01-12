package edu.yu.dbimpl.query;

import edu.yu.dbimpl.record.RID;

/** Extends the Scan interface beyond "read-only" operations to include "update
 * records" operations.
 *
 * Students MAY NOT change this interface IN ANY WAY!
 */
public interface UpdateScan extends Scan {

  /** Modifies the field value in the current record.
   *
   * @param fldname the name of the field
   * @param val the new value, expressed as a DatumBase
   * @throws IllegalArgumentException if fldname is not part of the schema or
   * if the type of val is incorrect for fldname's type
   */
  public void setVal(String fldname, DatumBase val);
   
  /** Modifies the field value in the current record.
   *
   * @param fldname the name of the field
   * @param val the new integer value
   * @throws IllegalArgumentException if fldname is not part of the schema or
   * if the type of val is incorrect for fldname's type
   */
  public void setInt(String fldname, int val);

  /** Modifies the field value in the current record.
   *
   * @param fldname the name of the field
   * @param val the new double value
   * @throws IllegalArgumentException if fldname is not part of the schema or
   * if the type of val is incorrect for fldname's type
   */
  public void setDouble(String fldname, double val);

  /** Modifies the field value in the current record.
   *
   * @param fldname the name of the field
   * @param val the new boolean value
   * @throws IllegalArgumentException if fldname is not part of the schema or
   * if the type of val is incorrect for fldname's type
   */
  public void setBoolean(String fldname, boolean val);
  
  /** Modifies the field value in the current record.
   *
   * @param fldname the name of the field
   * @param val the new string value
   * @throws IllegalArgumentException if fldname is not part of the schema or
   * if the type of val is incorrect for fldname's type
   */
  public void setString(String fldname, String val);
   
  /** Inserts a new record somewhere in the scan after the current record,
   * positioning the scan on that record.  If scanning a physical set of
   * records (cf TableScan), the record's "in-use" flag is set to true.
   */
  public void insert();
   
  /** Deletes the current record from the scan.  This operation does not
   * advance the cursor.
   */
  public void delete();
   
  /** Returns the id of the current record.
   *
   * @return the id of the current record
   */
  public RID  getRid();
   
  /**  Positions the scan so that the current record has the specified id.
   *
   * @param rid the id of the desired record
   */
  public void moveToRid(RID rid);
}
