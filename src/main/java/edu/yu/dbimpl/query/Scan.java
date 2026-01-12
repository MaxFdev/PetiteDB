package edu.yu.dbimpl.query;

/** A Scan represents the output of a relational algebra query.  The elegance
 * of the relational model implies that a "database table" can be represented
 * by a Scan instance since the output of a query is itself a relation (and a
 * "table" is a relation).  
 *
 * A Scan can be usefully thought of as a "read-only" Iterator (over the set of
 * records returned by the query).
 *
 * Students MAY NOT change this interface IN ANY WAY!
 */
public interface Scan {
   
  /** Positions the scan before its first record. A subsequent call to next()
   * will return the first "in-use" record.
   */
  public void beforeFirst();
   
  /** Moves the scan to the next "in-use" record (if one exists)
   *
   * @return true, iff succeeded in moving to the next "in-use" record; false
   * otherwise (no such record exists).
   */
  public boolean next();
   
  /** Returns the value of the specified integer field in the current record.
   *
   * @param fldname the name of the field
   * @return the field's integer value in the current record
   * @throws IllegalArgumentException if fldname is not part of the schema or
   * if the type associated with fldname is incompatible with the expected
   * return value.
   */
  public int getInt(String fldname);

  /** Returns the value of the specified boolean field in the current record.
   *
   * @param fldname the name of the field
   * @return the field's boolean value in the current record
   * @throws IllegalArgumentException if fldname is not part of the schema or
   * if the type associated with fldname is incompatible with the expected
   * return value.
   */
  public boolean getBoolean(String fldname);

  /** Returns the value of the specified double field in the current record.
   *
   * @param fldname the name of the field
   * @return the field's double value in the current record
   * @throws IllegalArgumentException if fldname is not part of the schema or
   * if the type associated with fldname is incompatible with the expected
   * return value.
   */
  public double getDouble(String fldname);
  
  /** Returns the value of the specified string field in the current record.
   *
   * @param fldname the name of the field
   * @return the field's string value in the current record
   * @throws IllegalArgumentException if fldname is not part of the schema or
   * if the type associated with fldname is incompatible with the expected
   * return value.
   */
  public String getString(String fldname);
   
  /** Returns the value of the specified field in the current record.  The
   * value is expressed as a DatumBase.
   *
   * @param fldname the name of the field
   * @return the value of that field, expressed as a DatumBase.
   * @throws IllegalArgumentException if fldname is not part of the schema
   */
  public DatumBase getVal(String fldname);
   
  /** Returns true iff the scan has the specified field.
   *
   * @param fldname the name of the field
   * @return true iff the scan has that field, false otherwise
   */
  public boolean hasField(String fldname);
   
  /** Returns the type of the specified field.
   *
   * return the type of the wrapped value as a value from the set of constants
   * defined by java.sql.Types
   * @param fldname the name of the field.
   * @throws IllegalArgumentException if fldname is not part of the schema
   */
  public int getType(String fldname);

  /** Terminate the scan processing (and automatically also close all
   * underlying scans, if any), closing all resources, including unpinning any
   * pinned blocks.
   */
  public void close();
}
