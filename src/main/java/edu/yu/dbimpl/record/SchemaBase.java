package edu.yu.dbimpl.record;

/** Specifies the public API for the Schema implementation by requiring all
 * Schema implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * A Schema represents the "logical" record schema of a table.  A schema
 * contains the name and type of each field of the table, as well as the length
 * of each varchar field.  Schemas have no knowledge of offsets within the
 * record.
 *
 * Design note: because I haven't architected a "delete field" API, it's ok for
 * clients to invoke "addField" (and its cousins) multiple times with the
 * semantics being an override of the previous state.
 *
 * NOTE: strings MUST BE typed as java.sql.Types.VARCHAR, booleans MUST be
 * typed as java.sql.Types.BOOLEAN, doubles as java.sql.Types.DOUBLE, and
 * integers as java.sql.Types.INTEGER.
 *
 * NOTE: Schema is conceptually a "value class", with all implications
 * concomitant thereto.
 */

import java.util.List;

public abstract class SchemaBase {

  /** No-arg constructor.
   */
  public SchemaBase() {
      // fill me in in your implementation class!
  }

  /** Adds a field to the schema having a specified name, type, and length.
   * Specifying "length" is very important for the "String" type because only
   * the client has knowledge of the "n" in "varchar(n)".  The server will
   * ignore the length value supplied for all fixed-char field types since it
   * will supply its own (presumabely correct) values.  No point in requiring
   * the client to be aware of the server's implementation choices.
   *
   * @param fldname the name of the field, cannot be null or empty
   * @param type the type of the field, using the constants in {@link
   * java.sql.Types}.
   * @param length the logical (in contrast to physical) length of a string
   * field, must be greater than 0.  The implementation must ignore all values
   * for all types other than VARCHAR.
   * @throws IllegalArgumentException if the specified pre-conditions aren't
   * met
   */
  public abstract void addField(String fldname, int type, int length);
   
  /** Adds an integer field to the schema.
   *
   * @param fldname the name of the field, cannot be null or empty
   * @throws IllegalArgumentException if the specified pre-conditions aren't
   * met
   */
  public abstract void addIntField(String fldname);

  /** Adds a boolean field to the schema.
   *
   * @param fldname the name of the field, cannot be null or empty
   * @throws IllegalArgumentException if the specified pre-conditions aren't
   * met
   */
  public abstract void addBooleanField(String fldname);

  /** Adds a double field to the schema.
   *
   * @param fldname the name of the field, cannot be null or empty
   * @throws IllegalArgumentException if the specified pre-conditions aren't
   * met
   */
  public abstract void addDoubleField(String fldname);
  
  /** Adds a string field to the schema.  The length is the logical length of
   * the field.  For example, if the field is defined as varchar(8), then its
   * length is 8.
   *
   * @param fldname the name of the field, cannot be null or empty
   * @param length the number of chars in the varchar definition
   * @throws IllegalArgumentException if the specified pre-conditions aren't
   * met
   */
  public abstract void addStringField(String fldname, int length);
   
  /** Adds a field to the schema, retrieving "by name" its type and length
   * information from the specified schema
   *
   * @param fldname the name of the field
   * @param sch the other schema
   * @throws IllegalArgumentException if the specified pre-conditions aren't
   * met
   */
  public abstract void add(String fldname, SchemaBase sch);
   
  /** Adds all fields from the specified schema to the current schema.
   *
   * @param sch the other schema
   * @throws IllegalArgumentException if the specified pre-conditions aren't
   * met
   */
  public abstract void addAll(SchemaBase sch);

  /** Returns the field names in this schema.
   *
   * @return the collection of the schema's field names
   */
  public abstract List<String> fields();
   
  /** Returns true iff the specified field is in the schema
   *
   * @param fldname the name of the field
   * @return true iff the field is in the schema
   */
  public abstract boolean hasField(String fldname);
   
  /** Returns the type of the specified field, using the
   * constants in {@link java.sql.Types}.
   *
   * @param fldname the name of the field
   * @return the integer type of the field
   * @throws IllegalArgumentException if the field doesn't exist.
   */
  public abstract int type(String fldname);
   
  /** Returns the logical length of the specified field.
   *
   * @param fldname the name of the field
   * @return the logical length of the field
   * @throws IllegalArgumentException if the field doesn't exist.
   */
  public abstract int length(String fldname);
} // class
