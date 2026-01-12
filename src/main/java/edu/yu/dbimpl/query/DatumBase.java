package edu.yu.dbimpl.query;

/** A "value class" encapsulating a single database value (datum): the value
 * can be exactly one of a set of possible Java types.  Although the
 * getters/setters are necessarily specified in terms of Java types, the API
 * requires that a datum be associated with an SQL type.  Specifically: strings
 * MUST BE typed as java.sql.Types.VARCHAR, binary arrays MUST be typed as
 * java.sql.Types.VARBINARY, booleans MUST be typed as java.sql.Types.BOOLEAN,
 * doubles as java.sql.Types.DOUBLE, and integers as java.sql.Types.INTEGER.
 * This approach follows that specified by SchemaBase.
 *
 * NOTE: a DatumBase is conceptually a "value class", with all implications
 * concomitant thereto.
 *
 * Design note: while the Datum constructors are passed Java objects, the asX()
 * methods returns primitive values corresponding to the object parameters.
 * Given constructor parameter Y, the semantics of a asX() method is specified
 * by JDK Y.xValue().  (Strings are convertible to byte arrays via
 * String.getBytes(), using the DBMS Charset), and byte arrays are convertible
 * to Strings via the appropriate String constructor.) The implementation must
 * throw a ClassCastException if the object parameters do not support the
 * implied Y.xValue() method invocation.
 * 
 * Design note: the semantics of Datum.equals are "compares this Datum to the
 * specified object. The result is true if and only if the argument is not null
 * and is a Datum object that contains the same (by ".equals" semantics)
 * wrapped value as this Datum instance."  In other words, no "implict type
 * conversion" is done by the DBMS.
 *
 * Design note: a case can be made that the semantics of compareTo should be
 * based on a Datum's primitive value and e.g., allow comparison between 42.0
 * and 42.  That said, having equivalent semantics for .equals and compareTo is
 * so important that Datum.compareTo semantics MUST be based on the object
 * passed to the Datum constructor.  Datum.compareTo MUST throw a
 * ClassCastException if the constructor objects are not of the same type.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * @author Avraham Leff 
 */

public abstract class DatumBase implements Comparable<DatumBase> {

  /** Constructor: wrap a Java integer.
   */
  public DatumBase(final Integer ival) {
    // subclass provides implementatuion
  }
   
  /** Constructor: wrap a Java String.
   */
  public DatumBase(final String sval) {
    // subclass provides implementatuion    
  }

  /** Constructor: wrap a Java boolean.
   */
  public DatumBase(final Boolean bval) {
    // subclass provides implementatuion    
  }

  /** Constructor: wrap a Java double.
   */
  public DatumBase(final Double dval) {
    // subclass provides implementatuion    
  }

  /** Constructor: wrap a binary array
   */
  public DatumBase(final byte[] array) {
    // subclass provides implementation
  }
  

  /** Returns the value encapsulated by the DatumBase with semantics specified
   * by "design note" above.
   *
   * @throws ClassCastException if the encapsulated value is the wrong type.
   */
  public abstract int asInt();

  /** Returns the value encapsulated by the DatumBase with semantics specified
   * by "design note" above.
   *
   * @throws ClassCastException if the encapsulated value is the wrong type.
   */
  public abstract boolean asBoolean();


  /** Returns the value encapsulated by the DatumBase with semantics specified
   * by "design note" above.
   *
   * @throws ClassCastException if the encapsulated value is the wrong type.
   */
  public abstract double asDouble();


  /** Returns the value encapsulated by the DatumBase with semantics specified
   * by "design note" above.
   *
   * @throws ClassCastException if the encapsulated value is the wrong type.
   */
  public abstract String asString();

  /** Returns the value encapsulated by the DatumBase with semantics specified
   * by "design note" above.
   *
   * @throws ClassCastException if the encapsulated value is the wrong type.
   */
  public abstract byte[] asBinaryArray();

  
  /** Return the type of the wrapped value as a value from the set of constants
   * defined by java.sql.Types
   *
   * @return the type of the datum.
   */
  public abstract int getSQLType();
}
