package edu.yu.dbimpl.parse;

/** Defines an API for artifacts returned by a Parser's processing of a select
 * statement (see the PetiteDB grammar for details)

 * Students MAY NOT modify this class in any way!.
 *
 * @author Avraham Leff
 */

import java.util.*;
import edu.yu.dbimpl.query.Predicate;

public abstract class ParseQueryBase {

  /** Constructor: saves the parameters.
   *
   * @param fields the fields specified in the select statement
   * @param tables the tables specified in the select statement
   * @param predicate the predicate specified in the select statement
   */
  public ParseQueryBase
    (List<String> fields, Collection<String> tables, Predicate predicate)
  {
    // specify implementation in your class
  }
   
  /** Returns the fields supplied to the constructor.
   *
   * @return a list of field names
   */
  public abstract List<String> fields();
   
  /** Returns the tables supplied to the constructor.
   *      
   * @return a collection of table names
   */
  public abstract Collection<String> tables();
   
  /** Returns the predicate supplied to the constructor.
   *
   * @return the query predicate
   */
  public abstract Predicate predicate();
} // ParseQueryBase
