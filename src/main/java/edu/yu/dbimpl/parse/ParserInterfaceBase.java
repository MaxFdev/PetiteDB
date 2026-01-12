package edu.yu.dbimpl.parse;

/** Defines a "black box" set of functionality that must be supported by a
 * PetiteDB parser.  The word "interface" is included in the class name despite
 * the fact that it's not formally an interface (see design note below).
 *
 * Clients can use this class to (only) parse sub-clauses in the PetiteDB
 * grammar (e.g., a Term or Expression).  If a client's request can be
 * satisfied from the current parser position, the parse MAY NOT throw an
 * exception, must satisfy the request, even if the remainder of the input is
 * not recognized by the grammar.  The problems with the input will only
 * manifest if the client continues to request PetiteDB clauses that can't be
 * satisfied subsequently.

 * Students MAY NOT modify this class in any way!.
 *
 * Design note: this class does NOT define the interface to the PetiteDB
 * parser.  I've deliberately not specified such an API in order to give
 * complete implementation freedom.  Instead, this class defines a "results
 * oriented" API: given a String to parse, does your parser construct the
 * correct return type (or, if necessary, throw BadSyntaxException)
 *
 * Design note: as a "results oriented" API, this class isn't responsible for
 * determining whether the overall input is valid.  Rather: given the current
 * state of the parsing in the input, determine whether the specified SQL
 * constructed can be parsed from the remaining portion of the input.
 *
 * Semantics note: the parser and lexer should fail-early-and-fatally after
 * throwing a BadSyntaxException.  Do not try to recover from the error, let
 * the client fix it and try again.  The behavior of subsequent API invocations
 * on the input by this parser or lexer instance is undefined.
 * 
 * @author Avraham Leff
 */

import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.query.Expression;
import edu.yu.dbimpl.query.Predicate;
import edu.yu.dbimpl.query.Term;

public abstract class ParserInterfaceBase {

  /** Constructor: stores the supplied string so as to be able to parse later
   * per the client's explicit subsequent "typing" invocations.  The
   * constructor MAY NOT throw a BadSyntaxException even if the string is not
   * recognized by the PetiteDB grammar.
   *
   * See lecture discussion for the parser requirements.
   *
   * @param s the string to be parsed
   */      
  public ParserInterfaceBase(String s) {
    // fill me in with your implementation
  }

  /** Parses the constructor parameter as a "field" (see PetiteDB grammar)
   *
   * @return the parsed field if valid
   * @throws BadSyntaxException if the parser (given its current state relative
   * to the input) can't extract the specified grammar construct.
   */
  public abstract String field();

  /** Parses the constructor parameter as a "constant" (see PetiteDB grammar)
   *
   * @return the parsed field if valid
   * @throws BadSyntaxException if the parser (given its current state relative
   * to the input) can't extract the specified grammar construct.
   */
  public abstract DatumBase constant();

  /** Parses the constructor parameter as an "Expression" (see PetiteDB grammar)
   *
   * @return the parsed field if valid
   * @throws BadSyntaxException if the parser (given its current state relative
   * to the input) can't extract the specified grammar construct.
   */
  public abstract Expression expression();

  /** Parses the constructor parameter as an "Term" (see PetiteDB grammar)
   *
   * @return the parsed field if valid
   * @throws BadSyntaxException if the parser (given its current state relative
   * to the input) can't extract the specified grammar construct.
   */
  public abstract Term term();

  /** Parses the constructor parameter as an "Predicate" (see PetiteDB grammar)
   *
   * @return the parsed field if valid
   * @throws BadSyntaxException if the parser (given its current state relative
   * to the input) can't extract the specified grammar construct.
   */
  public abstract Predicate predicate();

  /** Parses the constructor parameter as an "Query" (see PetiteDB grammar),
   * representing the parsed state as an instance of ParseQueryBase.
   *
   * @return the corresponding parsed query instance if input is valid
   * @throws BadSyntaxException if the parser (given its current state relative
   * to the input) can't extract the specified grammar construct.
   */
  public abstract ParseQueryBase query();
  

}
