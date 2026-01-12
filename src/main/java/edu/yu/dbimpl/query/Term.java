package edu.yu.dbimpl.query;

/** A Term is a comparison between two Expressions (see the lecture that
 * defines the current restrictions on what must be supported).  The class is
 * immutable by design.
 *
 * Students MAY NOT modify this class in any way!
 */

import java.util.Objects;
import edu.yu.dbimpl.parse.LexerBase;

public class Term {
  /** Constructor that creates a term that compares the supplied two
   * expressions for equality.
   *
   * @param lhs the LHS expression, cannot be null
   * @param op the comparison operator
   * @param rhs the RHS expression, cannot be null
   */
  public Term(final Expression lhs, final char op, final Expression rhs) {
    if (lhs == null) {
      throw new IllegalArgumentException("Null lhs");
    }
    if (rhs == null) {
      throw new IllegalArgumentException("Null rhs");
    }
    if (!LexerBase.comparisonOperators.contains(op)) {
      throw new IllegalArgumentException("Unknown operator <"+op+">");
    }
    
    this.lhs = lhs;
    this.op = op;
    this.rhs = rhs;
  }
   
  @Override
  public String toString() {
    return lhs.toString() + " " + op + " " + rhs.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
 
    // "null instanceof [type]" also returns false 
    if (!(obj instanceof Term)) {
      return false;
    }
         
    final Term that = (Term) obj;
    return this.rhs.equals(that.rhs) && this.op == that.op && this.lhs.equals(that.lhs);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(rhs, op, lhs);
  }

  // package protected, safe to do so because "immutable"
  final Expression lhs, rhs;
  final char op;
}
