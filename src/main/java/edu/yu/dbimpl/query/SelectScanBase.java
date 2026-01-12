package edu.yu.dbimpl.query;

/** Specifies the public API for the SelectScan implementation by requiring all
 * implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * The SelectScan class implements the "select" relational algebra operation
 * using a pipelining approach in which all methods EXCEPT next() are delegated
 * "as is" to the underlying scan.
 *
 * @author Avraham Leff
 */

import edu.yu.dbimpl.record.*;

public abstract class SelectScanBase implements UpdateScan {

	/** Constructor: creates a select scan having the specified underlying scan
	 * and predicate.
   *
	 * @param scan the scan representing the input relation, client transfers
	 * ownership, e.g., the implementation can invoke Scan.beforeFirst().
	 * @param predicate the selection predicate that will be applied to filter
	 * the input relation.
   * @throws IllegalArgumentException if predicate doesn't logically apply to
   * the scan: e.g., contains a Term with an op whose semantics don't apply to
   * either its RHS or LHS or a Term whose expressions reference a field name
   * that doesn't match the scan's schema.  See the "query processing" lecture
   * for further discussion.
	 */
	public SelectScanBase(Scan scan, Predicate predicate) {
    // Fill in your subclass's constructor with useful code!
  }
}
