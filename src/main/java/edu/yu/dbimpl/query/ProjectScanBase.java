package edu.yu.dbimpl.query;

/** Specifies the public API for the SelectScan implementation by requiring all
 * implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * The ProjectScan class implements the "project" relational algebra operation
 * using a pipelining approach in which all methods EXCEPT next() are delegated
 * "as is" to the underlying scan.
 *
 * @author Avraham Leff
 */

import java.util.List;

public abstract class ProjectScanBase implements Scan {

  /** Constructor: create a project scan having the specified underlying scan
   * and field list of columns to keep (all other field names will be dropped).
   *
   * @param scan representing the input relation, client transfers ownership,
   * e.g., the implementation can invoke Scan.beforeFirst().
   * @param fields list of field names to keep
   */
  public ProjectScanBase(Scan scan, List<String> fields) {
    // fill me in with your implementation!
  }
}
