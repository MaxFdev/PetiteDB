package edu.yu.dbimpl.index;

/** Specifies the public API for the IndexDescriptor implementation by
 * requiring all IndexDescriptor implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 * 
 * An IndexDescriptor is a main-memory enapsulation of database information
 * about an index.  An IndexMgr persists IndexDescriptor state (associating
 * instances with an "index id"), and instantiates an Index based on persisted
 * IndexDescriptor state.
 *
 * @author Avraham Leff
 */

import static edu.yu.dbimpl.index.IndexMgrBase.IndexType;
import edu.yu.dbimpl.record.SchemaBase;

public abstract class IndexDescriptorBase {

  /** Constructor creates an instance that encapsulates information about the
   * specified index.
   *
   * @param tableName the name of the table on which the index is defined
   * @param indexedTableSchema the schema of the table on which the index is defined
   * @param indexName must uniquely identify the index relative to the
   * specified table's scope
   * @param fieldName the name of the indexed field
   * @param indexType the index type
   */
  public IndexDescriptorBase(String tableName, SchemaBase indexedTableSchema,
                             String indexName, String fieldName,
                             IndexType indexType)
  {
    // use the implementation class to provide function
  }
   
  public abstract String getTableName();

  public abstract SchemaBase getIndexedTableSchema();

  public abstract String getIndexName();

  public abstract String getFieldName();

  public abstract IndexType getIndexType();

} // class
