package edu.yu.dbimpl.record;

/** Specifies the public API for Layout implementation by requiring all Layout
 * implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * A Layout encapsulates and augments the record "logical schema" meta-data
 * (name, type, length) provided by SchemaBase with per-field offset
 * information, and augments this information with "physical schema" meta-data
 * such as "offsets" and "slot size".  In contrast with the logical view
 * presented by the SchemaBase, a Layout presents a physical view.  Thus the
 * "length" of an int field does have meaning for Layout, even though it's
 * undefined for SchemaBase.
 *
 * Because the Layout encapsulates the Schema supplied by the client, the
 * client transfers ownership of the schema, and may no longer mutate the
 * Schema instance.
 *
 * The first bytes of the record MUST BE used to store the Boolean-valued
 * "in-use/empty" flag.  All records fields MUST BE layed out in the order that
 * the client invoked addField.  Aside from this requirement, layout offsets
 * are implementation dependent because field order is implementation
 * dependent.  Only the sum of the offsets ("slotSize") must be the same across
 * implementations.
 *
 * The implementation MUST assign the offset of field #i+1 to be located at the
 * location at which field #1 ends: i.e., a fixed-length layout with no extra
 * padding.
 *
 * NOTE: Layout is conceptually a "value class", with all implications
 * concomitant thereto.
 */

import java.util.Map;

public abstract class LayoutBase {

  /** Constructs a Layout object from a SchemaBase.  This constructor is used
   * when a table is created. It determines the physical offset of each field
   * within the record.
   *
   * @param schema the schema of the table's records
   */
  public LayoutBase(SchemaBase schema) {
    // fill me in in your implementation class!
  }

  /** Constructs a Layout object from a SchemaBase, and assumes that the supplied
   * offset and slot size information is correct.  Intended for when the
   * metadata is retrieved from the internal catalog.
   *
   * @param schema the schema of the table's records
   * @param offsets the already-calculated offsets of the fields within a record
   * @param slotSize pre-calculated length of each record slot
   */
  public LayoutBase(SchemaBase schema, Map<String,Integer> offsets,
                int slotSize)
  {
    // fill me in
  }

  /** Returns the encapsulated schema.
   *
   * @return the table's record schema
   */
  public abstract SchemaBase schema();


  /** Returns the offset of a specified field within a record
   *
   * @param fldname the name of the field
   * @return the offset of that field within a record
   * @throws IllegalArgumentException if fldname isn't defined for this Layout
   */
  public abstract int offset(String fldname);

  /** Returns the size of a slot, in bytes.
   *
   * @return the size of a slot
   */
  public abstract int slotSize();
} // class

