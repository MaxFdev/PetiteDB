package edu.yu.dbimpl.record;

/** An identifier for a record within a file using a pair of values to uniquely
 * specify the record: its block number in the file and its slot number within
 * the block.
 *
 * A RID is a value class and overrides the necessary base class methods
 * appropriately.
 *
 * Students MAY NOT change this class IN ANY WAY!
 */

public class RID {
	/** Constructor that supplies the pair of values used to uniquely identify a
   * record.
   *
	 * @param blknum the block number in the file where the record is stored
	 * @param slot the record's location within the specified block
	 */
	public RID(final int blknum, final int slot) {
		this.blknum = blknum;
		this.slot   = slot;
	}

	/** Return the block number associated with this RID.
   *
	 * @return the block number
	 */
	public int blockNumber() {
		return blknum;
	}

	/** Returns the slot associated with this RID.
   *
	 * @return the slot
	 */
	public int slot() {
		return slot;
	}

  @Override
	public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    
    // "null instanceof [type]" also returns false 
    if (!(obj instanceof RID)) {
      return false;
    }

		final RID r = (RID) obj;
		return blknum == r.blknum && slot == r.slot;
	}

  @Override
  public int hashCode() {
    // @fixme not using Objects.hash() because nervous about cost of autoboxing
    // (premature optimization?)
    int hash = 7;
    hash = 31 * hash + blknum;
    hash = 31 * hash + slot;
    return hash;
  }
  
  @Override
	public String toString() {
		return "[blk=" + blknum + ", slot=" + slot + "]";
	}

	private final int blknum;
	private final int slot;
}
