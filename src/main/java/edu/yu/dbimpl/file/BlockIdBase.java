package edu.yu.dbimpl.file;

/**
 * Specifies the public API for the BlockId implementation by requiring all
 * BlockId implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must support EXACTLY the
 * constructor signatures specified in the base class (and NO OTHER signatures).
 *
 * A BlockId object represents the physical position of a block using its file
 * name and logical block number. A BlockId is an immutable value class:
 * implementations should consider the implications of that design.
 *
 * @author Avraham Leff
 */

public abstract class BlockIdBase {
    /**
     * Constructor
     *
     * @param fileName Specified relative to the FileMgr dbDirectory; must have a
     *                 trimmed length that's greater than 0.
     * @param blknum   must be a non-negative integer
     * @throws IllegalArgumentException as appropriate
     */
    public BlockIdBase(String filename, int blknum) {
        // fill me in in your implementation class!
    }

    abstract public String fileName();

    abstract public int number();
}
