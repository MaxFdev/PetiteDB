package edu.yu.dbimpl.file;

/** Specifies the public API for the Page implementation by requiring all
 * Page implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must support EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * A Page represents the main-memory region used to hold the contents of a
 * block, and thus is used by the FileMgr in tandem with a BlockId.
 * Conceptually, the "main-memory" region is a sequence of bytes, into which
 * clients can insert values, and from which it can read values.
 *
 * A Page can hold any of the following value TYPES: ints, doubles, booleans,
 * strings, and "blobs" (i.e., arbitrary arrays of bytes).  At the Page API
 * abstraction, a client can store a value at any offset of the page but
 * remains responsible for knowing what values have been stored where.  The
 * result of trying to retrieve a value from the wrong offset is undefined.
 *
 * I prefer to not constrain implementation!  That said: your implementation
 * must follow the design for storing values of a given type (fixed-length
 * versus variable-length) as discussed in lecture and MUST BE FOLLOWED in your
 * implementation (see the Javadoc on maxLength() below).  Your implementation
 * MUST use exactly Integer.BYTES to store an int, Double.BYTES to store a
 * double, and 1 byte to store a boolean.
 *
 * Design note: a value MAY NOT be persisted across blocks!  Implication: the
 * length of a value's persisted bytes (obviously) cannot exceed block size nor
 * can the "offset + length of a value's persisted bytes" exceed block size.
 * Any attempt to "set" or "get" a value whose semantics imply "setting" or
 * "getting" a value that exceeds a single block size MUST RESULT in an
 * IllegalArgumentException.
 *
 * @author Avraham Leff
 */

import java.nio.charset.*;

public abstract class PageBase {
    /**
     * The conversion between a String and its byte representation is determined by
     * a character encoding. Several standard encodings exist (e.g., ASCII and
     * Unicode-16). Choosing one or the other encoding has large ramifications on a
     * DBMS storage layer. To simplify implementation, PetiteDB specifies that it
     * supports exactly one encoding, specified by the CHARSET constant.
     */
    public static Charset CHARSET = StandardCharsets.US_ASCII;

    /**
     * A charset chooses how many bytes each character encodes to. ASCII uses one
     * byte per character, whereas Unicode-16 uses between 2 bytes and 4 bytes per
     * character. We've already specified the only encoder that PetiteDB
     * implementations have to support (above), but to allow flexibility for system
     * evolution, this method explicitly deals with the fact that the DBMS cannot
     * know exactly how many bytes a given string will encode to. The maxLength
     * method calculates the maximum size of the blob for a string having a
     * specified number of characters. It does so by multiplying the number of
     * characters by the max number of bytes per character and adding 4 bytes for
     * the integer that is written with the bytes. See the "file_module" lecture for
     * the REQUIRED approach for persisting variable-length types such as String.
     */
    public static int maxLength(int strlen) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strlen * (int) bytesPerChar);
    }

    /**
     * Returns the logical length of a string
     *
     * @see maxLength
     */
    public static int logicalLength(final String s) {
        if (s == null) {
            throw new IllegalArgumentException("null string");
        }

        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return s.length() * ((int) bytesPerChar);
    }

    /**
     * Use this constructor when a Page's bytes are supplied implicitly by the Page
     * implementation.
     *
     * @param blocksize specifies the size of the blocks stored by a single Page:
     *                  must match the value supplied to the FileMgr constructor.
     *
     *                  Note: it's the client's responsibility to ensure that the
     *                  blocksize parameter is valid!
     */
    public PageBase(int blocksize) {
        // fill me in in your implementation class!
    }

    /**
     * Use this constructor when a Page's bytes are explicitly supplied by the
     * client. The client is responsible for ensuring that the byte[] has sufficient
     * space to store the data that are to be written into/read from the Page
     * instance.
     *
     * Space calculation should be done in terms of the raw space required by the
     * PetiteDB data-types, keeping in mind that Strings must be properly encoded,
     * per PageBase.maxLength, and per the rules for encoding variable-length data
     * types. That is: the client should not need to do any offset translation when
     * retrieving fields from the Page getter methods.
     *
     * @param b a byte array containing the memory that will be read from/written by
     *          the Page instance. Having invoked this constructor, the client is
     *          required to only set the byte array's state via calls to the
     *          PageBase API (e.g., setString()), and similarly to only get state
     *          via calls to the PageBase's API (e.g., getString()). Changes made by
     *          the Page instance will be reflected in the byte array parameter
     *          passed by the client.
     *
     *          Note: it's the client's responsibility to ensure that the byte
     *          array, after being serialized to disk, can be deserialized to fit
     *          into the blocksize supplied to the FileMgr constructor.
     */
    public PageBase(byte[] b) {
        // fill me in in your implementation class!
    }

    /**
     * For all of the getter methods
     *
     * @param offset the offset into the Page's main-memory from which the initial
     *               byte is read, cannot be a negative value
     * @throws IllegalArgumentException per design note above.
     */

    /**
     * For all of the setter methods
     *
     * @param offset the offset into the Page's main-memory at which the initial
     *               byte is written, cannot be a negative value
     * @param 2nd    parameter, the value to be written
     * @throws IllegalArgumentException per design note above.
     */

    public abstract int getInt(int offset);

    public abstract void setInt(int offset, int n);

    public abstract double getDouble(int offset);

    public abstract void setDouble(int offset, double d);

    public abstract boolean getBoolean(int offset);

    public abstract void setBoolean(int offset, boolean d);

    public abstract byte[] getBytes(int offset);

    /**
     * Writes the byte array to the specified offset in the block.
     *
     * @param offset the position to which the value will be written
     * @param b      the byte array to write
     * @throws IllegalArgumentException if the byte array is too large to fit into
     *                                  the block or if anything else goes wrong
     */
    public abstract void setBytes(int offset, byte[] b);

    public abstract String getString(int offset);

    /**
     * Writes the string to the specified offset in the block.
     *
     * @param offset the position to which the value will be written
     * @param s      the string to write
     * @throws IllegalArgumentException if the string is too large to fit into the
     *                                  block or if anything else goes wrong
     */
    public abstract void setString(int offset, String s);
}
