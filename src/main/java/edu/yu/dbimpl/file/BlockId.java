package edu.yu.dbimpl.file;

import java.util.Objects;

public class BlockId extends BlockIdBase {

    private final String filename;
    private final int number;

    public BlockId(String filename, int blknum) {
        super(filename, blknum);

        if (filename == null) {
            throw new IllegalArgumentException("Filename can't be null");
        }

        this.filename = filename.trim();
        if (this.filename.length() == 0) {
            throw new IllegalArgumentException("Filename must have a length greater than 0");
        }

        if (blknum < 0) {
            throw new IllegalArgumentException("Block number must be >= 0");
        }
        this.number = blknum;
    }

    @Override
    public String fileName() {
        return this.filename;
    }

    @Override
    public int number() {
        return this.number;
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof BlockId) {
            BlockId otherBlock = (BlockId) other;
            return this.filename.equals(otherBlock.filename) && this.number == otherBlock.number;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.filename, this.number);
    }

    @Override
    public String toString() {
        StringBuilder stb = new StringBuilder();
        stb.append("BlockID: [file: ")
                .append(this.filename)
                .append(", block: ")
                .append(this.number)
                .append("]");
        return stb.toString();
    }

}
