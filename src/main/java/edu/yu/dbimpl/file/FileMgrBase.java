package edu.yu.dbimpl.file;

/** Specifies the public API for the FileMgr implementation by requiring all
 * FileMgr implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must support EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * The file module as a whole is the lowest file in the PetiteDB stack.  It
 * enables higher-levels of the system to view the database as a collection of
 * blocks on disk, where all blocks contain the same number of (API-specified)
 * blocks.  The file module contains methods for random-access reading and
 * writing of blocks.
 *
 * The FileMgr always reads/writes/appends a block-sized number of bytes
 * from/to a file.  The FileMgr ensures that all file operations take place at
 * a block boundary, and ensures that each call to read/write/append incurs
 * exactly one disk access per call.
 *
 * The FileMgr class handles the actual interaction with the OS's file
 * system. Its constructor takes two arguments: a File specifying the root
 * directory of the database and an integer denoting the size of every database
 * block.  The root directory must be created relative to the directory from
 * which the JVM was invoked.  See e.g., https://stackoverflow.com/a/15954821
 * for the difference between user's directory and the current working
 * directory.
 *
 * Clients are forbidden for accessing the file system rooted in the directory
 * supplied to the FileMgr constructor EXCEPT as mediated by a PetiteDB API.
 *
 * Design note: the FileMgr creates files "on demand".  Specifically, if the
 * client invokes an API that references a file, and the file doesn't yet
 * exist, the FileMgr creates a new empty file with that name.
 *
 * Your implementation will likely invoke JDK methods that throw checked
 * exceptions.  As you can see, the API doesn't declare any checked exception:
 * you should catch and rethrow any such exception as a RuntimeException.
 *
 * The DBMS has exactly one FileMgr object (singleton pattern), which is
 * (conceptually) created during system startup, and in practice by a single
 * invocation of the constructor.
 *
 * Relevant lectures: intro-project and file_module
 *
 * @author Avraham Leff
 */

import java.io.*;

public abstract class FileMgrBase {

    /**
     * The file manager MUST access the DBConfiguration singleton to determine if it
     * is required to manage a brand-new database or to use an existing database. If
     * the former, the file manager is responsible for (re)initializing the file
     * system to a brand-new state (this is implementation specific). If the latter,
     * the file manager is responsible to not modify previously persisted state.
     *
     * @param dbDirectory specifies the location of the root database directory in
     *                    which files will be created. The root directory is the
     *                    containing directory for all database files. If no such
     *                    directory exists when the constructor is invoked, the
     *                    implementation must create it.
     * @param blockSize   size of blocks to be used in this database.
     * @throws IllegalStateException if DBConfiguration cannot supply startup
     *                               information.
     */
    public FileMgrBase(File dbDirectory, int blocksize) {
        // fill me in in your implementation class!
    }

    /**
     * Reads the specified data from disk into main-memory
     *
     * @param blk the disk location
     * @param p   the main-memory location
     */
    public abstract void read(BlockIdBase blk, PageBase p);

    /**
     * Writes the specified data from main-memory to disk
     *
     * @param blk the disk location
     * @param p   the main-memory location
     */
    public abstract void write(BlockIdBase blk, PageBase p);

    /**
     * Allocates a block at the end of the specified file. The contents of the new
     * block is implementation dependent, but a block of the correct size MUST be
     * written to disk after the call completes.
     *
     * @param filename specifies the file to which the block should be appended
     */
    public abstract BlockIdBase append(String filename);

    /**
     * Return the number of blocks of the specified file. If the File has not yet
     * been created, returns 0.
     *
     * @param filename specifies the file
     * @return the number of blocks of that file
     */
    public abstract int length(String filename);

    /**
     * Returns the value of the blockSize supplied to the constructor (and
     * conceptually a constant value used by all layers of the PetiteDB system).
     *
     * @return the block size used by the file module
     */
    public abstract int blockSize();
}
