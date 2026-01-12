package edu.yu.dbimpl.buffer;

/**  An unchecked exception that indicates that a client's request for a Buffer
 * instance could not be satisfied.
 *
 * @author Avraham Leff
 */
@SuppressWarnings("serial")
public class BufferAbortException extends RuntimeException {
  BufferAbortException() { super(); }

  BufferAbortException(final String msg) { super(msg); }
}
