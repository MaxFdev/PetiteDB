package edu.yu.dbimpl.parse;

/** An unchecked exception that indicates that the parser or lexer ran into
 * trouble
 *
 * @author Avraham Leff
 */

public class BadSyntaxException extends RuntimeException {
  BadSyntaxException() { super(); }

  BadSyntaxException(final String msg) { super(msg); }

  BadSyntaxException(final String msg, final Throwable t) { super(msg, t); }  
}
