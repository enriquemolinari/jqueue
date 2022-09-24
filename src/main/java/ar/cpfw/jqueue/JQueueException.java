package ar.cpfw.jqueue;

/**
 * JQueueException.
 * 
 * @author Enrique Molinari
 * @since 0.1
 */
public class JQueueException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /**
   * JQueueException.
   * 
   * @since 0.1
   */
  public JQueueException(final String msg) {
    super(msg);
  }

  /**
   * JQueueException.
   * 
   * @author Enrique Molinari
   * @since 0.1
   */
  public JQueueException(final Exception exception, final String msg) {
    super(msg, exception);
  }
}
