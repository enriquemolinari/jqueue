package ar.cpfw.jquery;

/**
 * JQueueException.
 *
 * @since 0.1
 */
public class JQueueException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public JQueueException(String msg) {
    super(msg);
  }

  public JQueueException(Exception e, String msg) {
    super(msg, e);
  }
}
