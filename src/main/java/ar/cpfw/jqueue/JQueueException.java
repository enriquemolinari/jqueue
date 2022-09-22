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
   * @author Enrique Molinari
   * @since 0.1
   */
  public JQueueException(String msg) {
    super(msg);
  }

  /**
   * JQueueException.
   * 
   * @author Enrique Molinari
   * @since 0.1
   */
  public JQueueException(Exception e, String msg) {
    super(msg, e);
  }
}
