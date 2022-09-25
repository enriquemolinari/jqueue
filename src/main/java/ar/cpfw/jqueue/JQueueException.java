package ar.cpfw.jqueue;

public class JQueueException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public JQueueException(final String msg) {
    super(msg);
  }

  public JQueueException(final Exception exception, final String msg) {
    super(msg, exception);
  }
}
