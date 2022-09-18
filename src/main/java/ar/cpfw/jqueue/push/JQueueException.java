package ar.cpfw.jqueue.push;

public class JQueueException extends RuntimeException {

  public JQueueException(String msg) {
    super(msg);
  }

  public JQueueException(Exception e, String msg) {
    super(msg, e);
  }
}
