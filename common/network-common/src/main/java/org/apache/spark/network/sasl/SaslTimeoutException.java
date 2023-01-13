package org.apache.spark.network.sasl;

public class SaslTimeoutException extends RuntimeException {
  public SaslTimeoutException(Throwable cause) {
    super(cause);
  }

  public SaslTimeoutException(String message) {
    super(message);
  }

  public SaslTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
