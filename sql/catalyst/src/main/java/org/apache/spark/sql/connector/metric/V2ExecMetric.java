package org.apache.spark.sql.connector.metric;

public interface V2ExecMetric {
  /**
   * Returns the type of V2 exec metric (ie, firstScan, secondScan, merge, etc).
   */
  String metricType();
  /**
   * Returns the name of V2 exec metric.
   */
  String name();

  /**
   * Returns the value of V2 exec metric.
   */
  String value();
}
