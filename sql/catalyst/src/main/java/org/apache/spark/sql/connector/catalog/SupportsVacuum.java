package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Experimental;
/**
 * A mix-in interface for {@link Table} vacuum support. Data sources can implement this
 * interface to provide the ability to perform table maintenance on request of the user.
 */
@Experimental
public interface SupportsVacuum {

  /**
   * Performs maintenance on the table.  This often includes removing unneeded data and
   * deleting stale records.
   *
   * @throws IllegalArgumentException If the vacuum is rejected due to required effort.
   */
  void vacuum();
}
