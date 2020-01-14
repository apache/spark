package org.apache.spark.sql.connector;

import org.apache.spark.annotation.Unstable;

import java.util.Map;

/**
 * @since 3.0.0
 */
@Unstable
public interface ExternalCommandRunnableProvider {
  /**
   * Execute a random DDL/DML command inside an external execution engine rather than Spark,
   * especially for JDBC data source. This could be useful when user has some custom commands
   * which Spark doesn't support, need to be executed. Please note that this is not appropriate
   * for query which returns lots of data.
   *
   * @param command the command provide by user
   * @param parameters data source-specific parameters
   * @return output information from the command
   */
  String[] executeCommand(String command, Map<String, String> parameters);
}