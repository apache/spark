package org.apache.spark.graph.api.v1

import org.apache.spark.sql.DataFrame

sealed trait CypherResult {
  def df: DataFrame
}
