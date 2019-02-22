package org.apache.spark.graph.api

import org.apache.spark.sql.DataFrame

trait CypherResult {
  def df: DataFrame

  def nodeDataFrame(varName: String): Seq[NodeDataFrame]

  def relationshipDataFrame(varName: String): Seq[RelationshipDataFrame]
}
