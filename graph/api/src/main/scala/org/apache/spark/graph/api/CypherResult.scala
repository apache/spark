package org.apache.spark.graph.api

import org.apache.spark.sql.DataFrame

trait CypherResult {
  def df: DataFrame

  def nodeFrames(varName: String): Seq[NodeFrame]

  def relationshipFrames(varName: String): Seq[RelationshipFrame]
}
