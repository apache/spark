package org.apache.spark.graph.api.v1

import org.apache.spark.sql.DataFrame

trait GraphElementDataFrame {

  def df: DataFrame

  def idColumn: String

  def properties: Map[String, String]
}

case class NodeDataFrame(
  df: DataFrame,
  idColumn: String,
  properties: Map[String, String],
  labels: Set[String],
  optionalLabels: Map[String, String]
) extends GraphElementDataFrame

case class RelationshipDataFrame(
  df: DataFrame,
  idColumn: String,
  properties: Map[String, String],
  relationshipType: String,
  sourceIdColumn: String,
  targetIdColumn: String
) extends GraphElementDataFrame
