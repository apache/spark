package org.apache.spark.graph.api

import org.apache.spark.sql.DataFrame

trait GraphElementDataFrame {

  def df: DataFrame

  def idColumn: String

  def properties: Map[String, String]
}

case class NodeDataFrame(
  df: DataFrame,
  idColumn: String,
  labels: Set[String] = Set.empty,
  properties: Map[String, String] = Map.empty,
  optionalLabels: Map[String, String] = Map.empty
) extends GraphElementDataFrame

case class RelationshipDataFrame(
  df: DataFrame,
  idColumn: String,
  sourceIdColumn: String,
  targetIdColumn: String,
  relationshipType: String,
  properties: Map[String, String] = Map.empty
) extends GraphElementDataFrame
