package org.apache.spark.graph.api

import org.apache.spark.sql.DataFrame

trait GraphElementFrame {

  def df: DataFrame

  def idColumn: String

  def properties: Map[String, String]
}

case class NodeFrame(
  df: DataFrame,
  idColumn: String,
  labels: Set[String] = Set.empty,
  properties: Map[String, String] = Map.empty
) extends GraphElementFrame

case class RelationshipFrame(
  df: DataFrame,
  idColumn: String,
  sourceIdColumn: String,
  targetIdColumn: String,
  relationshipType: String,
  properties: Map[String, String] = Map.empty
) extends GraphElementFrame
