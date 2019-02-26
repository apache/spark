package org.apache.spark.graph.api

import org.apache.spark.sql.DataFrame

trait GraphElementFrame {

  def df: DataFrame

  def idColumn: String

  def properties: Map[String, String]
}

object NodeFrame {

  def apply(
    df: DataFrame,
    idColumn: String,
    labels: Set[String] = Set.empty
  ): NodeFrame = {
    val properties = (df.columns.toSet - idColumn)
      .map(columnName => columnName -> columnName).toMap
    NodeFrame(df, idColumn, labels, properties)
  }

}

case class NodeFrame(
  df: DataFrame,
  idColumn: String,
  labels: Set[String],
  properties: Map[String, String]
) extends GraphElementFrame

object RelationshipFrame {

  def apply(
    df: DataFrame,
    idColumn: String,
    sourceIdColumn: String,
    targetIdColumn: String,
    relationshipType: String
  ): RelationshipFrame = {
    val properties = (df.columns.toSet - idColumn - sourceIdColumn - targetIdColumn)
      .map(columnName => columnName -> columnName).toMap
    RelationshipFrame(df, idColumn, sourceIdColumn, targetIdColumn, relationshipType, properties)
  }

}

case class RelationshipFrame(
  df: DataFrame,
  idColumn: String,
  sourceIdColumn: String,
  targetIdColumn: String,
  relationshipType: String,
  properties: Map[String, String]
) extends GraphElementFrame
