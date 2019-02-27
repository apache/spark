package org.apache.spark.graph.api

import org.apache.spark.sql.DataFrame

/**
  * Describes how to map an input [[DataFrame]] to graph elements (i.e. nodes or relationships).
  */
trait GraphElementFrame {

  /**
    * [[DataFrame]] containing element data. Each row represents a graph element.
    */
  def df: DataFrame

  /**
    * Name of the column that contains the graph element identifier.
    *
    * @note Column values need to be of [[org.apache.spark.sql.types.BinaryType]].
    */
  def idColumn: String

  /**
    * Mapping from graph element property keys to the columns that contain the corresponding property values.
    */
  def properties: Map[String, String]
}

object NodeFrame {

  /**
    * Describes how to map an input [[DataFrame]] to nodes.
    *
    * All columns apart from the given `idColumn` are mapped to node properties.
    *
    * @param df       [[DataFrame]] containing a single node in each row
    * @param idColumn column that contains the node identifier
    * @param labels   labels that are assigned to all nodes
    */
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

/**
  * Describes how to map an input [[DataFrame]] to nodes.
  *
  * @param df         [[DataFrame]] containing a single node in each row
  * @param idColumn   column that contains the node identifier
  * @param labels     labels that are assigned to all nodes
  * @param properties mapping from property keys to corresponding columns
  */
case class NodeFrame(
  df: DataFrame,
  idColumn: String,
  labels: Set[String],
  properties: Map[String, String]
) extends GraphElementFrame

object RelationshipFrame {

  /**
    * Describes how to map an input [[DataFrame]] to relationships.
    *
    * All columns apart from the given identifier columns are mapped to relationship properties.
    *
    * @param df               [[DataFrame]] containing a single relationship in each row
    * @param idColumn         column that contains the relationship identifier
    * @param sourceIdColumn   column that contains the source node identifier of the relationship
    * @param targetIdColumn   column that contains the target node identifier of the relationship
    * @param relationshipType relationship type that is assigned to all relationships
    */
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

/**
  * Describes how to map an input [[DataFrame]] to relationships.
  *
  * @param df               [[DataFrame]] containing a single relationship in each row
  * @param idColumn         column that contains the relationship identifier
  * @param sourceIdColumn   column that contains the source node identifier of the relationship
  * @param targetIdColumn   column that contains the target node identifier of the relationship
  * @param relationshipType relationship type that is assigned to all relationships
  * @param properties       mapping from property keys to corresponding columns
  */
case class RelationshipFrame(
  df: DataFrame,
  idColumn: String,
  sourceIdColumn: String,
  targetIdColumn: String,
  relationshipType: String,
  properties: Map[String, String]
) extends GraphElementFrame
