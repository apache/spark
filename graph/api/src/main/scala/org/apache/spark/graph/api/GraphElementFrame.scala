/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graph.api

import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame

/**
 * A [[PropertyGraph]] is created from GraphElementFrames.
 *
 * A graph element is either a node or a relationship.
 * A GraphElementFrame wraps a DataFrame and describes how it maps to graph elements.
 *
 * @since 3.0.0
 */
sealed abstract class GraphElementFrame {

  /**
   * Initial DataFrame that can still contain unmapped, arbitrarily ordered columns.
   *
   * @since 3.0.0
   */
  def df: DataFrame

  /**
   * Name of the column that contains the graph element identifier.
   *
   * @since 3.0.0
   */
  def idColumn: String

  /**
   * Name of all columns that contain graph element identifiers.
   *
   * @since 3.0.0
   */
  def idColumns: Seq[String] = Seq(idColumn)

  /**
   * Mapping from graph element property keys to the columns that contain the corresponding property
   * values.
   *
   * @since 3.0.0
   */
  def properties: Map[String, String]

}

/**
 * Interface used to build a [[NodeFrame]].
 *
 * @param df DataFrame containing a single node in each row
 * @since 3.0.0
 */
final class NodeFrameBuilder(var df: DataFrame) {

  private var idColumn: String = CypherSession.ID_COLUMN
  private var labelSet: Set[String] = Set.empty
  private var properties: Map[String, String] = Map.empty

  /**
   * @param idColumn column that contains the node identifier
   * @since 3.0.0
   */
  def idColumn(idColumn: String): NodeFrameBuilder = {
    if (idColumn.isEmpty) {
      throw new IllegalArgumentException("idColumn must not be empty")
    }
    this.idColumn = idColumn;
    this
  }

  /**
   * @param labelSet labels that are assigned to all nodes
   * @since 3.0.0
   */
  def labelSet(labelSet: Array[String]): NodeFrameBuilder = {
    this.labelSet = labelSet.toSet
    this
  }

  /**
   * @param properties mapping from property keys to corresponding columns
   * @since 3.0.0
   */
  def properties(properties: Map[String, String]): NodeFrameBuilder = {
    this.properties = properties
    this
  }

  /**
   * @param properties mapping from property keys to corresponding columns
   * @since 3.0.0
   */
  def properties(properties: java.util.Map[String, String]): NodeFrameBuilder = {
    this.properties = properties.asScala.toMap
    this
  }

  /**
   * Creates a `NodeFrame` from the specified builder parameters.
   *
   * @since 3.0.0
   */
  def build(): NodeFrame = {
    NodeFrame(df, idColumn, labelSet, properties)
  }

}

/**
 * Describes how to map a DataFrame to nodes.
 *
 * Each row in the DataFrame represents a node which has exactly the labels defined by the given
 * label set.
 *
 * @param df         DataFrame containing a single node in each row
 * @param idColumn   column that contains the node identifier
 * @param labelSet   labels that are assigned to all nodes
 * @param properties mapping from property keys to corresponding columns
 * @since 3.0.0
 */
case class NodeFrame private[graph] (
    df: DataFrame,
    idColumn: String,
    labelSet: Set[String],
    properties: Map[String, String])
    extends GraphElementFrame

/**
 * Interface used to build a [[RelationshipFrame]].
 *
 * @param df DataFrame containing a single relationship in each row
 * @since 3.0.0
 */
final class RelationshipFrameBuilder(val df: DataFrame) {

  private var idColumn: String = CypherSession.ID_COLUMN
  private var sourceIdColumn: String = CypherSession.SOURCE_ID_COLUMN
  private var targetIdColumn: String = CypherSession.TARGET_ID_COLUMN
  private var maybeRelationshipType: Option[String] = Option.empty
  private var properties: Map[String, String] = Map.empty

  /**
   * @param idColumn column that contains the relationship identifier
   * @since 3.0.0
   */
  def idColumn(idColumn: String): RelationshipFrameBuilder = {
    if (idColumn.isEmpty) {
      throw new IllegalArgumentException("idColumn must not be empty")
    }
    this.idColumn = idColumn;
    this
  }

  /**
   * @param sourceIdColumn column that contains the source node identifier of the relationship
   * @since 3.0.0
   */
  def sourceIdColumn(sourceIdColumn: String): RelationshipFrameBuilder = {
    if (sourceIdColumn.isEmpty) {
      throw new IllegalArgumentException("sourceIdColumn must not be empty")
    }
    this.sourceIdColumn = sourceIdColumn;
    this
  }

  /**
   * @param targetIdColumn column that contains the target node identifier of the relationship
   * @since 3.0.0
   */
  def targetIdColumn(targetIdColumn: String): RelationshipFrameBuilder = {
    if (targetIdColumn.isEmpty) {
      throw new IllegalArgumentException("targetIdColumn must not be empty")
    }
    this.targetIdColumn = targetIdColumn;
    this
  }

  /**
   * @param relationshipType relationship type that is assigned to all relationships
   * @since 3.0.0
   */
  def relationshipType(relationshipType: String): RelationshipFrameBuilder = {
    if (relationshipType.isEmpty) {
      throw new IllegalArgumentException("Relationship type must not be empty")
    }
    this.maybeRelationshipType = Some(relationshipType)
    this
  }

  /**
   * @param properties mapping from property keys to corresponding columns
   * @since 3.0.0
   */
  def properties(properties: Map[String, String]): RelationshipFrameBuilder = {
    this.properties = properties
    this
  }

  /**
   * @param properties mapping from property keys to corresponding columns
   * @since 3.0.0
   */
  def properties(properties: java.util.Map[String, String]): RelationshipFrameBuilder = {
    this.properties = properties.asScala.toMap
    this
  }

  /**
   * Creates a [[RelationshipFrame]] from the specified builder parameters.
   *
   * @since 3.0.0
   */
  def build(): RelationshipFrame = {
    maybeRelationshipType match {
      case Some(relationshipType) =>
        RelationshipFrame(df, idColumn, sourceIdColumn, targetIdColumn, relationshipType, properties)
      case None => throw new IllegalArgumentException("Relationship type must be set.")
    }
  }
}

/**
 * Describes how to map a DataFrame to relationships.
 *
 * Each row in the DataFrame represents a relationship with the given relationship type.
 *
 * @param df               DataFrame containing a single relationship in each row
 * @param idColumn         column that contains the relationship identifier
 * @param sourceIdColumn   column that contains the source node identifier of the relationship
 * @param targetIdColumn   column that contains the target node identifier of the relationship
 * @param relationshipType relationship type that is assigned to all relationships
 * @param properties       mapping from property keys to corresponding columns
 * @since 3.0.0
 */
case class RelationshipFrame private[graph] (
    df: DataFrame,
    idColumn: String,
    sourceIdColumn: String,
    targetIdColumn: String,
    relationshipType: String,
    properties: Map[String, String])
    extends GraphElementFrame {

  override def idColumns: Seq[String] = Seq(idColumn, sourceIdColumn, targetIdColumn)

}
