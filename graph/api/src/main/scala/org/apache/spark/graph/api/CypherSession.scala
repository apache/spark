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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{BooleanType, StructType}

/**
 * Contains constants used for convention based column naming.
 */
object CypherSession {

  /**
   * Naming convention for identifier columns, both node and relationship identifiers.
   */
  val ID_COLUMN = "$ID"

  /**
   * Naming convention for relationship source identifier.
   */
  val SOURCE_ID_COLUMN = "$SOURCE_ID"

  /**
   * Naming convention for relationship target identifier.
   */
  val TARGET_ID_COLUMN = "$TARGET_ID"

  /**
   * Naming convention both for node label and relationship type prefixes.
   */
  val LABEL_COLUMN_PREFIX = ":"
}

/**
 * A CypherSession allows for creating, storing and loading [[PropertyGraph]] instances as well as
 * executing Cypher queries on them.
 *
 * Wraps a [[org.apache.spark.sql.SparkSession]].
 *
 * @since 3.0.0
 */
trait CypherSession extends Logging {

  def sparkSession: SparkSession

  /**
   * Executes a Cypher query on the given input graph.
   *
   * @param graph [[PropertyGraph]] on which the query is executed
   * @param query Cypher query to execute
   * @since 3.0.0
   */
  def cypher(graph: PropertyGraph, query: String): CypherResult

  /**
   * Executes a Cypher query on the given input graph.
   *
   * Note that queries can take optional parameters:
   *
   * {{{
   *     Parameters:
   *
   *     {
   *        "name" : "Alice"
   *     }
   *
   *     Query:
   *
   *     MATCH (n:Person)
   *     WHERE n.name = $name
   *     RETURN n
   * }}}
   *
   * @param graph      [[PropertyGraph]] on which the query is executed
   * @param query      Cypher query to execute
   * @param parameters parameters used by the Cypher query
   * @since 3.0.0
   */
  def cypher(graph: PropertyGraph, query: String, parameters: Map[String, Any]): CypherResult

  /**
   * Executes a Cypher query on the given input graph.
   *
   * Note that queries can take optional parameters:
   *
   * {{{
   *     Parameters:
   *
   *     {
   *        "name" : "Alice"
   *     }
   *
   *     Query:
   *
   *     MATCH (n:Person)
   *     WHERE n.name = $name
   *     RETURN n
   * }}}
   *
   * @param graph      [[PropertyGraph]] on which the query is executed
   * @param query      Cypher query to execute
   * @param parameters parameters used by the Cypher query
   * @since 3.0.0
   */
  def cypher(
      graph: PropertyGraph,
      query: String,
      parameters: java.util.Map[String, Object]): CypherResult = {
    cypher(graph, query, parameters.asScala.toMap)
  }

  /**
   * Creates a [[PropertyGraph]] from a sequence of [[NodeFrame]]s and [[RelationshipFrame]]s.
   * At least one [[NodeFrame]] has to be provided.
   *
   * For each label set and relationship type there can be at most one [[NodeFrame]] and at most one
   * [[RelationshipFrame]], respectively.
   *
   * @param nodes         NodeFrames that define the nodes in the graph
   * @param relationships RelationshipFrames that define the relationships in the graph
   * @since 3.0.0
   */
  def createGraph(nodes: Array[NodeFrame], relationships: Array[RelationshipFrame]): PropertyGraph

  /**
   * Creates a [[PropertyGraph]] from nodes and relationships.
   *
   * The given DataFrames need to adhere to the following column naming conventions:
   *
   * {{{
   *     Id column:        `$ID`            (nodes and relationships)
   *     SourceId column:  `$SOURCE_ID`     (relationships)
   *     TargetId column:  `$TARGET_ID`     (relationships)
   *
   *     Label columns:    `:{LABEL_NAME}`  (nodes)
   *     RelType columns:  `:{REL_TYPE}`    (relationships)
   *
   *     Property columns: `{Property_Key}` (nodes and relationships)
   * }}}
   *
   * @note It is recommended to cache the input DataFrames if they represent multiple label sets and
   *       relationship types.
   *
   * @see [[CypherSession]]
   * @param nodes         node DataFrame
   * @param relationships relationship DataFrame
   * @since 3.0.0
   */
  def createGraph(nodes: DataFrame, relationships: DataFrame): PropertyGraph = {
    def validateLabelColumns(schema: StructType, columns: Set[String]): Unit = {
      schema.fields
        .filter(f => columns.contains(f.name))
        .foreach(field => {
          if (field.dataType != BooleanType) {
            throw new IllegalArgumentException(
              s"Column ${field.name} must be of type BooleanType.")
          }
        })
    }

    val idColumn = CypherSession.ID_COLUMN
    val sourceIdColumn = CypherSession.SOURCE_ID_COLUMN
    val targetIdColumn = CypherSession.TARGET_ID_COLUMN

    val labelColumns = nodes.columns.filter(_.startsWith(CypherSession.LABEL_COLUMN_PREFIX)).toSet
    validateLabelColumns(nodes.schema, labelColumns)

    val nodeProperties = (nodes.columns.toSet - idColumn -- labelColumns)
      .map(col => col -> col)
      .toMap

    val labelCount = labelColumns.size
    if (labelCount > 5) {
      log.warn(
        s"$labelCount label columns will result in ${Math.pow(labelCount, 2)} node frames.")
      if (labelCount > 10) {
        throw new IllegalArgumentException(
          s"Expected number of label columns to be less than or equal to 10, was $labelCount.")
      }
    }

    val labelSets = labelColumns.subsets().toSet

    val nodeFrames = labelSets.map { labelSet =>
      val predicate = labelColumns
        .map { labelColumn =>
          if (labelSet.contains(labelColumn)) {
            nodes.col(labelColumn)
          } else {
            !nodes.col(labelColumn)
          }
        }
        .reduce(_ && _)

      NodeFrame(nodes.filter(predicate), idColumn, labelSet.map(_.substring(1)), nodeProperties)
    }

    val relColumns = relationships.columns.toSet
    val relTypeColumns = relColumns.filter(_.startsWith(CypherSession.LABEL_COLUMN_PREFIX))
    validateLabelColumns(relationships.schema, relTypeColumns)
    val propertyColumns = relColumns - idColumn - sourceIdColumn - targetIdColumn -- relTypeColumns
    val relProperties = propertyColumns.map(col => col -> col).toMap
    val relFrames = relTypeColumns.map { relTypeColumn =>
      val predicate = relationships.col(relTypeColumn)

      RelationshipFrame(
        relationships.filter(predicate),
        idColumn,
        sourceIdColumn,
        targetIdColumn,
        relTypeColumn.substring(1),
        relProperties)
    }

    createGraph(nodeFrames.toArray, relFrames.toArray)
  }

  /**
   * Returns a [[PropertyGraphReader]] that can be used to read data in as a `PropertyGraph`.
   *
   * @since 3.0.0
   */
  def read: PropertyGraphReader

  /**
   * Returns a [[NodeFrameBuilder]] that can be used to construct a [[NodeFrame]].
   *
   * @param df DataFrame containing a single node in each row
   * @since 3.0.0
   */
  def buildNodeFrame(df: DataFrame): NodeFrameBuilder =
    new NodeFrameBuilder(df)

  /**
   * Returns a [[RelationshipFrameBuilder]] that can be used to construct a [[RelationshipFrame]].
   *
   * @param df DataFrame containing a single relationship in each row
   * @since 3.0.0
   */
  def buildRelationshipFrame(df: DataFrame): RelationshipFrameBuilder =
    new RelationshipFrameBuilder(df)

}
