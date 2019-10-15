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

import org.slf4j.LoggerFactory

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, StructType}

/**
 * Contains constants used for convention based column naming.
 */
@Evolving
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
   * Naming convention for node label prefixes.
   */
  val LABEL_COLUMN_PREFIX = ":"

  /**
   * Naming convention for relationship type prefixes.
   */
  val REL_TYPE_COLUMN_PREFIX = ":"

  /**
   * Extracts [[NodeDataset]]s from a [[Dataset]] using column name conventions.
   *
   * For information about naming conventions, see [[CypherSession.createGraph]].
   *
   * @param nodes node dataset
   * @since 3.0.0
   */
  def extractNodeDatasets(nodes: Dataset[Row]): Array[NodeDataset] = {
    val labelColumns = nodes.columns.filter(_.startsWith(LABEL_COLUMN_PREFIX)).toSet
    validateLabelOrRelTypeColumns(nodes.schema, labelColumns, LABEL_COLUMN_PREFIX)

    val nodeProperties = (nodes.columns.toSet - ID_COLUMN -- labelColumns)
      .map(col => col -> col)
      .toMap

    val labelCount = labelColumns.size
    if (labelCount > 5) {
      LoggerFactory.getLogger(CypherSession.getClass).warn(
        s"$labelCount label columns will result in ${Math.pow(labelCount, 2)} node datasets.")
      if (labelCount > 10) {
        throw new IllegalArgumentException(
          s"Expected number of label columns to be less than or equal to 10, was $labelCount.")
      }
    }

    val labelSets = labelColumns.subsets().toSet

    labelSets.map { labelSet =>
      val predicate = labelColumns
        .map { labelColumn =>
          if (labelSet.contains(labelColumn)) {
            nodes.col(labelColumn)
          } else {
            !nodes.col(labelColumn)
          }
        }
        .reduce(_ && _)

      NodeDataset(nodes.filter(predicate), ID_COLUMN, labelSet.map(_.substring(1)), nodeProperties)
    }.toArray
  }

  /**
   * Extracts [[RelationshipDataset]]s from a [[Dataset]] using column name conventions.
   *
   * For information about naming conventions, see [[CypherSession.createGraph]].
   *
   * @param relationships relationship dataset
   * @since 3.0.0
   */
  def extractRelationshipDatasets(relationships: Dataset[Row]): Array[RelationshipDataset] = {
    val relColumns = relationships.columns.toSet
    val relTypeColumns = relColumns.filter(_.startsWith(REL_TYPE_COLUMN_PREFIX))
    validateLabelOrRelTypeColumns(relationships.schema, relTypeColumns, REL_TYPE_COLUMN_PREFIX)
    val idColumns = Set(ID_COLUMN, SOURCE_ID_COLUMN, TARGET_ID_COLUMN)
    val propertyColumns = relColumns -- idColumns -- relTypeColumns
    val relProperties = propertyColumns.map(col => col -> col).toMap
    relTypeColumns.map { relTypeColumn =>
      val predicate = relationships.col(relTypeColumn)
      // TODO: Make sure that each row represents a single relationship type
      // see https://issues.apache.org/jira/browse/SPARK-29480
      RelationshipDataset(
        relationships.filter(predicate),
        ID_COLUMN,
        SOURCE_ID_COLUMN,
        TARGET_ID_COLUMN,
        relTypeColumn.substring(1),
        relProperties)
    }.toArray
  }

  /**
   * Validates if the given columns fulfil specific constraints for
   * representing node labels or relationship types.
   *
   * In particular, we check if the columns store boolean values and that
   * the column name represents a single node label or relationship type.
   *
   * @param schema  Dataset schema
   * @param columns columns to validate
   * @param prefix  node label or relationship type prefix
   */
  private def validateLabelOrRelTypeColumns(
      schema: StructType,
      columns: Set[String],
      prefix: String): Unit = {
    schema.fields.filter(f => columns.contains(f.name)).foreach(field => {
      if (field.dataType != BooleanType) {
        throw new IllegalArgumentException(s"Column ${field.name} must be of type BooleanType.")
      }
    })
    columns.foreach(typeColumn => {
      if (typeColumn.sliding(prefix.length).count(_ == prefix) != 1) {
        throw new IllegalArgumentException(s"Type column $typeColumn must contain exactly one type.")
      }
    })
  }

}

/**
 * A CypherSession allows for creating, storing and loading [[PropertyGraph]] instances as well as
 * executing Cypher queries on them.
 *
 * Wraps a [[org.apache.spark.sql.SparkSession]].
 *
 * @since 3.0.0
 */
@Evolving
trait CypherSession {

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
   * Creates a [[PropertyGraph]] from a sequence of [[NodeDataset]]s and [[RelationshipDataset]]s.
   * At least one [[NodeDataset]] has to be provided.
   *
   * For each label set and relationship type there can be at most one [[NodeDataset]] and at most
   * one [[RelationshipDataset]], respectively.
   *
   * @param nodes         NodeDataset that define the nodes in the graph
   * @param relationships RelationshipDataset that define the relationships in the graph
   * @since 3.0.0
   */
  def createGraph(
      nodes: Array[NodeDataset],
      relationships: Array[RelationshipDataset]): PropertyGraph

  /**
   * Creates a [[PropertyGraph]] from nodes and relationships.
   *
   * The given dataset needs to adhere to the following column naming conventions:
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
   * @note It is recommended to cache the input datasets if they represent multiple label sets and
   *       relationship types.
   *
   * @see [[CypherSession]]
   * @param nodes         node dataset
   * @param relationships relationship dataset
   * @since 3.0.0
   */
  def createGraph(nodes: Dataset[Row], relationships: Dataset[Row]): PropertyGraph = {
    val nodeFrames = CypherSession.extractNodeDatasets(nodes)
    val relationshipFrames = CypherSession.extractRelationshipDatasets(relationships)
    createGraph(nodeFrames, relationshipFrames)
  }

  /**
   * Returns a [[PropertyGraphReader]] that can be used to read data in as a `PropertyGraph`.
   *
   * @since 3.0.0
   */
  def read: PropertyGraphReader

  /**
   * Returns a [[NodeDatasetBuilder]] that can be used to construct a [[NodeDataset]].
   *
   * @param ds Dataset containing a single node in each row
   * @since 3.0.0
   */
  def buildNodeDataset(ds: Dataset[Row]): NodeDatasetBuilder =
    new NodeDatasetBuilder(ds)

  /**
   * Returns a [[RelationshipDatasetBuilder]] that can be used to construct
   * a [[RelationshipDataset]].
   *
   * @param ds Dataset containing a single relationship in each row
   * @since 3.0.0
   */
  def buildRelationshipDataset(ds: Dataset[Row]): RelationshipDatasetBuilder =
    new RelationshipDatasetBuilder(ds)

}
