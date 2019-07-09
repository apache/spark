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

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * A Property Graph as defined by the openCypher Property Graph Data Model.
 *
 * A graph is always tied to and managed by a [[CypherSession]].
 * The lifetime of a graph is bound by the session lifetime.
 *
 * @see <a href="http://www.opencypher.org/">openCypher project</a>
 * @since 3.0.0
 */
abstract class PropertyGraph {

  /**
   * The schema (graph type) describes the structure of this graph.
   *
   * @since 3.0.0
   */
  def schema: PropertyGraphType

  /**
   * The session in which this graph is managed.
   *
   * @since 3.0.0
   */
  def cypherSession: CypherSession

  /**
   * Executes a Cypher query in the [[CypherSession]] that manages this graph, using this graph as
   * the input graph.
   *
   * @param query Cypher query to execute
   * @since 3.0.0
   */
  def cypher(query: String): CypherResult = cypher(query, Map.empty[String, Any])

  /**
   * Executes a Cypher query in the [[CypherSession]] that manages this graph, using this graph as
   * the input graph.
   *
   * @param query      Cypher query to execute
   * @param parameters parameters used by the Cypher query
   * @since 3.0.0
   */
  def cypher(query: String, parameters: Map[String, Any]): CypherResult =
    cypherSession.cypher(this, query, parameters)

  /**
   * Executes a Cypher query in the [[CypherSession]] that manages this graph, using this graph as
   * the input graph.
   *
   * @param query      Cypher query to execute
   * @param parameters parameters used by the Cypher query
   * @since 3.0.0
   */
  def cypher(query: String, parameters: java.util.Map[String, Object]): CypherResult =
    cypherSession.cypher(this, query, parameters)

  /**
   * Returns the [[NodeFrame]] for a given node label set.
   *
   * @param labelSet Label set used for NodeFrame lookup
   * @return NodeFrame for the given label set
   * @since 3.0.0
   */
  def nodeFrame(labelSet: Set[String]): NodeFrame

  /**
   * Returns the [[RelationshipFrame]] for a given relationship type.
   *
   * @param relationshipType Relationship type used for RelationshipFrame lookup
   * @return RelationshipFrame for the given relationship type
   * @since 3.0.0
   */
  def relationshipFrame(relationshipType: String): RelationshipFrame

  /**
   * Returns a DataFrame that contains a row for each node in this graph.
   *
   * The DataFrame adheres to the following column naming conventions:
   *
   * {{{
   *     Id column:        `$ID`
   *     Label columns:    `:{LABEL_NAME}`
   *     Property columns: `{Property_Key}`
   * }}}
   *
   * @see `org.apache.spark.graph.api.CypherSession.createGraph(nodes, relationships)`
   * @since 3.0.0
   */
  def nodes: DataFrame

  /**
   * Returns a DataFrame that contains a row for each relationship in this
   * graph.
   *
   * The DataFrame adheres to column naming conventions:
   *
   * {{{
   *     Id column:        `$ID`
   *     SourceId column:  `$SOURCE_ID`
   *     TargetId column:  `$TARGET_ID`
   *     RelType columns:  `:{REL_TYPE}`
   *     Property columns: `{Property_Key}`
   * }}}
   *
   * @see `org.apache.spark.graph.api.CypherSession.createGraph(nodes, relationships)`
   * @since 3.0.0
   */
  def relationships: DataFrame

  /**
   * Saves this graph to the given location.
   *
   * @param path      directory in which the graph should be stored
   * @param saveMode  specifies what happens when the destination already exists
   * @since 3.0.0
   */
  def save(path: String, saveMode: SaveMode = SaveMode.ErrorIfExists): Unit =
    cypherSession.save(this, path, saveMode)
}
