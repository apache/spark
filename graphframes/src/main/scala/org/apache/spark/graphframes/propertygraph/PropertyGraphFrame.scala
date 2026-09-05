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

package org.apache.spark.graphframes.propertygraph

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.propertygraph.property.EdgePropertyGroup
import org.apache.spark.graphframes.propertygraph.property.VertexPropertyGroup
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit

/**
 * A high-level abstraction for working with property graphs that simplifies interaction with the
 * GraphFrames library.
 *
 * PropertyGraphFrame serves as a logical structure that manages collections of vertex and edge
 * property groups, providing a user-friendly API for graph operations. It handles various
 * internal complexities such as:
 *   - ID conversion and collision prevention
 *   - Management of directed/undirected graph representations
 *   - Handling of weighted/unweighted edges
 *   - Data consistency across different property groups
 *
 * The class maintains separate collections for vertex and edge properties, allowing for flexible
 * graph construction while ensuring data integrity. Each property (vertex or edge) handles its
 * data internally, while this class provides a simplified interface for working with the
 * underlying GraphFrame structure.
 *
 * @param vertexPropertyGroups
 *   Sequence of vertex property groups that define the graph's vertices
 * @param edgesPropertyGroups
 *   Sequence of edge property groups that define the graph's edges
 */
case class PropertyGraphFrame(
    vertexPropertyGroups: Seq[VertexPropertyGroup],
    edgesPropertyGroups: Seq[EdgePropertyGroup]) {
  import PropertyGraphFrame._
  lazy private val vertexGroups: Map[String, VertexPropertyGroup] =
    vertexPropertyGroups.map(pg => pg.name -> pg).toMap
  lazy private val edgeGroups: Map[String, EdgePropertyGroup] =
    edgesPropertyGroups.map(pg => pg.name -> pg).toMap

  /**
   * Converts a heterogeneous property graph into a unified GraphFrame representation.
   *
   * This method transforms a property graph that may contain multiple vertex types and both
   * directed and undirected edges into a single GraphFrame object where all vertices and edges
   * share the same schema. The conversion process handles:
   *
   *   - Internal ID generation and collision prevention by hashing vertex/edge IDs with their
   *     group names
   *   - Merging of different vertex types into a unified vertex DataFrame
   *   - Conversion of directed/undirected edge relationships into a consistent edge DataFrame
   *   - Filtering of vertices and edges based on provided predicates
   *
   * The method allows selecting a subset of property groups and applying filters to control which
   * data is included in the final GraphFrame.
   *
   * @param vertexPropertyGroups
   *   Sequence of vertex property group names to include in the GraphFrame
   * @param edgePropertyGroups
   *   Sequence of edge property group names to include in the GraphFrame
   * @param edgeGroupFilters
   *   Map of edge property group names to filter predicates (Column expressions)
   * @param vertexGroupFilters
   *   Map of vertex property group names to filter predicates (Column expressions)
   * @return
   *   A GraphFrame containing the unified representation of the selected and filtered property
   *   groups
   */
  def toGraphFrame(
      vertexPropertyGroups: Seq[String],
      edgePropertyGroups: Seq[String],
      edgeGroupFilters: Map[String, Column],
      vertexGroupFilters: Map[String, Column]): GraphFrame = {
    vertexPropertyGroups.foreach(name =>
      require(vertexGroups.contains(name), s"Vertex property group $name does not exist"))
    edgePropertyGroups.foreach(name =>
      require(edgeGroups.contains(name), s"Edge property group $name does not exist"))

    val vertices = vertexPropertyGroups
      .map(name => vertexGroups(name).getData(vertexGroupFilters(name)))
      .reduce(_ union _)

    val edges = edgePropertyGroups
      .map(name => edgeGroups(name).getData(edgeGroupFilters(name)))
      .reduce(_ union _)

    GraphFrame(vertices, edges)
  }

  /**
   * Projects a bipartite graph onto one of its parts, creating edges between vertices that share
   * neighbors in the other part. Drops the property group used for projection through and returns
   * a new property graph.
   *
   * @param leftBiGraphPart
   *   Name of the vertex property group to project onto
   * @param rightBiGraphPart
   *   Name of the vertex property group to project through
   * @param edgeGroup
   *   Name of the edge property group connecting the two parts
   * @param newEdgeWeight
   *   Optional function that takes two weight columns (Column objects) of edges as input and
   *   returns a new weight column. If None, a default weight of 1.0 is used for all projected
   *   edges.
   * @return
   *   A new PropertyGraphFrame containing the projected graph
   */
  def projectionBy(
      leftBiGraphPart: String,
      rightBiGraphPart: String,
      edgeGroup: String,
      newEdgeWeight: Option[(Column, Column) => Column] = None): PropertyGraphFrame = {
    require(
      edgeGroups(edgeGroup).srcPropertyGroup.name == leftBiGraphPart,
      s"Edge Property Group should have $leftBiGraphPart source group but has " +
        edgeGroups(edgeGroup).srcPropertyGroup.name)
    require(
      edgeGroups(edgeGroup).dstPropertyGroup.name == rightBiGraphPart,
      s"Edge Property Group should have $rightBiGraphPart destination group but has " +
        edgeGroups(edgeGroup).dstPropertyGroup.name)
    val keptVPropertyGroups = vertexPropertyGroups.filterNot(g => g.name == rightBiGraphPart)
    val keptEPropertyGroups = edgesPropertyGroups.filterNot(g => g.name == edgeGroup)
    val oldGroup = edgeGroups(edgeGroup)
    val oldEdgesData = oldGroup.data

    // Create new edges by joining vertices through their common neighbors
    val projectedEdges = oldEdgesData
      .as("e1")
      .join(oldEdgesData.as("e2"), col("e1.dst") === col("e2.dst"))
      .where("e1.src < e2.src")
      .select(
        col("e1.src").alias(GraphFrame.SRC),
        col("e2.src").alias(GraphFrame.DST),
        newEdgeWeight match {
          case Some(newEdgeFunc) =>
            newEdgeFunc(
              col(s"e1.${oldGroup.weightColumnName}"),
              col(s"e2.${oldGroup.weightColumnName}")).alias(GraphFrame.WEIGHT)
          case None => lit(1.0).alias(GraphFrame.WEIGHT)
        })

    val newEdgeGroup = EdgePropertyGroup(
      name = s"projected_$edgeGroup",
      data = projectedEdges,
      srcPropertyGroup = vertexGroups(leftBiGraphPart),
      dstPropertyGroup = vertexGroups(leftBiGraphPart),
      isDirected = false,
      srcColumnName = GraphFrame.SRC,
      dstColumnName = GraphFrame.DST,
      weightColumnName = GraphFrame.WEIGHT)

    PropertyGraphFrame(keptVPropertyGroups, keptEPropertyGroups :+ newEdgeGroup)
  }

  /**
   * Joins the vertices data with the specified vertex property groups to produce a unified
   * DataFrame. Each vertex property group defines how the data should be structured and filtered.
   *
   * @param verticesData
   *   The DataFrame containing the vertices data to join. It must include vertex properties and
   *   the group identifiers to filter and map. It is expected to be an output of calling graph
   *   algorithms on GraphFrame, made by the method toGraphFrame.
   * @param vertexGroups
   *   A sequence of vertex group names that are to be joined. Each name must correspond to a
   *   valid vertex property group defined in the PropertyGraphFrame.
   * @return
   *   A DataFrame representing the unified vertices data where each group has been appropriately
   *   filtered, joined, and processed based on its configuration.
   */
  def joinVertices(verticesData: DataFrame, vertexGroups: Seq[String]): DataFrame = {
    require(vertexGroups.forall(this.vertexGroups.contains))
    vertexGroups
      .map { (vg: String) =>
        {
          val associatedGroup = this.vertexGroups(vg)
          val filteredForGroup = verticesData.filter(col(PROPERTY_GROUP_COL_NAME) === lit(vg))
          if (associatedGroup.applyMaskOnId) {
            associatedGroup.internalIdMapping
              .join(filteredForGroup, Seq(GraphFrame.ID), "left")
              .drop(GraphFrame.ID)
          } else {
            associatedGroup
              .getData()
              .join(filteredForGroup, GraphFrame.ID, "left")
              .withColumnRenamed(GraphFrame.ID, EXTERNAL_ID)
          }
        }
      }
      .reduce(_ union _)
  }
}

object PropertyGraphFrame {

  /**
   * A constant representing the column name used for property grouping. It is used within the
   * context of a property graph structure to manage or identify property group associations.
   */
  val PROPERTY_GROUP_COL_NAME = "property_group"

  /**
   * A constant representing the column name used for external identifiers. It serves as a key to
   * associate external data or entities within the context of a property graph structure.
   */
  val EXTERNAL_ID = "external_id"
}
