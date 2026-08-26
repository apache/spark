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

package org.apache.spark.graphframes

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array, col, count, countDistinct, explode, expr, struct}
import org.apache.spark.storage.StorageLevel

/**
 * A graph whose vertices and edges are represented by Spark DataFrames.
 *
 * Vertices must contain a unique `id` column. Edges must contain `src` and `dst` columns whose
 * values identify the source and destination vertices. Additional columns are graph attributes.
 *
 * Use [[GraphFrame.apply]] to construct a graph.
 */
class GraphFrame private (
    @transient private val vertexDataFrame: DataFrame,
    @transient private val edgeDataFrame: DataFrame)
    extends Serializable {

  import GraphFrame._

  /** Persist the vertex and edge DataFrames with their default storage level. */
  def cache(): this.type = {
    vertices.cache()
    edges.cache()
    this
  }

  /** Persist the vertex and edge DataFrames with their default storage level. */
  def persist(): this.type = {
    vertices.persist()
    edges.persist()
    this
  }

  /** Persist the vertex and edge DataFrames with `storageLevel`. */
  def persist(storageLevel: StorageLevel): this.type = {
    vertices.persist(storageLevel)
    edges.persist(storageLevel)
    this
  }

  /** Remove the vertex and edge DataFrames from the cache. */
  def unpersist(): this.type = unpersist(blocking = false)

  /** Remove the vertex and edge DataFrames from the cache. */
  def unpersist(blocking: Boolean): this.type = {
    vertices.unpersist(blocking)
    edges.unpersist(blocking)
    this
  }

  /** The graph's vertex DataFrame. */
  def vertices: DataFrame = {
    requireDriverDataFrame(vertexDataFrame)
    vertexDataFrame
  }

  /** The graph's edge DataFrame. */
  def edges: DataFrame = {
    requireDriverDataFrame(edgeDataFrame)
    edgeDataFrame
  }

  /**
   * Return `(source vertex)-[edge]->(destination vertex)` triplets.
   *
   * The result has `src`, `edge`, and `dst` struct columns containing the complete corresponding
   * input rows.
   */
  @transient lazy val triplets: DataFrame = {
    val sourceVertices = vertices.select(
      col(quoted(ID)).alias("__graphframes_src_id"),
      nested(vertices, SRC))
    val graphEdges = edges.select(
      col(quoted(SRC)).alias("__graphframes_edge_src"),
      col(quoted(DST)).alias("__graphframes_edge_dst"),
      nested(edges, EDGE))
    val destinationVertices = vertices.select(
      col(quoted(ID)).alias("__graphframes_dst_id"),
      nested(vertices, DST))

    sourceVertices
      .join(
        graphEdges,
        col("__graphframes_src_id") === col("__graphframes_edge_src"))
      .join(
        destinationVertices,
        col("__graphframes_dst_id") === col("__graphframes_edge_dst"))
      .select(col(SRC), col(EDGE), col(DST))
  }

  /** Return the out-degree of every vertex having at least one outgoing edge. */
  @transient lazy val outDegrees: DataFrame = {
    edges
      .groupBy(col(quoted(SRC)).alias(ID))
      .agg(count("*").cast("int").alias(OUT_DEGREE))
  }

  /** Return the in-degree of every vertex having at least one incoming edge. */
  @transient lazy val inDegrees: DataFrame = {
    edges
      .groupBy(col(quoted(DST)).alias(ID))
      .agg(count("*").cast("int").alias(IN_DEGREE))
  }

  /** Return the total degree of every vertex incident to at least one edge. */
  @transient lazy val degrees: DataFrame = {
    edges
      .select(explode(array(col(quoted(SRC)), col(quoted(DST)))).alias(ID))
      .groupBy(ID)
      .agg(count("*").cast("int").alias(DEGREE))
  }

  /** Return a graph with the direction of every edge reversed. */
  def reverse: GraphFrame = {
    val attributes = edges.columns
      .filterNot(name => name == SRC || name == DST)
      .map(name => col(quoted(name)))
    val reversedColumns = Seq(
      col(quoted(DST)).alias(SRC),
      col(quoted(SRC)).alias(DST)) ++ attributes
    val reversedEdges = edges.select(reversedColumns: _*)
    GraphFrame(vertices, reversedEdges)
  }

  /** Return an undirected graph by adding a reversed copy of every edge. */
  def asUndirected: GraphFrame = GraphFrame(vertices, edges.unionByName(reverse.edges))

  /** Filter vertices and remove edges incident to any removed vertex. */
  def filterVertices(condition: Column): GraphFrame = {
    val filteredVertices = vertices.filter(condition)
    val vertexIds = filteredVertices.select(col(quoted(ID)))
    val filteredEdges = edges
      .join(vertexIds, col(quoted(SRC)) === vertexIds(ID), "left_semi")
      .join(vertexIds, col(quoted(DST)) === vertexIds(ID), "left_semi")
    GraphFrame(filteredVertices, filteredEdges)
  }

  /** Filter vertices using a SQL expression. */
  def filterVertices(condition: String): GraphFrame = filterVertices(expr(condition))

  /** Filter edges while keeping all vertices. */
  def filterEdges(condition: Column): GraphFrame = GraphFrame(vertices, edges.filter(condition))

  /** Filter edges using a SQL expression. */
  def filterEdges(condition: String): GraphFrame = filterEdges(expr(condition))

  /** Return a graph without vertices that are not incident to an edge. */
  def dropIsolatedVertices(): GraphFrame = {
    val incidentIds = edges.select(explode(array(col(quoted(SRC)), col(quoted(DST)))).alias(ID))
    GraphFrame(vertices.join(incidentIds, Seq(ID), "left_semi"), edges)
  }

  /**
   * Validate vertex uniqueness and ensure that every edge endpoint is present in `vertices`.
   *
   * This method runs Spark jobs and throws [[InvalidGraphException]] for an invalid graph.
   */
  def validate(): Unit = {
    val persistedVertices = vertices.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val vertexCount = persistedVertices.count()
      val distinctVertexCount = persistedVertices.select(countDistinct(col(quoted(ID)))).head()
        .getLong(0)
      if (vertexCount != distinctVertexCount) {
        throw new InvalidGraphException(
          s"Graph contains ${vertexCount - distinctVertexCount} duplicate vertices")
      }

      val endpoints = edges
        .select(col(quoted(SRC)).alias(ID))
        .union(edges.select(col(quoted(DST)).alias(ID)))
        .distinct()
      val missingEndpointCount = endpoints.join(persistedVertices, Seq(ID), "left_anti").count()
      if (missingEndpointCount > 0) {
        throw new InvalidGraphException(
          s"Graph contains $missingEndpointCount edge endpoints without matching vertices")
      }
    } finally {
      persistedVertices.unpersist()
    }
  }

  override def toString: String = {
    val vertexColumns = ID +: vertices.columns.filterNot(_ == ID).toSeq
    val edgeColumns = SRC +: DST +: edges.columns.filterNot(c => c == SRC || c == DST).toSeq
    val orderedVertices = vertices.select(vertexColumns.map(name => col(quoted(name))): _*)
    val orderedEdges = edges.select(edgeColumns.map(name => col(quoted(name))): _*)
    s"GraphFrame(v:$orderedVertices, e:$orderedEdges)"
  }

  private def requireDriverDataFrame(dataFrame: DataFrame): Unit = {
    if (dataFrame == null) {
      throw new IllegalStateException("GraphFrame objects cannot be used inside Spark closures")
    }
  }
}

object GraphFrame extends Logging {

  val ID: String = "id"
  val SRC: String = "src"
  val DST: String = "dst"
  val EDGE: String = "edge"
  val DEGREE: String = "degree"
  val IN_DEGREE: String = "inDegree"
  val OUT_DEGREE: String = "outDegree"

  /** Create a GraphFrame from vertex and edge DataFrames. */
  def apply(vertices: DataFrame, edges: DataFrame): GraphFrame = {
    requireColumn(vertices, ID, "Vertex ID")
    requireColumn(edges, SRC, "Source vertex ID")
    requireColumn(edges, DST, "Destination vertex ID")
    require(
      vertices.sparkSession eq edges.sparkSession,
      "Vertex and edge DataFrames must belong to the same SparkSession")
    new GraphFrame(vertices, edges)
  }

  /**
   * Create a GraphFrame from an edge DataFrame, deriving and persisting its distinct vertices.
   */
  def fromEdges(edges: DataFrame): GraphFrame = {
    fromEdges(edges, StorageLevel.MEMORY_AND_DISK)
  }

  /**
   * Create a GraphFrame from an edge DataFrame, deriving its distinct vertices.
   *
   * The caller is responsible for unpersisting the returned graph's vertex DataFrame.
   */
  def fromEdges(edges: DataFrame, storageLevel: StorageLevel): GraphFrame = {
    requireColumn(edges, SRC, "Source vertex ID")
    requireColumn(edges, DST, "Destination vertex ID")
    logWarning(
      s"GraphFrame.fromEdges persists derived vertices with storage level $storageLevel; " +
        "call vertices.unpersist() when the graph is no longer needed")
    val vertices = edges
      .select(col(quoted(SRC)).alias(ID))
      .union(edges.select(col(quoted(DST)).alias(ID)))
      .distinct()
      .persist(storageLevel)
    GraphFrame(vertices, edges)
  }

  private def requireColumn(dataFrame: DataFrame, columnName: String, label: String): Unit = {
    require(
      dataFrame.columns.contains(columnName),
      s"$label column '$columnName' is missing; available columns: " +
        dataFrame.columns.mkString(", "))
  }

  private def nested(dataFrame: DataFrame, name: String): Column = {
    val columns = dataFrame.columns.map(columnName => col(quoted(columnName))).toIndexedSeq
    struct(columns: _*).alias(name)
  }

  private def quoted(columnName: String): String = {
    s"`${columnName.replace("`", "``")}`"
  }
}
