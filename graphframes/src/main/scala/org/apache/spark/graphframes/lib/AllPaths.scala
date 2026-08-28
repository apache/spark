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

package org.apache.spark.graphframes.lib

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.WithCheckpointInterval
import org.apache.spark.graphframes.WithDirection
import org.apache.spark.graphframes.WithIntermediateStorageLevel
import org.apache.spark.graphframes.WithLocalCheckpoints
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.graphframes.GraphFrameInternals

/**
 * Computes all simple paths between source and destination vertices.
 *
 * This algorithm enumerates paths up to `maxPathLength` hops. It supports directed and undirected
 * traversal as well as optional edge filtering. It returns all simple paths between source and
 * destination vertices. Here the term "simple" means no repeated vertices. For example, if there
 * are paths A-B-C, A-D-C and the edge B-A, user asked to find all the paths between "A" and "C"
 * only A-B-C and A-D-C will be returned, but not the A-B-A-D-C. The default value of the
 * `maxPathLength` is `5`. Keep in mind that requesting `maxPathLength` of the scale of the graph
 * diameter may tend this algorithm will try to return (almost) all simple paths in the graph that
 * can create huge performance degradation or even OOM-like errors. Algorithm supports both
 * directed and undirected graphs.
 *
 * Returned DataFrame schema:
 *   - `path`: array of vertex ids in traversal order
 *   - `len`: number of edges in the path (Long)
 *
 * Note: in the case of undirected graph an algorithm run on the internal graph made by union
 * edges and reversed edges. It is assumed that graph does not have multi-edges. Results may be
 * unstable and unpredictable for the graph with multi-edges.
 */
class AllPaths private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with Serializable
    with WithDirection
    with WithLocalCheckpoints
    with WithCheckpointInterval
    with WithIntermediateStorageLevel {

  private var maxPathLength: Int = 5
  private var fromExpression: Column = _
  private var toExpression: Column = _
  private var edgeFilterExpression: Option[Column] = None

  /**
   * Sets the expression identifying the source (starting) vertices.
   *
   * @param value
   *   a Column expression evaluated against vertex attributes to select source vertices
   * @return
   *   this instance for method chaining
   */
  def fromExpr(value: Column): this.type = {
    fromExpression = value
    this
  }

  /**
   * Sets the expression identifying the source (starting) vertices.
   *
   * @param value
   *   a SQL expression string evaluated against vertex attributes to select source vertices
   * @return
   *   this instance for method chaining
   */
  def fromExpr(value: String): this.type = fromExpr(expr(value))

  /**
   * Sets the expression identifying the destination (target) vertices.
   *
   * @param value
   *   a Column expression evaluated against vertex attributes to select destination vertices
   * @return
   *   this instance for method chaining
   */
  def toExpr(value: Column): this.type = {
    toExpression = value
    this
  }

  /**
   * Sets the expression identifying the destination (target) vertices.
   *
   * @param value
   *   a SQL expression string evaluated against vertex attributes to select destination vertices
   * @return
   *   this instance for method chaining
   */
  def toExpr(value: String): this.type = toExpr(expr(value))

  /**
   * Sets the maximum path length (number of edges) for the enumerated paths.
   *
   * Setting a large value (e.g. on the scale of the graph diameter) may cause the algorithm to
   * attempt to collect a very large number of paths, leading to severe performance degradation or
   * out-of-memory errors. Use with caution on large or densely connected graphs.
   *
   * @param value
   *   the maximum number of edges in a path; must be greater than 0. Default is 5.
   * @return
   *   this instance for method chaining
   */
  def maxPathLength(value: Int): this.type = {
    require(value > 0, s"AllPaths maxPathLength must be > 0, but was set to $value")
    maxPathLength = value
    this
  }

  /**
   * Sets an optional filter expression applied to edges during traversal. Only edges satisfying
   * this condition will be considered.
   *
   * @param value
   *   a Column expression evaluated against edge attributes
   * @return
   *   this instance for method chaining
   */
  def edgeFilter(value: Column): this.type = {
    edgeFilterExpression = Some(value)
    this
  }

  /**
   * Sets an optional filter expression applied to edges during traversal. Only edges satisfying
   * this condition will be considered.
   *
   * @param value
   *   a SQL expression string evaluated against edge attributes
   * @return
   *   this instance for method chaining
   */
  def edgeFilter(value: String): this.type = edgeFilter(expr(value))

  /**
   * Executes the AllPaths algorithm and returns all simple paths between the specified source and
   * destination vertices.
   *
   * @return
   *   a DataFrame with the following columns:
   *   - `path`: an array of vertex ids in traversal order
   *   - `len`: the number of edges in the path (Long)
   */
  def run(): DataFrame = {
    require(fromExpression != null, "fromExpr is required.")
    require(toExpression != null, "toExpr is required.")
    require(
      graph.vertices.columns.toSet.intersect(Set("hop", "path", "len")).isEmpty,
      "columns `hop`, `path` and `len` are reserved by algorithm")

    val traversalGraph = if (isDirected) {
      graph
    } else {
      val edgeColumns = graph.edges.columns.toSeq
      val reversed = graph.edges.select(
        (Seq(
          col(GraphFrame.DST).alias(GraphFrame.SRC),
          col(GraphFrame.SRC).alias(GraphFrame.DST)) ++
          edgeColumns.filterNot(c => c == GraphFrame.SRC || c == GraphFrame.DST).map(col)): _*)
      GraphFrame(graph.vertices, graph.edges.unionByName(reversed))
    }

    val agg = traversalGraph.aggregateNeighbors
      .setStartingVertices(fromExpression)
      .setMaxHops(maxPathLength)
      .setTargetCondition(
        GraphFrameInternals.applyExprToCol(graph.spark, toExpression, "dst_attributes"))
      .setStoppingCondition(
        array_contains(col("path"), AggregateNeighbors.dstAttr(GraphFrame.ID)))
      .addAccumulator(
        "path",
        array(col(GraphFrame.ID)),
        concat(col("path"), array(AggregateNeighbors.dstAttr(GraphFrame.ID))))
      .setUseLocalCheckpoints(useLocalCheckpoints)
      .setCheckpointInterval(checkpointInterval)
      .setIntermediateStorageLevel(intermediateStorageLevel)

    edgeFilterExpression.foreach { ef =>
      agg.setEdgeFilter(GraphFrameInternals.applyExprToCol(graph.spark, ef, "edge_attributes"))
    }

    agg
      .run()
      .select(col("path"), col("hop").alias("len"))
      .distinct()
  }
}
