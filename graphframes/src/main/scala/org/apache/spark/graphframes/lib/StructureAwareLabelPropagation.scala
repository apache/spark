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
import org.apache.spark.graphframes.GraphFrame._
import org.apache.spark.graphframes.GraphFramesSparkVersionException
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithCheckpointInterval
import org.apache.spark.graphframes.WithDirection
import org.apache.spark.graphframes.WithIntermediateStorageLevel
import org.apache.spark.graphframes.WithLgNomEntries
import org.apache.spark.graphframes.WithLocalCheckpoints
import org.apache.spark.graphframes.WithMaxIter
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Neighborhood-aware community detection via weighted label propagation.
 *
 * This algorithm is a Label Propagation variant where each incoming label vote is weighted by a
 * combination of:
 *   - optional direct-link baseline strength (enabled unless `ignoreDirectLinks = true`), and
 *   - neighborhood-overlap strength (`structuralSimilarityMultiplier * commonNeighbors`).
 *
 * Intuitively, labels from neighbors that are structurally similar to the destination (many
 * common neighbors) can be amplified, instead of treating all edges equally.
 *
 * At each iteration, every vertex aggregates weighted incoming votes by label and picks the label
 * with maximum total weight.
 *
 * Main hyperparameters:
 *   - `maxIter` (required): maximum number of propagation rounds.
 *   - `ignoreDirectLinks` (default `false`): whether to drop direct-link baseline vote mass.
 *   - `structuralSimilarityMultiplier` (default `0.5`): scales neighborhood-overlap contribution.
 *
 * Edge-weight regimes:
 *   - `ignoreDirectLinks = false`:
 *     {{{
 *     edgeWeight(src, dst) = 1 + structuralSimilarityMultiplier * commonNeighbors(src, dst)
 *     }}}
 *   - `ignoreDirectLinks = true`:
 *     {{{
 *     edgeWeight(src, dst) = structuralSimilarityMultiplier * commonNeighbors(src, dst)
 *     }}}
 *
 * This implementation is inspired by neighborhood-strength-driven label propagation ideas from:
 * Xie, Jierui, and Boleslaw K. Szymanski. "Community detection using a neighborhood strength
 * driven label propagation algorithm." 2011 IEEE Network Science Workshop. IEEE, 2011.
 *
 * Note: this implementation does not strictly reproduce the paper; it adopts the core idea of
 * modulating label votes with a common-neighbor term within the GraphFrames/Pregel design.
 */
class StructureAwareLabelPropagation private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with WithCheckpointInterval
    with WithMaxIter
    with WithLocalCheckpoints
    with WithIntermediateStorageLevel
    with WithDirection
    with WithLgNomEntries
    with Logging {

  private var structuralSimilarityMultiplier: Double = 0.5
  private var ignoreDirectLinks: Boolean = false
  private var initialLabelCol: Option[String] = None

  import StructureAwareLabelPropagation._

  /**
   * Sets whether direct-link baseline vote mass is ignored.
   *
   * If `false` (default), each existing edge contributes a baseline of `1.0` before structural
   * overlap is added. If `true`, only structural overlap contributes vote mass.
   */
  def setIgnoreDirectLinks(value: Boolean): this.type = {
    ignoreDirectLinks = value
    this
  }

  /**
   * Sets multiplier for the neighborhood-overlap signal (common neighbors).
   *
   * Edge weighting is:
   *   - when direct links are included:
   *     {{{
   *     edgeWeight(src, dst) = 1 + structuralSimilarityMultiplier * commonNeighbors(src, dst)
   *     }}}
   *   - when direct links are ignored:
   *     {{{
   *     edgeWeight(src, dst) = structuralSimilarityMultiplier * commonNeighbors(src, dst)
   *     }}}
   *
   * `commonNeighbors(src, dst)` is the (approximate) number of shared out-neighbors between
   * source and destination.
   *
   * The value must be non-negative.
   */
  def setStructuralSimilarityMultiplier(value: Double): this.type = {
    require(value >= 0.0, "structuralSimilarityMultiplier must be >= 0")
    structuralSimilarityMultiplier = value
    this
  }

  /**
   * Sets an explicit vertex column to use as initial labels.
   *
   * By default, each vertex starts with its own `id` as label. When this setter is used, the
   * algorithm initializes labels from the provided attribute column instead, enabling
   * attribute-guided label propagation (attribute propagation): labels can start from domain
   * values such as categories, types, or seeds and then propagate through the graph structure.
   *
   * The output `label` column keeps the data type of the provided column.
   */
  def setInitialLabelCol(col: String): this.type = {
    require(
      graph.vertices.columns.contains(col),
      s"Initial label column '$col' does not exist in vertex columns: " +
        graph.vertices.columns.mkString(", "))
    initialLabelCol = Some(col)
    this
  }

  def run(): DataFrame = {
    // Validate parameters
    val maxIterChecked = check(maxIter, "maxIter")
    require(
      !(ignoreDirectLinks && structuralSimilarityMultiplier == 0.0),
      "structuralSimilarityMultiplier must be > 0 when ignoreDirectLinks is true")

    // Sketch-based features require Spark >= 4.1
    val sparkVersion = graph.vertices.sparkSession.version
    if (sparkVersion.substring(0, 3) < "4.1") {
      throw new GraphFramesSparkVersionException("4.1.0")
    }

    val edges = if (isDirected) {
      graph.edges.select(SRC, DST)
    } else {
      graph.edges
        .select(col(DST).alias(SRC), col(SRC).alias(DST))
        .union(graph.edges.select(SRC, DST))
        .distinct()
    }

    val directLinkScale = if (ignoreDirectLinks) 0.0 else 1.0

    // Compute approximate common neighbor counts on edges and materialize.
    val enrichedEdges =
      computeEdgeApproxCommonNeighbors(
        edges,
        lgNomEntries,
        structuralSimilarityMultiplier,
        directLinkScale,
        isDirected)

    val vertices = if (initialLabelCol.isDefined) {
      graph.vertices.select(col(GraphFrame.ID), col(initialLabelCol.get).alias(INITIAL_LABEL_COL))
    } else {
      graph.vertices.select(col(GraphFrame.ID), col(GraphFrame.ID).alias(INITIAL_LABEL_COL))
    }

    val preparedGraph = GraphFrame(vertices, enrichedEdges)

    val pregel = preparedGraph.pregel

    pregel
      .setMaxIter(maxIterChecked)
      .setCheckpointInterval(checkpointInterval)
      .setUseLocalCheckpoints(useLocalCheckpoints)
      .setIntermediateStorageLevel(intermediateStorageLevel)
      .requiredEdgeColumns(EDGE_WEIGHT_COL)
      .sendMsgToDst(struct(Pregel.src(LABEL_COL), Pregel.edge(EDGE_WEIGHT_COL)))
      .aggMsgs(aggregateMessages(Pregel.msg, vertices.schema(INITIAL_LABEL_COL).dataType))
      .withVertexColumn(
        LABEL_COL,
        col(INITIAL_LABEL_COL),
        coalesce(keyWithMaxValue(Pregel.msg), col(LABEL_COL)))
      .setSkipMessagesFromNonActiveVertices(false)
      .setUpdateActiveVertexExpression(
        col(LABEL_COL) =!= coalesce(keyWithMaxValue(Pregel.msg), col(LABEL_COL)))
      .setStopIfAllNonActiveVertices(true)
      .setEarlyStopping(false)

    val result = pregel.run().drop(INITIAL_LABEL_COL)
    resultIsPersistent()

    result
  }
}

object StructureAwareLabelPropagation extends Logging {

  val LABEL_COL = "label"
  private val INITIAL_LABEL_COL = "initial_label"
  private val EDGE_WEIGHT_COL = "edge_weight"

  private def aggregateMessages(msgCol: Column, idType: DataType): Column = reduce(
    collect_list(msgCol),
    map().cast(MapType(idType, DoubleType)),
    (acc, x) =>
      map_zip_with(
        acc,
        map(x.getField(LABEL_COL), x.getField(EDGE_WEIGHT_COL)),
        (_, left, right) => coalesce(left, lit(0.0)) + coalesce(right, lit(0.0))))

  private def keyWithMaxValue(column: Column): Column = array_max(
    transform(
      map_entries(column),
      x => struct(x.getField("value"), x.getField("key").alias("key"))))
    .getField("key")

  private def computeEdgeApproxCommonNeighbors(
      edges: DataFrame,
      lgNomEntries: Int,
      structuralSimilarityMultiplier: Double,
      directLinkScale: Double,
      isDirected: Boolean): DataFrame = {

    // For directed graphs we use weak neighborhoods for structural similarity:
    // N(v) = In(v) union Out(v).
    //
    // Label propagation still follows edge direction via sendMsgToDst, but the
    // common-neighbor term ignores orientation to avoid making the structural
    // signal too sparse on low-degree directed graphs.
    def thetaSketchAggExpr = (c: String) => expr(s"theta_sketch_agg($c, $lgNomEntries)")

    var vertexSketches = edges
      .groupBy(col(SRC).alias(ID))
      .agg(thetaSketchAggExpr(DST).alias("nbr_theta_sketch"))

    if (isDirected) {
      val thetaSketchUnion = (left: String, right: String) => expr(s"theta_union($left, $right)")

      vertexSketches = vertexSketches
        .join(
          edges
            .groupBy(col(DST).alias(ID))
            .agg(thetaSketchAggExpr(SRC).alias("nbr_theta_sketch_dst")),
          Seq(ID),
          "full")
        .withColumn(
          "nbr_theta_sketch",
          when(col("nbr_theta_sketch").isNull, col("nbr_theta_sketch_dst"))
            .when(col("nbr_theta_sketch_dst").isNull, col("nbr_theta_sketch"))
            .otherwise(thetaSketchUnion("nbr_theta_sketch", "nbr_theta_sketch_dst")))
    }

    val thetaSketchIntersect = (left: String, right: String) =>
      expr(s"theta_sketch_estimate(theta_intersection($left, $right))")

    // Prepare sketches for join (id, nbr_theta_sketch)
    val srcSketch = vertexSketches.select(
      col(ID).alias("sk_src_id"),
      col("nbr_theta_sketch").alias("src_nbr_sketch"))
    val dstSketch = vertexSketches.select(
      col(ID).alias("sk_dst_id"),
      col("nbr_theta_sketch").alias("dst_nbr_sketch"))

    // Join edges with sketches. Use left_outer so edges without sketches will have null sketches.
    val e = edges
      .select(SRC, DST)
      .join(srcSketch, col(SRC) === col("sk_src_id"), "left_outer")
      .join(dstSketch, col(DST) === col("sk_dst_id"), "left_outer")

    // Compute approximate intersection and coalesce nulls to 0.0
    val weightCol = lit(directLinkScale) + lit(structuralSimilarityMultiplier) * coalesce(
      thetaSketchIntersect("src_nbr_sketch", "dst_nbr_sketch"),
      lit(0.0))
    val edgesWithOverlap = e
      .select(col(SRC), col(DST), weightCol.alias(EDGE_WEIGHT_COL))

    edgesWithOverlap
  }
}
