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
import org.apache.spark.graphframes.GraphFramesUnsupportedVertexTypeException
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithCheckpointInterval
import org.apache.spark.graphframes.WithIntermediateStorageLevel
import org.apache.spark.graphframes.WithLocalCheckpoints
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.hll_sketch_agg
import org.apache.spark.sql.functions.hll_union_agg
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType

/**
 * HyperANF-style approximation of the neighbourhood function on top of GraphFrames.
 *
 * This implementation is inspired by Vigna, Boldi, and Rosa,
 * <a href="https://arxiv.org/pdf/1011.5599">"HyperANF: Approximating the Neighbourhood Function of
 * Very Large Graphs on a Budget"</a> (2010).
 *
 * The input graph is treated as directed: for each vertex, reachability is computed by following
 * outgoing edges from `src` to `dst`.
 *
 * Compared with the cumulative neighbourhood-function presentation in the paper, this
 * implementation returns one column per hop, `hop_0`, `hop_1`, `hop_2`, ..., `hop_N`. The `hop_0`
 * column contains a HyperLogLog sketch of the source vertex itself, and each `hop_k` column for
 * `k >= 1` contains a HyperLogLog sketch of the set of vertices reachable in exactly `k` hops. To
 * derive the cumulative approximate neighbourhood function for distances up to some hop `k`, a
 * user can combine `hop_0` through `hop_k` with `hll_union` and then apply `hll_sketch_estimate`
 * to the merged sketch.
 *
 * The computation can also be restricted to a subgraph by supplying an edge filter expression via
 * [[setEdgesFilterExpression]]. A common use case is to filter on `src`, for example
 * `src IN (...)`, to obtain sketches only for a selected set of starting vertices.
 *
 * @param graph
 *   input graph whose directed edges are used for reachability expansion
 */
class HyperANF private[graphframes] (graph: GraphFrame)
    extends Serializable
    with Logging
    with WithCheckpointInterval
    with WithIntermediateStorageLevel
    with WithLocalCheckpoints {
  private var nHops: Int = 3
  private var edgesFilterExpression: Column = lit(true)
  private var lgNomEntries: Int = 12

  /**
   * Sets the log2 of nominal entries used by HLL sketch aggregations.
   */
  def setLgNomEntries(value: Int): this.type = {
    require((value >= 4) && (value <= 21), "lgNomEntries must be between 4 and 21")
    lgNomEntries = value
    this
  }

  /**
   * Sets the edge filter expression used before running the computation.
   *
   * Only edges satisfying this predicate participate in the directed reachability expansion. This
   * effectively runs the algorithm on the subgraph induced by the filtered edge set.
   *
   * A common use case is filtering on `src`, for example `src IN (...)`, to limit the result to a
   * chosen set of starting vertices.
   *
   * @param value
   *   filter expression applied to `graph.edges`
   * @return
   *   this HyperANF instance
   */
  def setEdgesFilterExpression(value: Column): this.type = {
    edgesFilterExpression = value
    this
  }

  /**
   * Sets the maximum hop distance to compute.
   *
   * The result will contain `hop_0`, `hop_1`, `hop_2`, ..., `hop_N`, where `N` is the configured
   * number of hops.
   *
   * @param value
   *   positive number of hops to compute
   * @return
   *   this HyperANF instance
   */
  def setNHops(value: Int): this.type = {
    require(value > 0, "n-hops cannot be negative or zero")
    nHops = value
    this
  }

  /**
   * Runs the HyperANF-style computation.
   *
   * The returned `DataFrame` has one row per source vertex present in the filtered edge set. It
   * contains the vertex id column `id` and one sketch column per hop: `hop_0`, `hop_1`, `hop_2`,
   * ..., `hop_N`. The `hop_0` column stores a HyperLogLog sketch containing `id` itself. Each
   * `hop_k` column for `k >= 1` stores a HyperLogLog sketch for the set of vertices reachable
   * from `id` in exactly `k` directed hops.
   *
   * To obtain an approximate cumulative neighbourhood size up to hop `k`, union `hop_0` through
   * `hop_k` with `hll_union` and then apply `hll_sketch_estimate`.
   *
   * @return
   *   a `DataFrame` with exact-hop HyperLogLog sketches per source vertex
   */
  def run(): DataFrame = {
    val edges =
      graph.edges
        .filter(edgesFilterExpression)
        .select(GraphFrame.SRC, GraphFrame.DST)
        .persist(intermediateStorageLevel)
    var hop = 1

    val hop0Func = graph.vertices.schema(GraphFrame.ID).dataType match {
      case IntegerType => udf(HyperANF.hllInt(lgNomEntries))
      case LongType => udf(HyperANF.hllLong(lgNomEntries))
      case StringType => udf(HyperANF.hllString(lgNomEntries))
      case ShortType => udf(HyperANF.hllShort(lgNomEntries))
      case ByteType => udf(HyperANF.hllByte(lgNomEntries))
      case _ =>
        throw new GraphFramesUnsupportedVertexTypeException(
          s"Unsupported vertex ID type: ${graph.vertices.schema(GraphFrame.ID).dataType}")
    }
    var state = edges
      .groupBy(col(GraphFrame.SRC).alias(GraphFrame.ID))
      .agg(hll_sketch_agg(GraphFrame.DST, lgNomEntries).alias("hop_1"))
      .select(col(GraphFrame.ID), hop0Func(col(GraphFrame.ID)).alias("hop_0"), col("hop_1"))
      .persist(intermediateStorageLevel)

    // materialize
    val cnt = state.count()
    logInfo(s"found $cnt vertices with at least one outgoing edge")

    val shouldCheckpoint = (checkpointInterval > 0) && (checkpointInterval < nHops)

    while (hop < nHops) {
      hop += 1

      val nState = edges
        .join(
          state.select(GraphFrame.ID, s"hop_${hop - 1}"),
          col(GraphFrame.DST) === col(GraphFrame.ID),
          "left")
        .groupBy(col(GraphFrame.SRC).alias(GraphFrame.ID))
        .agg(hll_union_agg(s"hop_${hop - 1}").alias(s"hop_${hop}"))

      // standard GF persist-unpersist-checkpoint flow
      state = {
        val stateToPersist = state.join(nState, GraphFrame.ID)
        if (shouldCheckpoint && hop % checkpointInterval == 0) {
          if (useLocalCheckpoints) {
            stateToPersist.localCheckpoint(eager = false)
          } else {
            stateToPersist.checkpoint(eager = false)
          }
        } else {
          stateToPersist.persist(intermediateStorageLevel)
          // materialize
          stateToPersist.count()

          state.unpersist()
          stateToPersist
        }
      }

      logInfo(s"hop $hop / $nHops was computed")
    }

    // state is already persisted at the moment
    resultIsPersistent()
    edges.unpersist()

    state
  }
}

private object HyperANF extends Serializable {
  // If you are confusing to see 5 almost identical functions:
  // it was intentional. HLL does not have `update(Object)`.

  def hllInt(lgNomEntries: Int): Int => Array[Byte] = (id) => {
    val sketch = new org.apache.datasketches.hll.HllSketch(lgNomEntries)
    sketch.update(id.toLong)
    sketch.toCompactByteArray()
  }

  def hllLong(lgNomEntries: Int): Long => Array[Byte] = (id) => {
    val sketch = new org.apache.datasketches.hll.HllSketch(lgNomEntries)
    sketch.update(id)
    sketch.toCompactByteArray()
  }

  def hllString(lgNomEntries: Int): String => Array[Byte] = (id) => {
    val sketch = new org.apache.datasketches.hll.HllSketch(lgNomEntries)
    sketch.update(id)
    sketch.toCompactByteArray()
  }

  def hllShort(lgNomEntries: Int): Short => Array[Byte] = (id) => {
    val sketch = new org.apache.datasketches.hll.HllSketch(lgNomEntries)
    sketch.update(id.toLong)
    sketch.toCompactByteArray()
  }

  def hllByte(lgNomEntries: Int): Byte => Array[Byte] = (id) => {
    val sketch = new org.apache.datasketches.hll.HllSketch(lgNomEntries)
    sketch.update(id.toLong)
    sketch.toCompactByteArray()
  }
}
