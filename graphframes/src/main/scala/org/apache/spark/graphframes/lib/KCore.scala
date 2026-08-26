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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.call_function
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.graphframes.expressions.KCoreMerge
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithCheckpointInterval
import org.apache.spark.graphframes.WithIntermediateStorageLevel
import org.apache.spark.graphframes.WithLocalCheckpoints

/**
 * K-Core decomposition algorithm implementation for GraphFrames.
 *
 * This object provides the `run` method to compute the k-core decomposition of a graph, which
 * assigns each vertex the maximum k such that the vertex is part of a k-core. A k-core is a
 * maximal connected subgraph in which every vertex has degree at least k.
 *
 * The algorithm is based on the distributed k-core decomposition approach described in:
 *
 * Mandal, Aritra, and Mohammad Al Hasan. "A distributed k-core decomposition algorithm on spark."
 * 2017 IEEE International Conference on Big Data (Big Data). IEEE, 2017.
 *
 * '''Edge representation''': K-core decomposition is defined for undirected graphs. Since
 * GraphFrames represents edges as directed, each undirected edge `{u, v}` should be supplied as a
 * single directed edge in either direction — the algorithm symmetrizes internally. Supplying both
 * `(u, v)` and `(v, u)` will double-count the edge and produce incorrect results.
 */
class KCore private[graphframes] (private val graph: GraphFrame)
    extends Serializable
    with WithIntermediateStorageLevel
    with WithCheckpointInterval
    with WithLocalCheckpoints {
  import org.apache.spark.graphframes.lib.KCore.kCoreColumnName
  def run(): DataFrame = {
    val result =
      KCore.run(graph, intermediateStorageLevel, checkpointInterval, useLocalCheckpoints)
    val allVertices = graph.vertices
      .select(GraphFrame.ID)
      .join(result, Seq(GraphFrame.ID), "left")
      .withColumn(
        kCoreColumnName,
        when(col(kCoreColumnName).isNull, lit(0)).otherwise(col(kCoreColumnName)))
      .persist(intermediateStorageLevel)

    // materialize
    allVertices.count()
    result.unpersist()
    allVertices
  }
}

object KCore extends Serializable with Logging {
  val kCoreColumnName = "kcore"
  def run(
      graph: GraphFrame,
      storageLevel: StorageLevel,
      checkpointInterval: Int,
      useLocalCheckpoints: Boolean): DataFrame = {
    val degrees = graph.degrees
    val preparedGraph = GraphFrame(
      degrees.withColumn("degree", col("degree").cast(IntegerType)),
      graph.edges.select(GraphFrame.SRC, GraphFrame.DST))

    val functionRegistry = graph.vertices.sparkSession.sessionState.functionRegistry
    functionRegistry.registerFunction(
      new FunctionIdentifier("_kcoreMerge", Some("builtin"), Some("system")),
      (children: Seq[Expression]) => KCoreMerge(children(0), children(1)),
      "scala_udf")

    try {
      val pregel = preparedGraph.pregel
        .setMaxIter(Int.MaxValue)
        .setIntermediateStorageLevel(storageLevel)
        .setCheckpointInterval(checkpointInterval)
        .withVertexColumn(
          kCoreColumnName,
          col("degree"),
          call_function("_kcoreMerge", Pregel.msg, col(kCoreColumnName)))
        .sendMsgToSrc(Pregel.dst(kCoreColumnName))
        .sendMsgToDst(Pregel.src(kCoreColumnName))
        .setInitialActiveVertexExpression(lit(true))
        .setUpdateActiveVertexExpression(
          col(kCoreColumnName) =!= call_function("_kcoreMerge", Pregel.msg, col(kCoreColumnName)))
        .setEarlyStopping(false)
        .setStopIfAllNonActiveVertices(true)
        .setSkipMessagesFromNonActiveVertices(false)
        .setUseLocalCheckpoints(useLocalCheckpoints)
        .aggMsgs(collect_list(Pregel.msg))

      pregel.run()
    } finally {
      val dereg = functionRegistry.dropFunction(
        new FunctionIdentifier("_kcoreMerge", Some("builtin"), Some("system")))
      if (!dereg) {
        logWarn(
          "graphframes faced an internal error and was not able to de-register function _kcoreMerge; Spark' functionRegistry is in a bad state")
      }
    }
  }
}
