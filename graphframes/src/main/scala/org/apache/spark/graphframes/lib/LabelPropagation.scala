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

import org.apache.spark.graphx
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.MapType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithAlgorithmChoice
import org.apache.spark.graphframes.WithCheckpointInterval
import org.apache.spark.graphframes.WithIntermediateStorageLevel
import org.apache.spark.graphframes.WithLocalCheckpoints
import org.apache.spark.graphframes.WithMaxIter

/**
 * Run static Label Propagation for detecting communities in networks.
 *
 * Each node in the network is initially assigned to its own community. At every iteration, nodes
 * send their community affiliation to all neighbors and update their state to the mode community
 * affiliation of incoming messages.
 *
 * LPA is a standard community detection algorithm for graphs. It is very inexpensive
 * computationally, although (1) convergence is not guaranteed and (2) one can end up with trivial
 * solutions (all nodes are identified into a single community).
 *
 * The resulting DataFrame contains all the original vertex information and one additional column:
 *   - label (`LongType`): label of community affiliation
 */
class LabelPropagation private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with WithAlgorithmChoice
    with WithCheckpointInterval
    with WithMaxIter
    with WithLocalCheckpoints
    with WithIntermediateStorageLevel
    with Logging {

  def run(): DataFrame = {
    val maxIterChecked = check(maxIter, "maxIter")
    val res = algorithm match {
      case "graphx" => LabelPropagation.runInGraphX(graph, maxIterChecked)
      case "graphframes" =>
        LabelPropagation.runInGraphFrames(
          graph,
          maxIterChecked,
          checkpointInterval,
          useLocalCheckpoints = useLocalCheckpoints,
          intermediateStorageLevel = intermediateStorageLevel)
    }
    resultIsPersistent()
    res
  }
}

private object LabelPropagation {
  private def runInGraphX(graph: GraphFrame, maxIter: Int): DataFrame = {
    val gx = graphx.lib.LabelPropagation.run(graph.cachedTopologyGraphX, maxIter)
    val res = GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(LABEL_ID)).vertices
    res.persist(StorageLevel.MEMORY_AND_DISK_SER)
    res.count()
    gx.unpersist()
    res
  }

  private def keyWithMaxValue(column: Column): Column = {
    // Get the key with the highest value, using the key to break a tie. To do this, simply get
    // map entries, swap the value and key columns to create the natural ordering, multiply key by -1 and then
    // take the key from the max entry (multiply it by -1 again to get the original key).
    array_max(
      transform(
        map_entries(column),
        x => struct(x.getField("value"), (lit(-1) * x.getField("key")).alias("key"))))
      .getField("key") * lit(-1)
  }

  private def runInGraphFrames(
      graph: GraphFrame,
      maxIter: Int,
      checkpointInterval: Int,
      isDirected: Boolean = true,
      useLocalCheckpoints: Boolean,
      intermediateStorageLevel: StorageLevel): DataFrame = {
    // Overall:
    // - Initial labels - IDs
    // - Active vertex col (halt voting) - did the label changed?
    // - Choosing a new label - top across neighbours (tie-braking is deterministic)

    val preparedGraph = GraphFrame(
      graph.vertices.select(GraphFrame.ID),
      graph.edges.select(GraphFrame.SRC, GraphFrame.DST))

    var pregel = preparedGraph.pregel
      .withVertexColumn(LABEL_ID, col(GraphFrame.ID).alias(LABEL_ID), keyWithMaxValue(Pregel.msg))
      .setMaxIter(maxIter)
      .setStopIfAllNonActiveVertices(true)
      .setEarlyStopping(false)
      .setCheckpointInterval(checkpointInterval)
      .setSkipMessagesFromNonActiveVertices(false)
      .setUpdateActiveVertexExpression(col(LABEL_ID) =!= keyWithMaxValue(Pregel.msg))
      .setUseLocalCheckpoints(useLocalCheckpoints)
      .setIntermediateStorageLevel(intermediateStorageLevel)
      // Memory optimization: only include required columns in triplets
      .requiredSrcColumns(LABEL_ID)
      .requiredDstColumns(LABEL_ID)

    if (isDirected) {
      pregel = pregel.sendMsgToDst(Pregel.src(LABEL_ID))
    } else {
      pregel = pregel.sendMsgToDst(Pregel.src(LABEL_ID)).sendMsgToSrc(Pregel.dst(LABEL_ID))
    }

    pregel = pregel.aggMsgs(
      reduce(
        collect_list(Pregel.msg),
        map().cast(MapType(graph.vertices.schema(GraphFrame.ID).dataType, IntegerType)),
        (acc, x) =>
          map_zip_with(
            acc,
            map(x, lit(1)),
            (_, left, right) => coalesce(left, lit(0)) + coalesce(right, lit(0)))))

    pregel.run()
  }

  private val LABEL_ID = "label"
}
