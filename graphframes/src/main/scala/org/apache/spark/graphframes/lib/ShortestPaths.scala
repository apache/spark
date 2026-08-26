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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.map
import org.apache.spark.sql.functions.map_values
import org.apache.spark.sql.functions.map_zip_with
import org.apache.spark.sql.functions.reduce
import org.apache.spark.sql.functions.transform_keys
import org.apache.spark.sql.functions.transform_values
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.MapType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrame.quote
import org.apache.spark.graphframes.GraphFramesUnreachableException
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithAlgorithmChoice
import org.apache.spark.graphframes.WithCheckpointInterval
import org.apache.spark.graphframes.WithDirection
import org.apache.spark.graphframes.WithIntermediateStorageLevel
import org.apache.spark.graphframes.WithLocalCheckpoints

import java.util
import scala.jdk.CollectionConverters._

/**
 * Computes shortest paths from every vertex to the given set of landmark vertices. Note that this
 * takes edge direction into account.
 *
 * The returned DataFrame contains all the original vertex information as well as one additional
 * column:
 *   - distances (`MapType[vertex ID type, IntegerType]`): For each vertex v, a map containing the
 *     shortest-path distance to each reachable landmark vertex.
 */
class ShortestPaths private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with WithAlgorithmChoice
    with WithCheckpointInterval
    with WithLocalCheckpoints
    with WithIntermediateStorageLevel
    with WithDirection {
  import org.apache.spark.graphframes.lib.ShortestPaths._

  private var lmarks: Option[Seq[Any]] = None

  /**
   * The list of landmark vertex ids. Shortest paths will be computed to each landmark.
   */
  def landmarks(value: Seq[Any]): this.type = {
    // TODO(tjh) do some initial checks here, without running queries.
    lmarks = Some(value)
    this
  }

  /**
   * The list of landmark vertex ids. Shortest paths will be computed to each landmark.
   */
  def landmarks(value: util.ArrayList[Any]): this.type = {
    landmarks(value.asScala.toSeq)
  }

  def run(): DataFrame = {
    val lmarksChecked = check(lmarks, "landmarks")
    val res = algorithm match {
      case ALGO_GRAPHX => runInGraphX(graph, lmarksChecked, isDirected)
      case ALGO_GRAPHFRAMES =>
        runInGraphFrames(
          graph,
          lmarksChecked,
          checkpointInterval,
          useLocalCheckpoints = useLocalCheckpoints,
          intermediateStorageLevel = intermediateStorageLevel,
          isDirected = isDirected)
      case _ => throw new GraphFramesUnreachableException()
    }
    resultIsPersistent()
    res
  }
}

private object ShortestPaths extends Logging {

  private def runInGraphX(
      graph: GraphFrame,
      landmarks: Seq[Any],
      isDirected: Boolean): DataFrame = {
    val longIdToLandmark = landmarks.map(l => GraphXConversions.integralId(graph, l) -> l).toMap
    val topology = if (isDirected) {
      graph.cachedTopologyGraphX
    } else {
      val undirectedEdges = graph.cachedTopologyGraphX.edges.flatMap { edge =>
        Iterator(edge, graphx.Edge(edge.dstId, edge.srcId, ()))
      }
      graphx.Graph.fromEdges(undirectedEdges, ())
    }
    val gx = graphx.lib.ShortestPaths
      .run(topology, longIdToLandmark.keys.toSeq.sorted)
    val g = GraphXConversions.fromGraphX(graph, gx, vertexNames = Seq(DISTANCE_ID))
    val distanceCol: Column = if (graph.hasIntegralIdType) {
      g.vertices(DISTANCE_ID)
    } else {
      val longIdToLandmarkFlatten: Seq[Column] = longIdToLandmark.flatMap {
        case (k: Long, v: Any) => Seq(lit(k), lit(v))
      }.toSeq
      val longIdToLandmarkColumn = map(longIdToLandmarkFlatten: _*)
      transform_keys(col(DISTANCE_ID), (longId: Column, _) => longIdToLandmarkColumn(longId))
    }
    val cols = graph.vertices.columns.map(quote).map(col) :+ distanceCol.as(DISTANCE_ID)
    val res = g.vertices.select(cols.toSeq: _*)
    res.persist(StorageLevel.MEMORY_AND_DISK_SER)
    res.count()
    gx.unpersist()
    res
  }

  private def runInGraphFrames(
      graph: GraphFrame,
      landmarks: Seq[Any],
      checkpointInterval: Int,
      isDirected: Boolean,
      useLocalCheckpoints: Boolean,
      intermediateStorageLevel: StorageLevel): DataFrame = {
    val vertexType = graph.vertices.schema(GraphFrame.ID).dataType

    // For landmark vertices the initial distance to itself is set to 0
    // Example: graph with vertices a, b, c, d; landmarks = (c, d)
    // we should init the following:
    // (a, Map()), (b, Map()), (c, Map(c -> 0)), (d, Map(d -> 0))
    //
    // Inside the following function it is done by applying multiple case-when
    // because we know exactly that only one landmark could be equal to the nodeId.
    // For example, for vertex c it will be:
    // when(id == "a", Map(a -> 0))
    //   .when(id == "b", Map(b -> 0))
    //   .when(id == "c", Map(c -> 0)) --> this one is the only true
    //   .when(id == "d", Map(d -> 0))
    def initDistancesMap(vertexId: Column): Column = {
      val firstLmarkCol = lit(landmarks.head)
      var initCol = when(vertexId === firstLmarkCol, map(firstLmarkCol, lit(0)))
      for (lmark <- landmarks.tail) {
        initCol = initCol.when(vertexId === lit(lmark), map(lit(lmark), lit(0)))
      }
      initCol
    }

    // Concatenations of two distance maps:
    // If one map is null just take another.
    // In case both maps are not null:
    // - iterate over keys
    // - if value in the left map is null or greater than value from the right map take right one
    //   else take left one
    def concatMaps(distancesLeft: Column, distancesRight: Column): Column =
      when(distancesLeft.isNull, distancesRight)
        .when(distancesRight.isNull, distancesLeft)
        .otherwise(map_zip_with(
          distancesLeft,
          distancesRight,
          (_, leftDistance, rightDistance) => {
            when(leftDistance.isNull || (leftDistance > rightDistance), rightDistance)
              .otherwise(leftDistance)
          }))

    // If distance is null, result of d + 1 will be null too
    def incrementDistances(distancesMap: Column): Column =
      transform_values(distancesMap, (_, distance) => distance + lit(1))

    // Takes an array of distance maps and reduce them with concatMaps
    def aggregateArrayOfDistanceMaps(arrayCol: Column): Column =
      reduce(arrayCol, lit(null).cast(MapType(vertexType, IntegerType)), concatMaps)

    // Checks that a sent distances map can change the destination distances.
    // Evaluation would be "true" in case in the new distances map
    // for one of keys present a non-null value but in the old distances map it is null
    // or new distance is less than old one.
    def isDistanceImprovedWithMessage(newMap: Column, oldMap: Column): Column = reduce(
      map_values(
        map_zip_with(
          newMap,
          oldMap,
          (_, newDistance, rightDistance) =>
            (newDistance.isNotNull && rightDistance.isNull) || (newDistance < rightDistance))),
      lit(false),
      (left, right) => left || right)

    val srcDistanceCol = Pregel.src(DISTANCE_ID)
    val dstDistanceCol = Pregel.dst(DISTANCE_ID)

    // Initial active-vertex col expression: only landmarks
    val initialActiveVerticesExpr = col(GraphFrame.ID).isInCollection(landmarks)

    // Mark vertex as active only in the case idstance changed
    val updateActiveVierticesExpr = isDistanceImprovedWithMessage(Pregel.msg, col(DISTANCE_ID))

    val preparedGraph = GraphFrame(
      graph.vertices.select(GraphFrame.ID),
      graph.edges.select(GraphFrame.SRC, GraphFrame.DST))

    // Overall:
    // 1. Initialize distances
    // 2. If new message can improve distances send it
    // 3. Collect and aggregate messages
    val pregel = preparedGraph.pregel
      .setIntermediateStorageLevel(intermediateStorageLevel)
      .setMaxIter(Int.MaxValue) // That is how the GraphX implementation works
      .withVertexColumn(
        DISTANCE_ID,
        when(col(GraphFrame.ID).isInCollection(landmarks), initDistancesMap(col(GraphFrame.ID)))
          .otherwise(map().cast(MapType(vertexType, IntegerType))),
        concatMaps(col(DISTANCE_ID), Pregel.msg))
      .sendMsgToSrc(when(
        isDistanceImprovedWithMessage(incrementDistances(dstDistanceCol), srcDistanceCol),
        incrementDistances(dstDistanceCol)))
      .aggMsgs(aggregateArrayOfDistanceMaps(collect_list(Pregel.msg)))
      .setEarlyStopping(true)
      .setInitialActiveVertexExpression(initialActiveVerticesExpr)
      .setUpdateActiveVertexExpression(updateActiveVierticesExpr)
      .setStopIfAllNonActiveVertices(true)
      .setSkipMessagesFromNonActiveVertices(true)
      .setCheckpointInterval(checkpointInterval)
      .setUseLocalCheckpoints(useLocalCheckpoints)
      // Memory optimization: only include required columns in triplets
      .requiredSrcColumns(DISTANCE_ID)
      .requiredDstColumns(DISTANCE_ID)

    // Experimental feature
    if (isDirected) {
      pregel.run()
    } else {
      // For consider edges as undirected,
      // it is enough to send messages in both directions
      pregel
        .sendMsgToDst(
          when(
            isDistanceImprovedWithMessage(incrementDistances(srcDistanceCol), dstDistanceCol),
            incrementDistances(srcDistanceCol)))
        .run()
    }

  }

  private val DISTANCE_ID = "distances"
}
