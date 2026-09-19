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

import java.util.Locale

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFramesUnreachableException
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithBroadcastThreshold
import org.apache.spark.graphframes.WithCheckpointInterval
import org.apache.spark.graphframes.WithIntermediateStorageLevel
import org.apache.spark.graphframes.WithLocalCheckpoints
import org.apache.spark.graphframes.WithMaxIter
import org.apache.spark.graphframes.WithUseLabelsAsComponents
import org.apache.spark.graphx
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.graphframes.GraphFramesConf
import org.apache.spark.storage.StorageLevel

/**
 * Connected Components algorithm.
 *
 * Computes the connected component membership of each vertex and returns a DataFrame of vertex
 * information with each vertex assigned a component ID.
 *
 * The resulting DataFrame contains all the vertex information and one additional column:
 *   - component (`LongType`): unique ID for this component
 */
class ConnectedComponents private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with Logging
    with WithCheckpointInterval
    with WithBroadcastThreshold
    with WithIntermediateStorageLevel
    with WithUseLabelsAsComponents
    with WithMaxIter
    with WithLocalCheckpoints {

  import ConnectedComponents._

  private var algorithm: String = GraphFramesConf.getConnectedComponentsAlgorithm
    .getOrElse(ALGO_TWO_PHASE)

  private var isGraphPrepared: Boolean = false

  setCheckpointInterval(
    GraphFramesConf.getConnectedComponentsCheckpointInterval.getOrElse(checkpointInterval))
  setBroadcastThreshold(
    GraphFramesConf.getConnectedComponentsBroadcastThreshold.getOrElse(broadcastThreshold))
  setIntermediateStorageLevel(
    GraphFramesConf.getConnectedComponentsStorageLevel.getOrElse(intermediateStorageLevel))
  setUseLabelsAsComponents(
    GraphFramesConf.getUseLabelsAsComponents.getOrElse(useLabelsAsComponents))
  setUseLocalCheckpoints(GraphFramesConf.getUseLocalCheckpoints.getOrElse(useLocalCheckpoints))

  /**
   * Sets the algorithm to use for computing connected components. Supported values:
   *   - `graphx`: use the GraphX implementation
   *   - `graphframes`: deprecated alias for `two_phase`
   *   - `two_phase`: use the two-phase label propagation
   *     implementation
   *   - `randomized_contraction`: use the randomized contraction
   *     implementation
   */
  def setAlgorithm(value: String): this.type = {
    val normalized = value.toLowerCase(Locale.ROOT)
    normalized match {
      case ALGO_GRAPHX | ALGO_TWO_PHASE | ALGO_RANDOMIZED_CONTRACTION =>
        algorithm = normalized
      case ALGO_GRAPHFRAMES =>
        logWarn(
          s"Algorithm '$ALGO_GRAPHFRAMES' is deprecated and will be removed in a future release. " +
            s"Using '$ALGO_TWO_PHASE' instead.")
        algorithm = ALGO_TWO_PHASE
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported algorithm: '$value'. " +
            s"Supported values are: $ALGO_GRAPHX, $ALGO_TWO_PHASE, " +
            s"$ALGO_RANDOMIZED_CONTRACTION, $ALGO_GRAPHFRAMES (deprecated).")
    }
    this
  }

  /**
   * Gets the algorithm used for computing connected components.
   */
  def getAlgorithm: String = algorithm

  /**
   * !! WARNING: INTERNAL API - FOR VERY EXPERIENCED USERS ONLY !!
   *
   * Sets whether the graph has already been prepared before being passed to the algorithm,
   * skipping the internal graph preparation step. The default is `false`, meaning the algorithm
   * will always prepare the graph itself, which is the safe and recommended behaviour.
   *
   * Only set this to `true` if you have '''already performed all required preparation steps
   * yourself''' and you fully understand what those steps are for the specific algorithm you are
   * using. '''The preparation requirements differ significantly between algorithms:'''
   *
   *   - `two_phase` and `randomized_contraction` each require their own distinct preparation
   *     steps. These are NOT interchangeable. You MUST study the internal source code of the
   *     algorithm you intend to use and replicate its exact preparation logic before enabling
   *     this flag.
   *
   * '''Incorrect use of this flag WILL produce silently wrong results with no error or warning at
   * runtime.''' There is no validation that the graph has been correctly prepared. You are
   * entirely responsible for ensuring correctness.
   *
   * @param value
   *   true if the graph is already prepared, false otherwise (default: false)
   */
  def setIsGraphPrepared(value: Boolean): this.type = {
    logWarn(
      "INTERNAL API ONLY WAS CALLED. This is an internal option for advanced users who fully " +
        "understand graph preparation internals. Misuse will produce silently wrong results.")
    isGraphPrepared = value
    this
  }

  /**
   * Runs the algorithm.
   */
  def run(): DataFrame = {
    algorithm match {
      case ALGO_GRAPHX =>
        ConnectedComponents.runGraphX(
          graph,
          maxIter.getOrElse(Int.MaxValue),
          intermediateStorageLevel)
      case ALGO_TWO_PHASE =>
        if (broadcastThreshold == -1) {
          TwoPhase.runAQE(
            graph,
            checkpointInterval = checkpointInterval,
            intermediateStorageLevel = intermediateStorageLevel,
            useLabelsAsComponents = useLabelsAsComponents,
            useLocalCheckpoints = useLocalCheckpoints,
            isGraphPrepared = isGraphPrepared)
        } else {
          TwoPhase.run(
            graph,
            broadcastThreshold = broadcastThreshold,
            checkpointInterval = checkpointInterval,
            intermediateStorageLevel = intermediateStorageLevel,
            useLabelsAsComponents = useLabelsAsComponents,
            useLocalCheckpoints = useLocalCheckpoints,
            isGraphPrepared = isGraphPrepared)
        }
      case ALGO_RANDOMIZED_CONTRACTION =>
        RandomizedContraction.run(
          graph,
          useLabelsAsComponents = useLabelsAsComponents,
          intermediateStorageLevel = intermediateStorageLevel,
          useLocalCheckpoints = useLocalCheckpoints,
          checkpointInterval = checkpointInterval,
          isGraphPrepared = isGraphPrepared)
      // the check is inside the setter
      case _ => throw new GraphFramesUnreachableException()
    }
  }

  @deprecated("use graph.connectedComponents instead", "0.11.0")
  def run(graph: GraphFrame): DataFrame = {
    new ConnectedComponents(graph).run()
  }
}

object ConnectedComponents extends Logging {

  private[graphframes] val COMPONENT = "component"
  private[graphframes] val ORIG_ID = "orig_id"

  val ALGO_GRAPHX = "graphx"

  /**
   * @deprecated
   *   Use [[ALGO_TWO_PHASE]] instead.
   */
  val ALGO_GRAPHFRAMES = "graphframes"

  val ALGO_TWO_PHASE = "two_phase"
  val ALGO_RANDOMIZED_CONTRACTION = "randomized_contraction"

  /**
   * Runs the GraphX connected components implementation.
   */
  private[graphframes] def runGraphX(
      graph: GraphFrame,
      maxIter: Int,
      intermediateStorageLevel: StorageLevel): DataFrame = {
    val gx = graph.cachedTopologyGraphX
    val components =
      graphx.lib.ConnectedComponents.run(gx, maxIter)
    val result = GraphXConversions
      .fromGraphX(graph, components, vertexNames = Seq(ConnectedComponents.COMPONENT))
      .vertices
      .persist(intermediateStorageLevel)

    val _ = result.count()
    gx.unpersist()
    components.unpersist()

    resultIsPersistent()

    result
  }
}
