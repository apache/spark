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

import org.apache.spark.storage.StorageLevel

private[graphframes] trait WithAlgorithmChoice {
  protected val ALGO_GRAPHX = "graphx"
  protected val ALGO_GRAPHFRAMES = "graphframes"
  protected var algorithm: String = ALGO_GRAPHX
  val supportedAlgorithms: Array[String] = Array(ALGO_GRAPHX, ALGO_GRAPHFRAMES)

  /**
   * Set an algorithm to use. Supported algorithms are "graphx" and "graphframes".
   *
   * @param value
   * @return
   */
  def setAlgorithm(value: String): this.type = {
    require(
      supportedAlgorithms.contains(value),
      s"Supported algorithms are {${supportedAlgorithms.mkString(", ")}}, but got $value.")
    algorithm = value
    this
  }

  def getAlgorithm: String = algorithm
}

private[graphframes] trait WithCheckpointInterval extends Logging {
  protected var checkpointInterval: Int = 2

  /**
   * Sets checkpoint interval in terms of number of iterations (default: 2). Checkpointing
   * regularly helps recover from failures, clean shuffle files, shorten the lineage of the
   * computation graph, and reduce the complexity of plan optimization. As of Spark 2.0, the
   * complexity of plan optimization would grow exponentially without checkpointing. Hence,
   * disabling or setting longer-than-default checkpoint intervals are not recommended. Checkpoint
   * data is saved under `org.apache.spark.SparkContext.getCheckpointDir` with prefix of the
   * algorithm name. If the checkpoint directory is not set, this throws a `java.io.IOException`.
   * Set a nonpositive value to disable checkpointing. This parameter is only used when the
   * algorithm is set to "graphframes". Its default value might change in the future.
   * @see
   *   `org.apache.spark.SparkContext.setCheckpointDir` in Spark API doc
   */
  def setCheckpointInterval(value: Int): this.type = {
    if (value <= 0 || value > 2) {
      logWarn(
        s"Set checkpointInterval to $value. This would blow up the query plan and hang the " +
          "driver for large graphs.")
    }
    checkpointInterval = value
    this
  }

  // python-friendly setter
  private[graphframes] def setCheckpointInterval(value: java.lang.Integer): this.type = {
    setCheckpointInterval(value.toInt)
  }

  /**
   * Gets checkpoint interval.
   */
  def getCheckpointInterval: Int = checkpointInterval
}

private[graphframes] trait WithBroadcastThreshold extends Logging {
  protected var broadcastThreshold: Int = 1000000

  /**
   * Sets a broadcast threshold in propagating component assignments (default: 1,000,000). If a
   * node degree is greater than this threshold at some iteration, its component assignment will
   * be collected and then broadcasted back to propagate the assignment to its neighbors.
   * Otherwise, the assignment propagation is done by a normal Spark join. This parameter is only
   * used when the algorithm is set to "graphframes". If the value is -1, then the skewness
   * problem is left to the Apache Spark AQE optimizer.
   *
   * **WARNING** using a broadcast threshold is non-free! Under the hood it is calling an action,
   * and if a broadcast threshold is set, then AQE is disabled to avoid wrong results! If your
   * graph does not contain gigantic components, it is strongly recommended to set this value to
   * -1. On benchmarks setting it to -1 gains about x5 better results in performance.
   *
   * **WARNING** the current default value is 1,000,000. It is left for backward compatibility
   * only. In the future versions it may be set to -1 as more reasonable for the most real-world
   * cases (e.g., the data deduplication problem).
   */
  def setBroadcastThreshold(value: Int): this.type = {
    broadcastThreshold = value
    this
  }

  // python-friendly setter
  private[graphframes] def setBroadcastThreshold(value: java.lang.Integer): this.type = {
    setBroadcastThreshold(value.toInt)
  }

  /**
   * Gets broadcast threshold in propagating component assignment.
   * @see
   *   [[org.apache.spark.graphframes.lib.ConnectedComponents.setBroadcastThreshold]]
   */
  def getBroadcastThreshold: Int = broadcastThreshold
}

private[graphframes] trait WithIntermediateStorageLevel extends Logging {

  protected var intermediateStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK

  /**
   * Sets storage level for intermediate datasets that require multiple passes (default:
   * ``MEMORY_AND_DISK``).
   */
  def setIntermediateStorageLevel(value: StorageLevel): this.type = {
    intermediateStorageLevel = value
    this
  }

  /**
   * Gets storage level for intermediate datasets that require multiple passes.
   */
  def getIntermediateStorageLevel: StorageLevel = intermediateStorageLevel

}

private[graphframes] trait WithLgNomEntries {
  protected var lgNomEntries: Int = 12

  /**
   * Sets the log2 of nominal entries used by Theta sketch aggregations.
   */
  def setLgNomEntries(value: Int): this.type = {
    require((value >= 4) && (value <= 26), "lgNomEntries must be between 4 and 26")
    lgNomEntries = value
    this
  }

  /**
   * Gets log2 of nominal entries used by Theta sketch aggregations.
   */
  def getLgNomEntries: Int = lgNomEntries
}

private[graphframes] trait WithMaxIter {
  protected var maxIter: Option[Int] = None

  /**
   * The max number of iterations of algorithm to be performed.
   */
  def maxIter(value: Int): this.type = {
    maxIter = Some(value)
    this
  }
}

private[graphframes] trait WithUseLabelsAsComponents {
  protected var useLabelsAsComponents: Boolean = false

  /**
   * Sets whether to use vertex labels as component identifiers (default: false). When true,
   * vertex labels will be used as component identifiers instead of computing connected
   * components.
   */
  def setUseLabelsAsComponents(value: Boolean): this.type = {
    useLabelsAsComponents = value
    this
  }

  /**
   * Gets whether to use vertex labels as component identifiers.
   */
  def getUseLabelsAsComponents: Boolean = useLabelsAsComponents
}

/**
 * Provides support for local checkpoints in Spark computations.
 *
 * Local checkpoints offer a faster alternative to regular checkpoints as they don't require
 * configuration of checkpointDir in persistent storage (like HDFS or S3). While being more
 * performant, local checkpoints are less reliable since they don't survive node failures and the
 * data is not persisted across multiple nodes.
 */
private[graphframes] trait WithLocalCheckpoints {
  protected var useLocalCheckpoints: Boolean = false

  /**
   * Sets whether to use local checkpoints instead of regular checkpoints (default: false). Local
   * checkpoints are faster but less reliable as they don't survive node failures.
   *
   * @param value
   *   true to use local checkpoints, false for regular checkpoints
   * @return
   *   this instance
   */
  def setUseLocalCheckpoints(value: Boolean): this.type = {
    useLocalCheckpoints = value
    this
  }

  /**
   * Gets whether local checkpoints are being used instead of regular checkpoints.
   *
   * @return
   *   true if local checkpoints are enabled, false otherwise
   */
  def getUseLocalCheckpoints: Boolean = useLocalCheckpoints
}

/**
 * Provides support of graph directions for algorithms.
 */
private[graphframes] trait WithDirection {
  protected var isDirected: Boolean = true

  /**
   * Sets should graph be cosidered as directed.
   *
   * @param value
   *   true to handle graph as directed
   * @return
   */
  def setIsDirected(value: Boolean): this.type = {
    isDirected = value
    this
  }

  /**
   * Gets should graph be considered as directed.
   *
   * @return
   *   true if directed
   */
  def getIsDirected: Boolean = isDirected
}
