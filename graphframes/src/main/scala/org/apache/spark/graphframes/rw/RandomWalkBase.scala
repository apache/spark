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

package org.apache.spark.graphframes.rw

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.array_sort
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.reduce
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.xxhash64
import org.apache.spark.sql.graphframes.expressions.KMinSampling
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithIntermediateStorageLevel

import scala.util.Random

/**
 * Base trait for implementing random walk algorithms on graph data. Provides common functionality
 * for generating random walks across a graph structure.
 */
trait RandomWalkBase extends Serializable with Logging with WithIntermediateStorageLevel {

  /** Maximum number of neighbors to consider per vertex during random walks. */
  protected var maxNbrs: Int = 50

  /** GraphFrame on which random walks are performed. */
  protected var graph: GraphFrame = null

  /** Number of random walks to generate per node. */
  protected var numWalksPerNode: Int = 5

  /** Size of each batch in the random walk process. */
  protected var batchSize: Int = 10

  /** Number of batches to run in the random walk process. */
  protected var numBatches: Int = 5

  /** Whether to respect edge direction in the graph (true for directed graphs). */
  protected var useEdgeDirection: Boolean = false

  /** Global random seed for reproducibility. */
  protected var globalSeed: Long = 42L

  /** Optional prefix for temporary storage during random walks. */
  protected var temporaryPrefix: Option[String] = None

  /** Unique identifier for the current random walk run. */
  protected var runID: String = java.util.UUID.randomUUID().toString

  /** Starting batch index for continuous mode */
  protected var startingIteration: Int = 1

  /** Internal handler of vertex data type */
  private var idDataType: DataType = null

  /**
   * Sets the graph to perform random walks on.
   *
   * @param graph
   *   the GraphFrame to run random walks on
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def onGraph(graph: GraphFrame): this.type = {
    this.graph = graph
    this.idDataType = graph.vertices.schema(GraphFrame.ID).dataType
    this
  }

  /**
   * Sets the temporary prefix for storing intermediate results.
   *
   * @param value
   *   the prefix string
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setTemporaryPrefix(value: String): this.type = {
    temporaryPrefix = Some(value)
    this
  }

  /**
   * Sets the maximum number of neighbors per vertex.
   *
   * @param value
   *   the max number of neighbors
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setMaxNbrsPerVertex(value: Int): this.type = {
    maxNbrs = value
    this
  }

  /**
   * Sets the number of walks per node.
   *
   * @param value
   *   number of walks
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setNumWalksPerNode(value: Int): this.type = {
    numWalksPerNode = value
    this
  }

  /**
   * Sets the batch size.
   *
   * @param value
   *   batch size
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setBatchSize(value: Int): this.type = {
    batchSize = value
    this
  }

  /**
   * Sets the number of batches.
   *
   * @param value
   *   number of batches
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setNumBatches(value: Int): this.type = {
    numBatches = value
    this
  }

  /**
   * Sets whether to use edge direction.
   *
   * @param value
   *   true if the graph is directed
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setUseEdgeDirection(value: Boolean): this.type = {
    useEdgeDirection = value
    this
  }

  /**
   * Sets the global random seed.
   *
   * @param value
   *   the seed value
   * @return
   *   this RandomWalkBase instance for chaining
   */
  def setGlobalSeed(value: Long): this.type = {
    globalSeed = value
    this
  }

  /**
   * Sets the random walk runID. If provided, cached batches from existing random walk run will be
   * reused. User should be careful, that temporary prefix points to the right direction as well
   * the cached data starting from the set index exists.
   *
   * @param value
   * @return
   */
  def setRunId(value: String): this.type = {
    require(value != "", "empty string is not supported as run ID")
    runID = value
    this
  }

  /**
   * Get the generated (or provided) runID. This method returns current runID!
   *
   * @return
   */
  def getRunId(): String = runID

  /**
   * Sets the startng batch index for the continuous mode. See @setWalkId comment for details.
   *
   * @param value
   * @return
   */
  def setStartingFromBatch(value: Int): this.type = {
    require(value >= 1, s"batches are one-indexed but got $value")
    startingIteration = value
    this
  }

  /**
   * Generates a temporary path for a given iteration.
   *
   * @param iter
   *   iteration number
   * @return
   *   path string
   */
  private def iterationTmpPath(iter: Int): String = if (temporaryPrefix.get.endsWith("/")) {
    s"${temporaryPrefix.get}${runID}_batch_${iter}"
  } else {
    s"${temporaryPrefix.get}/${runID}_batch_${iter}"
  }

  private def sortAndConcat(arrCol: Column): Column = {
    def ordering(left: Column, right: Column): Column = {
      when(left < right, lit(-1)).when(left === right, lit(0)).otherwise(lit(1))
    }
    val sorted = array_sort(
      arrCol,
      (left, right) =>
        ordering(
          left.getField(RandomWalkBase.batchIDColName),
          right.getField(RandomWalkBase.batchIDColName)))

    reduce(
      sorted,
      array().cast(ArrayType(idDataType)),
      (left, right) => concat(left, right.getField(RandomWalkBase.rwColName)))
  }

  /**
   * Executes the random walk algorithm on the set graph.
   *
   * @return
   *   DataFrame containing the random walks
   */
  def run(): DataFrame = {
    if (graph == null) {
      throw new IllegalArgumentException("Graph is not set")
    }
    if (temporaryPrefix.isEmpty) {
      throw new IllegalArgumentException("Temporary prefix is required for random walks.")
    }

    logInfo(s"Starting random walk with runID: $runID")

    val iterationsRng = new Random(globalSeed)
    val spark = graph.vertices.sparkSession

    // If we're starting from a batch index > 1, we need to skip the seeds for previous batches
    // to ensure the same sequence of random numbers as if we started from batch 1
    if (startingIteration > 1) {
      logInfo(s"Skipping ${startingIteration - 1} seeds to maintain seed consistency")
      for (_ <- 1 until startingIteration) {
        iterationsRng.nextLong()
      }
    }

    for (i <- startingIteration to numBatches) {
      logInfo(s"Starting batch $i of $numBatches")
      val iterSeed = iterationsRng.nextLong()
      val preparedGraph = prepareGraph(iterSeed)
      val prevIterationDF = if (i == 1) { None }
      else {
        Some(spark.read.parquet(iterationTmpPath(i - 1)))
      }
      val iterationResult: DataFrame = runIter(preparedGraph, prevIterationDF, iterSeed)
        .withColumn(RandomWalkBase.batchIDColName, lit(i))
      iterationResult.write.mode("overwrite").parquet(iterationTmpPath(i))
    }

    logInfo("Finished all batches, merging results.")

    val result = (1 to numBatches)
      .map(i => spark.read.parquet(iterationTmpPath(i)))
      .reduce((a, b) => a.union(b))
      .groupBy(RandomWalkBase.walkIdCol)
      .agg(sortAndConcat(
        collect_list(struct(RandomWalkBase.batchIDColName, RandomWalkBase.rwColName)))
        .alias(RandomWalkBase.rwColName))
      .persist(intermediateStorageLevel)

    val cnt = result.count()
    resultIsPersistent()
    logInfo(s"$cnt random walks are returned")
    result
  }

  /**
   * Deletes all temporary files associated with a given instance. This method uses Hadoop
   * FileSystem to remove the directory containing batch files for the specified run ID. The
   * temporary prefix must be set and accessible via the current SparkContext's Hadoop
   * configuration.
   */
  def cleanUp(): Unit = {
    if (temporaryPrefix.isEmpty) {
      throw new IllegalArgumentException("Temporary prefix is required for clean-up.")
    }
    val spark = graph.vertices.sparkSession
    RandomWalkBase.cleanUp(temporaryPrefix.get, runID, numBatches, spark)
    logInfo(s"Clean-up completed for run ID: $runID")
  }

  /**
   * Prepares the graph for random walk by limiting neighbors and handling direction.
   *
   * @return
   *   prepared GraphFrame
   */
  protected def prepareGraph(iterationSeed: Long): GraphFrame = {
    val preAggs = (if (useEdgeDirection) {
                     graph.edges
                       .select(col(GraphFrame.SRC), col(GraphFrame.DST))

                   } else {
                     graph.edges
                       .select(GraphFrame.SRC, GraphFrame.DST)
                       .union(graph.edges.select(GraphFrame.DST, GraphFrame.SRC))
                       .distinct()
                   })
      // xxhash64(src, dst, seed) ~= fault tolerant random order
      .withColumn(
        "rand_rank",
        xxhash64(col(GraphFrame.SRC), col(GraphFrame.DST), lit(iterationSeed)))
      .groupBy(col(GraphFrame.SRC).alias(GraphFrame.ID))

    // typed sampling aggregator
    val dataType = graph.vertices.schema(GraphFrame.ID).dataType
    val encoder = KMinSampling.getEncoder(
      graph.vertices.sparkSession,
      dataType,
      Seq(GraphFrame.ID, "rand_rank"))
    val kMinSamplingUDAF =
      KMinSampling.fromSparkType(dataType, maxNbrs, encoder)

    // at most maxNbrs per vertex with a stable uniform sampling
    val vertices = preAggs.agg(
      kMinSamplingUDAF(col(GraphFrame.DST), col("rand_rank"))
        .alias(RandomWalkBase.nbrsColName))

    val edges = graph.edges
    GraphFrame(vertices, edges)
  }

  /**
   * Runs a single iteration of the random walk.
   *
   * @param graph
   *   prepared graph
   * @param prevIterationDF
   *   DataFrame from previous iteration (if any)
   * @param iterSeed
   *   seed for this iteration
   * @return
   *   DataFrame result of this iteration
   */
  protected def runIter(
      graph: GraphFrame,
      prevIterationDF: Option[DataFrame],
      iterSeed: Long): DataFrame
}

object RandomWalkBase extends Serializable {

  /** Column name for the random walk array. */
  val rwColName: String = "random_walk"

  /** Column name for the unique walk ID. */
  val walkIdCol: String = "random_walk_uuid"

  /** Column name for neighbors list. */
  val nbrsColName: String = "random_walk_nbrs"

  /** Column name for the current visiting vertex. */
  val currVisitingVertexColName: String = "random_walk_curr_vertex"

  /** Column name for batch ID inside walk. */
  val batchIDColName: String = "random_walk_batch_it"

  /**
   * Deletes all temporary files associated with a given runID. This method uses Hadoop FileSystem
   * to remove the directory containing batch files for the specified run ID. The temporary prefix
   * must be set and accessible via the current SparkContext's Hadoop configuration.
   *
   * @param temporaryPrefix
   *   the temporary prefix path
   * @param runID
   *   the run ID used to generate batch directories
   * @param numBatches
   *   the number of batch directories to look for
   * @param spark
   *   the SparkSession used to get the Hadoop configuration
   */
  def cleanUp(
      temporaryPrefix: String,
      runID: String,
      numBatches: Int,
      spark: org.apache.spark.sql.SparkSession): Unit = {
    val sc = spark.sparkContext
    val hadoopConf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val basePath = temporaryPrefix
    val runPath = if (basePath.endsWith("/")) {
      s"${basePath}${runID}_batch_"
    } else {
      s"${basePath}/${runID}_batch_"
    }
    // Delete all batch directories (1 to numBatches)
    for (i <- 1 to numBatches) {
      val path = new org.apache.hadoop.fs.Path(s"${runPath}${i}")
      if (fs.exists(path)) {
        fs.delete(path, true) // recursive delete
      }
    }
  }
}
