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

import java.io.IOException
import java.math.BigDecimal
import java.util.UUID

import org.apache.hadoop.fs.Path

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrame.{ATTR, DST, ID, LONG_DST, LONG_ID, LONG_SRC, SRC}
import org.apache.spark.graphframes.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.storage.StorageLevel

/**
 * Two-phase label propagation implementation of connected components.
 *
 * This is the primary GraphFrames-native implementation. It iteratively applies large-star and
 * small-star steps until convergence, using checkpointing to manage query plan growth.
 */
private[graphframes] object TwoPhase extends Logging {

  private val CHECKPOINT_NAME_PREFIX = "connected-components"
  private val MIN_NBR = "min_nbr"
  private val CNT = "cnt"

  /**
   * Returns the symmetric directed graph of the graph specified by input edges.
   * @param ee
   *   non-bidirectional edges
   */
  private def symmetrize(ee: DataFrame): DataFrame = {
    val EDGE = "_edge"
    ee.select(explode(
      array(struct(col(SRC), col(DST)), struct(col(DST).as(SRC), col(SRC).as(DST)))).as(EDGE))
      .select(col(s"$EDGE.$SRC").as(SRC), col(s"$EDGE.$DST").as(DST))
  }

  /**
   * Prepares the input graph for computing connected components by:
   *   - de-duplicating vertices and assigning unique long IDs to each,
   *   - changing edge directions to have increasing long IDs from src to dst,
   *   - de-duplicating edges and removing self-loops.
   *
   * In the returned GraphFrame, the vertex DataFrame has two columns:
   *   - column `id` stores a long ID assigned to the vertex,
   *   - column `attr` stores the original vertex attributes.
   *
   * The edge DataFrame has two columns:
   *   - column `src` stores the long ID of the source vertex,
   *   - column `dst` stores the long ID of the destination vertex, where we always have `src` <
   *     `dst`.
   */
  private def prepare(graph: GraphFrame): GraphFrame = {
    val vertices = graph.indexedVertices
      .select(col(LONG_ID).as(ID), col(ATTR))
    val edges = graph.indexedEdges
      .select(col(LONG_SRC).as(SRC), col(LONG_DST).as(DST))
    val orderedEdges = edges
      .filter(col(SRC) =!= col(DST))
      .select(minValue(col(SRC), col(DST)).as(SRC), maxValue(col(SRC), col(DST)).as(DST))
      .distinct()
    GraphFrame(vertices, orderedEdges)
  }

  /**
   * Returns the min vertex among each vertex and its neighbors in a DataFrame with three columns:
   *   - `src`, the ID of the vertex
   *   - `min_nbr`, the min vertex ID among itself and its neighbors
   *   - `cnt`, the total number of neighbors
   */
  private def minNbrs(ee: DataFrame): DataFrame = {
    symmetrize(ee)
      .groupBy(SRC)
      .agg(min(col(DST)).as(MIN_NBR), count("*").as(CNT))
      .withColumn(MIN_NBR, minValue(col(SRC), col(MIN_NBR)))
  }

  private def minValue(x: Column, y: Column): Column = {
    when(x < y, x).otherwise(y)
  }

  private def maxValue(x: Column, y: Column): Column = {
    when(x > y, x).otherwise(y)
  }

  /**
   * Computes the sum of all `min_nbr` values and the undirected edge count of the given DataFrame
   * in a single Spark job. The sum is cast to DecimalType(38, 0) for high precision. Used to
   * detect convergence between iterations and to check graph sparsity for the pruning
   * optimization. The edge count is derived as `sum(cnt) / 2` since each undirected edge appears
   * once in each direction in the symmetrized graph.
   */
  private def calcMinNbrSum(minNbrsDF: DataFrame): (BigDecimal, Long) = {
    val row = minNbrsDF
      .select(
        sum(col(MIN_NBR).cast(DecimalType(38, 0))),
        coalesce((sum(col(CNT)) / 2).cast("long"), lit(0L)))
      .first()
    (row.getAs[BigDecimal](0), row.getLong(1))
  }

  /**
   * Builds the output DataFrame by joining the indexed vertices with the final edge assignments
   * and resolving component labels.
   */
  private def buildOutput(
      graph: GraphFrame,
      vv: DataFrame,
      ee: DataFrame,
      useLabelsAsComponents: Boolean): DataFrame = {
    val indexedLabel = vv
      .join(ee, vv(ID) === ee(DST), "left_outer")
      .select(
        vv(ATTR),
        when(ee(SRC).isNull, vv(ID)).otherwise(ee(SRC)).as(ConnectedComponents.COMPONENT),
        col(ATTR + "." + ID).as(ID))

    if (graph.hasIntegralIdType || !useLabelsAsComponents) {
      indexedLabel
        .select(col(s"$ATTR.*"), col(ConnectedComponents.COMPONENT))
    } else {
      indexedLabel
        .join(
          indexedLabel
            .groupBy(col(ConnectedComponents.COMPONENT))
            .agg(min(col(ID)).as(ConnectedComponents.ORIG_ID))
            .select(col(ConnectedComponents.COMPONENT), col(ConnectedComponents.ORIG_ID)),
          ConnectedComponents.COMPONENT)
        .select(
          col(s"$ATTR.*"),
          col(ConnectedComponents.ORIG_ID).as(ConnectedComponents.COMPONENT))
    }
  }

  /**
   * Performs a possibly skewed join between edges and current component assignments. The skew
   * join is done by broadcast join for frequent keys and normal join for the rest.
   */
  private def skewedJoin(
      edges: DataFrame,
      minNbrsDF: DataFrame,
      broadcastThreshold: Int,
      logPrefix: String): DataFrame = {
    import edges.sparkSession.implicits._
    val hubs = minNbrsDF
      .filter(col(CNT) > broadcastThreshold)
      .select(SRC)
      .as[Long]
      .collect()
      .toSet
    GraphFrame.skewedJoin(edges, minNbrsDF, SRC, hubs, logPrefix)
  }

  /**
   * Prunes leaf nodes (vertices with out-degree 0 and in-degree 1) from the graph to create a
   * smaller shrunken graph. Returns Some((vertices, edges, nodeCount)) if the shrunken graph is
   * significantly smaller than the original (by shrinkageThreshold), None otherwise.
   */
  private[graphframes] def pruneLeafNodes(
      edges: DataFrame,
      intermediateStorageLevel: StorageLevel,
      numNodes: Long,
      shrinkageThreshold: Double): Option[(DataFrame, DataFrame, Long)] = {

    // vertices whose indegree > 1
    val v1 = edges
      .groupBy(DST)
      .agg(count("*").as(CNT))
      .filter(col(CNT) > 1)
      .select(col(DST).as(ID))

    // vertices whose outdegree > 0 or indegree > 1
    val newVV = edges
      .select(col(SRC).as(ID))
      .union(v1)
      .distinct()
      .persist(intermediateStorageLevel)
    val newVVCnt = newVV.count()

    if (newVVCnt * shrinkageThreshold < numNodes) {
      val newEE = edges
        .join(newVV.withColumnRenamed(ID, DST), DST)
        .persist(intermediateStorageLevel)
      Some((newVV, newEE, newVVCnt))
    } else {
      newVV.unpersist(blocking = false)
      None
    }
  }

  /**
   * Given the vertices and converged edges of the shrunken graph, joins back to reconstruct the
   * converged edges of the original graph.
   */
  private[graphframes] def joinBack(
      vertices: DataFrame,
      edges: DataFrame,
      edgesBeforePruning: DataFrame): DataFrame = {

    val cc = vertices
      .as("vertices")
      .join(edges.as("edges"), col(s"vertices.$ID") === col(s"edges.$DST"), "left_outer")
      .select(
        when(col(s"edges.$SRC").isNull, col(s"vertices.$ID"))
          .otherwise(col(s"edges.$SRC"))
          .as(SRC),
        col(s"vertices.$ID").as(DST))

    cc.as("cc")
      .join(
        edgesBeforePruning.as("edgesBeforePruning"),
        col(s"cc.$DST") === col(s"edgesBeforePruning.$SRC"))
      .select(col(s"cc.$SRC"), col(s"edgesBeforePruning.$DST"))
      .union(cc)
      .distinct()
  }

  /**
   * Runs the two-phase label propagation connected components algorithm.
   */
  private[graphframes] def run(
      graph: GraphFrame,
      broadcastThreshold: Int,
      checkpointInterval: Int,
      intermediateStorageLevel: StorageLevel,
      useLabelsAsComponents: Boolean,
      useLocalCheckpoints: Boolean,
      isGraphPrepared: Boolean,
      optStartIter: Int = 2,
      sparsityThreshold: Double = 2.0,
      shrinkageThreshold: Double = 2.0): DataFrame = {

    val spark = graph.spark
    val sc = spark.sparkContext
    val originalAQE = spark.conf.get("spark.sql.adaptive.enabled")

    try {
      spark.conf.set("spark.sql.adaptive.enabled", "false")

      val runId = UUID.randomUUID().toString.takeRight(8)
      val logPrefix = s"[CC $runId]"
      logInfo(s"$logPrefix Start connected components with run ID $runId.")

      val shouldCheckpoint = checkpointInterval > 0
      val checkpointDir: Option[String] = if (useLocalCheckpoints) { None }
      else if (shouldCheckpoint) {
        val dir = sc.getCheckpointDir
          .map { d =>
            new Path(d, s"$CHECKPOINT_NAME_PREFIX-$runId").toString
          }
          .getOrElse {
            spark.conf.getOption("spark.checkpoint.dir") match {
              case Some(d) => new Path(d, s"$CHECKPOINT_NAME_PREFIX-$runId").toString
              case None =>
                throw new IOException(
                  "Checkpoint directory is not set. Please set it first using " +
                    "sc.setCheckpointDir() or by specifying the conf 'spark.checkpoint.dir'.")
            }
          }
        logInfo(s"$logPrefix Using $dir for checkpointing with interval $checkpointInterval.")
        Some(dir)
      } else {
        logInfo(
          s"$logPrefix Checkpointing is disabled because checkpointInterval=$checkpointInterval.")
        None
      }

      logInfo(s"$logPrefix Preparing the graph for connected component computation ...")
      val g = if (isGraphPrepared) graph else prepare(graph)
      val vv = g.vertices
      var ee = g.edges.persist(intermediateStorageLevel) // src < dst
      logInfo(s"$logPrefix Found ${ee.count()} edges after preparation.")
      var numNodes = vv.count()
      logInfo(s"$logPrefix Found $numNodes nodes after preparation.")

      var converged = false
      var iteration = 1
      var isOptimized = false
      var triedToOptimize = false
      var shouldKeepCheckpoint = false
      var edgesBeforePruning: DataFrame = null
      var shrunkenGraphNodes: DataFrame = null

      var minNbrs1: DataFrame = minNbrs(ee) // src >= min_nbr
        .persist(intermediateStorageLevel)

      var (prevSum, _) = calcMinNbrSum(minNbrs1)

      var lastRoundPersistedDFs = Seq[DataFrame](ee, minNbrs1)
      while (!converged) {
        var currRoundPersistedDFs = Seq[DataFrame]()

        // large-star step
        // connect all strictly larger neighbors to the min neighbor (including self)
        ee = skewedJoin(ee, minNbrs1, broadcastThreshold, logPrefix)
          .select(col(DST).as(SRC), col(MIN_NBR).as(DST)) // src > dst
          .distinct()
          .persist(intermediateStorageLevel)
        currRoundPersistedDFs = currRoundPersistedDFs :+ ee

        // small-star step
        // compute min neighbors (excluding self-min)
        val minNbrs2 = ee
          .groupBy(col(SRC))
          .agg(min(col(DST)).as(MIN_NBR), count("*").as(CNT)) // src > min_nbr
          .persist(intermediateStorageLevel)
        currRoundPersistedDFs = currRoundPersistedDFs :+ minNbrs2

        // connect all smaller neighbors to the min neighbor
        ee = skewedJoin(ee, minNbrs2, broadcastThreshold, logPrefix)
          .select(col(MIN_NBR).as(SRC), col(DST)) // src <= dst
          .filter(col(SRC) =!= col(DST)) // src < dst
        // connect self to the min neighbor
        ee = ee
          .union(minNbrs2.select(col(MIN_NBR).as(SRC), col(SRC).as(DST))) // src < dst
          .distinct()

        // checkpointing
        if (shouldCheckpoint && (iteration % checkpointInterval == 0)) {
          if (useLocalCheckpoints) {
            ee = ee.localCheckpoint(eager = true)
          } else {
            val out = s"${checkpointDir.get}/$iteration"
            ee.write.parquet(out)
            ee = spark.read.parquet(out)

            if (iteration > checkpointInterval) {
              val path = new Path(s"${checkpointDir.get}/${iteration - checkpointInterval}")
              // keep the checkpoint when edgesBeforePruning points to it
              if (!shouldKeepCheckpoint) {
                path.getFileSystem(sc.hadoopConfiguration).delete(path, true)
              } else {
                shouldKeepCheckpoint = false
              }
            }

            System.gc()
          }
        }

        ee.persist(intermediateStorageLevel)
        currRoundPersistedDFs = currRoundPersistedDFs :+ ee

        minNbrs1 = minNbrs(ee) // src >= min_nbr
          .persist(intermediateStorageLevel)
        currRoundPersistedDFs = currRoundPersistedDFs :+ minNbrs1

        // test convergence
        val (currSum, edgeCnt) = calcMinNbrSum(minNbrs1)
        logInfo(s"$logPrefix Sum of assigned components in iteration $iteration: $currSum.")

        // Pruning Node Optimization: construct a new small graph with fewer nodes,
        // and find connected components of the shrunken graph, then join back to get the
        // connected components of the original graph.

        // If the graph becomes sparse and current iteration >= $optStartIter, we start to
        // try such optimization. However, the optimization is only performed if the shrunken
        // graph is much smaller than the original graph, otherwise we do not perform it (
        // in this case, the only additional cost is to determine the size of shrunken graph).
        // In current implementation, we only try such optimization one time and it is
        // performed at most one time. So the additional cost is bounded.

        // According to such heuristic rule, we can determine when and whether we should
        // perform the optimization. For the sparse graphs (defined by sparsityThreshold),
        // we will try such optimization at the end of $optStartIter iteration (default is 2).
        // For the dense graph, its edges will be pruned at each large/small star join iteration,
        // and we will try the optimization once the graph becomes sparse.
        if ((edgeCnt < sparsityThreshold * numNodes) && (edgeCnt > 0)
          && (iteration >= optStartIter) && (!triedToOptimize)) {
          edgesBeforePruning = ee
          pruneLeafNodes(ee, intermediateStorageLevel, numNodes, shrinkageThreshold) match {
            case Some(r) =>
              shrunkenGraphNodes = r._1
              ee = r._2
              currRoundPersistedDFs = currRoundPersistedDFs :+ ee
              numNodes = r._3
              isOptimized = true
              shouldKeepCheckpoint = true
              logInfo(s"$logPrefix Pruning node optimization performed in iteration $iteration.")
              logInfo(s"$logPrefix Shrunken graph node count: $numNodes.")
            case None =>
              logInfo(s"$logPrefix Pruning node optimization not performed.")
          }
          triedToOptimize = true
        }

        if (currSum == prevSum) {
          converged = true
        } else {
          prevSum = currSum
        }

        for (persistedDF <- lastRoundPersistedDFs) {
          persistedDF.unpersist()
        }
        lastRoundPersistedDFs = currRoundPersistedDFs
        iteration += 1
      }

      if (isOptimized) {
        ee = joinBack(shrunkenGraphNodes, ee, edgesBeforePruning)
      }

      logInfo(s"$logPrefix Connected components converged in ${iteration - 1} iterations.")
      logInfo(s"$logPrefix Join and return component assignments with original vertex IDs.")

      val output = buildOutput(graph, vv, ee, useLabelsAsComponents)
        .persist(intermediateStorageLevel)

      output.count()

      for (persistedDF <- lastRoundPersistedDFs) {
        persistedDF.unpersist()
      }
      if (shrunkenGraphNodes != null) {
        shrunkenGraphNodes.unpersist()
      }

      resultIsPersistent()

      output
    } finally {
      spark.conf.set("spark.sql.adaptive.enabled", originalAQE)
    }
  }

  /**
   * Runs the two-phase label propagation connected components algorithm using Adaptive Query
   * Execution (AQE). Unlike `run`, this method does not manipulate AQE settings, does not use
   * skewed joins, and uses simpler checkpointing.
   */
  private[graphframes] def runAQE(
      graph: GraphFrame,
      checkpointInterval: Int,
      intermediateStorageLevel: StorageLevel,
      useLabelsAsComponents: Boolean,
      useLocalCheckpoints: Boolean,
      isGraphPrepared: Boolean,
      optStartIter: Int = 2,
      sparsityThreshold: Double = 2.0,
      shrinkageThreshold: Double = 2.0): DataFrame = {

    val runId = UUID.randomUUID().toString.takeRight(8)
    val logPrefix = s"[CC $runId]"
    logInfo(s"$logPrefix Start connected components with run ID $runId.")

    val shouldCheckpoint = checkpointInterval > 0

    logInfo(s"$logPrefix Preparing the graph for connected component computation ...")
    val g = if (isGraphPrepared) graph else prepare(graph)
    val vv = g.vertices
    var ee = g.edges.persist(intermediateStorageLevel) // src < dst
    logInfo(s"$logPrefix Found ${ee.count()} edges after preparation.")
    var numNodes = vv.count()
    logInfo(s"$logPrefix Found $numNodes nodes after preparation.")

    var converged = false
    var iteration = 1
    var isOptimized = false
    var triedToOptimize = false
    var edgesBeforePruning: DataFrame = null
    var shrunkenGraphNodes: DataFrame = null

    var minNbrs1: DataFrame = symmetrize(ee)
      .groupBy(SRC)
      .agg(min(col(DST)).as(MIN_NBR), count("*").as(CNT))
      .withColumn(MIN_NBR, minValue(col(SRC), col(MIN_NBR)))
      .persist(intermediateStorageLevel)

    var (prevSum, _) = calcMinNbrSum(minNbrs1)

    var lastRoundPersistedDFs = Seq[DataFrame](ee, minNbrs1)
    while (!converged) {
      var currRoundPersistedDFs = Seq[DataFrame]()

      // large-star step
      // connect all strictly larger neighbors to the min neighbor (including self)
      ee = ee
        .join(minNbrs1, SRC)
        .select(col(DST).as(SRC), col(MIN_NBR).as(DST)) // src > dst
        .distinct()
        .persist(intermediateStorageLevel)
      currRoundPersistedDFs = currRoundPersistedDFs :+ ee

      // small-star step
      // compute min neighbors (excluding self-min)
      val minNbrs2 = ee
        .groupBy(col(SRC))
        .agg(min(col(DST)).as(MIN_NBR)) // src > min_nbr
        .persist(intermediateStorageLevel)
      currRoundPersistedDFs = currRoundPersistedDFs :+ minNbrs2

      // connect all smaller neighbors to the min neighbor
      ee = ee
        .join(minNbrs2, SRC)
        .select(col(MIN_NBR).as(SRC), col(DST)) // src <= dst
        .filter(col(SRC) =!= col(DST)) // src < dst
      // connect self to the min neighbor
      ee = ee
        .union(minNbrs2.select(col(MIN_NBR).as(SRC), col(SRC).as(DST))) // src < dst
        .distinct()

      // checkpointing
      if (shouldCheckpoint && (iteration % checkpointInterval == 0)) {
        if (useLocalCheckpoints) {
          ee = ee.localCheckpoint(eager = true)
        } else {
          ee = ee.checkpoint(eager = true)
        }
      }

      ee.persist(intermediateStorageLevel)
      currRoundPersistedDFs = currRoundPersistedDFs :+ ee

      minNbrs1 = symmetrize(ee)
        .groupBy(SRC)
        .agg(min(col(DST)).as(MIN_NBR), count("*").as(CNT))
        .withColumn(MIN_NBR, minValue(col(SRC), col(MIN_NBR)))
        .persist(intermediateStorageLevel)
      currRoundPersistedDFs = currRoundPersistedDFs :+ minNbrs1

      // test convergence
      val (currSum, edgeCnt) = calcMinNbrSum(minNbrs1)
      logInfo(s"$logPrefix Sum of assigned components in iteration $iteration: $currSum.")

      // Pruning Node Optimization: construct a new small graph with fewer nodes,
      // and find connected components of the shrunken graph, then join back to get the
      // connected components of the original graph.

      // If the graph becomes sparse and current iteration >= $optStartIter, we start to
      // try such optimization. However, the optimization is only performed if the shrunken
      // graph is much smaller than the original graph, otherwise we do not perform it (
      // in this case, the only additional cost is to determine the size of shrunken graph).
      // In current implementation, we only try such optimization one time and it is
      // performed at most one time. So the additional cost is bounded.

      // According to such heuristic rule, we can determine when and whether we should
      // perform the optimization. For the sparse graphs (defined by sparsityThreshold),
      // we will try such optimization at the end of $optStartIter iteration (default is 2).
      // For the dense graph, its edges will be pruned at each large/small star join iteration,
      // and we will try the optimization once the graph becomes sparse.
      if ((edgeCnt < sparsityThreshold * numNodes) && (edgeCnt > 0)
        && (iteration >= optStartIter) && (!triedToOptimize)) {
        edgesBeforePruning = ee
        pruneLeafNodes(ee, intermediateStorageLevel, numNodes, shrinkageThreshold) match {
          case Some(r) =>
            shrunkenGraphNodes = r._1
            ee = r._2
            currRoundPersistedDFs = currRoundPersistedDFs :+ ee
            numNodes = r._3
            isOptimized = true
            logInfo(s"$logPrefix Pruning node optimization performed in iteration $iteration.")
            logInfo(s"$logPrefix Shrunken graph node count: $numNodes.")
          case None =>
            logInfo(s"$logPrefix Pruning node optimization not performed.")
        }
        triedToOptimize = true
      }

      if (currSum == prevSum) {
        converged = true
      } else {
        prevSum = currSum
      }

      for (persistedDF <- lastRoundPersistedDFs) {
        persistedDF.unpersist()
      }
      lastRoundPersistedDFs = currRoundPersistedDFs
      iteration += 1
    }

    if (isOptimized) {
      ee = joinBack(shrunkenGraphNodes, ee, edgesBeforePruning)
    }

    logInfo(s"$logPrefix Connected components converged in ${iteration - 1} iterations.")
    logInfo(s"$logPrefix Join and return component assignments with original vertex IDs.")

    val output = buildOutput(graph, vv, ee, useLabelsAsComponents)
      .persist(intermediateStorageLevel)

    output.count()

    for (persistedDF <- lastRoundPersistedDFs) {
      persistedDF.unpersist()
    }
    if (shrunkenGraphNodes != null) {
      shrunkenGraphNodes.unpersist()
    }

    resultIsPersistent()

    output
  }
}
