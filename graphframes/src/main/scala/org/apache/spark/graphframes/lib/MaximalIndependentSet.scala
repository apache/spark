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

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithCheckpointInterval
import org.apache.spark.graphframes.WithIntermediateStorageLevel
import org.apache.spark.graphframes.WithLocalCheckpoints
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.storage.StorageLevel

/**
 * This class implements a distributed algorithm for finding a Maximal Independent Set (MIS) in a
 * graph.
 *
 * An MIS is a set of vertices such that no two vertices in the set are adjacent (i.e., there is
 * no edge between any two vertices in the set), and the set is maximal, meaning that adding any
 * other vertex to the set would violate the independence property. Note that this implementation
 * finds a maximal (but not necessarily maximum) independent set; that is, it ensures no more
 * vertices can be added to the set, but does not guarantee that the set has the largest possible
 * number of vertices among all possible independent sets in the graph.
 *
 * The algorithm implemented here is based on the paper: Ghaffari, Mohsen. "An improved
 * distributed algorithm for maximal independent set." Proceedings of the twenty-seventh annual
 * ACM-SIAM symposium on Discrete algorithms. Society for Industrial and Applied Mathematics,
 * 2016.
 *
 * Note: This is a randomized, non-deterministic algorithm. The result may vary between runs even
 * if a fixed random seed is provided because how Apache Spark works.
 *
 * @param graph
 */
class MaximalIndependentSet private[graphframes] (private val graph: GraphFrame)
    extends Serializable
    with WithIntermediateStorageLevel
    with WithCheckpointInterval
    with WithLocalCheckpoints {
  def run(seed: Long): DataFrame = {
    MaximalIndependentSet.run(
      graph,
      checkpointInterval,
      useLocalCheckpoints,
      intermediateStorageLevel,
      seed)
  }
}

object MaximalIndependentSet extends Serializable with Logging {
  private val probCol = "prob"
  private val degCol = "effectiveDegree"
  private val isNominated = "isNominated"
  private val notJoinedMISCol = "notJoinMIS"
  private val isMIS = "isMIS"

  private def run(
      graph: GraphFrame,
      checkpointInterval: Int,
      useLocalCheckpoints: Boolean,
      storageLevel: StorageLevel,
      seed: Long): DataFrame = {
    // initial p = 1/2
    var vertices =
      graph.vertices
        .select(col(GraphFrame.ID), lit(0.5).cast(DoubleType).alias(probCol))
        .persist(storageLevel)

    // make edges undirected and de-duplicate
    // persist() for future usage
    val edges = graph.edges
      .select(GraphFrame.SRC, GraphFrame.DST)
      .union(
        graph.edges.select(
          col(GraphFrame.DST).alias(GraphFrame.SRC),
          col(GraphFrame.SRC).alias(GraphFrame.DST)))
      .filter(col(GraphFrame.SRC) =!= col(GraphFrame.DST))
      .distinct()
      .persist(storageLevel)

    var misDF = graph.vertices.select(col(GraphFrame.ID), lit(false).alias(isMIS))

    var i = 0
    var converged = false
    val spark = graph.vertices.sparkSession

    val shouldCheckpoint = checkpointInterval > 0
    if (!useLocalCheckpoints && spark.sparkContext.getCheckpointDir.isEmpty) {
      // Spark-Connect workaround
      spark.sparkContext
        .setCheckpointDir(spark.conf
          .getOption("spark.checkpoint.dir") match {
          case Some(d) => d
          case None =>
            throw new IOException(
              "Checkpoint directory is not set. Please set it first using sc.setCheckpointDir()" +
                "or by specifying the conf 'spark.checkpoint.dir'.")
        })
    }

    val rng = new util.Random(seed)

    // randomized algorithms are not working with AQE well
    val originalAQE = spark.conf.get("spark.sql.adaptive.enabled")
    try {
      spark.conf.set("spark.sql.adaptive.enabled", "false")

      while (!converged) {
        val iterSeed = rng.nextLong()
        // compute effective degree as a sum of nbrs p
        val effectiveDegrees =
          edges
            .join(vertices, col(GraphFrame.ID) === col(GraphFrame.DST))
            .groupBy(GraphFrame.SRC)
            .agg(sum(col(probCol)).alias(degCol))

        // update p per vertex by condition:
        // if effective degree >= 2 then p / 2
        // else min(2p, 1/2)
        //
        // + mark vertices based on p
        val probs = vertices
          .join(effectiveDegrees, col(GraphFrame.ID) === col(GraphFrame.SRC))
          .drop(GraphFrame.SRC)
          .withColumn(
            probCol,
            when(col(degCol) >= lit(2), col(probCol) / lit(2.0)).otherwise(
              when(lit(2) * col(probCol) <= lit(0.5), lit(2) * col(probCol)).otherwise(lit(0.5))))
          .withColumn(isNominated, col(probCol) >= rand(iterSeed))
          .select(GraphFrame.ID, isNominated, probCol)
          .persist(storageLevel)

        val isolatedVertices =
          vertices
            .join(probs.select(col(GraphFrame.ID)), Seq(GraphFrame.ID), "left_anti")
            .select(GraphFrame.ID)

        // if no nbr of v is marked and v is marked,
        // v is joined MIS and removed with all it's nbrs
        val isJoinedMIS = probs
          .join(
            edges
              .join(probs, col(GraphFrame.ID) === col(GraphFrame.DST))
              .groupBy(GraphFrame.SRC)
              .agg(bool_or(col(isNominated)).alias(notJoinedMISCol)),
            col(GraphFrame.SRC) === col(GraphFrame.ID))
          .select(GraphFrame.ID, probCol, isNominated, notJoinedMISCol)

        val joinedMIS =
          isJoinedMIS.filter((!col(notJoinedMISCol)) && col(isNominated)).select(GraphFrame.ID)

        // update current MIS
        val updatedMIS = misDF
          .join(
            isolatedVertices.select(col(GraphFrame.ID), lit(true).alias("f")),
            Seq(GraphFrame.ID),
            "left")
          .select(col(GraphFrame.ID), (col(isMIS) || col("f")).alias(isMIS))
          .join(
            joinedMIS.select(col(GraphFrame.ID), lit(true).alias("f")),
            Seq(GraphFrame.ID),
            "left")
          .select(col(GraphFrame.ID), (col(isMIS) || col("f")).alias(isMIS))
          .persist(storageLevel)

        // We cannot not checkpoint current MIS, otherwise it is almost not working.
        if (useLocalCheckpoints) {
          val newMis = updatedMIS.localCheckpoint(eager = true)
          newMis.count()
          misDF.unpersist()
          misDF = newMis
        } else {
          val newMis = updatedMIS.checkpoint(eager = true)
          newMis.count()
          misDF.unpersist()
          misDF = newMis
        }

        val neighborsOfMIS = edges
          .join(joinedMIS, col(GraphFrame.ID) === col(GraphFrame.DST))
          .select(col(GraphFrame.SRC))

        val updatedVertices = probs
          .join(joinedMIS, Seq(GraphFrame.ID), "left_anti")
          .join(neighborsOfMIS, col(GraphFrame.ID) === col(GraphFrame.SRC), "left_anti")
          .select(GraphFrame.ID, probCol)

        // checkpointing of vertices
        if (shouldCheckpoint && (i % checkpointInterval == 0)) {
          if (useLocalCheckpoints) {
            vertices = updatedVertices.localCheckpoint(eager = true)
          } else {
            vertices = updatedVertices.checkpoint(eager = true)
          }
        } else {
          vertices = updatedVertices
        }

        // algorithm stops if no more vertex left
        converged = vertices.isEmpty

        updatedVertices.unpersist()
        probs.unpersist()

        logInfo(s"iteration $i finished, vertices left: ${vertices.count()}")
        i += 1
      }

      vertices.unpersist(true)
      edges.unpersist(true)

      val mis = misDF.filter(col(isMIS)).select(GraphFrame.ID).persist(storageLevel)
      // materialize
      mis.count()
      resultIsPersistent()
      misDF.unpersist(true)

      mis
    } finally {
      // Restore original AQE setting
      spark.conf.set("spark.sql.adaptive.enabled", originalAQE)
    }
  }
}
