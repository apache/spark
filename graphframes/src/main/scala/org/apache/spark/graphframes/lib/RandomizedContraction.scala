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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions._
import org.apache.spark.sql.graphframes.expressions.FiniteAXPlusB
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrame.DST
import org.apache.spark.graphframes.GraphFrame.ID
import org.apache.spark.graphframes.GraphFrame.LONG_DST
import org.apache.spark.graphframes.GraphFrame.LONG_ID
import org.apache.spark.graphframes.GraphFrame.LONG_SRC
import org.apache.spark.graphframes.GraphFrame.SRC
import org.apache.spark.graphframes.Logging

import java.io.IOException
import java.util.UUID
import scala.collection.mutable.Stack
import scala.util.Random

/**
 * Implementation of parallel connected components algorithm using randomized contraction, based
 * on Bögeholz, Harald, Michael Brand, and Radu-Alexandru Todor. "In-database connected component
 * analysis." 2020 IEEE 36th International Conference on Data Engineering (ICDE). IEEE, 2020.
 *
 * The algorithm contracts the graph iteratively using random linear functions, until no edges
 * remain, then reconstructs the component identifiers.
 */
private[graphframes] object RandomizedContraction extends Logging with Serializable {
  private val CHECKPOINT_NAME_PREFIX = "randomized-contraction"

  private def prepare(graph: GraphFrame): GraphFrame = {
    val vertices = graph.indexedVertices
      .select(col(LONG_ID).as(ID))

    val edges = graph.indexedEdges
      .select(col(LONG_SRC).as(SRC), col(LONG_DST).as(DST))
    val symmetricEdges = edges
      .union(edges.select(col(DST).alias(SRC), col(SRC).alias(DST)))
      .distinct()
    GraphFrame(vertices, symmetricEdges)

  }

  def run(
      inputGraph: GraphFrame,
      useLabelsAsComponents: Boolean,
      intermediateStorageLevel: StorageLevel,
      useLocalCheckpoints: Boolean,
      checkpointInterval: Int,
      isGraphPrepared: Boolean): DataFrame = {
    val spark = inputGraph.vertices.sparkSession
    val sc = spark.sparkContext
    val runId = UUID.randomUUID().toString.takeRight(8)
    val logPrefix = s"[CC $runId]"

    val checkpointDir = sc.getCheckpointDir
      .map { d =>
        new Path(d, s"$CHECKPOINT_NAME_PREFIX-$runId").toString
      }
      .getOrElse {
        // Spark-Connect workaround
        spark.conf.getOption("spark.checkpoint.dir") match {
          case Some(d) => new Path(d, s"$CHECKPOINT_NAME_PREFIX-$runId").toString
          case None =>
            throw new IOException(
              "Checkpoint directory is not set. Please set it first using sc.setCheckpointDir()" +
                "or by specifying the conf 'spark.checkpoint.dir'.")
        }
      }
    logInfo(s"$logPrefix Using $checkpointDir for storing intermediate tables.")

    val functionRegistry = spark.sessionState.functionRegistry
    functionRegistry.registerFunction(
      new FunctionIdentifier("_axpb", Some("builtin"), Some("system")),
      (children: Seq[Expression]) => FiniteAXPlusB(children(0), children(1), children(2)),
      "scala_udf")

    val random = new Random()
    random.setSeed(42L)
    val stackA = Stack.empty[Long]
    val stackB = Stack.empty[Long]
    var iter = 0

    def tableName(iter: Int): String = s"${checkpointDir}/ccreps-${iter}"

    val graph = if (isGraphPrepared) {
      inputGraph
    } else {
      prepare(inputGraph)
    }

    var edges =
      graph.edges.select(SRC, DST).persist(intermediateStorageLevel)

    def axpb(a: Long, x: Column, b: Long): Column = call_function("_axpb", lit(a), x, lit(b))

    try {
      var rA = 0L
      var graphSize = edges.count()
      var ccRepresentatives: DataFrame = null

      // "no edges graph"
      if (graphSize == 0L) {
        val result = inputGraph.vertices
          .select(col(ID), col(ID).alias(ConnectedComponents.COMPONENT))
          .persist(intermediateStorageLevel)
        result.count()
        edges.unpersist()

        return result
      }

      while (graphSize > 0) {
        logInfo(s"iteration ${iter}, edges left ${graphSize}")
        iter += 1
        rA = 0L
        while (rA == 0L) {
          rA = random.nextLong()
        }
        val rB = random.nextLong()
        stackA.push(rA)
        stackB.push(rB)

        ccRepresentatives = edges
          .groupBy(SRC)
          .agg(min(axpb(rA, col(DST), rB)).alias("rep"))
          .select(col(SRC).alias("v"), least(axpb(rA, col(SRC), rB), col("rep")).alias("rep"))

        // "free" checkpointing
        ccRepresentatives.write.parquet(tableName(iter))
        ccRepresentatives = spark.read.parquet(tableName(iter))

        val edges2 = edges
          .join(ccRepresentatives, col(SRC) === col("v"))
          .select(col("rep").alias(SRC), col(DST))

        // save ref to unpersist
        val oldEdges = edges

        edges = {
          val te = edges2
            .alias("e")
            .join(
              ccRepresentatives.alias("r2"),
              col(s"e.$DST") === col("r2.v") &&
                col(s"e.$SRC") =!= col("r2.rep"))
            .select(col(s"e.$SRC").alias(SRC), col("r2.rep").alias(DST))
            .distinct()

          if ((iter > 0) && (iter % checkpointInterval == 0)) {
            if (useLocalCheckpoints) {
              te.localCheckpoint()
            } else {
              te.checkpoint()
            }
          } else {
            te.persist(intermediateStorageLevel)
          }
        }

        graphSize = edges.count()
        oldEdges.unpersist()
      }

      logInfo(s"graph was successfully contracted for $iter iterations")
      logInfo("start reverse transformation")

      var accA = 1L
      var accB = 0L

      edges.unpersist(true)

      while (iter > 1) {
        iter -= 1
        val poppedA = stackA.pop()
        val poppedB = stackB.pop()

        val oldAccA = accA
        accA = FiniteAXPlusB.axpb(oldAccA, poppedA, 0L)
        accB = FiniteAXPlusB.axpb(oldAccA, poppedB, accB)

        val ccRepsR = tableName(iter)
        val ccRepsR1 = tableName(iter + 1)

        val result = spark.read
          .parquet(ccRepsR)
          .alias("r1")
          .join(
            spark.read.parquet(ccRepsR1).alias("r2"),
            col("r1.rep") === col("r2.v"),
            "left_outer")
          .select(
            col("r1.v"),
            coalesce(col("r2.rep"), axpb(accA, col("r1.rep"), accB)).alias("rep"))
          .persist(intermediateStorageLevel)

        result.write.mode("overwrite").parquet(ccRepsR)

        result.unpersist()
        val oldPath = new Path(ccRepsR1)
        val fs = oldPath.getFileSystem(sc.hadoopConfiguration)

        if (fs.exists(oldPath)) {
          fs.delete(oldPath, true)
        }
      }

      val finalReps = spark.read
        .parquet(tableName(1))
        .select(col("v").alias(ID), col("rep").alias(ConnectedComponents.COMPONENT))

      val outputComponents = if (useLabelsAsComponents && (!inputGraph.hasIntegralIdType)) {
        val labels = inputGraph.indexedVertices
          .withColumnRenamed(ID, ConnectedComponents.ORIG_ID)
          .join(finalReps, col(ID) === col(LONG_ID))
          .groupBy(ConnectedComponents.COMPONENT)
          .agg(min(ConnectedComponents.ORIG_ID).alias("new_component"))

        inputGraph.indexedVertices
          .withColumnRenamed(ID, ConnectedComponents.ORIG_ID)
          .join(finalReps, col(ID) === col(LONG_ID), "left")
          .join(labels, ConnectedComponents.COMPONENT, "left")
          .select(
            col(ConnectedComponents.ORIG_ID).alias(ID),
            coalesce(col("new_component"), col(ConnectedComponents.ORIG_ID))
              .alias(ConnectedComponents.COMPONENT))
      } else if (useLabelsAsComponents) {
        val labels =
          finalReps.groupBy(ConnectedComponents.COMPONENT).agg(min(ID).alias("new_component"))
        inputGraph.vertices
          .join(finalReps, ID, "left")
          .join(labels, ConnectedComponents.COMPONENT, "left")
          .select(
            col(ID),
            coalesce(col("new_component"), col(ID))
              .alias(ConnectedComponents.COMPONENT))
      } else {
        inputGraph.vertices
          .join(finalReps, ID, "left")
          .select(
            col(ID),
            coalesce(col(ConnectedComponents.COMPONENT), col(ID))
              .alias(ConnectedComponents.COMPONENT))
      }

      outputComponents.persist(intermediateStorageLevel)
      // materialize to be able to clean up everything
      outputComponents.count()

      // clean-up
      val chDirPath = new Path(checkpointDir)
      val fs = chDirPath.getFileSystem(sc.hadoopConfiguration)
      if (fs.exists(chDirPath)) {
        fs.delete(chDirPath, true)
      }

      outputComponents
    } finally {
      // to be 100% sure;
      edges.unpersist()
      val dereg = functionRegistry.dropFunction(
        new FunctionIdentifier("_axpb", Some("builtin"), Some("system")))
      if (!dereg) {
        logWarn(
          "graphframes faced an internal error and was not able to de-register function _axpb; Spark' functionRegistry is in a bad state")
      }
    }
  }
}
