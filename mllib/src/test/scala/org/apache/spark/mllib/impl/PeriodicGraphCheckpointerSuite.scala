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

package org.apache.spark.mllib.impl

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils


class PeriodicGraphCheckpointerSuite extends SparkFunSuite with MLlibTestSparkContext {

  import PeriodicGraphCheckpointerSuite._

  test("Persisting") {
    var graphsToCheck = Seq.empty[GraphToCheck]

    val graph1 = createGraph(sc)
    val checkpointer =
      new PeriodicGraphCheckpointer[Double, Double](10, graph1.vertices.sparkContext)
    checkpointer.update(graph1)
    graphsToCheck = graphsToCheck :+ GraphToCheck(graph1, 1)
    checkPersistence(graphsToCheck, 1)

    var iteration = 2
    while (iteration < 9) {
      val graph = createGraph(sc)
      checkpointer.update(graph)
      graphsToCheck = graphsToCheck :+ GraphToCheck(graph, iteration)
      checkPersistence(graphsToCheck, iteration)
      iteration += 1
    }
  }

  test("Checkpointing") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString
    val checkpointInterval = 2
    var graphsToCheck = Seq.empty[GraphToCheck]
    sc.setCheckpointDir(path)
    val graph1 = createGraph(sc)
    val checkpointer = new PeriodicGraphCheckpointer[Double, Double](
      checkpointInterval, graph1.vertices.sparkContext)
    checkpointer.update(graph1)
    graph1.edges.count()
    graph1.vertices.count()
    graphsToCheck = graphsToCheck :+ GraphToCheck(graph1, 1)
    checkCheckpoint(graphsToCheck, 1, checkpointInterval)

    var iteration = 2
    while (iteration < 9) {
      val graph = createGraph(sc)
      checkpointer.update(graph)
      graph.vertices.count()
      graph.edges.count()
      graphsToCheck = graphsToCheck :+ GraphToCheck(graph, iteration)
      checkCheckpoint(graphsToCheck, iteration, checkpointInterval)
      iteration += 1
    }

    checkpointer.deleteAllCheckpoints()
    graphsToCheck.foreach { graph =>
      confirmCheckpointRemoved(graph.graph)
    }

    Utils.deleteRecursively(tempDir)
  }
}

private object PeriodicGraphCheckpointerSuite {

  case class GraphToCheck(graph: Graph[Double, Double], gIndex: Int)

  val edges = Seq(
    Edge[Double](0, 1, 0),
    Edge[Double](1, 2, 0),
    Edge[Double](2, 3, 0),
    Edge[Double](3, 4, 0))

  def createGraph(sc: SparkContext): Graph[Double, Double] = {
    Graph.fromEdges[Double, Double](sc.parallelize(edges), 0)
  }

  def checkPersistence(graphs: Seq[GraphToCheck], iteration: Int): Unit = {
    graphs.foreach { g =>
      checkPersistence(g.graph, g.gIndex, iteration)
    }
  }

  /**
   * Check storage level of graph.
   * @param gIndex  Index of graph in order inserted into checkpointer (from 1).
   * @param iteration  Total number of graphs inserted into checkpointer.
   */
  def checkPersistence(graph: Graph[_, _], gIndex: Int, iteration: Int): Unit = {
    try {
      if (gIndex + 2 < iteration) {
        assert(graph.vertices.getStorageLevel == StorageLevel.NONE)
        assert(graph.edges.getStorageLevel == StorageLevel.NONE)
      } else {
        assert(graph.vertices.getStorageLevel != StorageLevel.NONE)
        assert(graph.edges.getStorageLevel != StorageLevel.NONE)
      }
    } catch {
      case _: AssertionError =>
        throw new Exception(s"PeriodicGraphCheckpointerSuite.checkPersistence failed with:\n" +
          s"\t gIndex = $gIndex\n" +
          s"\t iteration = $iteration\n" +
          s"\t graph.vertices.getStorageLevel = ${graph.vertices.getStorageLevel}\n" +
          s"\t graph.edges.getStorageLevel = ${graph.edges.getStorageLevel}\n")
    }
  }

  def checkCheckpoint(graphs: Seq[GraphToCheck], iteration: Int, checkpointInterval: Int): Unit = {
    graphs.reverse.foreach { g =>
      checkCheckpoint(g.graph, g.gIndex, iteration, checkpointInterval)
    }
  }

  def confirmCheckpointRemoved(graph: Graph[_, _]): Unit = {
    // Note: We cannot check graph.isCheckpointed since that value is never updated.
    //       Instead, we check for the presence of the checkpoint files.
    //       This test should continue to work even after this graph.isCheckpointed issue
    //       is fixed (though it can then be simplified and not look for the files).
    val fs = FileSystem.get(graph.vertices.sparkContext.hadoopConfiguration)
    graph.getCheckpointFiles.foreach { checkpointFile =>
      assert(!fs.exists(new Path(checkpointFile)),
        "Graph checkpoint file should have been removed")
    }
  }

  /**
   * Check checkpointed status of graph.
   * @param gIndex  Index of graph in order inserted into checkpointer (from 1).
   * @param iteration  Total number of graphs inserted into checkpointer.
   */
  def checkCheckpoint(
      graph: Graph[_, _],
      gIndex: Int,
      iteration: Int,
      checkpointInterval: Int): Unit = {
    try {
      if (gIndex % checkpointInterval == 0) {
        // We allow 2 checkpoint intervals since we perform an action (checkpointing a second graph)
        // only AFTER PeriodicGraphCheckpointer decides whether to remove the previous checkpoint.
        if (iteration - 2 * checkpointInterval < gIndex && gIndex <= iteration) {
          assert(graph.isCheckpointed, "Graph should be checkpointed")
          assert(graph.getCheckpointFiles.length == 2, "Graph should have 2 checkpoint files")
        } else {
          confirmCheckpointRemoved(graph)
        }
      } else {
        // Graph should never be checkpointed
        assert(!graph.isCheckpointed, "Graph should never have been checkpointed")
        assert(graph.getCheckpointFiles.isEmpty, "Graph should not have any checkpoint files")
      }
    } catch {
      case e: AssertionError =>
        throw new Exception(s"PeriodicGraphCheckpointerSuite.checkCheckpoint failed with:\n" +
          s"\t gIndex = $gIndex\n" +
          s"\t iteration = $iteration\n" +
          s"\t checkpointInterval = $checkpointInterval\n" +
          s"\t graph.isCheckpointed = ${graph.isCheckpointed}\n" +
          s"\t graph.getCheckpointFiles = ${graph.getCheckpointFiles.mkString(", ")}\n" +
          s"  AssertionError message: ${e.getMessage}")
    }
  }

}
