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

import scala.collection.mutable

import org.apache.hadoop.fs.{Path, FileSystem}

import org.apache.spark.Logging
import org.apache.spark.graphx.Graph
import org.apache.spark.storage.StorageLevel


/**
 * This class helps with persisting and checkpointing Graphs.
 * Specifically, it automatically handles persisting and (optionally) checkpointing, as well as
 * unpersisting and removing checkpoint files.
 *
 * Users should call [[PeriodicGraphCheckpointer.updateGraph()]] when a new graph has been created,
 * before the graph has been materialized.  After updating [[PeriodicGraphCheckpointer]], users are
 * responsible for materializing the graph to ensure that persisting and checkpointing actually
 * occur.
 *
 * When [[PeriodicGraphCheckpointer.updateGraph()]] is called, this does the following:
 *  - Persist new graph (if not yet persisted), and put in queue of persisted graphs.
 *  - Unpersist graphs from queue until there are at most 3 persisted graphs.
 *  - If using checkpointing and the checkpoint interval has been reached,
 *     - Checkpoint the new graph, and put in a queue of checkpointed graphs.
 *     - Remove older checkpoints.
 *
 * WARNINGS:
 *  - This class should NOT be copied (since copies may conflict on which Graphs should be
 *    checkpointed).
 *  - This class removes checkpoint files once later graphs have been checkpointed.
 *    However, references to the older graphs will still return isCheckpointed = true.
 *
 * Example usage:
 * {{{
 *  val (graph1, graph2, graph3, ...) = ...
 *  val cp = new PeriodicGraphCheckpointer(graph1, dir, 2)
 *  graph1.vertices.count(); graph1.edges.count()
 *  // persisted: graph1
 *  cp.updateGraph(graph2)
 *  graph2.vertices.count(); graph2.edges.count()
 *  // persisted: graph1, graph2
 *  // checkpointed: graph2
 *  cp.updateGraph(graph3)
 *  graph3.vertices.count(); graph3.edges.count()
 *  // persisted: graph1, graph2, graph3
 *  // checkpointed: graph2
 *  cp.updateGraph(graph4)
 *  graph4.vertices.count(); graph4.edges.count()
 *  // persisted: graph2, graph3, graph4
 *  // checkpointed: graph4
 *  cp.updateGraph(graph5)
 *  graph5.vertices.count(); graph5.edges.count()
 *  // persisted: graph3, graph4, graph5
 *  // checkpointed: graph4
 * }}}
 *
 * @param currentGraph  Initial graph
 * @param checkpointDir The directory for storing checkpoint files
 * @param checkpointInterval Graphs will be checkpointed at this interval
 * @tparam VD  Vertex descriptor type
 * @tparam ED  Edge descriptor type
 *
 * TODO: Generalize this for Graphs and RDDs, and move it out of MLlib.
 */
private[mllib] class PeriodicGraphCheckpointer[VD, ED](
    var currentGraph: Graph[VD, ED],
    val checkpointDir: Option[String],
    val checkpointInterval: Int) extends Logging {

  /** FIFO queue of past checkpointed RDDs */
  private val checkpointQueue = mutable.Queue[Graph[VD, ED]]()

  /** FIFO queue of past persisted RDDs */
  private val persistedQueue = mutable.Queue[Graph[VD, ED]]()

  /** Number of times [[updateGraph()]] has been called */
  private var updateCount = 0

  /**
   * Spark Context for the Graphs given to this checkpointer.
   * NOTE: This code assumes that only one SparkContext is used for the given graphs.
   */
  private val sc = currentGraph.vertices.sparkContext

  // If a checkpoint directory is given, and there's no prior checkpoint directory,
  // then set the checkpoint directory with the given one.
  if (checkpointDir.nonEmpty && sc.getCheckpointDir.isEmpty) {
    sc.setCheckpointDir(checkpointDir.get)
  }

  updateGraph(currentGraph)

  /**
   * Update [[currentGraph]] with a new graph. Handle persistence and checkpointing as needed.
   * Since this handles persistence and checkpointing, this should be called before the graph
   * has been materialized.
   *
   * @param newGraph  New graph created from previous graphs in the lineage.
   */
  def updateGraph(newGraph: Graph[VD, ED]): Unit = {
    if (newGraph.vertices.getStorageLevel == StorageLevel.NONE) {
      newGraph.persist()
    }
    persistedQueue.enqueue(newGraph)
    // We try to maintain 2 Graphs in persistedQueue to support the semantics of this class:
    // Users should call [[updateGraph()]] when a new graph has been created,
    // before the graph has been materialized.
    while (persistedQueue.size > 3) {
      val graphToUnpersist = persistedQueue.dequeue()
      graphToUnpersist.unpersist(blocking = false)
    }
    updateCount += 1

    // Handle checkpointing (after persisting)
    if ((updateCount % checkpointInterval) == 0 && sc.getCheckpointDir.nonEmpty) {
      // Add new checkpoint before removing old checkpoints.
      newGraph.checkpoint()
      checkpointQueue.enqueue(newGraph)
      // Remove checkpoints before the latest one.
      var canDelete = true
      while (checkpointQueue.size > 1 && canDelete) {
        // Delete the oldest checkpoint only if the next checkpoint exists.
        if (checkpointQueue.get(1).get.isCheckpointed) {
          removeCheckpointFile()
        } else {
          canDelete = false
        }
      }
    }
  }

  /**
   * Call this at the end to delete any remaining checkpoint files.
   */
  def deleteAllCheckpoints(): Unit = {
    while (checkpointQueue.size > 0) {
      removeCheckpointFile()
    }
  }

  /**
   * Dequeue the oldest checkpointed Graph, and remove its checkpoint files.
   * This prints a warning but does not fail if the files cannot be removed.
   */
  private def removeCheckpointFile(): Unit = {
    val old = checkpointQueue.dequeue()
    // Since the old checkpoint is not deleted by Spark, we manually delete it.
    val fs = FileSystem.get(sc.hadoopConfiguration)
    old.getCheckpointFiles.foreach { checkpointFile =>
      try {
        fs.delete(new Path(checkpointFile), true)
      } catch {
        case e: Exception =>
          logWarning("PeriodicGraphCheckpointer could not remove old checkpoint file: " +
            checkpointFile)
      }
    }
  }

}
