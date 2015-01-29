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
 *
 * This class maintains a FIFO queue of Graphs, each of which is persisted and some of which are
 * checkpointed.  Once one Graph has been checkpointed, then previous RDDs are unpersisted and their
 * checkpoint files are removed.
 *
 * Users should call [[PeriodicGraphCheckpointer.updateGraph()]] when a new graph has been created,
 * before the graph has been materialized.
 *
 * NOTE: This class should NOT be copied (since copies may conflict on which Graphs should be
 *       checkpointed).
 *
 * @param currentGraph  Initial graph
 * @param checkpointDir The directory for storing checkpoint files
 * @param checkpointInterval Graphs will be checkpointed at this interval
 * @tparam VD  Vertex descriptor type
 * @tparam ED  Edge descriptor type
 */
private[mllib] class PeriodicGraphCheckpointer[VD, ED](
    var currentGraph: Graph[VD, ED],
    val checkpointDir: Option[String],
    val checkpointInterval: Int) extends Logging {

  /** FIFO queue of past checkpointed RDDs*/
  private val checkpointQueue = mutable.Queue[Graph[VD, ED]]()

  /** FIFO queue of past persisted RDDs*/
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
      println(s"PeriodicGraphCheckpointer.updateGraph: persisting ${newGraph.vertices.id}")
      newGraph.persist()
    }
    persistedQueue.enqueue(newGraph)
    // We try to maintain 2 Graphs in persistedQueue to support the semantics of this class:
    // Users should call [[updateGraph()]] when a new graph has been created,
    // before the graph has been materialized.
    while (persistedQueue.size > 3) {
      val graphToUnpersist = persistedQueue.dequeue()
      println(s"PeriodicGraphCheckpointer.updateGraph: unpersisting ${graphToUnpersist.vertices.id}")
      graphToUnpersist.unpersist(blocking = false)
    }
    updateCount += 1

    // Handle checkpointing (after persisting)
    if ((updateCount % checkpointInterval) == 0 && sc.getCheckpointDir.nonEmpty) {
      // Add new checkpoint before removing old checkpoints.
      println(s"PeriodicGraphCheckpointer.updateGraph: checkpointing ${newGraph.vertices.id}")
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
    println(s"PeriodicGraphCheckpointer.updateGraph: removing checkpoint ${old.vertices.id}")
    // Since the old checkpoint is not deleted by Spark, we manually delete it.
    val fs = FileSystem.get(sc.hadoopConfiguration)
    old.getCheckpointFiles.foreach { checkpointFile =>
      try {
        println(s"  --removing file: $checkpointFile")
        fs.delete(new Path(checkpointFile), true)
      } catch {
        case e: Exception =>
          println("PeriodicGraphCheckpointer could not remove old checkpoint file: " +
            checkpointFile)
          logWarning("PeriodicGraphCheckpointer could not remove old checkpoint file: " +
            checkpointFile)
      }
    }
  }

}
