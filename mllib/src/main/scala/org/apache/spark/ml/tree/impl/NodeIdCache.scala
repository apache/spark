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

package org.apache.spark.ml.tree.impl

import java.io.IOException

import scala.collection.mutable

import org.apache.hadoop.fs.{Path, FileSystem}

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.tree.{LearningNode, Split}
import org.apache.spark.mllib.tree.impl.BaggedPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


/**
 * This is used by the node id cache to find the child id that a data point would belong to.
 * @param split Split information.
 * @param nodeIndex The current node index of a data point that this will update.
 */
private[tree] case class NodeIndexUpdater(split: Split, nodeIndex: Int) {

  /**
   * Determine a child node index based on the feature value and the split.
   * @param binnedFeature Binned feature value.
   * @param splits Split information to convert the bin indices to approximate feature values.
   * @return Child node index to update to.
   */
  def updateNodeIndex(binnedFeature: Int, splits: Array[Split]): Int = {
    if (split.shouldGoLeft(binnedFeature, splits)) {
      LearningNode.leftChildIndex(nodeIndex)
    } else {
      LearningNode.rightChildIndex(nodeIndex)
    }
  }
}

/**
 * Each TreePoint belongs to a particular node per tree.
 * Each row in the nodeIdsForInstances RDD is an array over trees of the node index
 * in each tree. Initially, values should all be 1 for root node.
 * The nodeIdsForInstances RDD needs to be updated at each iteration.
 * @param nodeIdsForInstances The initial values in the cache
 *                           (should be an Array of all 1's (meaning the root nodes)).
 * @param checkpointInterval The checkpointing interval
 *                           (how often should the cache be checkpointed.).
 */
private[spark] class NodeIdCache(
  var nodeIdsForInstances: RDD[Array[Int]],
  val checkpointInterval: Int) extends Logging {

  // Keep a reference to a previous node Ids for instances.
  // Because we will keep on re-persisting updated node Ids,
  // we want to unpersist the previous RDD.
  private var prevNodeIdsForInstances: RDD[Array[Int]] = null

  // To keep track of the past checkpointed RDDs.
  private val checkpointQueue = mutable.Queue[RDD[Array[Int]]]()
  private var rddUpdateCount = 0

  // Indicates whether we can checkpoint
  private val canCheckpoint = nodeIdsForInstances.sparkContext.getCheckpointDir.nonEmpty

  // FileSystem instance for deleting checkpoints as needed
  private val fs = FileSystem.get(nodeIdsForInstances.sparkContext.hadoopConfiguration)

  /**
   * Update the node index values in the cache.
   * This updates the RDD and its lineage.
   * TODO: Passing bin information to executors seems unnecessary and costly.
   * @param data The RDD of training rows.
   * @param nodeIdUpdaters A map of node index updaters.
   *                       The key is the indices of nodes that we want to update.
   * @param splits  Split information needed to find child node indices.
   */
  def updateNodeIndices(
      data: RDD[BaggedPoint[TreePoint]],
      nodeIdUpdaters: Array[mutable.Map[Int, NodeIndexUpdater]],
      splits: Array[Array[Split]]): Unit = {
    if (prevNodeIdsForInstances != null) {
      // Unpersist the previous one if one exists.
      prevNodeIdsForInstances.unpersist()
    }

    prevNodeIdsForInstances = nodeIdsForInstances
    nodeIdsForInstances = data.zip(nodeIdsForInstances).map { case (point, ids) =>
      var treeId = 0
      while (treeId < nodeIdUpdaters.length) {
        val nodeIdUpdater = nodeIdUpdaters(treeId).getOrElse(ids(treeId), null)
        if (nodeIdUpdater != null) {
          val featureIndex = nodeIdUpdater.split.featureIndex
          val newNodeIndex = nodeIdUpdater.updateNodeIndex(
            binnedFeature = point.datum.binnedFeatures(featureIndex),
            splits = splits(featureIndex))
          ids(treeId) = newNodeIndex
        }
        treeId += 1
      }
      ids
    }

    // Keep on persisting new ones.
    nodeIdsForInstances.persist(StorageLevel.MEMORY_AND_DISK)
    rddUpdateCount += 1

    // Handle checkpointing if the directory is not None.
    if (canCheckpoint && checkpointInterval != -1 && (rddUpdateCount % checkpointInterval) == 0) {
      // Let's see if we can delete previous checkpoints.
      var canDelete = true
      while (checkpointQueue.size > 1 && canDelete) {
        // We can delete the oldest checkpoint iff
        // the next checkpoint actually exists in the file system.
        if (checkpointQueue(1).getCheckpointFile.isDefined) {
          val old = checkpointQueue.dequeue()
          // Since the old checkpoint is not deleted by Spark, we'll manually delete it here.
          try {
            fs.delete(new Path(old.getCheckpointFile.get), true)
          } catch {
            case e: IOException =>
              logError("Decision Tree learning using cacheNodeIds failed to remove checkpoint" +
                s" file: ${old.getCheckpointFile.get}")
          }
        } else {
          canDelete = false
        }
      }

      nodeIdsForInstances.checkpoint()
      checkpointQueue.enqueue(nodeIdsForInstances)
    }
  }

  /**
   * Call this after training is finished to delete any remaining checkpoints.
   */
  def deleteAllCheckpoints(): Unit = {
    while (checkpointQueue.nonEmpty) {
      val old = checkpointQueue.dequeue()
      if (old.getCheckpointFile.isDefined) {
        try {
          fs.delete(new Path(old.getCheckpointFile.get), true)
        } catch {
          case e: IOException =>
            logError("Decision Tree learning using cacheNodeIds failed to remove checkpoint" +
              s" file: ${old.getCheckpointFile.get}")
        }
      }
    }
    if (prevNodeIdsForInstances != null) {
      // Unpersist the previous one if one exists.
      prevNodeIdsForInstances.unpersist()
    }
  }
}

@DeveloperApi
private[spark] object NodeIdCache {
  /**
   * Initialize the node Id cache with initial node Id values.
   * @param data The RDD of training rows.
   * @param numTrees The number of trees that we want to create cache for.
   * @param checkpointInterval The checkpointing interval
   *                           (how often should the cache be checkpointed.).
   * @param initVal The initial values in the cache.
   * @return A node Id cache containing an RDD of initial root node Indices.
   */
  def init(
      data: RDD[BaggedPoint[TreePoint]],
      numTrees: Int,
      checkpointInterval: Int,
      initVal: Int = 1): NodeIdCache = {
    new NodeIdCache(
      data.map(_ => Array.fill[Int](numTrees)(initVal)),
      checkpointInterval)
  }
}
