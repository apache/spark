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

package org.apache.spark.mllib.tree.impl

import scala.collection.mutable

import org.apache.hadoop.fs.{Path, FileSystem}

import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.tree.model.{Bin, Node, Split}

/**
 * :: DeveloperApi ::
 * This is used by the node id cache to find the child id that a data point would belong to.
 * @param split Split information.
 * @param nodeIndex The current node index of a data point that this will update.
 */
@DeveloperApi
private[tree] case class NodeIndexUpdater(
    split: Split,
    nodeIndex: Int) {
  /**
   * Determine a child node index based on the feature value and the split.
   * @param binnedFeatures Binned feature values.
   * @param bins Bin information to convert the bin indices to approximate feature values.
   * @return Child node index to update to.
   */
  def updateNodeIndex(binnedFeatures: Array[Int], bins: Array[Array[Bin]]): Int = {
    if (split.featureType == Continuous) {
      val featureIndex = split.feature
      val binIndex = binnedFeatures(featureIndex)
      val featureValueUpperBound = bins(featureIndex)(binIndex).highSplit.threshold
      if (featureValueUpperBound <= split.threshold) {
        Node.leftChildIndex(nodeIndex)
      } else {
        Node.rightChildIndex(nodeIndex)
      }
    } else {
      if (split.categories.contains(binnedFeatures(split.feature).toDouble)) {
        Node.leftChildIndex(nodeIndex)
      } else {
        Node.rightChildIndex(nodeIndex)
      }
    }
  }
}

/**
 * :: DeveloperApi ::
 * A given TreePoint would belong to a particular node per tree.
 * Each row in the nodeIdsForInstances RDD is an array over trees of the node index
 * in each tree. Initially, values should all be 1 for root node.
 * The nodeIdsForInstances RDD needs to be updated at each iteration.
 * @param nodeIdsForInstances The initial values in the cache
 *            (should be an Array of all 1's (meaning the root nodes)).
 * @param checkpointDir The checkpoint directory where
 *                      the checkpointed files will be stored.
 * @param checkpointInterval The checkpointing interval
 *                           (how often should the cache be checkpointed.).
 */
@DeveloperApi
private[tree] class NodeIdCache(
  var nodeIdsForInstances: RDD[Array[Int]],
  val checkpointDir: Option[String],
  val checkpointInterval: Int) {

  // Keep a reference to a previous node Ids for instances.
  // Because we will keep on re-persisting updated node Ids,
  // we want to unpersist the previous RDD.
  private var prevNodeIdsForInstances: RDD[Array[Int]] = null

  // To keep track of the past checkpointed RDDs.
  private val checkpointQueue = mutable.Queue[RDD[Array[Int]]]()
  private var rddUpdateCount = 0

  // If a checkpoint directory is given, and there's no prior checkpoint directory,
  // then set the checkpoint directory with the given one.
  if (checkpointDir.nonEmpty && nodeIdsForInstances.sparkContext.getCheckpointDir.isEmpty) {
    nodeIdsForInstances.sparkContext.setCheckpointDir(checkpointDir.get)
  }

  /**
   * Update the node index values in the cache.
   * This updates the RDD and its lineage.
   * TODO: Passing bin information to executors seems unnecessary and costly.
   * @param data The RDD of training rows.
   * @param nodeIdUpdaters A map of node index updaters.
   *                       The key is the indices of nodes that we want to update.
   * @param bins Bin information needed to find child node indices.
   */
  def updateNodeIndices(
      data: RDD[BaggedPoint[TreePoint]],
      nodeIdUpdaters: Array[mutable.Map[Int, NodeIndexUpdater]],
      bins: Array[Array[Bin]]): Unit = {
    if (prevNodeIdsForInstances != null) {
      // Unpersist the previous one if one exists.
      prevNodeIdsForInstances.unpersist()
    }

    prevNodeIdsForInstances = nodeIdsForInstances
    nodeIdsForInstances = data.zip(nodeIdsForInstances).map {
      dataPoint => {
        var treeId = 0
        while (treeId < nodeIdUpdaters.length) {
          val nodeIdUpdater = nodeIdUpdaters(treeId).getOrElse(dataPoint._2(treeId), null)
          if (nodeIdUpdater != null) {
            val newNodeIndex = nodeIdUpdater.updateNodeIndex(
              binnedFeatures = dataPoint._1.datum.binnedFeatures,
              bins = bins)
            dataPoint._2(treeId) = newNodeIndex
          }

          treeId += 1
        }

        dataPoint._2
      }
    }

    // Keep on persisting new ones.
    nodeIdsForInstances.persist(StorageLevel.MEMORY_AND_DISK)
    rddUpdateCount += 1

    // Handle checkpointing if the directory is not None.
    if (nodeIdsForInstances.sparkContext.getCheckpointDir.nonEmpty &&
      (rddUpdateCount % checkpointInterval) == 0) {
      // Let's see if we can delete previous checkpoints.
      var canDelete = true
      while (checkpointQueue.size > 1 && canDelete) {
        // We can delete the oldest checkpoint iff
        // the next checkpoint actually exists in the file system.
        if (checkpointQueue.get(1).get.getCheckpointFile != None) {
          val old = checkpointQueue.dequeue()

          // Since the old checkpoint is not deleted by Spark,
          // we'll manually delete it here.
          val fs = FileSystem.get(old.sparkContext.hadoopConfiguration)
          fs.delete(new Path(old.getCheckpointFile.get), true)
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
    while (checkpointQueue.size > 0) {
      val old = checkpointQueue.dequeue()
      if (old.getCheckpointFile != None) {
        val fs = FileSystem.get(old.sparkContext.hadoopConfiguration)
        fs.delete(new Path(old.getCheckpointFile.get), true)
      }
    }
  }
}

@DeveloperApi
private[tree] object NodeIdCache {
  /**
   * Initialize the node Id cache with initial node Id values.
   * @param data The RDD of training rows.
   * @param numTrees The number of trees that we want to create cache for.
   * @param checkpointDir The checkpoint directory where the checkpointed files will be stored.
   * @param checkpointInterval The checkpointing interval
   *                           (how often should the cache be checkpointed.).
   * @param initVal The initial values in the cache.
   * @return A node Id cache containing an RDD of initial root node Indices.
   */
  def init(
      data: RDD[BaggedPoint[TreePoint]],
      numTrees: Int,
      checkpointDir: Option[String],
      checkpointInterval: Int,
      initVal: Int = 1): NodeIdCache = {
    new NodeIdCache(
      data.map(_ => Array.fill[Int](numTrees)(initVal)),
      checkpointDir,
      checkpointInterval)
  }
}
