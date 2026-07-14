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

package org.apache.spark.shuffle

import org.apache.spark.{ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{NUM_MERGER_LOCATIONS, SHUFFLE_ID, STAGE_ID}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.util.PostStatusUpdateListener

/**
 * The interface for customizing shuffle write process. The driver create a ShuffleWriteProcessor
 * and put it into [[ShuffleDependency]], and executors use it in each ShuffleMapTask.
 */
private[spark] class ShuffleWriteProcessor extends Serializable with Logging {

  /**
   * Create a [[ShuffleWriteMetricsReporter]] from the task context. As the reporter is a
   * per-row operator, here need a careful consideration on performance.
   */
  protected def createMetricsReporter(context: TaskContext): ShuffleWriteMetricsReporter = {
    context.taskMetrics().shuffleWriteMetrics
  }

  /**
   * The write process for particular partition, it controls the life circle of [[ShuffleWriter]]
   * get from [[ShuffleManager]] finally return the [[MapStatus]] for this task.
   */
  def write(
      inputs: Iterator[_],
      dep: ShuffleDependency[_, _, _],
      mapId: Long,
      mapIndex: Int,
      context: TaskContext): MapStatus = {
    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManagerFor(dep)
      writer = manager.getWriter[Any, Any](
        dep.shuffleHandle,
        mapId,
        context,
        createMetricsReporter(context))
      writer.write(inputs.asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      val mapStatus = writer.stop(success = true)
      if (mapStatus.isDefined) {
        // Check if sufficient shuffle mergers are available now for the ShuffleMapTask to push
        if (dep.shuffleMergeAllowed && dep.getMergerLocs.isEmpty) {
          val mapOutputTracker = SparkEnv.get.mapOutputTracker
          val mergerLocs =
            mapOutputTracker.getShufflePushMergerLocations(dep.shuffleId)
          if (mergerLocs.nonEmpty) {
            dep.setMergerLocs(mergerLocs)
          }
        }
        // Initiate shuffle push process if push based shuffle is enabled
        // The map task only takes care of converting the shuffle data file into multiple
        // block push requests. It delegates pushing the blocks to a different thread-pool -
        // ShuffleBlockPusher.BLOCK_PUSHER_POOL.
        if (!dep.shuffleMergeFinalized) {
          manager.shuffleBlockResolver match {
            case resolver: IndexShuffleBlockResolver =>
              logInfo(log"Shuffle merge enabled with" +
                log" ${MDC(NUM_MERGER_LOCATIONS, dep.getMergerLocs.size)} merger locations" +
                log" for stage ${MDC(STAGE_ID, context.stageId())}" +
                log" with shuffle ID ${MDC(SHUFFLE_ID, dep.shuffleId)}")
              logDebug(s"Starting pushing blocks for the task ${context.taskAttemptId()}")
              val dataFile = resolver.getDataFile(dep.shuffleId, mapId)
              val blockPusher = new ShuffleBlockPusher(SparkEnv.get.conf)
              // Register a post-status-update listener to defer push until after the task
              // result has been sent to the driver. This ensures the driver processes the
              // task result (and can mark stale partitions from speculative duplicates)
              // before any push data reaches the merger, avoiding stale data being merged
              // without detection.
              //
              // The listener callback runs on the Task thread and only does a lightweight
              // submitTask; actual push I/O runs on BLOCK_PUSHER_POOL threads.
              context.addPostStatusUpdateListener(new PostStatusUpdateListener {
                override def onStatusUpdateSent(context: TaskContext): Unit = {
                  if (!context.isInterrupted() && !context.isFailed()) {
                    logDebug(s"Task ${context.taskAttemptId()} status update sent, " +
                      s"proceeding with shuffle block push for shuffle ${dep.shuffleId}")
                    blockPusher.initiateBlockPush(
                      dataFile, writer.getPartitionLengths(), dep, mapIndex)
                  } else {
                    logInfo(s"Task ${context.taskAttemptId()} was " +
                      s"${if (context.isInterrupted()) "killed" else "failed"}, " +
                      s"skipping shuffle block push for shuffle ${dep.shuffleId}")
                  }
                }
              })
            case _ =>
          }
        }
      }
      mapStatus.get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }
}
