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

package org.apache.spark.shuffle.streaming

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{ShuffleDependency, SparkConf, SparkContext, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{BlockingShuffleManager, ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.streaming.MultiShuffleManager.isStreamingShuffleEnabled

class MultiShuffleHandle(
    val streamingShuffleHandle: ShuffleHandle,
    val otherShuffleHandle: ShuffleHandle)
  extends ShuffleHandle(streamingShuffleHandle.shuffleId)

object MultiShuffleManager {
  // Streaming shuffle is used for queries running in Real-Time Mode (concurrent stages), gated by
  // the same per-query local property that the RTM micro-batch execution sets.
  // TODO(SPARK-57000): once ConcurrentStageDAGScheduler is merged (apache/spark#56055), reference
  // ConcurrentStageDAGScheduler.CONCURRENT_STAGES_ENABLED_PROPERTY here (and delegate to
  // ConcurrentStageDAGScheduler.isConcurrentStagesEnabled) instead of hardcoding the property.
  val STREAMING_SHUFFLE_ENABLED_PROPERTY = "streaming.concurrent.stages.enabled"

  def isStreamingShuffleEnabled(properties: Properties): Boolean =
    "true" == properties.getProperty(STREAMING_SHUFFLE_ENABLED_PROPERTY)
}

/* This shuffle manager is used to allow real-time queries that depends on streaming shuffle
and normal queries that depends on sort shuffle to coexist in a cluster. Right now, we only
allows configuration of shuffle manager at cluster level, so consider using this shuffle
manager if you want to run batch and real time queries at the same time.

Deprecated: this cluster-level single-slot approach is superseded by per-dependency routing --
configure spark.shuffle.manager (regular/blocking) and spark.shuffle.manager.incremental
(pipelined) so each shuffle is routed by its dependency type.
 */
@deprecated("Use per-dependency routing via spark.shuffle.manager and " +
  "spark.shuffle.manager.incremental instead", "4.3.0")
class MultiShuffleManager(conf: SparkConf) extends BlockingShuffleManager with Logging {
  // To make sure the type of shuffle manager used for a shuffle is the same during its lifetime
  private val shuffleIdToManager = new ConcurrentHashMap[Int, ShuffleManager]()
  private var streamingShuffleManager: Option[StreamingShuffleManager] = None
  private var sortShuffleManager: Option[SortShuffleManager] = None

  private def shuffleManager(shuffleId: Int): ShuffleManager = {
    shuffleIdToManager.computeIfAbsent(shuffleId, _ => {
      val properties = SparkContext.getActive.map(_.getLocalProperties)
        .orElse(Option(TaskContext.get()).map(_.getLocalProperties))
        .getOrElse(throw SparkException.internalError(
          "Cannot determine streaming shuffle routing: no active SparkContext or TaskContext"))
      if (isStreamingShuffleEnabled(properties)) {
        if (streamingShuffleManager.isEmpty) {
          streamingShuffleManager = Some(new StreamingShuffleManager)
        }
        streamingShuffleManager.get
      } else {
        if (sortShuffleManager.isEmpty) {
          sortShuffleManager = Some(new SortShuffleManager(conf))
        }
        sortShuffleManager.get
      }
    })
  }

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    shuffleIdToManager.synchronized {
      shuffleManager(shuffleId).registerShuffle(shuffleId, dependency)
    }
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    shuffleIdToManager.synchronized {
      shuffleManager(handle.shuffleId).getWriter(handle, mapId, context, metrics)
    }
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    shuffleIdToManager.synchronized {
      shuffleManager(handle.shuffleId).getReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    }
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    shuffleIdToManager.synchronized {
      val manager = shuffleIdToManager.get(shuffleId)
      // During unregistering shuffle, which happens when shuffleDependency is garbage
      // collected, the context might not be active anymore, in this case, we will
      // perform no-op since there is no cached shuffle manager, meaning
      // there are no other calls (i.e registerShuffle, getWriter, or getReader) previously
      // invoked, thereby no state to cleanup
      if (manager == null) {
        return true
      }

      shuffleIdToManager.remove(shuffleId)
      manager.unregisterShuffle(shuffleId)
    }
  }

  // As a BlockingShuffleManager, MultiShuffleManager resolves blocks through the inner
  // SortShuffleManager it delegates regular shuffles to.
  override def shuffleBlockResolver: ShuffleBlockResolver = {
    shuffleIdToManager.synchronized {
      if (sortShuffleManager.nonEmpty) {
        sortShuffleManager.get.shuffleBlockResolver
      } else {
        // don't need to support this for the streaming shuffle implementation
        // since block manager is not used
        throw new UnsupportedOperationException()
      }
    }
  }

  override def stop(): Unit = {
    shuffleIdToManager.synchronized {
      if (streamingShuffleManager.nonEmpty) {
        streamingShuffleManager.get.stop()
      }
      if (sortShuffleManager.nonEmpty) {
        sortShuffleManager.get.stop()
      }
    }
  }
}
