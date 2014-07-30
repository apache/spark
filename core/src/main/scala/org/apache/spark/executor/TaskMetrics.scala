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

package org.apache.spark.executor

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.{BlockId, BlockStatus}

/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
 */
@DeveloperApi
class TaskMetrics extends Serializable {
  /**
   * Host's name the task runs on
   */
  var hostname: String = _

  /**
   * Time taken on the executor to deserialize this task
   */
  var executorDeserializeTime: Long = _

  /**
   * Time the executor spends actually running the task (including fetching shuffle data)
   */
  var executorRunTime: Long = _

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult
   */
  var resultSize: Long = _

  /**
   * Amount of time the JVM spent in garbage collection while executing this task
   */
  var jvmGCTime: Long = _

  /**
   * Amount of time spent serializing the task result
   */
  var resultSerializationTime: Long = _

  /**
   * The number of in-memory bytes spilled by this task
   */
  var memoryBytesSpilled: Long = _

  /**
   * The number of on-disk bytes spilled by this task
   */
  var diskBytesSpilled: Long = _

  /**
   * If this task reads from a HadoopRDD or from persisted data, metrics on how much data was read
   * are stored here.
   */
  var inputMetrics: Option[InputMetrics] = None

  /**
   * If this task reads from shuffle output, metrics on getting shuffle data will be collected here
   */
  private var _shuffleReadMetrics: Option[ShuffleReadMetrics] = None

  def shuffleReadMetrics = _shuffleReadMetrics

  /**
   * If this task writes to shuffle output, metrics on the written shuffle data will be collected
   * here
   */
  var shuffleWriteMetrics: Option[ShuffleWriteMetrics] = None

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   */
  var updatedBlocks: Option[Seq[(BlockId, BlockStatus)]] = None

  /** Adds the given ShuffleReadMetrics to any existing shuffle metrics for this task. */
  def updateShuffleReadMetrics(newMetrics: ShuffleReadMetrics) = synchronized {
    _shuffleReadMetrics match {
      case Some(existingMetrics) =>
        existingMetrics.shuffleFinishTime = math.max(
          existingMetrics.shuffleFinishTime, newMetrics.shuffleFinishTime)
        existingMetrics.fetchWaitTime += newMetrics.fetchWaitTime
        existingMetrics.localBlocksFetched += newMetrics.localBlocksFetched
        existingMetrics.remoteBlocksFetched += newMetrics.remoteBlocksFetched
        existingMetrics.remoteBytesRead += newMetrics.remoteBytesRead
      case None =>
        _shuffleReadMetrics = Some(newMetrics)
    }
  }
}

private[spark] object TaskMetrics {
  def empty: TaskMetrics = new TaskMetrics
}

/**
 * :: DeveloperApi ::
 * Method by which input data was read.  Network means that the data was read over the network
 * from a remote block manager (which may have stored the data on-disk or in-memory).
 */
@DeveloperApi
object DataReadMethod extends Enumeration with Serializable {
  type DataReadMethod = Value
  val Memory, Disk, Hadoop, Network = Value
}

/**
 * :: DeveloperApi ::
 * Metrics about reading input data.
 */
@DeveloperApi
case class InputMetrics(readMethod: DataReadMethod.Value) {
  /**
   * Total bytes read.
   */
  var bytesRead: Long = 0L
}


/**
 * :: DeveloperApi ::
 * Metrics pertaining to shuffle data read in a given task.
 */
@DeveloperApi
class ShuffleReadMetrics extends Serializable {
  /**
   * Absolute time when this task finished reading shuffle data
   */
  var shuffleFinishTime: Long = _

  /**
   * Number of blocks fetched in this shuffle by this task (remote or local)
   */
  def totalBlocksFetched: Int = remoteBlocksFetched + localBlocksFetched

  /**
   * Number of remote blocks fetched in this shuffle by this task
   */
  var remoteBlocksFetched: Int = _

  /**
   * Number of local blocks fetched in this shuffle by this task
   */
  var localBlocksFetched: Int = _

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   */
  var fetchWaitTime: Long = _

  /**
   * Total number of remote bytes read from the shuffle by this task
   */
  var remoteBytesRead: Long = _
}

/**
 * :: DeveloperApi ::
 * Metrics pertaining to shuffle data written in a given task.
 */
@DeveloperApi
class ShuffleWriteMetrics extends Serializable {
  /**
   * Number of bytes written for the shuffle by this task
   */
  var shuffleBytesWritten: Long = _

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds
   */
  var shuffleWriteTime: Long = _
}
