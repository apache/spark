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
   * If this task reads from shuffle output, metrics on getting shuffle data will be collected here
   */
  var shuffleReadMetrics: Option[ShuffleReadMetrics] = None

  /**
   * If this task writes to shuffle output, metrics on the written shuffle data will be collected
   * here
   */
  var shuffleWriteMetrics: Option[ShuffleWriteMetrics] = None

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   */
  var updatedBlocks: Option[Seq[(BlockId, BlockStatus)]] = None

  /**
   * Custom task-specific metrics. Any piece of Spark machinery that cares to track custom task-related metrics can
   * now use the setCustomMetric(name,value) method on TaskMetrics to do that.
   * e.g. Any RDD that wants to track a custom metric of its interest (related to execution of a task) can
   * call setCustomMetric(name,value) inside its compute() method.
   * Map Key -> name of custom metric
   * Map Value -> list of numeric values for custom metric
   */
  private val _customMetrics: scala.collection.mutable.HashMap[String, List[Long]] = scala.collection.mutable.HashMap()

  /**
   * Custom task-specific metrics. Any piece of Spark machinery that cares to track custom task-related metrics can
   * now use the setCustomMetric(name,value) method on TaskMetrics to do that.
   * e.g. Any RDD that wants to track a custom metric of its interest (related to execution of a task) can
   * call setCustomMetric(name,value) inside its compute() method.
   * Map Key -> name of custom metric
   * Map Value -> list of numeric values for custom metric
   */
  def customMetrics = _customMetrics

  /**
   * Convenience method for setting a custom metric
   * @param metricName  name of custom metric
   * @param metricValue value for custom metric
   */
  def setCustomMetric(metricName: String, metricValue: Long) {
    if (_customMetrics.contains(metricName)) {
      _customMetrics(metricName) = _customMetrics(metricName) ++ List(metricValue)
    } else {
      _customMetrics(metricName) = List(metricValue)
    }
  }
}

private[spark] object TaskMetrics {
  def empty: TaskMetrics = new TaskMetrics
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
  var totalBlocksFetched: Int = _

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
