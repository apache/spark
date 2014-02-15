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

import org.apache.spark.scheduler.JsonSerializable

import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._
import net.liftweb.json.DefaultFormats

class TaskMetrics extends Serializable with JsonSerializable {
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
   * If this task writes to shuffle output, metrics on the written shuffle data will be
   * collected here
   */
  var shuffleWriteMetrics: Option[ShuffleWriteMetrics] = None

  override def toJson = {
    ("Host Name" -> hostname) ~
    ("Executor Deserialize Time" -> executorDeserializeTime) ~
    ("Executor Run Time" -> executorRunTime) ~
    ("Result Size" -> resultSize) ~
    ("JVM GC Time" -> jvmGCTime) ~
    ("Result Serialization Time" -> resultSerializationTime) ~
    ("Memory Bytes Spilled" -> memoryBytesSpilled) ~
    ("Disk Bytes Spilled" -> diskBytesSpilled) ~
    ("Shuffle Read Metrics" -> shuffleReadMetrics.map(_.toJson).getOrElse(JNothing)) ~
    ("Shuffle Write Metrics" -> shuffleWriteMetrics.map(_.toJson).getOrElse(JNothing))
  }
}

class ShuffleReadMetrics extends Serializable with JsonSerializable {
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
   * Total time spent fetching remote shuffle blocks. This aggregates the time spent fetching all
   * input blocks. Since block fetches are both pipelined and parallelized, this can
   * exceed fetchWaitTime and executorRunTime.
   */
  var remoteFetchTime: Long = _

  /**
   * Total number of remote bytes read from the shuffle by this task
   */
  var remoteBytesRead: Long = _

  override def toJson = {
    ("Shuffle Finish Time" -> shuffleFinishTime) ~
    ("Total Blocks Fetched" -> totalBlocksFetched) ~
    ("Remote Blocks Fetched" -> remoteBlocksFetched) ~
    ("Local Blocks Fetched" -> localBlocksFetched) ~
    ("Fetch Wait Time" -> fetchWaitTime) ~
    ("Remote Fetch Time" -> remoteFetchTime) ~
    ("Remote Bytes Read" -> remoteBytesRead)
  }
}

class ShuffleWriteMetrics extends Serializable with JsonSerializable {
  /**
   * Number of bytes written for the shuffle by this task
   */
  var shuffleBytesWritten: Long = _

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds
   */
  var shuffleWriteTime: Long = _

  override def toJson = {
    ("Shuffle Bytes Written" -> shuffleBytesWritten) ~
    ("Shuffle Write Time" -> shuffleWriteTime)
  }
}

object TaskMetrics {
  private[spark] def empty(): TaskMetrics = new TaskMetrics

  def fromJson(json: JValue): TaskMetrics = {
    implicit val format = DefaultFormats
    val metrics = new TaskMetrics
    metrics.hostname = (json \ "Host Name").extract[String]
    metrics.executorDeserializeTime = (json \ "Executor Deserialize Time").extract[Long]
    metrics.executorRunTime = (json \ "Executor Run Time").extract[Long]
    metrics.jvmGCTime = (json \ "JVM GC Time").extract[Long]
    metrics.resultSerializationTime = (json \ "Result Serialization Time").extract[Long]
    metrics.memoryBytesSpilled = (json \ "Memory Bytes Spilled").extract[Long]
    metrics.diskBytesSpilled = (json \ "Disk Bytes Spilled").extract[Long]
    metrics.shuffleReadMetrics =
      json \ "Shuffle Read Metrics" match {
        case JNothing => None
        case value: JValue => Some(ShuffleReadMetrics.fromJson(value))
      }
    metrics.shuffleWriteMetrics =
      json \ "Shuffle Write Metrics" match {
        case JNothing => None
        case value: JValue => Some(ShuffleWriteMetrics.fromJson(value))
      }
    metrics
  }
}

object ShuffleReadMetrics {
  def fromJson(json: JValue): ShuffleReadMetrics = {
    implicit val format = DefaultFormats
    val metrics = new ShuffleReadMetrics
    metrics.shuffleFinishTime = (json \ "Shuffle Finish Time").extract[Long]
    metrics.totalBlocksFetched = (json \ "Total Blocks Fetched").extract[Int]
    metrics.remoteBlocksFetched = (json \ "Remote Blocks Fetched").extract[Int]
    metrics.localBlocksFetched = (json \ "Local Blocks Fetched").extract[Int]
    metrics.fetchWaitTime = (json \ "Fetch Wait Time").extract[Long]
    metrics.remoteFetchTime = (json \ "Remote Fetch Time").extract[Long]
    metrics.remoteBytesRead = (json \ "Remote Bytes Read").extract[Long]
    metrics
  }
}

object ShuffleWriteMetrics {
  def fromJson(json: JValue): ShuffleWriteMetrics = {
    implicit val format = DefaultFormats
    val metrics = new ShuffleWriteMetrics
    metrics.shuffleBytesWritten = (json \ "Shuffle Bytes Written").extract[Long]
    metrics.shuffleWriteTime = (json \ "Shuffle Write Time").extract[Long]
    metrics
  }
}
