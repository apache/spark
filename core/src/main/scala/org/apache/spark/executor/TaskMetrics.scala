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

class TaskMetrics extends Serializable {
  /**
   * Host's name the task runs on 
   */
  var hostname: String = _

  /**
   * Time taken on the executor to deserialize this task
   */
  var executorDeserializeTime: Int = _

  /**
   * Time the executor spends actually running the task (including fetching shuffle data)
   */
  var executorRunTime: Int = _

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult
   */
  var resultSize: Long = _

  /**
   * Amount of time the JVM spent in garbage collection while executing this task
   */
  var jvmGCTime: Long = _

  /**
   * If this task reads from shuffle output, metrics on getting shuffle data will be collected here
   */
  var shuffleReadMetrics: Option[ShuffleReadMetrics] = None

  /**
   * If this task writes to shuffle output, metrics on the written shuffle data will be collected here
   */
  var shuffleWriteMetrics: Option[ShuffleWriteMetrics] = None
}

object TaskMetrics {
  private[spark] def empty(): TaskMetrics = new TaskMetrics
}


class ShuffleReadMetrics extends Serializable {
  /**
   * Time when shuffle finishs
   */
  var shuffleFinishTime: Long = _

  /**
   * Total number of blocks fetched in a shuffle (remote or local)
   */
  var totalBlocksFetched: Int = _

  /**
   * Number of remote blocks fetched in a shuffle
   */
  var remoteBlocksFetched: Int = _

  /**
   * Local blocks fetched in a shuffle
   */
  var localBlocksFetched: Int = _

  /**
   * Total time that is spent blocked waiting for shuffle to fetch data
   */
  var fetchWaitTime: Long = _

  /**
   * The total amount of time for all the shuffle fetches.  This adds up time from overlapping
   *     shuffles, so can be longer than task time
   */
  var remoteFetchTime: Long = _

  /**
   * Total number of remote bytes read from a shuffle
   */
  var remoteBytesRead: Long = _
}

class ShuffleWriteMetrics extends Serializable {
  /**
   * Number of bytes written for a shuffle
   */
  var shuffleBytesWritten: Long = _
}
