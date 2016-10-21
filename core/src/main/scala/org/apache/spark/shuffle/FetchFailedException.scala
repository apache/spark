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

import org.apache.spark.{FetchFailed, TaskFailedReason}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Failed to fetch a shuffle block. The executor catches this exception and propagates it
 * back to DAGScheduler (through TaskEndReason) so we'd resubmit the previous stage.
 *
 * Note that bmAddress can be null.
 */
private[spark] class FetchFailedException(
    bmAddress: BlockManagerId,
    shuffleId: Int,
    mapId: Int,
    reduceId: Int,
    message: String,
    cause: Throwable = null)
  extends Exception(message, cause) {

  def this(
      bmAddress: BlockManagerId,
      shuffleId: Int,
      mapId: Int,
      reduceId: Int,
      cause: Throwable) {
    this(bmAddress, shuffleId, mapId, reduceId, cause.getMessage, cause)
  }

  def toTaskFailedReason: TaskFailedReason = FetchFailed(bmAddress, shuffleId, mapId, reduceId,
    Utils.exceptionString(this))
}

/**
 * Failed to get shuffle metadata from [[org.apache.spark.MapOutputTracker]].
 */
private[spark] class MetadataFetchFailedException(
    shuffleId: Int,
    reduceId: Int,
    message: String)
  extends FetchFailedException(null, shuffleId, -1, reduceId, message)
