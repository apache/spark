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

import org.apache.spark.{FetchFailed, TaskContext, TaskFailedReason}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Failed to fetch a shuffle block. The executor catches this exception and propagates it
 * back to DAGScheduler (through TaskEndReason) so we'd resubmit the previous stage.
 *
 * Note that bmAddress can be null.
 *
 * To prevent user code from hiding this fetch failure, in the constructor we call
 * [[TaskContext.setFetchFailed()]].  This means that you *must* throw this exception immediately
 * after creating it -- you cannot create it, check some condition, and then decide to ignore it
 * (or risk triggering any other exceptions).  See SPARK-19276.
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

  // SPARK-19276. We set the fetch failure in the task context, so that even if there is user-code
  // which intercepts this exception (possibly wrapping it), the Executor can still tell there was
  // a fetch failure, and send the correct error msg back to the driver.  We wrap with an Option
  // because the TaskContext is not defined in some test cases.
  Option(TaskContext.get()).map(_.setFetchFailed(this))

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
