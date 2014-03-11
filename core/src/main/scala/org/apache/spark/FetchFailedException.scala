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

package org.apache.spark

import org.apache.spark.storage.BlockManagerId

private[spark] class FetchFailedException(
    taskEndReason: TaskEndReason,
    message: String,
    cause: Throwable)
  extends Exception {

  def this (bmAddress: BlockManagerId, shuffleId: Int, mapId: Int, reduceId: Int,
      cause: Throwable) =
    this(FetchFailed(bmAddress, shuffleId, mapId, reduceId),
      "Fetch failed: %s %d %d %d".format(bmAddress, shuffleId, mapId, reduceId),
      cause)

  def this (shuffleId: Int, reduceId: Int, cause: Throwable) =
    this(FetchFailed(null, shuffleId, -1, reduceId),
      "Unable to fetch locations from master: %d %d".format(shuffleId, reduceId), cause)

  override def getMessage(): String = message


  override def getCause(): Throwable = cause

  def toTaskEndReason: TaskEndReason = taskEndReason

}
