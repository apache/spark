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

package org.apache.spark.streaming.scheduler

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, TaskLocation}
import org.apache.spark.streaming.scheduler.ReceiverState._

private[streaming] case class ReceiverErrorInfo(
    lastErrorMessage: String = "", lastError: String = "", lastErrorTime: Long = -1L)

/**
 * Class having information about a receiver.
 *
 * @param receiverId the unique receiver id
 * @param state the current Receiver state
 * @param scheduledLocations the scheduled locations provided by ReceiverSchedulingPolicy
 * @param runningExecutor the running executor if the receiver is active
 * @param name the receiver name
 * @param endpoint the receiver endpoint. It can be used to send messages to the receiver
 * @param errorInfo the receiver error information if it fails
 */
private[streaming] case class ReceiverTrackingInfo(
    receiverId: Int,
    state: ReceiverState,
    scheduledLocations: Option[Seq[TaskLocation]],
    runningExecutor: Option[ExecutorCacheTaskLocation],
    name: Option[String] = None,
    endpoint: Option[RpcEndpointRef] = None,
    errorInfo: Option[ReceiverErrorInfo] = None) {

  def toReceiverInfo: ReceiverInfo = ReceiverInfo(
    receiverId,
    name.getOrElse(""),
    state == ReceiverState.ACTIVE,
    location = runningExecutor.map(_.host).getOrElse(""),
    executorId = runningExecutor.map(_.executorId).getOrElse(""),
    lastErrorMessage = errorInfo.map(_.lastErrorMessage).getOrElse(""),
    lastError = errorInfo.map(_.lastError).getOrElse(""),
    lastErrorTime = errorInfo.map(_.lastErrorTime).getOrElse(-1L)
  )
}
