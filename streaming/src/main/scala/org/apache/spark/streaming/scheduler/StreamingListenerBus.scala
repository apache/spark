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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.Logging
import org.apache.spark.util.AsynchronousListenerBus

/** Asynchronously passes StreamingListenerEvents to registered StreamingListeners. */
private[spark] class StreamingListenerBus
  extends AsynchronousListenerBus[StreamingListener, StreamingListenerEvent]("StreamingListenerBus")
  with Logging {

  private val logDroppedEvent = new AtomicBoolean(false)

  override def onPostEvent(listener: StreamingListener, event: StreamingListenerEvent): Unit = {
    event match {
      case receiverStarted: StreamingListenerReceiverStarted =>
        listener.onReceiverStarted(receiverStarted)
      case receiverError: StreamingListenerReceiverError =>
        listener.onReceiverError(receiverError)
      case receiverStopped: StreamingListenerReceiverStopped =>
        listener.onReceiverStopped(receiverStopped)
      case batchSubmitted: StreamingListenerBatchSubmitted =>
        listener.onBatchSubmitted(batchSubmitted)
      case batchStarted: StreamingListenerBatchStarted =>
        listener.onBatchStarted(batchStarted)
      case batchCompleted: StreamingListenerBatchCompleted =>
        listener.onBatchCompleted(batchCompleted)
      case outputOperationStarted: StreamingListenerOutputOperationStarted =>
        listener.onOutputOperationStarted(outputOperationStarted)
      case outputOperationCompleted: StreamingListenerOutputOperationCompleted =>
        listener.onOutputOperationCompleted(outputOperationCompleted)
      case _ =>
    }
  }

  override def onDropEvent(event: StreamingListenerEvent): Unit = {
    if (logDroppedEvent.compareAndSet(false, true)) {
      // Only log the following message once to avoid duplicated annoying logs.
      logError("Dropping StreamingListenerEvent because no remaining room in event queue. " +
        "This likely means one of the StreamingListeners is too slow and cannot keep up with the " +
        "rate at which events are being started by the scheduler.")
    }
  }
}
