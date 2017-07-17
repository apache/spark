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

import org.apache.spark.scheduler.{LiveListenerBus, SparkListener, SparkListenerEvent}
import org.apache.spark.util.ListenerBus

/**
 * A Streaming listener bus to forward events to StreamingListeners. This one will wrap received
 * Streaming events as WrappedStreamingListenerEvent and send them to Spark listener bus. It also
 * registers itself with Spark listener bus, so that it can receive WrappedStreamingListenerEvents,
 * unwrap them as StreamingListenerEvent and dispatch them to StreamingListeners.
 */
private[streaming] class StreamingListenerBus(sparkListenerBus: LiveListenerBus)
  extends SparkListener with ListenerBus[StreamingListener, StreamingListenerEvent] {

  /**
   * Post a StreamingListenerEvent to the Spark listener bus asynchronously. This event will be
   * dispatched to all StreamingListeners in the thread of the Spark listener bus.
   */
  def post(event: StreamingListenerEvent) {
    sparkListenerBus.post(new WrappedStreamingListenerEvent(event))
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case WrappedStreamingListenerEvent(e) =>
        postToAll(e)
      case _ =>
    }
  }

  protected override def doPostEvent(
      listener: StreamingListener,
      event: StreamingListenerEvent): Unit = {
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
      case streamingStarted: StreamingListenerStreamingStarted =>
        listener.onStreamingStarted(streamingStarted)
      case _ =>
    }
  }

  /**
   * Register this one with the Spark listener bus so that it can receive Streaming events and
   * forward them to StreamingListeners.
   */
  def start(): Unit = {
    sparkListenerBus.addListener(this) // for getting callbacks on spark events
  }

  /**
   * Unregister this one with the Spark listener bus and all StreamingListeners won't receive any
   * events after that.
   */
  def stop(): Unit = {
    sparkListenerBus.removeListener(this)
  }

  /**
   * Wrapper for StreamingListenerEvent as SparkListenerEvent so that it can be posted to Spark
   * listener bus.
   */
  private case class WrappedStreamingListenerEvent(streamingListenerEvent: StreamingListenerEvent)
    extends SparkListenerEvent {

    // Do not log streaming events in event log as history server does not support streaming
    // events (SPARK-12140). TODO Once SPARK-12140 is resolved we should set it to true.
    protected[spark] override def logEvent: Boolean = false
  }
}
