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

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.streaming.ui.{StreamingJobProgressListener, StreamingTab}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.ListenerBus

/**
 * A Streaming listener bus to forward events to StreamingListeners. This one will forward received
 * Streaming events as SparkListenerEvent and send them to Spark listener bus. It also
 * registers itself with Spark listener bus, so that it can receive SparkListenerEvents,
 * forward them as StreamingListenerEvent and dispatch them to StreamingListeners.
 */
private[streaming] abstract class StreamingListenerBus
  extends SparkListener with ListenerBus[StreamingListener, StreamingListenerEvent] {

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: StreamingListenerEvent =>
        postToAll(e)
      case _ =>
    }
  }

  protected override def doPostEvent(
      listener: StreamingListener,
      event: StreamingListenerEvent): Unit = {
    event match {
      case applicationStarted: StreamingListenerApplicationStart =>
        listener.onStreamingApplicationStarted(applicationStarted)
      case applicationEnd: StreamingListenerApplicationEnd =>
        listener.onStreamingApplicationEnd(applicationEnd)
      case inputStreamRegistered: StreamingListenerInputStreamRegistered =>
        listener.onInputStreamRegistered(inputStreamRegistered)
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
}

private[streaming] class LiveStreamingListenerBus(sparkListenerBus: LiveListenerBus)
  extends StreamingListenerBus {

  /**
   * Post a StreamingListenerEvent to the Spark listener bus asynchronously. This event will be
   * dispatched to all StreamingListeners in the thread of the Spark listener bus.
   */
  def post(event: StreamingListenerEvent) {
    sparkListenerBus.post(event)
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
}

private[streaming] class ReplayStreamingListenerBus extends StreamingListenerBus

private[streaming] class StreamingHistoryListenerFactory extends SparkHistoryListenerFactory {
  override def createListeners(conf: SparkConf, sparkUI: SparkUI): Seq[SparkListener] = {
    val listenerBus = new ReplayStreamingListenerBus()
    val streamingListener = new StreamingJobProgressListener(conf)
    listenerBus.addListener(streamingListener)
    val streamingTab = new StreamingTab(streamingListener, sparkUI)
    streamingTab.attach()
    List(listenerBus)
  }
}
