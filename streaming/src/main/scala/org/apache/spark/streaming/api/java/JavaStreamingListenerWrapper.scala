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

package org.apache.spark.streaming.api.java

import scala.collection.JavaConverters._

import org.apache.spark.streaming.scheduler._

/**
 * A wrapper to convert a [[JavaStreamingListener]] to a [[StreamingListener]].
 */
private[streaming] class JavaStreamingListenerWrapper(javaStreamingListener: JavaStreamingListener)
  extends StreamingListener {

  private def toJavaReceiverInfo(receiverInfo: ReceiverInfo): JavaReceiverInfo = {
    JavaReceiverInfo(
      receiverInfo.streamId,
      receiverInfo.name,
      receiverInfo.active,
      receiverInfo.location,
      receiverInfo.executorId,
      receiverInfo.lastErrorMessage,
      receiverInfo.lastError,
      receiverInfo.lastErrorTime
    )
  }

  private def toJavaStreamInputInfo(streamInputInfo: StreamInputInfo): JavaStreamInputInfo = {
    JavaStreamInputInfo(
      streamInputInfo.inputStreamId,
      streamInputInfo.numRecords: Long,
      streamInputInfo.metadata.asJava,
      streamInputInfo.metadataDescription.orNull
    )
  }

  private def toJavaOutputOperationInfo(
      outputOperationInfo: OutputOperationInfo): JavaOutputOperationInfo = {
    JavaOutputOperationInfo(
      outputOperationInfo.batchTime,
      outputOperationInfo.id,
      outputOperationInfo.name,
      outputOperationInfo.description: String,
      outputOperationInfo.startTime.getOrElse(-1),
      outputOperationInfo.endTime.getOrElse(-1),
      outputOperationInfo.failureReason.orNull
    )
  }

  private def toJavaBatchInfo(batchInfo: BatchInfo): JavaBatchInfo = {
    JavaBatchInfo(
      batchInfo.batchTime,
      batchInfo.streamIdToInputInfo.mapValues(toJavaStreamInputInfo).toMap.asJava,
      batchInfo.submissionTime,
      batchInfo.processingStartTime.getOrElse(-1),
      batchInfo.processingEndTime.getOrElse(-1),
      batchInfo.schedulingDelay.getOrElse(-1),
      batchInfo.processingDelay.getOrElse(-1),
      batchInfo.totalDelay.getOrElse(-1),
      batchInfo.numRecords,
      batchInfo.outputOperationInfos.mapValues(toJavaOutputOperationInfo).toMap.asJava
    )
  }

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    javaStreamingListener.onStreamingStarted(
      new JavaStreamingListenerStreamingStarted(streamingStarted.time))
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
    javaStreamingListener.onReceiverStarted(
      new JavaStreamingListenerReceiverStarted(toJavaReceiverInfo(receiverStarted.receiverInfo)))
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    javaStreamingListener.onReceiverError(
      new JavaStreamingListenerReceiverError(toJavaReceiverInfo(receiverError.receiverInfo)))
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
    javaStreamingListener.onReceiverStopped(
      new JavaStreamingListenerReceiverStopped(toJavaReceiverInfo(receiverStopped.receiverInfo)))
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    javaStreamingListener.onBatchSubmitted(
      new JavaStreamingListenerBatchSubmitted(toJavaBatchInfo(batchSubmitted.batchInfo)))
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    javaStreamingListener.onBatchStarted(
      new JavaStreamingListenerBatchStarted(toJavaBatchInfo(batchStarted.batchInfo)))
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    javaStreamingListener.onBatchCompleted(
      new JavaStreamingListenerBatchCompleted(toJavaBatchInfo(batchCompleted.batchInfo)))
  }

  override def onOutputOperationStarted(
      outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = {
    javaStreamingListener.onOutputOperationStarted(new JavaStreamingListenerOutputOperationStarted(
      toJavaOutputOperationInfo(outputOperationStarted.outputOperationInfo)))
  }

  override def onOutputOperationCompleted(
      outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    javaStreamingListener.onOutputOperationCompleted(
      new JavaStreamingListenerOutputOperationCompleted(
        toJavaOutputOperationInfo(outputOperationCompleted.outputOperationInfo)))
  }

}
