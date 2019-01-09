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

import org.apache.spark.streaming.Time

private[streaming] trait PythonStreamingListener{

  /** Called when the streaming has been started */
  def onStreamingStarted(streamingStarted: JavaStreamingListenerStreamingStarted) { }

  /** Called when a receiver has been started */
  def onReceiverStarted(receiverStarted: JavaStreamingListenerReceiverStarted) { }

  /** Called when a receiver has reported an error */
  def onReceiverError(receiverError: JavaStreamingListenerReceiverError) { }

  /** Called when a receiver has been stopped */
  def onReceiverStopped(receiverStopped: JavaStreamingListenerReceiverStopped) { }

  /** Called when a batch of jobs has been submitted for processing. */
  def onBatchSubmitted(batchSubmitted: JavaStreamingListenerBatchSubmitted) { }

  /** Called when processing of a batch of jobs has started.  */
  def onBatchStarted(batchStarted: JavaStreamingListenerBatchStarted) { }

  /** Called when processing of a batch of jobs has completed. */
  def onBatchCompleted(batchCompleted: JavaStreamingListenerBatchCompleted) { }

  /** Called when processing of a job of a batch has started. */
  def onOutputOperationStarted(
      outputOperationStarted: JavaStreamingListenerOutputOperationStarted) { }

  /** Called when processing of a job of a batch has completed. */
  def onOutputOperationCompleted(
      outputOperationCompleted: JavaStreamingListenerOutputOperationCompleted) { }
}

private[streaming] class PythonStreamingListenerWrapper(listener: PythonStreamingListener)
  extends JavaStreamingListener {

  /** Called when the streaming has been started */
  override def onStreamingStarted(streamingStarted: JavaStreamingListenerStreamingStarted): Unit = {
    listener.onStreamingStarted(streamingStarted)
  }

  /** Called when a receiver has been started */
  override def onReceiverStarted(receiverStarted: JavaStreamingListenerReceiverStarted): Unit = {
    listener.onReceiverStarted(receiverStarted)
  }

  /** Called when a receiver has reported an error */
  override def onReceiverError(receiverError: JavaStreamingListenerReceiverError): Unit = {
    listener.onReceiverError(receiverError)
  }

  /** Called when a receiver has been stopped */
  override def onReceiverStopped(receiverStopped: JavaStreamingListenerReceiverStopped): Unit = {
    listener.onReceiverStopped(receiverStopped)
  }

  /** Called when a batch of jobs has been submitted for processing. */
  override def onBatchSubmitted(batchSubmitted: JavaStreamingListenerBatchSubmitted): Unit = {
    listener.onBatchSubmitted(batchSubmitted)
  }

  /** Called when processing of a batch of jobs has started.  */
  override def onBatchStarted(batchStarted: JavaStreamingListenerBatchStarted): Unit = {
    listener.onBatchStarted(batchStarted)
  }

  /** Called when processing of a batch of jobs has completed. */
  override def onBatchCompleted(batchCompleted: JavaStreamingListenerBatchCompleted): Unit = {
    listener.onBatchCompleted(batchCompleted)
  }

  /** Called when processing of a job of a batch has started. */
  override def onOutputOperationStarted(
    outputOperationStarted: JavaStreamingListenerOutputOperationStarted): Unit = {
      listener.onOutputOperationStarted(outputOperationStarted)
  }

  /** Called when processing of a job of a batch has completed. */
  override def onOutputOperationCompleted(
    outputOperationCompleted: JavaStreamingListenerOutputOperationCompleted): Unit = {
      listener.onOutputOperationCompleted(outputOperationCompleted)
  }
}

/**
 * A listener interface for receiving information about an ongoing streaming  computation.
 */
private[streaming] class JavaStreamingListener {

  /** Called when the streaming has been started */
  def onStreamingStarted(streamingStarted: JavaStreamingListenerStreamingStarted): Unit = { }

  /** Called when a receiver has been started */
  def onReceiverStarted(receiverStarted: JavaStreamingListenerReceiverStarted): Unit = { }

  /** Called when a receiver has reported an error */
  def onReceiverError(receiverError: JavaStreamingListenerReceiverError): Unit = { }

  /** Called when a receiver has been stopped */
  def onReceiverStopped(receiverStopped: JavaStreamingListenerReceiverStopped): Unit = { }

  /** Called when a batch of jobs has been submitted for processing. */
  def onBatchSubmitted(batchSubmitted: JavaStreamingListenerBatchSubmitted): Unit = { }

  /** Called when processing of a batch of jobs has started.  */
  def onBatchStarted(batchStarted: JavaStreamingListenerBatchStarted): Unit = { }

  /** Called when processing of a batch of jobs has completed. */
  def onBatchCompleted(batchCompleted: JavaStreamingListenerBatchCompleted): Unit = { }

  /** Called when processing of a job of a batch has started. */
  def onOutputOperationStarted(
      outputOperationStarted: JavaStreamingListenerOutputOperationStarted): Unit = { }

  /** Called when processing of a job of a batch has completed. */
  def onOutputOperationCompleted(
      outputOperationCompleted: JavaStreamingListenerOutputOperationCompleted): Unit = { }
}

/**
 * Base trait for events related to JavaStreamingListener
 */
private[streaming] sealed trait JavaStreamingListenerEvent

private[streaming] class JavaStreamingListenerStreamingStarted(val time: Long)
  extends JavaStreamingListenerEvent

private[streaming] class JavaStreamingListenerBatchSubmitted(val batchInfo: JavaBatchInfo)
  extends JavaStreamingListenerEvent

private[streaming] class JavaStreamingListenerBatchCompleted(val batchInfo: JavaBatchInfo)
  extends JavaStreamingListenerEvent

private[streaming] class JavaStreamingListenerBatchStarted(val batchInfo: JavaBatchInfo)
  extends JavaStreamingListenerEvent

private[streaming] class JavaStreamingListenerOutputOperationStarted(
    val outputOperationInfo: JavaOutputOperationInfo) extends JavaStreamingListenerEvent

private[streaming] class JavaStreamingListenerOutputOperationCompleted(
    val outputOperationInfo: JavaOutputOperationInfo) extends JavaStreamingListenerEvent

private[streaming] class JavaStreamingListenerReceiverStarted(val receiverInfo: JavaReceiverInfo)
  extends JavaStreamingListenerEvent

private[streaming] class JavaStreamingListenerReceiverError(val receiverInfo: JavaReceiverInfo)
  extends JavaStreamingListenerEvent

private[streaming] class JavaStreamingListenerReceiverStopped(val receiverInfo: JavaReceiverInfo)
  extends JavaStreamingListenerEvent

/**
 * Class having information on batches.
 *
 * @param batchTime Time of the batch
 * @param streamIdToInputInfo A map of input stream id to its input info
 * @param submissionTime Clock time of when jobs of this batch was submitted to the streaming
 *                       scheduler queue
 * @param processingStartTime Clock time of when the first job of this batch started processing.
 *                            `-1` means the batch has not yet started
 * @param processingEndTime Clock time of when the last job of this batch finished processing. `-1`
 *                          means the batch has not yet completed.
 * @param schedulingDelay Time taken for the first job of this batch to start processing from the
 *                        time this batch was submitted to the streaming scheduler. Essentially, it
 *                        is `processingStartTime` - `submissionTime`. `-1` means the batch has not
 *                        yet started
 * @param processingDelay Time taken for the all jobs of this batch to finish processing from the
 *                        time they started processing. Essentially, it is
 *                        `processingEndTime` - `processingStartTime`. `-1` means the batch has not
 *                        yet completed.
 * @param totalDelay Time taken for all the jobs of this batch to finish processing from the time
 *                   they were submitted.  Essentially, it is `processingDelay` + `schedulingDelay`.
 *                   `-1` means the batch has not yet completed.
 * @param numRecords The number of recorders received by the receivers in this batch
 * @param outputOperationInfos The output operations in this batch
 */
private[streaming] case class JavaBatchInfo(
    batchTime: Time,
    streamIdToInputInfo: java.util.Map[Int, JavaStreamInputInfo],
    submissionTime: Long,
    processingStartTime: Long,
    processingEndTime: Long,
    schedulingDelay: Long,
    processingDelay: Long,
    totalDelay: Long,
    numRecords: Long,
    outputOperationInfos: java.util.Map[Int, JavaOutputOperationInfo])

/**
 * Track the information of input stream at specified batch time.
 *
 * @param inputStreamId the input stream id
 * @param numRecords the number of records in a batch
 * @param metadata metadata for this batch. It should contain at least one standard field named
 *                 "Description" which maps to the content that will be shown in the UI.
 * @param metadataDescription description of this input stream
 */
private[streaming] case class JavaStreamInputInfo(
    inputStreamId: Int,
    numRecords: Long,
    metadata: java.util.Map[String, Any],
    metadataDescription: String)

/**
 * Class having information about a receiver
 */
private[streaming] case class JavaReceiverInfo(
    streamId: Int,
    name: String,
    active: Boolean,
    location: String,
    executorId: String,
    lastErrorMessage: String,
    lastError: String,
    lastErrorTime: Long)

/**
 * Class having information on output operations.
 *
 * @param batchTime Time of the batch
 * @param id Id of this output operation. Different output operations have different ids in a batch.
 * @param name The name of this output operation.
 * @param description The description of this output operation.
 * @param startTime Clock time of when the output operation started processing. `-1` means the
 *                  output operation has not yet started
 * @param endTime Clock time of when the output operation started processing. `-1` means the output
 *                operation has not yet completed
 * @param failureReason Failure reason if this output operation fails. If the output operation is
 *                      successful, this field is `null`.
 */
private[streaming] case class JavaOutputOperationInfo(
    batchTime: Time,
    id: Int,
    name: String,
    description: String,
    startTime: Long,
    endTime: Long,
    failureReason: String)
