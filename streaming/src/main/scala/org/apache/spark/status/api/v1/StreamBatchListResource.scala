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

package org.apache.spark.status.api.v1

import javax.ws.rs.{GET, Produces}
import javax.ws.rs.core.MediaType

import org.apache.spark.streaming.ui.StreamingJobProgressListener
import org.apache.spark.ui.SparkUI

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StreamBatchListResource(ui: SparkUI) {

  @GET
  def streamBatchInfoList(): Seq[StreamBatchInfoSummary] = {
    val listener = ui.getStreamingListener
    if (listener.isDefined) {
      val streamingJobProgressListener = listener.get.asInstanceOf[StreamingJobProgressListener]
      val waitingBatchUIData = streamingJobProgressListener.waitingBatchUIData
      val runningBatchUIData = streamingJobProgressListener.runningBatchUIData
      val completedBatchUIData = streamingJobProgressListener.completedBatchUIData

      runningBatchUIData.toArray.map(runningBatchUIData => {
        new StreamBatchInfoSummary(
          "runningBatch",
          runningBatchUIData._1.milliseconds,
          runningBatchUIData._2.submissionTime,
          runningBatchUIData._2.processingStartTime.getOrElse(-1L),
          runningBatchUIData._2.processingEndTime.getOrElse(-1L),
          runningBatchUIData._2.schedulingDelay.getOrElse(-1L),
          runningBatchUIData._2.processingDelay.getOrElse(-1L),
          runningBatchUIData._2.totalDelay.getOrElse(0L),
          runningBatchUIData._2.numRecords,
          runningBatchUIData._2.numActiveOutputOp,
          runningBatchUIData._2.numFailedOutputOp,
          runningBatchUIData._2.numCompletedOutputOp,
          runningBatchUIData._2.isFailed,
          runningBatchUIData._2.streamIdToInputInfo.map(e => {
            (e._1, (e._2.inputStreamId, e._2.numRecords, e._2.metadata))
          }))
      }) ++
      waitingBatchUIData.toArray.map(runningBatchUIData => {
        new StreamBatchInfoSummary(
          "waitingBatch",
          runningBatchUIData._1.milliseconds,
          runningBatchUIData._2.submissionTime,
          runningBatchUIData._2.processingStartTime.getOrElse(-1L),
          runningBatchUIData._2.processingEndTime.getOrElse(-1L),
          runningBatchUIData._2.schedulingDelay.getOrElse(-1L),
          runningBatchUIData._2.processingDelay.getOrElse(-1L),
          runningBatchUIData._2.totalDelay.getOrElse(0L),
          runningBatchUIData._2.numRecords,
          runningBatchUIData._2.numActiveOutputOp,
          runningBatchUIData._2.numFailedOutputOp,
          runningBatchUIData._2.numCompletedOutputOp,
          runningBatchUIData._2.isFailed,
          runningBatchUIData._2.streamIdToInputInfo.map(e => {
            (e._1, (e._2.inputStreamId, e._2.numRecords, e._2.metadata))
          }))
      }) ++
      completedBatchUIData.toArray.map(runningBatchUIData => {
        new StreamBatchInfoSummary(
          "completedBatch",
          runningBatchUIData.batchTime.milliseconds,
          runningBatchUIData.submissionTime,
          runningBatchUIData.processingStartTime.getOrElse(-1L),
          runningBatchUIData.processingEndTime.getOrElse(-1L),
          runningBatchUIData.schedulingDelay.getOrElse(-1L),
          runningBatchUIData.processingDelay.getOrElse(-1L),
          runningBatchUIData.totalDelay.getOrElse(0L),
          runningBatchUIData.numRecords,
          runningBatchUIData.numActiveOutputOp,
          runningBatchUIData.numFailedOutputOp,
          runningBatchUIData.numCompletedOutputOp,
          runningBatchUIData.isFailed,
          runningBatchUIData.streamIdToInputInfo.map(e => {
            (e._1, (e._2.inputStreamId, e._2.numRecords, e._2.metadata))
          }))
      })
    } else {
      Seq.empty
    }
  }
}
