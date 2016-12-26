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

package org.apache.spark.status.api.v1.streaming

import java.util.{ArrayList => JArrayList, Arrays => JArrays, Date, List => JList}
import javax.ws.rs.{GET, Produces, QueryParam}
import javax.ws.rs.core.MediaType

import org.apache.spark.status.api.v1.streaming.AllBatchesResource._
import org.apache.spark.streaming.ui.StreamingJobProgressListener

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class AllBatchesResource(listener: StreamingJobProgressListener) {

  @GET
  def batchesList(@QueryParam("status") statusParams: JList[BatchStatus]): Seq[BatchInfo] = {
    batchInfoList(listener, statusParams).sortBy(- _.batchId)
  }
}

private[v1] object AllBatchesResource {

  def batchInfoList(
      listener: StreamingJobProgressListener,
      statusParams: JList[BatchStatus] = new JArrayList[BatchStatus]()): Seq[BatchInfo] = {

    listener.synchronized {
      val statuses =
        if (statusParams.isEmpty) JArrays.asList(BatchStatus.values(): _*) else statusParams
      val statusToBatches = Seq(
        BatchStatus.COMPLETED -> listener.retainedCompletedBatches,
        BatchStatus.QUEUED -> listener.waitingBatches,
        BatchStatus.PROCESSING -> listener.runningBatches
      )

      val batchInfos = for {
        (status, batches) <- statusToBatches
        batch <- batches if statuses.contains(status)
      } yield {
        val batchId = batch.batchTime.milliseconds
        val firstFailureReason = batch.outputOperations.flatMap(_._2.failureReason).headOption

        new BatchInfo(
          batchId = batchId,
          batchTime = new Date(batchId),
          status = status.toString,
          batchDuration = listener.batchDuration,
          inputSize = batch.numRecords,
          schedulingDelay = batch.schedulingDelay,
          processingTime = batch.processingDelay,
          totalDelay = batch.totalDelay,
          numActiveOutputOps = batch.numActiveOutputOp,
          numCompletedOutputOps = batch.numCompletedOutputOp,
          numFailedOutputOps = batch.numFailedOutputOp,
          numTotalOutputOps = batch.outputOperations.size,
          firstFailureReason = firstFailureReason
        )
      }

      batchInfos
    }
  }
}
