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

import java.util.{Arrays => JArrays, Collections, Date, List => JList}

import jakarta.ws.rs.{GET, Path, PathParam, Produces, QueryParam}
import jakarta.ws.rs.core.MediaType

import org.apache.spark.status.api.v1.NotFoundException
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.ui.StreamingJobProgressListener._

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class ApiStreamingRootResource extends BaseStreamingAppResource {

  @GET
  @Path("statistics")
  def streamingStatistics(): StreamingStatistics = withListener { listener =>
    val batches = listener.retainedBatches
    val avgInputRate = avgRate(batches.map(_.numRecords * 1000.0 / listener.batchDuration))
    val avgSchedulingDelay = avgTime(batches.flatMap(_.schedulingDelay))
    val avgProcessingTime = avgTime(batches.flatMap(_.processingDelay))
    val avgTotalDelay = avgTime(batches.flatMap(_.totalDelay))

    new StreamingStatistics(
      startTime = new Date(listener.startTime),
      batchDuration = listener.batchDuration,
      numReceivers = listener.numReceivers,
      numActiveReceivers = listener.numActiveReceivers,
      numInactiveReceivers = listener.numInactiveReceivers,
      numTotalCompletedBatches = listener.numTotalCompletedBatches,
      numRetainedCompletedBatches = listener.retainedCompletedBatches.size,
      numActiveBatches = listener.numUnprocessedBatches,
      numProcessedRecords = listener.numTotalProcessedRecords,
      numReceivedRecords = listener.numTotalReceivedRecords,
      avgInputRate = avgInputRate,
      avgSchedulingDelay = avgSchedulingDelay,
      avgProcessingTime = avgProcessingTime,
      avgTotalDelay = avgTotalDelay
    )
  }

  @GET
  @Path("receivers")
  def receiversList(): Seq[ReceiverInfo] = withListener { listener =>
    listener.receivedRecordRateWithBatchTime.map { case (streamId, eventRates) =>
      val receiverInfo = listener.receiverInfo(streamId)
      val streamName = receiverInfo.map(_.name)
        .orElse(listener.streamName(streamId)).getOrElse(s"Stream-$streamId")
      val avgEventRate =
        if (eventRates.isEmpty) None else Some(eventRates.map(_._2).sum / eventRates.size)

      val (errorTime, errorMessage, error) = receiverInfo match {
        case None => (None, None, None)
        case Some(info) =>
          val someTime =
            if (info.lastErrorTime >= 0) Some(new Date(info.lastErrorTime)) else None
          val someMessage =
            if (info.lastErrorMessage.length > 0) Some(info.lastErrorMessage) else None
          val someError =
            if (info.lastError.length > 0) Some(info.lastError) else None

          (someTime, someMessage, someError)
      }

      new ReceiverInfo(
        streamId = streamId,
        streamName = streamName,
        isActive = receiverInfo.map(_.active),
        executorId = receiverInfo.map(_.executorId),
        executorHost = receiverInfo.map(_.location),
        lastErrorTime = errorTime,
        lastErrorMessage = errorMessage,
        lastError = error,
        avgEventRate = avgEventRate,
        eventRates = eventRates
      )
    }.toSeq.sortBy(_.streamId)
  }

  @GET
  @Path("receivers/{streamId: \\d+}")
  def oneReceiver(@PathParam("streamId") streamId: Int): ReceiverInfo = {
    receiversList().find { _.streamId == streamId }.getOrElse(
      throw new NotFoundException("unknown receiver: " + streamId))
  }

  @GET
  @Path("batches")
  def batchesList(@QueryParam("status") statusParams: JList[BatchStatus]): Seq[BatchInfo] = {
    withListener { listener =>
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

      batchInfos.sortBy(- _.batchId)
    }
  }

  @GET
  @Path("batches/{batchId: \\d+}")
  def oneBatch(@PathParam("batchId") batchId: Long): BatchInfo = {
    batchesList(Collections.emptyList()).find { _.batchId == batchId }.getOrElse(
      throw new NotFoundException("unknown batch: " + batchId))
  }

  @GET
  @Path("batches/{batchId: \\d+}/operations")
  def operationsList(@PathParam("batchId") batchId: Long): Seq[OutputOperationInfo] = {
    withListener { listener =>
      val ops = listener.getBatchUIData(Time(batchId)) match {
        case Some(batch) =>
          for ((opId, op) <- batch.outputOperations) yield {
            val jobIds = batch.outputOpIdSparkJobIdPairs
              .filter(_.outputOpId == opId).map(_.sparkJobId).toSeq.sorted

            new OutputOperationInfo(
              outputOpId = opId,
              name = op.name,
              description = op.description,
              startTime = op.startTime.map(new Date(_)),
              endTime = op.endTime.map(new Date(_)),
              duration = op.duration,
              failureReason = op.failureReason,
              jobIds = jobIds
            )
          }
        case None => throw new NotFoundException("unknown batch: " + batchId)
      }
      ops.toSeq
    }
  }

  @GET
  @Path("batches/{batchId: \\d+}/operations/{outputOpId: \\d+}")
  def oneOperation(
      @PathParam("batchId") batchId: Long,
      @PathParam("outputOpId") opId: OutputOpId): OutputOperationInfo = {
    operationsList(batchId).find { _.outputOpId == opId }.getOrElse(
      throw new NotFoundException("unknown output operation: " + opId))
  }

  private def avgRate(data: Seq[Double]): Option[Double] = {
    if (data.isEmpty) None else Some(data.sum / data.size)
  }

  private def avgTime(data: Seq[Long]): Option[Long] = {
    if (data.isEmpty) None else Some(data.sum / data.size)
  }

}
