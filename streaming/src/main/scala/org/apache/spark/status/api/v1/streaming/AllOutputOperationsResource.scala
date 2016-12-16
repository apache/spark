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

import java.util.Date
import javax.ws.rs.{GET, PathParam, Produces}
import javax.ws.rs.core.MediaType

import org.apache.spark.status.api.v1.NotFoundException
import org.apache.spark.status.api.v1.streaming.AllOutputOperationsResource._
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.ui.StreamingJobProgressListener

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class AllOutputOperationsResource(listener: StreamingJobProgressListener) {

  @GET
  def operationsList(@PathParam("batchId") batchId: Long): Seq[OutputOperationInfo] = {
    outputOperationInfoList(listener, batchId).sortBy(_.outputOpId)
  }
}

private[v1] object AllOutputOperationsResource {

  def outputOperationInfoList(
      listener: StreamingJobProgressListener,
      batchId: Long): Seq[OutputOperationInfo] = {

    listener.synchronized {
      listener.getBatchUIData(Time(batchId)) match {
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
    }.toSeq
  }
}
