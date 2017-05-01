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
import javax.ws.rs.{GET, Produces}
import javax.ws.rs.core.MediaType

import org.apache.spark.streaming.ui.StreamingJobProgressListener

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StreamingStatisticsResource(listener: StreamingJobProgressListener) {

  @GET
  def streamingStatistics(): StreamingStatistics = {
    listener.synchronized {
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
  }

  private def avgRate(data: Seq[Double]): Option[Double] = {
    if (data.isEmpty) None else Some(data.sum / data.size)
  }

  private def avgTime(data: Seq[Long]): Option[Long] = {
    if (data.isEmpty) None else Some(data.sum / data.size)
  }
}
