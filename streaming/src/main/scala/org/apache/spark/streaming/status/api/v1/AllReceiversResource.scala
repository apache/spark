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

package org.apache.spark.streaming.status.api.v1

import java.util.Date
import javax.ws.rs.{GET, Produces}
import javax.ws.rs.core.MediaType

import org.apache.spark.streaming.ui.StreamingJobProgressListener
import org.apache.spark.streaming.status.api.v1.AllReceiversResource._

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class AllReceiversResource(listener: StreamingJobProgressListener) {

  @GET
  def receiversList(): Seq[ReceiverInfo] = {
    receiverInfoList(listener).sortBy(_.streamId)
  }
}

private[v1] object AllReceiversResource {

  def receiverInfoList(listener: StreamingJobProgressListener): Seq[ReceiverInfo] = {
    listener.synchronized {
      listener.receivedEventRateWithBatchTime.map { case (streamId, eventRates) =>

        val receiverInfo = listener.receiverInfo(streamId)
        val streamName = receiverInfo.map(_.name).
          orElse(listener.streamName(streamId)).getOrElse(s"Stream-$streamId")
        val avgEventRate =
          if (eventRates.isEmpty) None
          else Some(eventRates.map(_._2).sum / eventRates.size)

        val lastErrorInfo = receiverInfo match {
          case None => (None, None, None)
          case Some(info) =>
            val someTime =
              if (info.lastErrorTime >= 0) Some(new Date(info.lastErrorTime))
              else None
            val someMessage =
              if (info.lastErrorMessage.length > 0) Some(info.lastErrorMessage)
              else None
            val someError =
              if (info.lastError.length > 0) Some(info.lastError)
              else None

            (someTime, someMessage, someError)
        }

        new ReceiverInfo(
          streamId = streamId,
          streamName = streamName,
          isActive = receiverInfo.map(_.active),
          executorId = receiverInfo.map(_.executorId),
          executorHost = receiverInfo.map(_.location),
          lastErrorTime = lastErrorInfo._1,
          lastErrorMessage = lastErrorInfo._2,
          lastError = lastErrorInfo._3,
          avgEventRate = avgEventRate,
          eventRates = eventRates
        )
      }.toSeq
    }
  }
}
