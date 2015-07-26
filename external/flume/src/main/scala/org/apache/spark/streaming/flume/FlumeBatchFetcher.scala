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
package org.apache.spark.streaming.flume

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import com.google.common.base.Throwables

import org.apache.spark.Logging
import org.apache.spark.streaming.flume.sink._

/**
 * This class implements the core functionality of [[FlumePollingReceiver]]. When started it
 * pulls data from Flume, stores it to Spark and then sends an Ack or Nack. This class should be
 * run via an [[java.util.concurrent.Executor]] as this implements [[Runnable]]
 *
 * @param receiver The receiver that owns this instance.
 */

private[flume] class FlumeBatchFetcher(receiver: FlumePollingReceiver) extends Runnable with
  Logging {

  def run(): Unit = {
    while (!receiver.isStopped()) {
      val connection = receiver.getConnections.poll()
      val client = connection.client
      var batchReceived = false
      var seq: CharSequence = null
      try {
        getBatch(client) match {
          case Some(eventBatch) =>
            batchReceived = true
            seq = eventBatch.getSequenceNumber
            val events = toSparkFlumeEvents(eventBatch.getEvents)
            if (store(events)) {
              sendAck(client, seq)
            } else {
              sendNack(batchReceived, client, seq)
            }
          case None =>
        }
      } catch {
        case e: Exception =>
          Throwables.getRootCause(e) match {
            // If the cause was an InterruptedException, then check if the receiver is stopped -
            // if yes, just break out of the loop. Else send a Nack and log a warning.
            // In the unlikely case, the cause was not an Exception,
            // then just throw it out and exit.
            case interrupted: InterruptedException =>
              if (!receiver.isStopped()) {
                logWarning("Interrupted while receiving data from Flume", interrupted)
                sendNack(batchReceived, client, seq)
              }
            case exception: Exception =>
              logWarning("Error while receiving data from Flume", exception)
              sendNack(batchReceived, client, seq)
          }
      } finally {
        receiver.getConnections.add(connection)
      }
    }
  }

  /**
   * Gets a batch of events from the specified client. This method does not handle any exceptions
   * which will be propogated to the caller.
   * @param client Client to get events from
   * @return [[Some]] which contains the event batch if Flume sent any events back, else [[None]]
   */
  private def getBatch(client: SparkFlumeProtocol.Callback): Option[EventBatch] = {
    val eventBatch = client.getEventBatch(receiver.getMaxBatchSize)
    if (!SparkSinkUtils.isErrorBatch(eventBatch)) {
      // No error, proceed with processing data
      logDebug(s"Received batch of ${eventBatch.getEvents.size} events with sequence " +
        s"number: ${eventBatch.getSequenceNumber}")
      Some(eventBatch)
    } else {
      logWarning("Did not receive events from Flume agent due to error on the Flume agent: " +
        eventBatch.getErrorMsg)
      None
    }
  }

  /**
   * Store the events in the buffer to Spark. This method will not propogate any exceptions,
   * but will propogate any other errors.
   * @param buffer The buffer to store
   * @return true if the data was stored without any exception being thrown, else false
   */
  private def store(buffer: ArrayBuffer[SparkFlumeEvent]): Boolean = {
    try {
      receiver.store(buffer)
      true
    } catch {
      case e: Exception =>
        logWarning("Error while attempting to store data received from Flume", e)
        false
    }
  }

  /**
   * Send an ack to the client for the sequence number. This method does not handle any exceptions
   * which will be propagated to the caller.
   * @param client client to send the ack to
   * @param seq sequence number of the batch to be ack-ed.
   * @return
   */
  private def sendAck(client: SparkFlumeProtocol.Callback, seq: CharSequence): Unit = {
    logDebug("Sending ack for sequence number: " + seq)
    client.ack(seq)
    logDebug("Ack sent for sequence number: " + seq)
  }

  /**
   * This method sends a Nack if a batch was received to the client with the given sequence
   * number. Any exceptions thrown by the RPC call is simply thrown out as is - no effort is made
   * to handle it.
   * @param batchReceived true if a batch was received. If this is false, no nack is sent
   * @param client The client to which the nack should be sent
   * @param seq The sequence number of the batch that is being nack-ed.
   */
  private def sendNack(batchReceived: Boolean, client: SparkFlumeProtocol.Callback,
    seq: CharSequence): Unit = {
    if (batchReceived) {
      // Let Flume know that the events need to be pushed back into the channel.
      logDebug("Sending nack for sequence number: " + seq)
      client.nack(seq) // If the agent is down, even this could fail and throw
      logDebug("Nack sent for sequence number: " + seq)
    }
  }

  /**
   * Utility method to convert [[SparkSinkEvent]]s to [[SparkFlumeEvent]]s
   * @param events - Events to convert to SparkFlumeEvents
   * @return - The SparkFlumeEvent generated from SparkSinkEvent
   */
  private def toSparkFlumeEvents(events: java.util.List[SparkSinkEvent]):
    ArrayBuffer[SparkFlumeEvent] = {
    // Convert each Flume event to a serializable SparkFlumeEvent
    val buffer = new ArrayBuffer[SparkFlumeEvent](events.size())
    var j = 0
    while (j < events.size()) {
      val event = events(j)
      val sparkFlumeEvent = new SparkFlumeEvent()
      sparkFlumeEvent.event.setBody(event.getBody)
      sparkFlumeEvent.event.setHeaders(event.getHeaders)
      buffer += sparkFlumeEvent
      j += 1
    }
    buffer
  }
}
