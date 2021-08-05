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

// scalastyle:off println
package org.apache.spark.examples.sql.streaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{LongType, StringType, StructType}


/**
 * Sessionize events in UTF8 encoded, '\n' delimited text received from the network.
 * Each line composes an event, and the line should match to the json format.
 *
 * The schema of the event is following:
 *
 * - user_id: String
 * - event_type: String
 * - timestamp: Long
 *
 * The supported types are following:
 *
 * - NEW_EVENT
 * - CLOSE_SESSION
 *
 * This example focuses to demonstrate the complex sessionization which uses two conditions
 * on closing session; conditions are following:
 *
 * - No further event is provided for the user ID within 10 seconds
 * - An event having CLOSE_SESSION as event_type is provided for the user ID
 *
 * Usage: StructuredComplexSessionization <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example sql.streaming.StructuredComplexSessionization
 * localhost 9999`
 */
object StructuredComplexSessionization {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: StructuredComplexSessionization <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt

    val spark = SparkSession
      .builder
      .appName("StructuredComplexSessionization")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    val jsonSchema = new StructType()
      .add("user_id", StringType, nullable = false)
      .add("event_type", StringType, nullable = false)
      .add("timestamp", LongType, nullable = false)

    val gapDuration = 1000 * 60 * 5 // 5 mins

    // Parse the line into event, as described in classdoc.
    val events = lines
      .select(from_json(col("value"), jsonSchema).as("event"))
      .selectExpr("event.user_id", "event.event_type", "event.timestamp")
      .as[(String, String, Long)]
      .map { case (userId, eventType, timestamp) =>
        SessionEvent(userId, EventTypes.withName(eventType), timestamp, gapDuration)
      }

    // Sessionize the events. Track number of events, start and end timestamps of session,
    // and report session when session is closed.
    // FIXME: ...implement from here...
    val sessionUpdates = events
      .groupByKey(event => event.userId)
      .mapGroupsWithState[List[SessionAcc], Session](GroupStateTimeout.EventTimeTimeout) {
        case (userId: String, events: Iterator[SessionEvent],
            state: GroupState[List[SessionAcc]]) =>

          def handleEvict(): Iterator[Session] = {
            state.getOption match {
              case Some(lst) =>
                // we sort sessions by timestamp
                val (evicted, kept) = lst.span {
                  s => s.endTime < state.getCurrentWatermarkMs()
                }

                if (kept.isEmpty) {
                  state.remove()
                } else {
                  state.update(kept)
                  state.setTimeoutTimestamp(kept.head.endTime)
                }

                evicted.map { session =>
                  Session(userId, session.endTime - session.startTime, session.events.length)
                }.iterator

              case None =>
                state.remove()
                Seq.empty[Session].iterator
            }
          }

          def handleEvent(event: SessionEvent): Unit = {
            state.getOption match {
              case Some(lst) =>
                var idx = 0


              case None =>

            }
          }
      }


    // the timestamp is added by Spark, hence technically it's working as "processing time"
    val sessionUpdates = events
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[Events, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {
        case (sessionId: String, events: Iterator[Event], state: GroupState[Events]) =>


          // If timed out, then remove session and send final update
          if (state.hasTimedOut) {
            val finalUpdate =
              SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
            state.remove()
            finalUpdate
          } else {
            // Update start and end timestamps in session
            val timestamps = events.map(_.timestamp.getTime).toSeq
            val updatedSession = if (state.exists) {
              val oldSession = state.get
              SessionInfo(
                oldSession.numEvents + timestamps.size,
                oldSession.startTimestampMs,
                math.max(oldSession.endTimestampMs, timestamps.max))
            } else {
              SessionInfo(timestamps.size, timestamps.min, timestamps.max)
            }
            state.update(updatedSession)

            // Set timeout such that the session will be expired if no data received for 10 seconds
            state.setTimeoutDuration("10 seconds")
            Session(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
          }
      }

    // Start running the query that prints the session updates to the console
    val query = sessionUpdates
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}

object EventTypes extends Enumeration {
  type EventTypes = Value
  val NEW_EVENT, CLOSE_SESSION = Value
}

case class SessionEvent(
    userId: String,
    eventType: EventTypes.Value,
    startTimestamp: Long,
    endTimestamp: Long)

object SessionEvent {
  def apply(
      userId: String,
      eventTypeStr: String,
      timestamp: Long,
      gapDuration: Long): SessionEvent = {
    val eventType = EventTypes.withName(eventTypeStr)
    val endTime = if (eventType == EventTypes.CLOSE_SESSION)  {
      timestamp
    } else {
      timestamp + gapDuration
    }
    SessionEvent(userId, eventType, timestamp, endTime)
  }
}

case class SessionAcc(events: List[SessionEvent]) {
  private val sortedEvents: List[SessionEvent] = events.sortBy(_.startTimestamp)
  def startTime: Long = sortedEvents.head.startTimestamp
  def endTime: Long = sortedEvents.last.endTimestamp
}

/**
 * User-defined data type representing the session information returned by mapGroupsWithState.
 *
 * @param id          Id of the user
 * @param durationMs  Duration the session was active, that is, from first event to its expiry
 * @param numEvents   Number of events received by the session while it was active
 */
case class Session(
  id: String,
  durationMs: Long,
  numEvents: Int)

// scalastyle:on println
