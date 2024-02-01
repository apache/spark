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

import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}


/**
 * Sessionize events in UTF8 encoded, '\n' delimited text received from the network.
 * Each line composes an event, and the line should match to the json format.
 *
 * The schema of the event is following:
 * - user_id: String
 * - event_type: String
 * - timestamp: Long
 *
 * The supported types are following:
 * - NEW_EVENT
 * - CLOSE_SESSION
 *
 * This example focuses to demonstrate the complex sessionization which uses two conditions
 * on closing session; conditions are following:
 * - No further event is provided for the user ID within 5 seconds
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
 *
 * Here's a set of events for example:
 *
 * {"user_id": "user1", "event_type": "NEW_EVENT", "timestamp": 13}
 * {"user_id": "user1", "event_type": "NEW_EVENT", "timestamp": 10}
 * {"user_id": "user1", "event_type": "CLOSE_SESSION", "timestamp": 15}
 * {"user_id": "user1", "event_type": "NEW_EVENT", "timestamp": 17}
 * {"user_id": "user1", "event_type": "NEW_EVENT", "timestamp": 19}
 * {"user_id": "user1", "event_type": "NEW_EVENT", "timestamp": 29}
 *
 * {"user_id": "user2", "event_type": "NEW_EVENT", "timestamp": 45}
 *
 * {"user_id": "user1", "event_type": "NEW_EVENT", "timestamp": 65}
 *
 * and results (the output can be split across micro-batches):
 *
 * +-----+----------+---------+
 * |   id|durationMs|numEvents|
 * +-----+----------+---------+
 * |user1|      5000|        3|
 * |user1|      7000|        2|
 * |user1|      5000|        1|
 * |user2|      5000|        1|
 * +-----+----------+---------+
 * (The last event is not reflected into output due to watermark.)
 *
 * Note that there're three different sessions for 'user1'. The events in first two sessions
 * are occurred within gap duration for nearest events, but they don't compose a single session
 * due to the event of CLOSE_SESSION.
 *
 * Also note that the implementation is simplified one. This example doesn't address
 * - UPDATE MODE (the semantic is not clear for session window with event time processing)
 * - partial merge (events in session which are earlier than watermark can be aggregated)
 * - other possible optimizations
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
      .builder()
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
      .add("timestamp", TimestampType, nullable = false)

    val gapDuration: Long = 5 * 1000 // 5 seconds

    // Parse the line into event, as described in classdoc.
    val events = lines
      .select(from_json(col("value"), jsonSchema).as("event"))
      .selectExpr("event.user_id AS user_id", "event.event_type AS event_type",
        "event.timestamp AS timestamp")
      .withWatermark("timestamp", "10 seconds")
      .as[(String, String, Timestamp)]

    // Sessionize the events. Track number of events, start and end timestamps of session,
    // and report session when session is closed.
    val sessionUpdates = events
      .groupByKey(event => event._1)
      .flatMapGroupsWithState[List[SessionAcc], Session](OutputMode.Append(),
        GroupStateTimeout.EventTimeTimeout) {

        case (userId: String, events: Iterator[(String, String, Timestamp)],
            state: GroupState[List[SessionAcc]]) =>

          def handleEvict(sessions: List[SessionAcc]): Iterator[Session] = {
            // we sorted sessions by timestamp
            val (evicted, kept) = sessions.span {
              s => s.endTime.getTime < state.getCurrentWatermarkMs()
            }

            if (kept.isEmpty) {
              state.remove()
            } else {
              state.update(kept)
              // trigger timeout at the end time of the first session
              state.setTimeoutTimestamp(kept.head.endTime.getTime)
            }

            evicted.map { sessionAcc =>
              Session(userId, sessionAcc.endTime.getTime - sessionAcc.startTime.getTime,
                sessionAcc.events.length)
            }.iterator
          }

          def mergeSessions(sessions: List[SessionAcc]): Unit = {
            // we sorted sessions by timestamp
            val updatedSessions = new mutable.ArrayBuffer[SessionAcc]()
            updatedSessions ++= sessions

            var curIdx = 0
            while (curIdx < updatedSessions.length - 1) {
              val curSession = updatedSessions(curIdx)
              val nextSession = updatedSessions(curIdx + 1)

              // Current session and next session can be merged
              if (curSession.endTime.getTime > nextSession.startTime.getTime) {
                val accumulatedEvents =
                  (curSession.events ++ nextSession.events).sortBy(_.startTimestamp.getTime)

                val newSessions = new mutable.ArrayBuffer[SessionAcc]()
                var eventsForCurSession = new mutable.ArrayBuffer[SessionEvent]()
                accumulatedEvents.foreach { event =>
                  eventsForCurSession += event
                  if (event.eventType == EventTypes.CLOSE_SESSION) {
                    newSessions += SessionAcc(eventsForCurSession.toList)
                    eventsForCurSession = new mutable.ArrayBuffer[SessionEvent]()
                  }
                }
                if (eventsForCurSession.nonEmpty) {
                  newSessions += SessionAcc(eventsForCurSession.toList)
                }

                // replace current session and next session with new session(s)
                updatedSessions.remove(curIdx + 1)
                updatedSessions(curIdx) = newSessions.head
                if (newSessions.length > 1) {
                  updatedSessions.insertAll(curIdx + 1, newSessions.tail)
                }

                // move the cursor to the last new session(s)
                curIdx += newSessions.length - 1
              } else {
                // move to the next session
                curIdx += 1
              }
            }

            // update state
            state.update(updatedSessions.toList)
          }

          if (state.hasTimedOut && state.exists) {
            handleEvict(state.get.sortBy(_.startTime.getTime))
          } else {
            // convert each event as individual session
            val sessionsFromEvents = events.map { case (userId, eventType, timestamp) =>
              val e = SessionEvent(userId, eventType, timestamp, gapDuration)
              SessionAcc(List(e))
            }.toList
            if (sessionsFromEvents.nonEmpty) {
              val sessionsFromState = if (state.exists) {
                state.get
              } else {
                List.empty
              }

              // sort sessions via start timestamp, and merge
              mergeSessions((sessionsFromEvents ++ sessionsFromState).sortBy(_.startTime.getTime))
              // we still need to handle eviction here
              handleEvict(state.get.sortBy(_.startTime.getTime))
            } else {
              Iterator.empty
            }
          }
      }

    // Start running the query that prints the session updates to the console
    val query = sessionUpdates
      .writeStream
      .outputMode("append")
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
    startTimestamp: Timestamp,
    endTimestamp: Timestamp)

object SessionEvent {
  def apply(
      userId: String,
      eventTypeStr: String,
      timestamp: Timestamp,
      gapDuration: Long): SessionEvent = {
    val eventType = EventTypes.withName(eventTypeStr)
    val endTime = if (eventType == EventTypes.CLOSE_SESSION)  {
      timestamp
    } else {
      new Timestamp(timestamp.getTime + gapDuration)
    }
    SessionEvent(userId, eventType, timestamp, endTime)
  }
}

case class SessionAcc(events: List[SessionEvent]) {
  private val sortedEvents: List[SessionEvent] = events.sortBy(_.startTimestamp.getTime)

  require(!sortedEvents.dropRight(1).exists(_.eventType == EventTypes.CLOSE_SESSION),
    "CLOSE_SESSION event cannot be placed except the last event!")

  def eventsAsSorted: List[SessionEvent] = sortedEvents
  def startTime: Timestamp = sortedEvents.head.startTimestamp
  def endTime: Timestamp = sortedEvents.last.endTimestamp

  override def toString: String = {
    s"SessionAcc(events: $events / sorted: $sortedEvents / " +
      s"start time: $startTime / endTime: $endTime)"
  }
}

/**
 * User-defined data type representing the session information returned by flatMapGroupsWithState.
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
