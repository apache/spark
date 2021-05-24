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
import org.apache.spark.sql.streaming._


/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 *
 * Usage: MapGroupsWithState <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example sql.streaming.StructuredSessionization
 * localhost 9999`
 */
object StructuredSessionization {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: StructuredSessionization <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt

    val spark = SparkSession
      .builder
      .appName("StructuredSessionization")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    // Split the lines into words, treat words as sessionId of events
    val events = lines
      .as[(String, Timestamp)]
      .flatMap { case (line, timestamp) =>
        line.split(" ").map(word => Event(sessionId = word, timestamp))
      }

    // Sessionize the events. Track number of events, start and end timestamps of session,
    // and report session updates.
    val sessionUpdates = events
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {

        case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>

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
            SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
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
/** User-defined data type representing the input events */
case class Event(sessionId: String, timestamp: Timestamp)

/**
 * User-defined data type for storing a session information as state in mapGroupsWithState.
 *
 * @param numEvents        total number of events received in the session
 * @param startTimestampMs timestamp of first event received in the session when it started
 * @param endTimestampMs   timestamp of last event received in the session before it expired
 */
case class SessionInfo(
    numEvents: Int,
    startTimestampMs: Long,
    endTimestampMs: Long) {

  /** Duration of the session, between the first and last events */
  def durationMs: Long = endTimestampMs - startTimestampMs
}

/**
 * User-defined data type representing the update information returned by mapGroupsWithState.
 *
 * @param id          Id of the session
 * @param durationMs  Duration the session was active, that is, from first event to its expiry
 * @param numEvents   Number of events received by the session while it was active
 * @param expired     Is the session active or expired
 */
case class SessionUpdate(
    id: String,
    durationMs: Long,
    numEvents: Int,
    expired: Boolean)

// scalastyle:on println

