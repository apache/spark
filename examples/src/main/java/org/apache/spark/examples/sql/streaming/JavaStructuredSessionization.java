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
package org.apache.spark.examples.sql.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 * <p>
 * Usage: JavaStructuredNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 * <p>
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example sql.streaming.JavaStructuredSessionization
 * localhost 9999`
 */
public final class JavaStructuredSessionization {

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: JavaStructuredSessionization <hostname> <port>");
      System.exit(1);
    }

    String host = args[0];
    int port = Integer.parseInt(args[1]);

    SparkSession spark = SparkSession
        .builder()
        .appName("JavaStructuredSessionization")
        .getOrCreate();

    // Create DataFrame representing the stream of input lines from connection to host:port
    Dataset<Row> lines = spark
        .readStream()
        .format("socket")
        .option("host", host)
        .option("port", port)
        .option("includeTimestamp", true)
        .load();

    FlatMapFunction<LineWithTimestamp, Event> linesToEvents =
      new FlatMapFunction<LineWithTimestamp, Event>() {
        @Override
        public Iterator<Event> call(LineWithTimestamp lineWithTimestamp) {
          ArrayList<Event> eventList = new ArrayList<>();
          for (String word : lineWithTimestamp.getLine().split(" ")) {
            eventList.add(new Event(word, lineWithTimestamp.getTimestamp()));
          }
          return eventList.iterator();
        }
      };

    // Split the lines into words, treat words as sessionId of events
    Dataset<Event> events = lines
        .withColumnRenamed("value", "line")
        .as(Encoders.bean(LineWithTimestamp.class))
        .flatMap(linesToEvents, Encoders.bean(Event.class));

    // Sessionize the events. Track number of events, start and end timestamps of session, and
    // and report session updates.
    //
    // Step 1: Define the state update function
    MapGroupsWithStateFunction<String, Event, SessionInfo, SessionUpdate> stateUpdateFunc =
      new MapGroupsWithStateFunction<String, Event, SessionInfo, SessionUpdate>() {
        @Override public SessionUpdate call(
            String sessionId, Iterator<Event> events, GroupState<SessionInfo> state) {
          // If timed out, then remove session and send final update
          if (state.hasTimedOut()) {
            SessionUpdate finalUpdate = new SessionUpdate(
                sessionId, state.get().calculateDuration(), state.get().getNumEvents(), true);
            state.remove();
            return finalUpdate;

          } else {
            // Find max and min timestamps in events
            long maxTimestampMs = Long.MIN_VALUE;
            long minTimestampMs = Long.MAX_VALUE;
            int numNewEvents = 0;
            while (events.hasNext()) {
              Event e = events.next();
              long timestampMs = e.getTimestamp().getTime();
              maxTimestampMs = Math.max(timestampMs, maxTimestampMs);
              minTimestampMs = Math.min(timestampMs, minTimestampMs);
              numNewEvents += 1;
            }
            SessionInfo updatedSession = new SessionInfo();

            // Update start and end timestamps in session
            if (state.exists()) {
              SessionInfo oldSession = state.get();
              updatedSession.setNumEvents(oldSession.numEvents + numNewEvents);
              updatedSession.setStartTimestampMs(oldSession.startTimestampMs);
              updatedSession.setEndTimestampMs(Math.max(oldSession.endTimestampMs, maxTimestampMs));
            } else {
              updatedSession.setNumEvents(numNewEvents);
              updatedSession.setStartTimestampMs(minTimestampMs);
              updatedSession.setEndTimestampMs(maxTimestampMs);
            }
            state.update(updatedSession);
            // Set timeout such that the session will be expired if no data received for 10 seconds
            state.setTimeoutDuration("10 seconds");
            return new SessionUpdate(
                sessionId, state.get().calculateDuration(), state.get().getNumEvents(), false);
          }
        }
      };

    // Step 2: Apply the state update function to the events streaming Dataset grouped by sessionId
    Dataset<SessionUpdate> sessionUpdates = events
        .groupByKey(
            new MapFunction<Event, String>() {
              @Override public String call(Event event) {
                return event.getSessionId();
              }
            }, Encoders.STRING())
        .mapGroupsWithState(
            stateUpdateFunc,
            Encoders.bean(SessionInfo.class),
            Encoders.bean(SessionUpdate.class),
            GroupStateTimeout.ProcessingTimeTimeout());

    // Start running the query that prints the session updates to the console
    StreamingQuery query = sessionUpdates
        .writeStream()
        .outputMode("update")
        .format("console")
        .start();

    query.awaitTermination();
  }

  /**
   * User-defined data type representing the raw lines with timestamps.
   */
  public static class LineWithTimestamp implements Serializable {
    private String line;
    private Timestamp timestamp;

    public Timestamp getTimestamp() { return timestamp; }
    public void setTimestamp(Timestamp timestamp) { this.timestamp = timestamp; }

    public String getLine() { return line; }
    public void setLine(String sessionId) { this.line = sessionId; }
  }

  /**
   * User-defined data type representing the input events
   */
  public static class Event implements Serializable {
    private String sessionId;
    private Timestamp timestamp;

    public Event() { }
    public Event(String sessionId, Timestamp timestamp) {
      this.sessionId = sessionId;
      this.timestamp = timestamp;
    }

    public Timestamp getTimestamp() { return timestamp; }
    public void setTimestamp(Timestamp timestamp) { this.timestamp = timestamp; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }
  }

  /**
   * User-defined data type for storing a session information as state in mapGroupsWithState.
   */
  public static class SessionInfo implements Serializable {
    private int numEvents = 0;
    private long startTimestampMs = -1;
    private long endTimestampMs = -1;

    public int getNumEvents() { return numEvents; }
    public void setNumEvents(int numEvents) { this.numEvents = numEvents; }

    public long getStartTimestampMs() { return startTimestampMs; }
    public void setStartTimestampMs(long startTimestampMs) {
      this.startTimestampMs = startTimestampMs;
    }

    public long getEndTimestampMs() { return endTimestampMs; }
    public void setEndTimestampMs(long endTimestampMs) { this.endTimestampMs = endTimestampMs; }

    public long calculateDuration() { return endTimestampMs - startTimestampMs; }

    @Override public String toString() {
      return "SessionInfo(numEvents = " + numEvents +
          ", timestamps = " + startTimestampMs + " to " + endTimestampMs + ")";
    }
  }

  /**
   * User-defined data type representing the update information returned by mapGroupsWithState.
   */
  public static class SessionUpdate implements Serializable {
    private String id;
    private long durationMs;
    private int numEvents;
    private boolean expired;

    public SessionUpdate() { }

    public SessionUpdate(String id, long durationMs, int numEvents, boolean expired) {
      this.id = id;
      this.durationMs = durationMs;
      this.numEvents = numEvents;
      this.expired = expired;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public long getDurationMs() { return durationMs; }
    public void setDurationMs(long durationMs) { this.durationMs = durationMs; }

    public int getNumEvents() { return numEvents; }
    public void setNumEvents(int numEvents) { this.numEvents = numEvents; }

    public boolean isExpired() { return expired; }
    public void setExpired(boolean expired) { this.expired = expired; }
  }
}
