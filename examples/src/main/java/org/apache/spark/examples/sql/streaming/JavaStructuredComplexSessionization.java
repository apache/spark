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

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.apache.spark.sql.functions.*;

/**
 * Sessionize events in UTF8 encoded, '\n' delimited text received from the network.
 * Each line composes an event, and the line should match to the json format.
 * <p>
 * The schema of the event is following:
 * - user_id: String
 * - event_type: String
 * - timestamp: Long
 * <p>
 * The supported types are following:
 * - NEW_EVENT
 * - CLOSE_SESSION
 * <p>
 * This example focuses to demonstrate the complex sessionization which uses two conditions
 * on closing session; conditions are following:
 * - No further event is provided for the user ID within 5 seconds
 * - An event having CLOSE_SESSION as event_type is provided for the user ID
 * <p>
 * Usage: JavaStructuredComplexSessionization <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 * <p>
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example sql.streaming.JavaStructuredComplexSessionization
 * localhost 9999`
 * <p>
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
 * <p>
 * Note that there're three different sessions for 'user1'. The events in first two sessions
 * are occurred within gap duration for nearest events, but they don't compose a single session
 * due to the event of CLOSE_SESSION.
 * <p>
 * Also note that the implementation is simplified one. This example doesn't address
 * - UPDATE MODE (the semantic is not clear for session window with event time processing)
 * - partial merge (events in session which are earlier than watermark can be aggregated)
 * - other possible optimizations (especially the implementation is ported from Scala example)
 */
public final class JavaStructuredComplexSessionization {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: JavaStructuredComplexSessionization <hostname> <port>");
      System.exit(1);
    }

    String host = args[0];
    int port = Integer.parseInt(args[1]);

    SparkSession spark = SparkSession
        .builder()
        .appName("JavaStructuredComplexSessionization")
        .getOrCreate();

    // Create DataFrame representing the stream of input lines from connection to host:port
    Dataset<Row> lines = spark
        .readStream()
        .format("socket")
        .option("host", host)
        .option("port", port)
        .option("includeTimestamp", true)
        .load();

    StructType jsonSchema = new StructType()
        .add("user_id", StringType)
        .add("event_type", StringType)
        .add("timestamp", TimestampType);

    long gapDuration = 5 * 1000; // 5 seconds

    // Parse the line into event, as described in classdoc.
    Dataset<Row> events = lines
        .select(from_json(col("value"), jsonSchema).as("event"))
        .selectExpr("event.user_id AS user_id", "event.event_type AS event_type",
            "event.timestamp AS timestamp")
        .withWatermark("timestamp", "10 seconds");

    // Sessionize the events. Track number of events, start and end timestamps of session,
    // and report session when session is closed.
    FlatMapGroupsWithStateFunction<String, Row, Sessions, Session> stateUpdateFunc =
        new FlatMapGroupsWithStateFunction<String, Row, Sessions, Session>() {
          private Iterator<Session> handleEvict(String userId, GroupState<Sessions> state) {
            Sessions sessions = state.get();

            List<SessionAcc> evicted = new ArrayList<>();
            List<SessionAcc> kept = new ArrayList<>();

            // we sorted sessions by timestamp
            sessions.getSessions().forEach(session -> {
              if (session.endTime().getTime() < state.getCurrentWatermarkMs()) {
                evicted.add(session);
              } else {
                kept.add(session);
              }
            });

            if (kept.isEmpty()) {
              state.remove();
            } else {
              state.update(Sessions.newInstance(kept));
              // trigger timeout at the end time of the first session
              state.setTimeoutTimestamp(kept.get(0).endTime().getTime());
            }

            return evicted.stream()
                .map(sessionAcc -> Session.newInstance(
                    userId,
                    sessionAcc.endTime().getTime() - sessionAcc.startTime().getTime(),
                    sessionAcc.getEvents().size()))
                .iterator();
          }

          private void mergeSessions(List<SessionAcc> sessionAccs, GroupState<Sessions> state) {
            // we sorted sessionAccs by timestamp

            int curIdx = 0;
            while (curIdx < sessionAccs.size() - 1) {
              SessionAcc curSession = sessionAccs.get(curIdx);
              SessionAcc nextSession = sessionAccs.get(curIdx + 1);

              // Current session and next session can be merged
              if (curSession.endTime().getTime() > nextSession.startTime().getTime()) {
                List<SessionEvent> accumulatedEvents = new ArrayList<>(curSession.getEvents());
                accumulatedEvents.addAll(nextSession.getEvents());
                accumulatedEvents.sort(
                    Comparator.comparingLong(e -> e.getStartTimestamp().getTime()));

                List<SessionAcc> newSessions = new ArrayList<>();
                List<SessionEvent> eventsForCurSession = new ArrayList<>();
                for (SessionEvent event : accumulatedEvents) {
                  eventsForCurSession.add(event);
                  if (event.eventType == EventTypes.CLOSE_SESSION) {
                    SessionAcc newSessionAcc = SessionAcc.newInstance(eventsForCurSession);
                    newSessions.add(newSessionAcc);
                    eventsForCurSession = new ArrayList<>();
                  }
                }
                if (!eventsForCurSession.isEmpty()) {
                  SessionAcc newSessionAcc = SessionAcc.newInstance(eventsForCurSession);
                  newSessions.add(newSessionAcc);
                }

                // replace current session and next session with new session(s)
                sessionAccs.remove(curIdx + 1);
                sessionAccs.set(curIdx, newSessions.get(0));
                if (newSessions.size() > 1) {
                  sessionAccs.addAll(curIdx + 1,
                      newSessions.stream().skip(1).collect(Collectors.toList()));
                }

                // move the cursor to the last new session(s)
                curIdx += newSessions.size() - 1;
              } else {
                // move to the next session
                curIdx++;
              }
            }

            // update state
            state.update(Sessions.newInstance(sessionAccs));
          }

          @Override
          public Iterator<Session> call(
              String userId, Iterator<Row> events, GroupState<Sessions> state) {

            if (state.hasTimedOut() && state.exists()) {
              return handleEvict(userId, state);
            }

            // convert each event as individual session
            Stream<Row> stream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                events,
                Spliterator.ORDERED), false);
            List<SessionAcc> sessionsFromEvents = stream.map(r -> {
              SessionEvent event = SessionEvent.newInstance(userId, r.getString(1),
                  r.getTimestamp(2), gapDuration);
              return SessionAcc.newInstance(event);
            }).collect(Collectors.toList());

            if (sessionsFromEvents.isEmpty()) {
              return Collections.emptyIterator();
            }

            // sort sessions via start timestamp
            List<SessionAcc> allSessions = new ArrayList<>(sessionsFromEvents);
            if (state.exists()) {
              allSessions.addAll(state.get().getSessions());
            }
            allSessions.sort(Comparator.comparingLong(s -> s.startTime().getTime()));

            // merge sessions
            mergeSessions(allSessions, state);

            // we still need to handle eviction here
            return handleEvict(userId, state);
          }
        };

    Dataset<Session> sessionUpdates = events
        .groupByKey((MapFunction<Row, String>) event -> event.getString(0), Encoders.STRING())
        .flatMapGroupsWithState(
            stateUpdateFunc,
            OutputMode.Append(),
            Encoders.bean(Sessions.class),
            Encoders.bean(Session.class),
            GroupStateTimeout.EventTimeTimeout());

    // Start running the query that prints the session updates to the console
    StreamingQuery query = sessionUpdates
        .writeStream()
        .outputMode("append")
        .format("console")
        .start();

    query.awaitTermination();
  }

  public static class Sessions {
    private List<SessionAcc> sessions;

    public List<SessionAcc> getSessions() {
      return sessions;
    }

    public void setSessions(List<SessionAcc> sessions) {
      // `sessions` should not be empty, and be sorted by start time
      if (sessions.isEmpty()) {
        throw new IllegalArgumentException("events should not be empty!");
      }

      List<SessionAcc> sorted = new ArrayList<>(sessions);
      sorted.sort(Comparator.comparingLong(session -> session.startTime().getTime()));

      this.sessions = sorted;
    }

    public static Sessions newInstance(List<SessionAcc> sessions) {
      Sessions instance = new Sessions();
      instance.setSessions(sessions);
      return instance;
    }
  }

  public enum EventTypes {
    NEW_EVENT, CLOSE_SESSION;
  }

  public static class SessionEvent implements Serializable {
    private String userId;
    private EventTypes eventType;
    private Timestamp startTimestamp;
    private Timestamp endTimestamp;

    public String getUserId() {
      return userId;
    }

    public void setUserId(String userId) {
      this.userId = userId;
    }

    public EventTypes getEventType() {
      return eventType;
    }

    public void setEventType(EventTypes eventType) {
      this.eventType = eventType;
    }

    public Timestamp getStartTimestamp() {
      return startTimestamp;
    }

    public void setStartTimestamp(Timestamp startTimestamp) {
      this.startTimestamp = startTimestamp;
    }

    public Timestamp getEndTimestamp() {
      return endTimestamp;
    }

    public void setEndTimestamp(Timestamp endTimestamp) {
      this.endTimestamp = endTimestamp;
    }

    public static SessionEvent newInstance(String userId, String eventTypeStr,
                                           Timestamp startTimestamp, long gapDuration) {
      SessionEvent instance = new SessionEvent();
      instance.setUserId(userId);
      instance.setEventType(EventTypes.valueOf(eventTypeStr));
      instance.setStartTimestamp(startTimestamp);

      if (instance.getEventType() == EventTypes.CLOSE_SESSION) {
        instance.setEndTimestamp(instance.getStartTimestamp());
      } else {
        instance.setEndTimestamp(
            new Timestamp(instance.getStartTimestamp().getTime() + gapDuration));
      }

      return instance;
    }
  }

  public static class SessionAcc implements Serializable {
    private List<SessionEvent> events;

    public Timestamp startTime() {
      return events.get(0).startTimestamp;
    }

    public Timestamp endTime() {
      return events.get(events.size() - 1).getEndTimestamp();
    }

    public List<SessionEvent> getEvents() {
      return events;
    }

    public void setEvents(List<SessionEvent> events) {
      // `events` should not be empty, and be sorted by start time
      if (events.isEmpty()) {
        throw new IllegalArgumentException("events should not be empty!");
      }

      List<SessionEvent> sorted = new ArrayList<>(events);
      sorted.sort(Comparator.comparingLong(event -> event.startTimestamp.getTime()));

      boolean eventCloseSessionExistBeforeLastEvent = sorted
          .stream()
          .limit(sorted.size() - 1)
          .anyMatch(e -> e.eventType == EventTypes.CLOSE_SESSION);

      if (eventCloseSessionExistBeforeLastEvent) {
        throw new IllegalStateException("CLOSE_SESSION event cannot be placed except " +
            "the last event!");
      }

      this.events = sorted;
    }

    public static SessionAcc newInstance(SessionEvent event) {
      return newInstance(Collections.singletonList(event));
    }

    public static SessionAcc newInstance(List<SessionEvent> events) {
      SessionAcc instance = new SessionAcc();
      instance.setEvents(events);
      return instance;
    }
  }

  public static class Session implements Serializable {
    private String id;
    private long duration;
    private int numEvents;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public long getDuration() {
      return duration;
    }

    public void setDuration(long duration) {
      this.duration = duration;
    }

    public int getNumEvents() {
      return numEvents;
    }

    public void setNumEvents(int numEvents) {
      this.numEvents = numEvents;
    }

    public static Session newInstance(String id, long duration, int numEvents) {
      Session instance = new Session();
      instance.setId(id);
      instance.setDuration(duration);
      instance.setNumEvents(numEvents);
      return instance;
    }
  }
}
