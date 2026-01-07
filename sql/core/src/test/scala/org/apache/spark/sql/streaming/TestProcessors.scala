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
package org.apache.spark.sql.streaming

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Encoders

/** Test processor that exercises all state methods for coverage testing. */
class AllMethodsTestProcessor extends StatefulProcessor[String, String, (String, String)] {

  @transient private var valueState: ValueState[Int] = _
  @transient private var listState: ListState[String] = _
  @transient private var mapState: MapState[String, Int] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    valueState = getHandle.getValueState[Int]("value", Encoders.scalaInt, TTLConfig.NONE)
    listState = getHandle.getListState[String]("list", Encoders.STRING, TTLConfig.NONE)
    mapState =
      getHandle.getMapState[String, Int]("map", Encoders.STRING, Encoders.scalaInt, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val results = ArrayBuffer[(String, String)]()

    inputRows.foreach { cmd =>
      cmd match {
        case "value-exists" =>
          results += ((key, s"value-exists:${valueState.exists()}"))
        case "value-get" =>
          results += ((key, if (valueState.exists()) valueState.get().toString() else ""))
        case "value-set" =>
          valueState.update(42)
          results += ((key, "value-set:done"))
        case "value-clear" =>
          valueState.clear()
          results += ((key, "value-clear:done"))
        case "list-exists" =>
          results += ((key, s"list-exists:${listState.exists()}"))
        case "list-append" =>
          listState.appendValue("a")
          listState.appendValue("b")
          results += ((key, "list-append:done"))
        case "list-append-array" =>
          listState.appendList(Array("c", "d"))
          results += ((key, "list-append-array:done"))
        case "list-get" =>
          val items = listState.get().toList.mkString(",")
          results += ((key, s"list-get:$items"))
        case "list-put" =>
          listState.put(Array("put"))
          results += ((key, "list-put:done"))
        case "list-clear" =>
          listState.clear()
          results += ((key, "list-clear:done"))
        case "map-exists" =>
          results += ((key, s"map-exists:${mapState.exists()}"))
        case "map-add" =>
          mapState.updateValue("x", 1)
          mapState.updateValue("y", 2)
          mapState.updateValue("z", 3)
          results += ((key, "map-add:done"))
        case "map-keys" =>
          val keys = mapState.keys().toList.sorted.mkString(",")
          results += ((key, s"map-keys:$keys"))
        case "map-values" =>
          val values = mapState.values().toList.sorted.mkString(",")
          results += ((key, s"map-values:$values"))
        case "map-iterator" =>
          val pairs =
            mapState.iterator().toList.sortBy(_._1).map(p => s"${p._1}=${p._2}").mkString(",")
          results += ((key, s"map-iterator:$pairs"))
        case "map-remove" =>
          mapState.removeKey("y")
          results += ((key, "map-remove:done"))
        case "map-clear" =>
          mapState.clear()
          results += ((key, "map-clear:done"))
      }
    }

    results.iterator
  }
}

/** Test StatefulProcessor implementation that maintains a running count. */
class RunningCountProcessor[T](ttl: TTLConfig = TTLConfig.NONE)
    extends StatefulProcessorWithInitialState[String, T, (String, Long), Long] {

  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState[Long]("count", Encoders.scalaLong, ttl)
  }

  override def handleInitialState(
      key: String,
      initialState: Long,
      timerValues: TimerValues): Unit = {
    countState.update(initialState)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[T],
      timerValues: TimerValues): Iterator[(String, Long)] = {
    val incoming = inputRows.size
    val current = countState.get()
    val updated = current + incoming
    countState.update(updated)
    Iterator.single((key, updated))
  }
}

/**
 * Processor that registers a processing time timer on first input and emits a message on expiry.
 */
class SessionTimeoutProcessor extends StatefulProcessor[String, String, (String, String)] {

  @transient private var lastSeenState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    lastSeenState = getHandle.getValueState[Long]("lastSeen", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val currentTime = timerValues.getCurrentProcessingTimeInMs()

    // Clear any existing timer if we have previous state
    if (lastSeenState.exists()) {
      val oldTimerTime = lastSeenState.get() + 10000 // old timeout was 10s after last seen
      getHandle.deleteTimer(oldTimerTime)
    }

    // Update last seen time and register new timer
    lastSeenState.update(currentTime)
    getHandle.registerTimer(currentTime + 10000) // 10 second timeout

    inputRows.map(value => (key, s"received:$value"))
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    lastSeenState.clear()
    Iterator.single((key, s"session-expired@${expiredTimerInfo.getExpiryTimeInMs()}"))
  }
}

/**
 * Processor that registers an event time timer based on watermark.
 * Input format: (eventTimeMs: Long, value: String)
 * Registers a timer at eventTime + 5000ms. Timer fires when watermark passes that time.
 * Assumes that there are no out of order events and events are always in order based on timestamp.
 */
class EventTimeSessionProcessor
    extends StatefulProcessor[String, (Long, String), (String, String)] {

  @transient private var lastEventTimeState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    lastEventTimeState =
      getHandle.getValueState[Long]("lastEventTime", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(Long, String)],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val results = scala.collection.mutable.ArrayBuffer[(String, String)]()

    // Clear any existing timer if we have previous state
   if (lastEventTimeState.exists()) {
      val oldTimerTime = lastEventTimeState.get() + 5000
      getHandle.deleteTimer(oldTimerTime)
    }

    inputRows.foreach {
      case (eventTimeMs, value) =>
        // Update last event time and register new timer
        lastEventTimeState.update(eventTimeMs)
        getHandle.registerTimer(eventTimeMs + 5000) // 5 second timeout from event time

        results += ((key, s"received:$value@$eventTimeMs"))
    }

    results.iterator
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    val watermark = timerValues.getCurrentWatermarkInMs()
    lastEventTimeState.clear()
    Iterator.single(
      (key, s"session-expired@${expiredTimerInfo.getExpiryTimeInMs()}")
    )
  }
}

/**
 * Processor that counts events in EventTime mode.
 * Input format: (eventTimeMs: Long, value: String)
 * Output: (key, count) for current count after processing input
 * Used to test late event filtering - late events should not increment the count.
 */
class EventTimeCountProcessor extends StatefulProcessor[String, (Long, String), (String, Long)] {

  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState[Long]("count", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(Long, String)],
      timerValues: TimerValues): Iterator[(String, Long)] = {
    val incoming = inputRows.size
    val current = if (countState.exists()) countState.get() else 0L
    val updated = current + incoming
    countState.update(updated)
    Iterator.single((key, updated))
  }
}

// Input: (key, score) as (String, Double)
// Output: (key, score) as (String, Double) for the top K snapshot each batch
class TopKProcessor(k: Int, ttl: TTLConfig = TTLConfig.NONE)
    extends StatefulProcessor[String, (String, Double), (String, Double)] {

  @transient private var topKState: ListState[Double] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    topKState = getHandle.getListState[Double]("topK", Encoders.scalaDouble, ttl)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Double)],
      timerValues: TimerValues): Iterator[(String, Double)] = {
    // Load existing list into a buffer.
    val current = ArrayBuffer[Double]()
    topKState.get().foreach(current += _)

    // Add new values and recompute top-K.
    inputRows.foreach {
      case (_, score) =>
        current += score
    }
    val updatedTopK = current.sorted(Ordering[Double].reverse).take(k)

    // Store the new state.
    topKState.put(updatedTopK.toArray)

    // Emit snapshot of top-K for this key.
    updatedTopK.iterator.map(v => (key, v))
  }
}

// Input: (key, word) as (String, String)
// Output: (key, word, count) as (String, String, Long) for each word in the batch
class WordFrequencyProcessor(ttl: TTLConfig = TTLConfig.NONE)
    extends StatefulProcessor[String, (String, String), (String, String, Long)] {

  @transient private var freqState: MapState[String, Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    freqState = getHandle
      .getMapState[String, Long]("frequencies", Encoders.STRING, Encoders.scalaLong, ttl)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues): Iterator[(String, String, Long)] = {
    val results = ArrayBuffer[(String, String, Long)]()

    inputRows.foreach {
      case (_, word) =>
        val currentCount = if (freqState.containsKey(word)) {
          freqState.getValue(word)
        } else {
          0L
        }
        val updatedCount = currentCount + 1
        freqState.updateValue(word, updatedCount)
        results += ((key, word, updatedCount))
    }

    results.iterator
  }
}

// Case classes for complex data type testing
case class UserEvent(action: String, amount: Double, timestamp: Long)
case class UserProfile(totalAmount: Double, eventCount: Long, lastEventTime: Long)
case class UserSummary(key: String, profile: UserProfile)

/**
 * Processor that uses complex case classes for input, state, and output.
 * Tracks user activity by aggregating events into a profile.
 */
class UserProfileProcessor extends StatefulProcessor[String, UserEvent, UserSummary] {

  @transient private var profileState: ValueState[UserProfile] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    profileState = getHandle.getValueState[UserProfile](
      "profile",
      Encoders.product[UserProfile],
      TTLConfig.NONE
    )
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[UserEvent],
      timerValues: TimerValues): Iterator[UserSummary] = {
    val current = if (profileState.exists()) profileState.get()
                  else UserProfile(0.0, 0L, 0L)

    var updated = current
    inputRows.foreach { event =>
      updated = UserProfile(
        updated.totalAmount + event.amount,
        updated.eventCount + 1,
        math.max(updated.lastEventTime, event.timestamp)
      )
    }

    profileState.update(updated)
    Iterator.single(UserSummary(key, updated))
  }
}
