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
package org.apache.spark.sql.streaming.processors

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.{MapState, OutputMode, StatefulProcessor, TimeMode, TimerValues, TTLConfig}

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
      timerValues: TimerValues
  ): Iterator[(String, String, Long)] = {
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

