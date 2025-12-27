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
import org.apache.spark.sql.streaming.{ListState, OutputMode, StatefulProcessor, TimeMode, TimerValues, TTLConfig}

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
      timerValues: TimerValues
  ): Iterator[(String, Double)] = {
    // Load existing list into a buffer
    val current = ArrayBuffer[Double]()
    topKState.get().foreach(current += _)
    println(s"AAA loaded state for key=$key: $current")

    // Add new values and recompute top-K
    inputRows.foreach {
      case (_, score) =>
        current += score
    }
    val updatedTopK = current.sorted(Ordering[Double].reverse).take(k)
    println(s"AAA updatedTopK for key=$key: $updatedTopK")

    // Persist back
    topKState.clear()
    topKState.put(updatedTopK.toArray)

    // Emit snapshot of top-K for this key
    updatedTopK.iterator.map(v => (key, v))
  }
}

