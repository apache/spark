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

package org.apache.spark.sql.execution.streaming

import org.scalactic.TolerantNumerics

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.ManualClock

class StreamingMetricsSuite extends SparkFunSuite {

  // To make === between double tolerate inexact values
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.01)

  test("all rates and latency") {
    val m = newStreamMetrics()
    assert(m.currentInputRate() === 0.0)
    assert(m.currentProcessingRate() === 0.0)
    assert(m.currentOutputRate() === 0.0)
    assert(m.currentSourceInputRate(source) === 0.0)
    assert(m.currentSourceProcessingRate(source) === 0.0)
    assert(m.currentLatency() === None)

    // When trigger started, the rates should not change
    m.reportTriggerStarted(1)
    assert(m.currentInputRate() === 0.0)
    assert(m.currentProcessingRate() === 0.0)
    assert(m.currentOutputRate() === 0.0)
    assert(m.currentSourceInputRate(source) === 0.0)
    assert(m.currentSourceProcessingRate(source) === 0.0)
    assert(m.currentLatency() === None)

    // Finishing the trigger should calculate the rates, except input rate which needs
    // to have another trigger interval
    m.reportNumRows(Map(source -> 100L), Some(10)) // 100 input rows, 10 output rows
    clock.advance(1000)
    m.reportTriggerFinished()
    assert(m.currentInputRate() === 0.0)
    assert(m.currentProcessingRate() === 100.0)  // 100 input rows processed in 1 sec
    assert(m.currentOutputRate() === 10.0)       // 10 output rows generated in 1 sec
    assert(m.currentSourceInputRate(source) === 0.0)
    assert(m.currentSourceProcessingRate(source) === 100.0)
    assert(m.currentLatency() === None)

    // Another trigger should calculate the input rate
    clock.advance(1000)
    m.reportTriggerStarted(2)
    m.reportNumRows(Map(source -> 200L), Some(20))     // 200 input rows, 20 output rows
    clock.advance(500)
    m.reportTriggerFinished()
    assert(m.currentInputRate() === 100.0)       // 200 input rows generated in 2 seconds b/w starts
    assert(m.currentProcessingRate() === 400.0)  // 200 output rows processed in 0.5 sec
    assert(m.currentOutputRate() === 40.0)       // 20 output rows generated in 0.5 sec
    assert(m.currentSourceInputRate(source) === 100.0)
    assert(m.currentSourceProcessingRate(source) === 400.0)
    assert(m.currentLatency().get === 1500.0)       // 2000 ms / 2 + 500 ms

    // Rates should be set to 0 after stop
    m.stop()
    assert(m.currentInputRate() === 0.0)
    assert(m.currentProcessingRate() === 0.0)
    assert(m.currentOutputRate() === 0.0)
    assert(m.currentSourceInputRate(source) === 0.0)
    assert(m.currentSourceProcessingRate(source) === 0.0)
    assert(m.currentLatency() === None)
  }

  private def newStreamMetrics(): StreamMetrics = new StreamMetrics(
    Set[Source](source), clock, "test")

  private val clock = new ManualClock()
  private val source: Source = new Source {
    override def schema: StructType = StructType(Array.empty[StructField])
    override def getOffset: Option[Offset] = Some(new LongOffset(0))
    override def getBatch(start: Option[Offset], end: Offset): DataFrame = { null }
    override def stop() {}
  }
}
