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

class StreamMetricsSuite extends SparkFunSuite {

  // To make === between double tolerate inexact values
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.01)

  test("rates and latencies - basic life cycle") {
    val sm = newStreamMetrics(source)
    assert(sm.currentInputRate() === 0.0)
    assert(sm.currentProcessingRate() === 0.0)
    assert(sm.currentSourceInputRate(source) === 0.0)
    assert(sm.currentSourceProcessingRate(source) === 0.0)
    assert(sm.currentLatency() === None)

    // When trigger started, the rates should not change
    sm.reportTriggerStarted(1)
    assert(sm.currentInputRate() === 0.0)
    assert(sm.currentProcessingRate() === 0.0)
    assert(sm.currentSourceInputRate(source) === 0.0)
    assert(sm.currentSourceProcessingRate(source) === 0.0)
    assert(sm.currentLatency() === None)

    // Finishing the trigger should calculate the rates, except input rate which needs
    // to have another trigger interval
    sm.reportNumInputRows(Map(source -> 100L)) // 100 input rows, 10 output rows
    clock.advance(1000)
    sm.reportTriggerFinished()
    assert(sm.currentInputRate() === 0.0)
    assert(sm.currentProcessingRate() === 100.0)  // 100 input rows processed in 1 sec
    assert(sm.currentSourceInputRate(source) === 0.0)
    assert(sm.currentSourceProcessingRate(source) === 100.0)
    assert(sm.currentLatency() === None)

    // Another trigger should calculate the input rate
    clock.advance(1000)
    sm.reportTriggerStarted(2)
    sm.reportNumInputRows(Map(source -> 200L))     // 200 input rows
    clock.advance(500)
    sm.reportTriggerFinished()
    assert(sm.currentInputRate() === 100.0)      // 200 input rows generated in 2 seconds b/w starts
    assert(sm.currentProcessingRate() === 400.0) // 200 output rows processed in 0.5 sec
    assert(sm.currentSourceInputRate(source) === 100.0)
    assert(sm.currentSourceProcessingRate(source) === 400.0)
    assert(sm.currentLatency().get === 1500.0)       // 2000 ms / 2 + 500 ms

    // Rates should be set to 0 after stop
    sm.stop()
    assert(sm.currentInputRate() === 0.0)
    assert(sm.currentProcessingRate() === 0.0)
    assert(sm.currentSourceInputRate(source) === 0.0)
    assert(sm.currentSourceProcessingRate(source) === 0.0)
    assert(sm.currentLatency() === None)
  }

  test("rates and latencies - after trigger with no data") {
    val sm = newStreamMetrics(source)
    // Trigger 1 with data
    sm.reportTriggerStarted(1)
    sm.reportNumInputRows(Map(source -> 100L)) // 100 input rows
    clock.advance(1000)
    sm.reportTriggerFinished()

    // Trigger 2 with data
    clock.advance(1000)
    sm.reportTriggerStarted(2)
    sm.reportNumInputRows(Map(source -> 200L)) // 200 input rows
    clock.advance(500)
    sm.reportTriggerFinished()

    // Make sure that all rates are set
    require(sm.currentInputRate() === 100.0) // 200 input rows generated in 2 seconds b/w starts
    require(sm.currentProcessingRate() === 400.0) // 200 output rows processed in 0.5 sec
    require(sm.currentSourceInputRate(source) === 100.0)
    require(sm.currentSourceProcessingRate(source) === 400.0)
    require(sm.currentLatency().get === 1500.0) // 2000 ms / 2 + 500 ms

    // Trigger 3 with data
    clock.advance(500)
    sm.reportTriggerStarted(3)
    clock.advance(500)
    sm.reportTriggerFinished()

    // Rates are set to zero and latency is set to None
    assert(sm.currentInputRate() === 0.0)
    assert(sm.currentProcessingRate() === 0.0)
    assert(sm.currentSourceInputRate(source) === 0.0)
    assert(sm.currentSourceProcessingRate(source) === 0.0)
    assert(sm.currentLatency() === None)
    sm.stop()
  }

  test("rates - after trigger with multiple sources, and one source having no info") {
    val source1 = TestSource(1)
    val source2 = TestSource(2)
    val sm = newStreamMetrics(source1, source2)
    // Trigger 1 with data
    sm.reportTriggerStarted(1)
    sm.reportNumInputRows(Map(source1 -> 100L, source2 -> 100L))
    clock.advance(1000)
    sm.reportTriggerFinished()

    // Trigger 2 with data
    clock.advance(1000)
    sm.reportTriggerStarted(2)
    sm.reportNumInputRows(Map(source1 -> 200L, source2 -> 200L))
    clock.advance(500)
    sm.reportTriggerFinished()

    // Make sure that all rates are set
    assert(sm.currentInputRate() === 200.0) // 200*2 input rows generated in 2 seconds b/w starts
    assert(sm.currentProcessingRate() === 800.0) // 200*2 output rows processed in 0.5 sec
    assert(sm.currentSourceInputRate(source1) === 100.0)
    assert(sm.currentSourceInputRate(source2) === 100.0)
    assert(sm.currentSourceProcessingRate(source1) === 400.0)
    assert(sm.currentSourceProcessingRate(source2) === 400.0)

    // Trigger 3 with data
    clock.advance(500)
    sm.reportTriggerStarted(3)
    clock.advance(500)
    sm.reportNumInputRows(Map(source1 -> 200L))
    sm.reportTriggerFinished()

    // Rates are set to zero and latency is set to None
    assert(sm.currentInputRate() === 200.0)
    assert(sm.currentProcessingRate() === 400.0)
    assert(sm.currentSourceInputRate(source1) === 200.0)
    assert(sm.currentSourceInputRate(source2) === 0.0)
    assert(sm.currentSourceProcessingRate(source1) === 400.0)
    assert(sm.currentSourceProcessingRate(source2) === 0.0)
    sm.stop()
  }

  test("registered Codahale metrics") {
    import scala.collection.JavaConverters._
    val sm = newStreamMetrics(source)
    val gaugeNames = sm.metricRegistry.getGauges().keySet().asScala

    // so that all metrics are considered as a single metric group in Ganglia
    assert(!gaugeNames.exists(_.contains(".")))
    assert(gaugeNames === Set(
      "inputRate-total",
      "inputRate-source0",
      "processingRate-total",
      "processingRate-source0",
      "latency"))
  }

  private def newStreamMetrics(sources: Source*): StreamMetrics = {
    new StreamMetrics(sources.toSet, clock, "test")
  }

  private val clock = new ManualClock()
  private val source = TestSource(0)

  case class TestSource(id: Int) extends Source {
    override def schema: StructType = StructType(Array.empty[StructField])
    override def getOffset: Option[Offset] = Some(new LongOffset(0))
    override def getBatch(start: Option[Offset], end: Offset): DataFrame = { null }
    override def stop() {}
    override def toString(): String = s"source$id"
  }
}
