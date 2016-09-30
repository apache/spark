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

import scala.collection.mutable

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.{Source => CodahaleSource}
import org.apache.spark.util.Clock

class StreamMetrics(sources: Set[Source], triggerClock: Clock, codahaleSourceName: String)
  extends CodahaleSource with Logging {

  import StreamMetrics._

  // Trigger infos
  private val triggerInfo = new mutable.HashMap[String, String]
  private val sourceTriggerInfo = new mutable.HashMap[Source, mutable.HashMap[String, String]]

  // Rate estimators for sources and sinks
  private val inputRates = new mutable.HashMap[Source, RateCalculator]
  private val processingRates = new mutable.HashMap[Source, RateCalculator]
  private val outputRate = new RateCalculator

  // Number of input rows in the current trigger
  private val numInputRows = new mutable.HashMap[Source, Long]
  private var numOutputRows: Option[Long] = None
  private var currentTriggerStartTimestamp: Long = -1
  private var previousTriggerStartTimestamp: Long = -1
  private var latency: Option[Double] = None

  override val sourceName: String = codahaleSourceName
  override val metricRegistry: MetricRegistry = new MetricRegistry

  // =========== Initialization ===========

  registerGauge("inputRate.total", currentInputRate)
  registerGauge("processingRate.total", () => currentProcessingRate)
  registerGauge("outputRate.total", () => currentOutputRate)
  registerGauge("latencyMs", () => currentLatency().getOrElse(-1.0))

  sources.foreach { s =>
    inputRates.put(s, new RateCalculator)
    processingRates.put(s, new RateCalculator)
    sourceTriggerInfo.put(s, new mutable.HashMap[String, String])

    registerGauge(s"inputRate.${s.toString}", () => currentSourceInputRate(s))
    registerGauge(s"processingRate.${s.toString}", () => currentSourceProcessingRate(s))
  }

  // =========== Setter methods ===========

  def reportTriggerStarted(triggerId: Long): Unit = synchronized {
    numInputRows.clear()
    numOutputRows = None
    triggerInfo.clear()
    sourceTriggerInfo.values.foreach(_.clear())

    reportTriggerInfo(TRIGGER_ID, triggerId)
    sources.foreach(s => reportSourceTriggerInfo(s, TRIGGER_ID, triggerId))
    reportTriggerInfo(ACTIVE, true)
    currentTriggerStartTimestamp = triggerClock.getTimeMillis()
    reportTriggerInfo(START_TIMESTAMP, currentTriggerStartTimestamp)
  }

  def reportTimestamp(key: String): Unit = synchronized {
    triggerInfo.put(key, triggerClock.getTimeMillis().toString)
  }

  def reportLatency(key: String, latencyMs: Long): Unit = synchronized {
    triggerInfo.put(key, latencyMs.toString)
  }

  def reportLatency(source: Source, key: String, latencyMs: Long): Unit = synchronized {
    sourceTriggerInfo(source).put(key, latencyMs.toString)
  }

  def reportTriggerInfo[T](key: String, value: T): Unit = synchronized {
    triggerInfo.put(key, value.toString)
  }

  def reportSourceTriggerInfo[T](source: Source, key: String, value: T): Unit = synchronized {
    sourceTriggerInfo(source).put(key, value.toString)
  }

  def reportNumRows(inputRows: Map[Source, Long], outputRows: Option[Long]): Unit = synchronized {
    numInputRows ++= inputRows
    numOutputRows = outputRows
  }

  def reportTriggerFinished(): Unit = synchronized {
    require(currentTriggerStartTimestamp >= 0)
    val currentTriggerFinishTimestamp = triggerClock.getTimeMillis()
    reportTriggerInfo(FINISH_TIMESTAMP, currentTriggerFinishTimestamp)
    reportTriggerInfo(STATUS_MESSAGE, "")
    reportTriggerInfo(ACTIVE, false)

    // Report number of rows
    val totalNumInputRows = numInputRows.values.sum
    reportTriggerInfo(NUM_INPUT_ROWS, totalNumInputRows)
    reportTriggerInfo(NUM_OUTPUT_ROWS, numOutputRows.getOrElse(0))
    numInputRows.foreach { case (s, r) =>
      reportSourceTriggerInfo(s, NUM_SOURCE_INPUT_ROWS, r)
    }

    val currentTriggerDuration = currentTriggerFinishTimestamp - currentTriggerStartTimestamp
    val previousInputIntervalOption = if (previousTriggerStartTimestamp >= 0) {
      Some(currentTriggerStartTimestamp - previousTriggerStartTimestamp)
    } else None

    // Update input rate = num rows received by each source during the previous trigger interval
    // Interval is measures as interval between start times of previous and current trigger.
    //
    // TODO: Instead of trigger start, we should use time when getOffset was called on each source
    // as this may be different for each source if there are many sources in the query plan
    // and getOffset is called serially on them.
    if (previousInputIntervalOption.nonEmpty) {
      numInputRows.foreach { case (s, v) =>
        inputRates(s).update(v, previousInputIntervalOption.get)
      }
    }

    // Update processing rate = num rows processed for each source in current trigger duration
    numInputRows.foreach { case (s, v) =>
      processingRates(s).update(v, currentTriggerDuration)
    }

    // Update output rate = num rows output to the sink in current trigger duration
    outputRate.update(numOutputRows.getOrElse(0), currentTriggerDuration)
    logDebug("Output rate updated to " + outputRate.currentRate)

    // Update latency = if data present, 0.5 * previous trigger interval + current trigger duration
    if (previousInputIntervalOption.nonEmpty && totalNumInputRows > 0) {
      latency = Some((previousInputIntervalOption.get.toDouble / 2) + currentTriggerDuration)
    } else {
      latency = None
    }

    previousTriggerStartTimestamp = currentTriggerStartTimestamp
    currentTriggerStartTimestamp = -1
  }

  // =========== Getter methods ===========

  def currentInputRate(): Double = synchronized {
    // Since we are calculating source input rates using the same time interval for all sources
    // it is fine to calculate total input rate as the sum of per source input rate.
    inputRates.map(_._2.currentRate).sum
  }

  def currentSourceInputRate(source: Source): Double = synchronized {
    inputRates(source).currentRate
  }

  def currentProcessingRate(): Double = synchronized {
    // Since we are calculating source processing rates using the same time interval for all sources
    // it is fine to calculate total processing rate as the sum of per source processing rate.
    processingRates.map(_._2.currentRate).sum
  }

  def currentSourceProcessingRate(source: Source): Double = synchronized {
    processingRates(source).currentRate
  }

  def currentOutputRate(): Double = synchronized { outputRate.currentRate }

  def currentLatency(): Option[Double] = synchronized { latency }

  def currentTriggerInfo(): Map[String, String] = synchronized { triggerInfo.toMap }

  def currentSourceTriggerInfo(source: Source): Map[String, String] = synchronized {
    sourceTriggerInfo(source).toMap
  }

  // =========== Other methods ===========

  private def registerGauge[T](name: String, f: () => T)(implicit num: Numeric[T]): Unit = {
    synchronized {
      metricRegistry.register(name, new Gauge[T] {
        override def getValue: T = f()
      })
    }
  }

  def stop(): Unit = synchronized {
    inputRates.valuesIterator.foreach { _.stop() }
    processingRates.valuesIterator.foreach { _.stop() }
    outputRate.stop()
    latency = None
  }
}

object StreamMetrics extends Logging {

  class RateCalculator {
    @volatile private var rate: Option[Double] = None

    def update(numRows: Long, timeGapMs: Long): Unit = {
      if (timeGapMs > 0) {
        rate = Some(numRows.toDouble * 1000 / timeGapMs)
      } else {
        rate = None
        logDebug(s"Rate updates cannot with zero or negative time gap $timeGapMs")
      }
    }

    def currentRate: Double = rate.getOrElse(0.0)

    def stop(): Unit = { rate = None }
  }

  val TRIGGER_ID = "triggerId"
  val ACTIVE = "isActive"
  val DATA_AVAILABLE = "isDataAvailable"
  val STATUS_MESSAGE = "statusMessage"

  val START_TIMESTAMP = "timestamp.triggerStart"
  val GET_OFFSET_TIMESTAMP = "timestamp.afterGetOffset"
  val GET_BATCH_TIMESTAMP = "timestamp.afterGetBatch"
  val FINISH_TIMESTAMP = "timestamp.triggerFinish"

  val SOURCE_GET_OFFSET_LATENCY = "latency.sourceGetOffset"
  val GET_OFFSET_LATENCY = "latency.getOffset"
  val OFFSET_WAL_WRITE_LATENCY = "latency.offsetLogWrite"
  val GET_BATCH_LATENCY = "latency.getBatch"
  val TRIGGER_LATENCY = "latency.fullTrigger"

  val NUM_INPUT_ROWS = "numRows.input"
  val NUM_OUTPUT_ROWS = "numRows.output"
  val NUM_SOURCE_INPUT_ROWS = "numRows.sourceInput"
  def NUM_TOTAL_STATE_ROWS(aggId: Int): String = s"numRows.state.$aggId.total"
  def NUM_UPDATED_STATE_ROWS(aggId: Int): String = s"numRows.state.$aggId.updated"
}
