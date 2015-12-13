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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.{Accumulator, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.execution.streaming.state.{GroupWindows, StatefulPlanner}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.{SparkPlan, QueryExecution, LogicalRDD}

class EventTimeSource(val max: Accumulator[LongWatermark]) extends Source with Serializable {
  override def watermark: Watermark = max.value

  override def getSlice(
      sqlContext: SQLContext, start: Watermark, end: Watermark): RDD[InternalRow] = ???

  // HACK
  override def equals(other: Any): Boolean = other.isInstanceOf[EventTimeSource]
  override def hashCode: Int = 0

  override def toString: String = "EventTime"
}

class StreamExecution(
    sqlContext: SQLContext,
    private[sql] val logicalPlan: LogicalPlan,
    val sink: Sink) extends Logging {

  /** Tracks how much data we have processed from each input source. */
  private[sql] val currentWatermarks = new StreamProgress

  import org.apache.spark.sql.execution.streaming.state.MaxWatermark
  private[sql] val maxEventTime =
    sqlContext.sparkContext.accumulator(LongWatermark(-1))

  private[sql] val eventTimeSource = new EventTimeSource(maxEventTime)

  /** All stream sources present the query plan. */
  private val sources =
    logicalPlan.collect { case s: Source => s: Source } :+ eventTimeSource

  // Start the execution at the current watermark for the sink. (i.e. avoid reprocessing data
  // that we have already processed).
  sources.foreach { s =>
    val sourceWatermark = sink.currentWatermark(s).getOrElse(LongWatermark(-1))
    currentWatermarks.update(s, sourceWatermark)
  }

  // Restore the position of the eventtime watermark accumulator
  currentWatermarks.get(eventTimeSource).foreach {
    case w: LongWatermark => eventTimeSource.max.setValue(w)
  }

  logWarning(s"Stream running at $currentWatermarks")

  /** When false, signals to the microBatchThread that it should stop running. */
  @volatile private var shouldRun = true

  /** The thread that runs the micro-batches of this stream. */
  private[sql] val microBatchThread = new Thread("stream execution thread") {
    override def run(): Unit = {
      SQLContext.setActive(sqlContext)
      while (shouldRun) { attemptBatch() }
    }
  }
  microBatchThread.setDaemon(true)
  microBatchThread.start()

  var lastExecution: QueryExecution = null

  /**
   * Checks to see if any new data is present in any of the sources.  When new data is available,
   * a batch is executed and passed to the sink, updating the currentWatermarks.
   */
  private def attemptBatch(): Unit = {
    // Check to see if any of the input sources have data that has not been processed.
    val newData = sources.flatMap {
      case s if s.watermark > currentWatermarks(s) => s -> s.watermark :: Nil
      case _ => Nil
    }.toMap

    if (newData.nonEmpty) {
      val startTime = System.nanoTime()
      logInfo(s"Running with new data up to: $newData")

      // Replace sources in the logical plan with data that has arrived since the last batch.
      val newPlan = logicalPlan transform {
        case s: Source if newData.contains(s) =>
          val batchInput = s.getSlice(sqlContext, currentWatermarks(s), newData(s))
          LogicalRDD(s.output, batchInput)(sqlContext)
        case s: Source => LocalRelation(s.output)
      }

      val optimizerStart = System.nanoTime()

      lastExecution = new QueryExecution(sqlContext, newPlan) {
        // Skip the optimizer for now cause the streaming planner is not great.
        override lazy val optimizedPlan: LogicalPlan = EliminateSubQueries(analyzed)
        override lazy val sparkPlan: SparkPlan = {
          SQLContext.setActive(sqlContext)
          val batchPlanner = new StatefulPlanner(
            sqlContext,
            maxEventTime,
            currentWatermarks.copy(),
            currentWatermarks ++ newData)

          logInfo(s"BatchPlanner initialized: $currentWatermarks, $maxEventTime")

          batchPlanner.plan(optimizedPlan).next()
        }
      }
      val executedPlan = lastExecution.executedPlan
      val optimizerTime = (System.nanoTime() - optimizerStart).toDouble / 1000000
      logDebug(s"Optimized batch in ${optimizerTime}ms")

      val results = executedPlan.execute().map(_.copy())
      newData.foreach(currentWatermarks.update)
      sink.addBatch(currentWatermarks.copy(), results)

      logInfo(s"EventTime Watermark: ${maxEventTime.value}")
      StreamExecution.this.synchronized {
        StreamExecution.this.notifyAll()
      }

      val batchTime = (System.nanoTime() - startTime).toDouble / 1000000
      logInfo(s"Compete up to $newData in ${batchTime}ms")
    }

    logDebug(s"Waiting for data, current: $currentWatermarks")
    Thread.sleep(10)
  }

  /**
   * Signals to the thread executing micro-batches that it should stop running after the next
   * batch. This method blocks until the thread stops running.
   */
  def stop(): Unit = {
    shouldRun = false
    while (microBatchThread.isAlive) { Thread.sleep(100) }
  }

  /**
   * Blocks the current thread until processing for data from the given `source` has reached at
   * least the given `watermark`. This method is indented for use primarily when writing tests.
   */
  def awaitWatermark(source: Source, newWatermark: Watermark): Unit = {
    assert(microBatchThread.isAlive)

    while (currentWatermarks(source) < newWatermark) {
      logInfo(s"Waiting until $newWatermark at $source")
      synchronized { wait() }
    }
    logDebug(s"Unblocked at $newWatermark for $source")
  }
}