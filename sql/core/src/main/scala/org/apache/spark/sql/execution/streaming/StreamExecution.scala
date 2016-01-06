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

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.{SparkPlan, QueryExecution, LogicalRDD}

/**
 * Manages the execution of a streaming Spark SQL query that is occuring in a separate thread.
 * Unlike a standard query, a streaming query executes repeatedly each time new data arrives at any
 * [[Source]] present in the query plan. Whenever new data arrives, a [[QueryExecution]] is created
 * and the results are committed transactionally to the given [[Sink]].
 */
class StreamExecution(
    sqlContext: SQLContext,
    private[sql] val logicalPlan: LogicalPlan,
    val sink: Sink) extends Logging {

  /** Minimum amount of time in between the start of each batch. */
  val minBatchTime = 10

  /** Tracks how much data we have processed from each input source. */
  private[sql] val currentOffsets = new StreamProgress

  /** All stream sources present the query plan. */
  private val sources =
    logicalPlan.collect { case s: StreamingRelation => s.source }

  // Start the execution at the current Offset for the sink. (i.e. avoid reprocessing data
  // that we have already processed).
  {
    val storedProgress = sink.currentProgress
    sources.foreach { s =>
      storedProgress.get(s).foreach { offset =>
        currentOffsets.update(s, offset)
      }
    }
  }

  logInfo(s"Stream running at $currentOffsets")

  /** When false, signals to the microBatchThread that it should stop running. */
  @volatile private var shouldRun = true

  // TODO: add exception handling to batch thread
  /** The thread that runs the micro-batches of this stream. */
  private[sql] val microBatchThread = new Thread("stream execution thread") {
    override def run(): Unit = {
      SQLContext.setActive(sqlContext)
      while (shouldRun) { attemptBatch() }
    }
  }
  microBatchThread.setDaemon(true)
  microBatchThread.start()

  @volatile
  private[sql] var lastExecution: QueryExecution = null

  /**
   * Checks to see if any new data is present in any of the sources.  When new data is available,
   * a batch is executed and passed to the sink, updating the currentOffsets.
   */
  private def attemptBatch(): Unit = {

    val newData = sources.flatMap { s =>
      val prevOffset = currentOffsets.get(s)
      val latestOffset = s.getCurrentOffset
      if (prevOffset.isEmpty || latestOffset > prevOffset.get) {
        Some(s -> latestOffset)
      } else None
    }.toMap

    if (newData.nonEmpty) {
      val startTime = System.nanoTime()
      logInfo(s"Running with new data up to: $newData")

      // Replace sources in the logical plan with data that has arrived since the last batch.
      val newPlan = logicalPlan transform {
        case StreamingRelation(source, output) =>
          if (newData.contains(source)) {
            val batchInput =
              source.getSlice(sqlContext, currentOffsets.get(source), newData(source))
            LogicalRDD(output, batchInput)(sqlContext)
          } else {
            LocalRelation(output)
          }
      }

      val optimizerStart = System.nanoTime()

      lastExecution = new QueryExecution(sqlContext, newPlan)
      val executedPlan = lastExecution.executedPlan
      val optimizerTime = (System.nanoTime() - optimizerStart).toDouble / 1000000
      logDebug(s"Optimized batch in ${optimizerTime}ms")

      val results = executedPlan.execute().map(_.copy())
      newData.foreach(currentOffsets.update)
      sink.addBatch(currentOffsets.copy(), results)

      StreamExecution.this.synchronized {
        StreamExecution.this.notifyAll()
      }

      val batchTime = (System.nanoTime() - startTime).toDouble / 1000000
      logInfo(s"Compete up to $newData in ${batchTime}ms")
    }

    logDebug(s"Waiting for data, current: $currentOffsets")

    // TODO: this could be tighter...
    Thread.sleep(minBatchTime)
  }

  /**
   * Signals to the thread executing micro-batches that it should stop running after the next
   * batch. This method blocks until the thread stops running.
   */
  def stop(): Unit = {
    shouldRun = false
    if (microBatchThread.isAlive) { microBatchThread.join() }
  }

  /**
   * Blocks the current thread until processing for data from the given `source` has reached at
   * least the given `Offset`. This method is indented for use primarily when writing tests.
   */
  def awaitOffset(source: Source, newOffset: Offset): Unit = {
    while (!currentOffsets.contains(source) || currentOffsets(source) < newOffset) {
      logInfo(s"Waiting until $newOffset at $source")
      synchronized { wait() }
    }
    logDebug(s"Unblocked at $newOffset for $source")
  }
}

