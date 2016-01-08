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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.{SparkPlan, QueryExecution, LogicalRDD}

import scala.collection.mutable.ArrayBuffer

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

  // Start the execution at the current offsets stored in the sink. (i.e. avoid reprocessing data
  // that we have already processed).
  {
    sink.currentProgress match {
      case Some(c: CompositeOffset) =>
        val storedProgress = c.offsets
        val sources = logicalPlan collect {
          case StreamingRelation(source, _) => source
        }

        assert(sources.size == storedProgress.size)
        sources.zip(storedProgress).foreach { case (source, offset) =>
          offset.foreach(currentOffsets.update(source, _))
        }
      case None => // We are starting this stream for the first time.
      case _ => throw new IllegalArgumentException("Expected composite offset from sink")
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
    val startTime = System.nanoTime()

    // A list of offsets that need to be updated if this batch is successful.
    // Populated while walking the tree.
    val newOffsets = new ArrayBuffer[(Source, Offset)]
    // A list of attributes that will need to be updated.
    var replacements = new ArrayBuffer[(Attribute, Attribute)]
    // Replace sources in the logical plan with data that has arrived since the last batch.
    val withNewSources = logicalPlan transform {
      case StreamingRelation(source, output) =>
        val prevOffset = currentOffsets.get(source)
        val newBatch = source.getNextBatch(prevOffset)

        newBatch.map { batch =>
          newOffsets += ((source, batch.end))
          val newPlan = batch.data.logicalPlan

          assert(output.size == newPlan.output.size)
          replacements ++= output.zip(newPlan.output)
          newPlan
        }.getOrElse {
          LocalRelation(output)
        }
    }

    // Rewire the plan to use the new attributes that were returned by the source.
    val replacementMap = AttributeMap(replacements)
    val newPlan = withNewSources transformAllExpressions {
      case a: Attribute if replacementMap.contains(a) => replacementMap(a)
    }

    if (newOffsets.nonEmpty) {
      val optimizerStart = System.nanoTime()

      lastExecution = new QueryExecution(sqlContext, newPlan)
      val executedPlan = lastExecution.executedPlan
      val optimizerTime = (System.nanoTime() - optimizerStart).toDouble / 1000000
      logDebug(s"Optimized batch in ${optimizerTime}ms")

      // Update the offsets and calculate a new composite offset
      newOffsets.foreach(currentOffsets.update)
      val newStreamProgress = logicalPlan.collect {
        case StreamingRelation(source, _) => currentOffsets.get(source)
      }
      val batchOffset = CompositeOffset(newStreamProgress)

      // Construct the batch and send it to the sink.
      val nextBatch = new Batch(batchOffset, new DataFrame(sqlContext, newPlan))
      sink.addBatch(nextBatch)

      // Wake up any threads that are waiting for the stream to progress.
      StreamExecution.this.synchronized {
        StreamExecution.this.notifyAll()
      }

      val batchTime = (System.nanoTime() - startTime).toDouble / 1000000
      logInfo(s"Compete up to $newOffsets in ${batchTime}ms")
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

