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
import org.apache.spark.sql.{DataFrame, Strategy, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.{SparkPlanner, LogicalRDD}

class StreamExecution(
    sqlContext: SQLContext,
    logicalPlan: LogicalPlan,
    val sink: Sink) extends Logging {
  private val sources = logicalPlan.collect { case s: Source => s: Source }

  private val currentWatermarks = new StreamProgress

  // Start the execution at the current watermark for the sink. (i.e. avoid reprocessing data
  // that we have already processed).
  sources.foreach { s =>
    val sourceWatermark = sink.currentWatermark(s).getOrElse(new Watermark(-1))
    currentWatermarks.update(s, sourceWatermark)
  }

  @volatile
  private var shouldRun = true

  private val thread = new Thread("stream execution thread") {
    override def run(): Unit = {
      while (shouldRun) { attemptBatch() }
    }
  }

  thread.setDaemon(true)
  thread.start()

  private def attemptBatch(): Unit = {
    val newData = sources.flatMap {
      case s if s.watermark > currentWatermarks(s) => s -> s.watermark :: Nil
      case _ => Nil
    }.toMap

    if (newData.nonEmpty) {
      val startTime = System.nanoTime()
      logDebug(s"Running with new data upto: $newData")

      val newPlan = logicalPlan transform {
        case s: Source if newData.contains(s) =>
          val batchInput = s.getSlice(sqlContext, currentWatermarks(s), newData(s))
          LogicalRDD(s.output, batchInput)(sqlContext)
        case s: Source => LocalRelation(s.output)
      }

      // TODO: Duplicated with QueryExecution
      val analyzedPlan = sqlContext.analyzer.execute(newPlan)
      val optimizedPlan = sqlContext.optimizer.execute(analyzedPlan)
      val physicalPlan = sqlContext.planner.plan(optimizedPlan).next()
      val executedPlan = sqlContext.prepareForExecution.execute(physicalPlan)

      val results = executedPlan.execute().map(_.copy()).cache()
      logInfo(s"Processed ${results.count()} records using plan\n$executedPlan")
      sink.addBatch(newData, results)

      StreamExecution.this.synchronized {
        newData.foreach(currentWatermarks.update)
        StreamExecution.this.notifyAll()
      }

      val batchTime = (System.nanoTime() - startTime).toDouble / 1000000
      logWarning(s"Compete up to $newData in ${batchTime}ms")
    }

    logDebug(s"Waiting for data, current: $currentWatermarks")
    Thread.sleep(10)
  }

  def current: DataFrame = sink.allData(sqlContext)

  def stop(): Unit = {
    shouldRun = false
    while (thread.isAlive) { Thread.sleep(100) }
  }

  def waitUntil(source: Source, newWatermark: Watermark): Unit = {
    assert(thread.isAlive)

    while (currentWatermarks(source) < newWatermark) {
      logInfo(s"Waiting until $newWatermark at $source")
      synchronized { wait() }
    }
    logDebug(s"Unblocked at $newWatermark for $source")
  }
}