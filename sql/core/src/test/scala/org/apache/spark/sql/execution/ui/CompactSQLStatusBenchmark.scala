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

package org.apache.spark.sql.execution.ui

import java.util.Date

import scala.collection.mutable

import org.apache.spark.JobExecutionStatus
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.status.protobuf.KVStoreProtobufSerializer
import org.apache.spark.util.{MetricUtils, SizeEstimator}
import org.apache.spark.util.kvstore.{CompactInMemoryStore, InMemoryStore, KVStore}

/**
 * Synthetic UI memory and CPU benchmark without a SparkSession. Arguments are tasks per stage,
 * metrics per task, retained executions, and plan lines per execution. Heap figures estimate
 * object layouts after traversing the full reachable graph without sampling. They do not
 * measure post-GC retained heap or process RSS.
 *
 * Run: build/sbt "sql/Test/runMain org.apache.spark.sql.execution.ui.CompactSQLStatusBenchmark"
 */
object CompactSQLStatusBenchmark extends BenchmarkBase {
  @volatile private var result = 0L

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val tasks = mainArgs.headOption.map(_.toInt).getOrElse(100000)
    val metrics = mainArgs.lift(1).map(_.toInt).getOrElse(30)
    val executions = mainArgs.lift(2).map(_.toInt).getOrElse(1000)
    val planLines = mainArgs.lift(3).map(_.toInt).getOrElse(128)
    require(tasks > 0 && metrics > 0 && executions > 0 && planLines > 0)
    runBenchmark("SQL metric state and retained execution payloads") {
      val baseline = populatedStage(tasks, metrics, compact = false)
      val compact = populatedStage(tasks, metrics, compact = true)
      // scalastyle:off println
      println("SQL metrics full-graph estimated bytes: " +
        s"baseline=${SizeEstimator.estimateWithoutSampling(baseline)}, " +
        s"compact=${SizeEstimator.estimateWithoutSampling(compact)}")
      // scalastyle:on println

      val update = new Benchmark("SQL metric task updates", tasks, output = output)
      Seq(false, true).foreach { enabled =>
        update.addCase(if (enabled) "compact" else "baseline") { _ =>
          result = populatedStage(tasks, metrics, enabled).metricIds().size
        }
      }
      update.run()

      val aggregation = new Benchmark("SQL metric aggregation (one request)", 1, output = output)
      Seq("baseline" -> baseline, "compact" -> compact).foreach { case (name, stage) =>
        aggregation.addCase(name) { _ =>
          result = stage.metricIds().iterator.map { id =>
            MetricUtils.stringValue(stage.accumIdsToMetricType(id), stage.metricValues(id).get,
              Array.emptyLongArray).hashCode.toLong
          }.sum
        }
      }
      aggregation.run()

      Seq(false, true).foreach { enabled =>
        val name = if (enabled) "compact" else "baseline"
        val store: KVStore = if (enabled) {
          new CompactInMemoryStore(new KVStoreProtobufSerializer)
        } else {
          new InMemoryStore
        }
        try {
          val start = System.nanoTime()
          (0 until executions).foreach { id =>
            val data = execution(id, metrics, planLines)
            if (enabled) {
              store.write(CompactSQLExecutionData.details(data))
              store.write(CompactSQLExecutionData.summary(data))
            } else {
              store.write(data)
            }
            store.write(new SparkPlanGraphWrapper(id,
              Seq(new SparkPlanGraphNodeWrapper(new SparkPlanGraphNode(
                0L, "Scan", data.physicalPlanDescription, data.metrics), null)), Nil))
          }
          val populationMillis = (System.nanoTime() - start) / 1000000L
          // scalastyle:off println
          println(s"$name executions: full-graph estimated bytes=" +
            s"${SizeEstimator.estimateWithoutSampling(store)}, " +
            s"single population time ms=$populationMillis")
          // scalastyle:on println
          val status = new SQLAppStatusStore(store)
          val read = new Benchmark(s"$name SQL reads (one request)", 1, output = output)
          read.addCase("summary listing") { _ =>
            result = status.executionSummariesList().iterator.map(_.executionId).sum
          }
          read.addCase("one detail") { _ =>
            result = status.execution(executions / 2).get.physicalPlanDescription.length
          }
          read.run()
        } finally {
          store.close()
        }
      }
    }
  }

  private def populatedStage(tasks: Int, metrics: Int, compact: Boolean): LiveStageMetrics = {
    val types = mutable.Map.from((0 until metrics).map { id =>
      id.toLong -> (if (id % 2 == 0) "sum" else "timing")
    })
    val stage = new LiveStageMetrics(1, 0, tasks, types, compact)
    var task = 0
    while (task < tasks) {
      val updates = (0 until metrics).map { id =>
        val value = if (task % 17 == 0 && id % 2 != 0) -1L else (task % 64).toLong
        AccumulableInfo(id.toLong, None, Some(value), None, false, false)
      }
      stage.updateTaskMetrics(task.toLong, task, finished = true, updates)
      task += 1
    }
    stage.compact()
    stage
  }

  private def execution(id: Int, metrics: Int, planLines: Int): SQLExecutionUIData = {
    new SQLExecutionUIData(
      id, id, s"query $id", "query details",
      s"== Physical Plan $id ==\n" + "Scan parquet [id, value, partition]\n" * planLines,
      Map("spark.sql.shuffle.partitions" -> "200"),
      (0 until metrics).map(m => SQLPlanMetric(s"metric$m", m.toLong, "sum")),
      1L, Some(new Date(2L)), Some(""), Map(id -> JobExecutionStatus.SUCCEEDED),
      Set(id), (0 until metrics).map(m => m.toLong -> "1234").toMap)
  }
}
