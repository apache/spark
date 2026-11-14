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

package org.apache.spark.status

import java.util.Date

import org.apache.spark.{JobExecutionStatus, SparkConf, TaskState}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.Status.{ASYNC_TRACKING_ENABLED, COMPACT_UI_STORE_ENABLED}
import org.apache.spark.scheduler.{TaskInfo, TaskLocality}
import org.apache.spark.status.api.v1.JobData
import org.apache.spark.util.SizeEstimator

/**
 * Compares task and job storage without starting a SparkContext. Arguments are total tasks,
 * tasks per stage and retained jobs. Memory figures estimate object layouts after traversing
 * the full reachable graph without sampling. They do not measure post-GC retained heap,
 * query peaks or process RSS.
 *
 * Run: build/sbt "core/Test/runMain org.apache.spark.status.CompactUIStoreBenchmark"
 */
object CompactUIStoreBenchmark extends BenchmarkBase {
  @volatile private var result = 0L

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val tasks = mainArgs.headOption.map(_.toInt).getOrElse(100000)
    val stageSize = mainArgs.lift(1).map(_.toInt).getOrElse(10000)
    val jobs = mainArgs.lift(2).map(_.toInt).getOrElse(1000)
    require(tasks > 0 && stageSize > 0 && jobs > 0)
    runBenchmark("Compact task and job storage") {
      Seq(false, true).foreach { enabled =>
        val name = if (enabled) "compact" else "baseline"
        val store = createStore(enabled)
        try {
          val emptyBytes = SizeEstimator.estimateWithoutSampling(store)
          val start = System.nanoTime()
          populate(store, tasks, stageSize, jobs)
          val writeMillis = (System.nanoTime() - start) / 1000000L
          val bytes = SizeEstimator.estimateWithoutSampling(store) - emptyBytes
          // scalastyle:off println
          println(s"$name: tasks=$tasks stageSize=$stageSize jobs=$jobs " +
            s"full-graph estimated incremental bytes=$bytes population ms=$writeMillis")
          // scalastyle:on println
          val status = new AppStatusStore(store)
          val read = new Benchmark(s"$name UI reads (one request)", 1, output = output)
          read.addCase("first task page by runtime") { _ =>
            result = status.taskList(0, 0, 0, 100,
              Some(TaskIndexNames.EXEC_RUN_TIME), ascending = false).map(_.taskId).sum
          }
          read.addCase("deep task page by runtime") { _ =>
            result = status.taskList(0, 0, math.min(stageSize, tasks) / 2, 100,
              Some(TaskIndexNames.EXEC_RUN_TIME), ascending = false).map(_.taskId).sum
          }
          read.addCase("job summaries") { _ =>
            result = status.jobSummaries().iterator.map(_.jobId.toLong).sum
          }
          read.addCase("job details") { _ =>
            result = status.job(jobs / 2).killedTasksSummary.size
          }
          read.run()
          val summaries = new Benchmark(s"$name exact quantiles (one request)", 1, output = output)
          summaries.addTimerCase("uncached") { timer =>
            store.removeAllByIndexValues(classOf[CachedQuantile], "stage", Seq(Array(0, 0)))
            timer.startTiming()
            result = status.taskSummary(0, 0, Array(0.0, 0.25, 0.5, 0.75, 1.0))
              .map(_.executorRunTime.sum.toLong).getOrElse(0L)
            timer.stopTiming()
          }
          summaries.run()
        } finally {
          store.close()
        }
      }
      val writes = new Benchmark("Task and job population", tasks, output = output)
      Seq(false, true).foreach { enabled =>
        writes.addCase(if (enabled) "compact" else "baseline") { _ =>
          val store = createStore(enabled)
          try {
            populate(store, tasks, stageSize, jobs)
            result = store.count(classOf[TaskDataWrapper])
          } finally {
            store.close()
          }
        }
      }
      writes.run()
    }
  }

  private def createStore(compact: Boolean): ElementTrackingStore = {
    val conf = new SparkConf(false).set(COMPACT_UI_STORE_ENABLED, compact)
      .set(ASYNC_TRACKING_ENABLED, false)
    new ElementTrackingStore(KVUtils.createInMemoryStore(conf), conf)
  }

  private def populate(
      store: ElementTrackingStore,
      tasks: Int,
      stageSize: Int,
      jobs: Int): Unit = {
    var id = 0
    while (id < tasks) {
      val info = new TaskInfo(id.toLong, id % stageSize, 0, id % stageSize,
        1000000000000L + id, "executor", "host", TaskLocality.PROCESS_LOCAL, false)
      info.markFinished(TaskState.FINISHED, info.launchTime + 100 + id % 1000)
      val task = new LiveTask(info, id / stageSize, 0, None)
      val metrics = TaskMetrics.empty
      metrics.setExecutorRunTime(100L + id % 1000)
      metrics.setExecutorCpuTime((100L + id % 1000) * 1000000L)
      task.updateMetrics(metrics)
      task.write(store, 0L)
      id += 1
    }
    (0 until jobs).foreach { job => store.write(jobData(job)) }
    store.compact()
  }

  private def jobData(id: Int): JobDataWrapper = {
    val reasons = (0 until 20).map { reason => s"reason $reason for job $id" -> 1 }.toMap
    val info = new JobData(id, s"job $id", Some(s"description $id"), Some(new Date(100)),
      Some(new Date(200)), Seq(id), None, Seq("batch"), JobExecutionStatus.SUCCEEDED,
      100, 0, 100, 0, 0, 20, 100, 0, 1, 0, 0, reasons)
    new JobDataWrapper(info, Set.empty, Some(id.toLong))
  }
}
