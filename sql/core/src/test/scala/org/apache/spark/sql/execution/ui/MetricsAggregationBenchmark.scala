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

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.duration._

import org.apache.spark.{SparkConf, TaskState}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.config.Status._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.{AccumulatorMetadata, LongAccumulator, Utils}
import org.apache.spark.util.kvstore.InMemoryStore

/**
 * Benchmark for metrics aggregation in the SQL listener.
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <core test jar> <spark sql test jar>
 *   2. build/sbt "core/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/test:runMain <this class>"
 *      Results will be written to "benchmarks/MetricsAggregationBenchmark-results.txt".
 * }}}
 */
object MetricsAggregationBenchmark extends BenchmarkBase {

  private def metricTrackingBenchmark(
      timer: Benchmark.Timer,
      numMetrics: Int,
      numTasks: Int,
      numStages: Int): Measurements = {
    val conf = new SparkConf()
      .set(LIVE_ENTITY_UPDATE_PERIOD, 0L)
      .set(ASYNC_TRACKING_ENABLED, false)
    val kvstore = new ElementTrackingStore(new InMemoryStore(), conf)
    val listener = new SQLAppStatusListener(conf, kvstore, live = true)
    val store = new SQLAppStatusStore(kvstore, Some(listener))

    val metrics = (0 until numMetrics).map { i =>
      new SQLMetricInfo(s"metric$i", i.toLong, "average")
    }

    val planInfo = new SparkPlanInfo(
      getClass().getName(),
      getClass().getName(),
      Nil,
      Map.empty,
      metrics)

    val idgen = new AtomicInteger()
    val executionId = idgen.incrementAndGet()
    val executionStart = SparkListenerSQLExecutionStart(
      executionId,
      getClass().getName(),
      getClass().getName(),
      getClass().getName(),
      planInfo,
      System.currentTimeMillis(),
      Map.empty)

    val executionEnd = SparkListenerSQLExecutionEnd(executionId, System.currentTimeMillis())

    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)

    timer.startTiming()
    listener.onOtherEvent(executionStart)

    val taskEventsTime = (0 until numStages).map { _ =>
      val stageInfo = new StageInfo(idgen.incrementAndGet(), 0, getClass().getName(),
        numTasks, Nil, Nil, getClass().getName(),
        resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)

      val jobId = idgen.incrementAndGet()
      val jobStart = SparkListenerJobStart(
        jobId = jobId,
        time = System.currentTimeMillis(),
        stageInfos = Seq(stageInfo),
        properties)

      val stageStart = SparkListenerStageSubmitted(stageInfo)

      val taskOffset = idgen.incrementAndGet().toLong
      val taskEvents = (0 until numTasks).map { i =>
        val info = new TaskInfo(
          taskId = taskOffset + i.toLong,
          index = i,
          attemptNumber = 0,
          // The following fields are not used.
          launchTime = 0,
          executorId = "",
          host = "",
          taskLocality = null,
          speculative = false)
        info.markFinished(TaskState.FINISHED, 1L)

        val accumulables = (0 until numMetrics).map { mid =>
          val acc = new LongAccumulator
          acc.metadata = AccumulatorMetadata(mid, None, false)
          acc.toInfo(Some(i.toLong), None)
        }

        info.setAccumulables(accumulables)

        val start = SparkListenerTaskStart(stageInfo.stageId, stageInfo.attemptNumber, info)
        val end = SparkListenerTaskEnd(stageInfo.stageId, stageInfo.attemptNumber,
          taskType = "",
          reason = null,
          info,
          new ExecutorMetrics(),
          null)

        (start, end)
      }

      val jobEnd = SparkListenerJobEnd(
        jobId = jobId,
        time = System.currentTimeMillis(),
        JobSucceeded)

      listener.onJobStart(jobStart)
      listener.onStageSubmitted(stageStart)

      val (_, _taskEventsTime) = Utils.timeTakenMs {
        taskEvents.foreach { case (start, end) =>
          listener.onTaskStart(start)
          listener.onTaskEnd(end)
        }
      }

      listener.onJobEnd(jobEnd)
      _taskEventsTime
    }

    val (_, aggTime) = Utils.timeTakenMs {
      listener.onOtherEvent(executionEnd)
      val metrics = store.executionMetrics(executionId)
      assert(metrics.size == numMetrics, s"${metrics.size} != $numMetrics")
    }

    timer.stopTiming()
    kvstore.close()

    Measurements(taskEventsTime, aggTime)
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val metricCount = 50
    val taskCount = 100000
    val stageCounts = Seq(1, 2, 3)

    val benchmark = new Benchmark(
      s"metrics aggregation ($metricCount metrics, $taskCount tasks per stage)", 1,
      warmupTime = 0.seconds, output = output)

    // Run this outside the measurement code so that classes are loaded and JIT is triggered,
    // otherwise the first run tends to be much slower than others. Also because this benchmark is a
    // bit weird and doesn't really map to what the Benchmark class expects, so it's a bit harder
    // to use warmupTime and friends effectively.
    stageCounts.foreach { count =>
      metricTrackingBenchmark(new Benchmark.Timer(-1), metricCount, taskCount, count)
    }

    val measurements = mutable.HashMap[Int, Seq[Measurements]]()

    stageCounts.foreach { count =>
      benchmark.addTimerCase(s"$count stage(s)") { timer =>
        val m = metricTrackingBenchmark(timer, metricCount, taskCount, count)
        val all = measurements.getOrElse(count, Nil)
        measurements(count) = all ++ Seq(m)
      }
    }

    benchmark.run()

    benchmark.out.printf("Stage Count    Stage Proc. Time    Aggreg. Time\n")
    stageCounts.foreach { count =>
      val data = measurements(count)
      val eventsTimes = data.flatMap(_.taskEventsTimes)
      val aggTimes = data.map(_.aggregationTime)

      val msg = "     %d              %d                %d\n".format(
        count,
        eventsTimes.sum / eventsTimes.size,
        aggTimes.sum / aggTimes.size)
      benchmark.out.printf(msg)
    }
  }

  /**
   * Finer-grained measurements of how long it takes to run some parts of the benchmark. This is
   * collected by the benchmark method, so this collection slightly affects the overall benchmark
   * results, but this data helps with seeing where the time is going, since this benchmark is
   * triggering a whole lot of code in the listener class.
   */
  case class Measurements(
      taskEventsTimes: Seq[Long],
      aggregationTime: Long)
}
