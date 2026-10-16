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

package org.apache.spark.scheduler

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

import com.google.common.util.concurrent.Uninterruptibles
import org.scalatest.time.SpanSugar._

import org.apache.spark.{PipelinedShuffleDependency, Success, TaskContext}
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.execution.datasources.v2.RealTimeStreamScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper
import org.apache.spark.sql.execution.streaming.sources.LowLatencyMemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryException, StreamRealTimeModeManualClockSuiteBase}
import org.apache.spark.util.ThreadUtils

class PipelinedRealTimeQuerySuite extends StreamRealTimeModeManualClockSuiteBase {

  import testImplicits._

  test("Real-Time query cancellation notifies released completions before stage and job end") {
    PipelinedRealTimeQuerySuite.reset()
    val sc = spark.sparkContext
    val scheduler = sc.dagScheduler
    val buffered = new CountDownLatch(1)
    val released = new CountDownLatch(1)
    val replayProcessed = new CountDownLatch(1)
    val errors = new ConcurrentLinkedQueue[Throwable]()
    val notifications = new ConcurrentLinkedQueue[(String, Int, Long)]()
    val accumulator = sc.longAccumulator("cancelled Real-Time query results")
    var jobId = -1
    var consumerStageId = -1
    var consumerTaskId = -1L
    val listener = new SparkListener {
      override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
        notifications.add(("task", event.stageId, event.taskInfo.taskId))
      }
      override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
        notifications.add(("stage", event.stageInfo.stageId, -1L))
      }
      override def onJobEnd(event: SparkListenerJobEnd): Unit = {
        notifications.add(("job", event.jobId, -1L))
      }
    }
    val eventLoop = new DAGSchedulerEventProcessLoop(scheduler) {
      override def onReceive(event: DAGSchedulerEvent): Unit = event match {
        case completion: CompletionEvent if completion.reason == Success &&
            completion.task.isInstanceOf[ShuffleMapTask] && buffered.getCount == 0 =>
          val producer = scheduler.stageIdToStage(completion.task.stageId)
            .asInstanceOf[ShuffleMapStage]
          assert(producer.shuffleDep.isInstanceOf[PipelinedShuffleDependency[_, _, _]])
          // Release posts the replay behind this cancellation on the real event-loop queue.
          post(JobCancelled(jobId, None))
          super.onReceive(event)
          assert(scheduler.pipelinedTaskCompletionReplays(consumerStageId)
            .values.exists(_.taskInfo.taskId == consumerTaskId))
          released.countDown()

        case completion: CompletionEvent if completion.reason == Success &&
            completion.task.isInstanceOf[ResultTask[_, _]] =>
          super.onReceive(event)
          val consumer = scheduler.stageIdToStage(completion.task.stageId)
          val deferral = scheduler.dependentStageMap(consumer)
          assert(deferral.parents.nonEmpty)
          assert(deferral.delayedTaskCompletionEvents.exists(_ eq completion))
          jobId = consumer.asInstanceOf[ResultStage].activeJob.get.jobId
          consumerStageId = consumer.id
          consumerTaskId = completion.taskInfo.taskId
          buffered.countDown()

        case _: PipelinedTaskCompletionReplay =>
          super.onReceive(event)
          assert(scheduler.dependentStageMap.isEmpty)
          assert(scheduler.pipelinedTaskCompletionReplays.isEmpty)
          replayProcessed.countDown()

        case _ => super.onReceive(event)
      }

      override def onError(error: Throwable): Unit = {
        errors.add(error)
      }
    }
    sc.addSparkListener(listener)
    var query: StreamingQuery = null
    try {
      scheduler.eventProcessLoop.stop()
      scheduler.setEventProcessLoop(eventLoop)
      eventLoop.start()
      withTempDir { checkpoint =>
        val input = LowLatencyMemoryStream.singlePartition[Int]
        val result = input.toDS().mapPartitions { rows =>
          TaskContext.get().addTaskCompletionListener[Unit] { _ =>
            PipelinedRealTimeQuerySuite.producerFinished.countDown()
            Uninterruptibles.awaitUninterruptibly(PipelinedRealTimeQuerySuite.finishProducer)
          }
          rows
        }.repartition(1).map { value =>
          accumulator.add(1L)
          value
        }
        query = result.writeStream
          .outputMode(OutputMode.Update())
          .foreach(new ForeachWriter[Int] {
            override def open(partitionId: Long, epochId: Long): Boolean = true
            override def process(value: Int): Unit = {
              PipelinedRealTimeQuerySuite.rows.add(value)
            }
            override def close(errorOrNull: Throwable): Unit = {}
          })
          .option("checkpointLocation", checkpoint.getAbsolutePath)
          .trigger(defaultTrigger)
          .start()
        input.addData(1, 2)
        eventually(timeout(60.seconds)) {
          assert(errors.isEmpty, errors.asScala.mkString(", "))
          assert(PipelinedRealTimeQuerySuite.rows.asScala.toSeq.sorted == Seq(1, 2))
        }
        val execution = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution
        assert(execution.executedPlan.exists(_.isInstanceOf[RealTimeStreamScanExec]))
        val exchanges = execution.executedPlan.collect { case s: ShuffleExchangeExec => s }
        assert(exchanges.size == 1)
        assert(exchanges.head.pipelined)
        assert(exchanges.head.shuffleDependency
          .isInstanceOf[PipelinedShuffleDependency[_, _, _]])

        clock.advance(defaultTrigger.batchDurationMs)
        assert(PipelinedRealTimeQuerySuite.producerFinished.await(60, TimeUnit.SECONDS))
        assert(buffered.await(60, TimeUnit.SECONDS), errors.asScala.mkString(", "))
        sc.listenerBus.waitUntilEmpty()
        val beforeRelease = notifications.asScala.toSeq
        assert(!beforeRelease.exists(e => e._1 == "task" && e._2 == consumerStageId))
        assert(!beforeRelease.contains(("stage", consumerStageId, -1L)))
        assert(!beforeRelease.contains(("job", jobId, -1L)))
        assert(accumulator.value == 0L)

        PipelinedRealTimeQuerySuite.finishProducer.countDown()
        assert(released.await(60, TimeUnit.SECONDS), errors.asScala.mkString(", "))
        assert(replayProcessed.await(60, TimeUnit.SECONDS), errors.asScala.mkString(", "))
        intercept[StreamingQueryException] {
          query.awaitTermination(60000)
        }
        sc.listenerBus.waitUntilEmpty()
        assert(errors.isEmpty, errors.asScala.mkString(", "))
        val recorded = notifications.asScala.toSeq
        val taskEnd = ("task", consumerStageId, consumerTaskId)
        val stageCompleted = ("stage", consumerStageId, -1L)
        val jobCompleted = ("job", jobId, -1L)
        val stageEnd = recorded.indexOf(stageCompleted)
        val jobEnd = recorded.indexOf(jobCompleted)
        assert(recorded.count(_ == taskEnd) == 1, recorded.toString)
        assert(recorded.count(_ == stageCompleted) == 1, recorded.toString)
        assert(recorded.count(_ == jobCompleted) == 1, recorded.toString)
        assert(recorded.indexOf(taskEnd) < stageEnd, recorded.toString)
        assert(stageEnd >= 0 && jobEnd > stageEnd, recorded.toString)
        assert(accumulator.value == 0L)
      }
    } finally {
      PipelinedRealTimeQuerySuite.finishProducer.countDown()
      try {
        if (query != null) {
          query.stop()
        }
      } finally {
        try {
          eventLoop.stop()
          val restored = new DAGSchedulerEventProcessLoop(scheduler)
          scheduler.setEventProcessLoop(restored)
          restored.start()
        } finally {
          sc.removeSparkListener(listener)
        }
      }
    }

    // The suite shares its SparkContext, so subsequent jobs must use the standard event loop.
    val nextJob = sc.parallelize(Seq((1, 1), (1, 2), (2, 3)), 2)
      .reduceByKey(_ + _)
      .collectAsync()
    try {
      val result = ThreadUtils.awaitResult(nextJob, Duration(60, TimeUnit.SECONDS))
      assert(result.toMap == Map(1 -> 3, 2 -> 3))
      sc.listenerBus.waitUntilEmpty()
      assert(errors.isEmpty, errors.asScala.mkString(", "))
    } finally {
      if (!nextJob.isCompleted) {
        nextJob.cancel()
      }
    }
  }
}

object PipelinedRealTimeQuerySuite {
  @volatile var producerFinished = new CountDownLatch(1)
  @volatile var finishProducer = new CountDownLatch(1)
  val rows = new ConcurrentLinkedQueue[Int]()

  def reset(): Unit = {
    producerFinished = new CountDownLatch(1)
    finishProducer = new CountDownLatch(1)
    rows.clear()
  }
}
