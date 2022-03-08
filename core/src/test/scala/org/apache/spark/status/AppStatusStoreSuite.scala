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

import scala.util.Random

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{TaskInfo, TaskLocality}
import org.apache.spark.util.{Distribution, Utils}
import org.apache.spark.util.kvstore._

class AppStatusStoreSuite extends SparkFunSuite {

  private val uiQuantiles = Array(0.0, 0.25, 0.5, 0.75, 1.0)
  private val stageId = 1
  private val attemptId = 1

  test("quantile calculation: 1 task") {
    compareQuantiles(1, uiQuantiles)
  }

  test("quantile calculation: few tasks") {
    compareQuantiles(4, uiQuantiles)
  }

  test("quantile calculation: more tasks") {
    compareQuantiles(100, uiQuantiles)
  }

  test("quantile calculation: lots of tasks") {
    compareQuantiles(4096, uiQuantiles)
  }

  test("quantile calculation: custom quantiles") {
    compareQuantiles(4096, Array(0.01, 0.33, 0.5, 0.42, 0.69, 0.99))
  }

  test("quantile cache") {
    val store = new InMemoryStore()
    (0 until 4096).foreach { i => store.write(newTaskData(i)) }

    val appStore = new AppStatusStore(store)

    appStore.taskSummary(stageId, attemptId, Array(0.13d))
    intercept[NoSuchElementException] {
      store.read(classOf[CachedQuantile], Array(stageId, attemptId, "13"))
    }

    appStore.taskSummary(stageId, attemptId, Array(0.25d))
    val d1 = store.read(classOf[CachedQuantile], Array(stageId, attemptId, "25"))

    // Add a new task to force the cached quantile to be evicted, and make sure it's updated.
    store.write(newTaskData(4096))
    appStore.taskSummary(stageId, attemptId, Array(0.25d, 0.50d, 0.73d))

    val d2 = store.read(classOf[CachedQuantile], Array(stageId, attemptId, "25"))
    assert(d1.taskCount != d2.taskCount)

    store.read(classOf[CachedQuantile], Array(stageId, attemptId, "50"))
    intercept[NoSuchElementException] {
      store.read(classOf[CachedQuantile], Array(stageId, attemptId, "73"))
    }

    assert(store.count(classOf[CachedQuantile]) === 2)
  }

  private def createAppStore(disk: Boolean, live: Boolean): AppStatusStore = {
    val conf = new SparkConf()
    if (live) {
      return AppStatusStore.createLiveStore(conf)
    }

    val store: KVStore = if (disk) {
      val testDir = Utils.createTempDir()
      val diskStore = KVUtils.open(testDir, getClass.getName)
      new ElementTrackingStore(diskStore, conf)
    } else {
      new ElementTrackingStore(new InMemoryStore, conf)
    }
    new AppStatusStore(store)
  }

  Seq(
    "disk" -> createAppStore(disk = true, live = false),
    "in memory" -> createAppStore(disk = false, live = false),
    "in memory live" -> createAppStore(disk = false, live = true)
  ).foreach { case (hint, appStore) =>
    test(s"SPARK-26260: summary should contain only successful tasks' metrics (store = $hint)") {
      val store = appStore.store

      // Success and failed tasks metrics
      for (i <- 0 to 5) {
        if (i % 2 == 0) {
          writeTaskDataToStore(i, store, "FAILED")
        } else {
          writeTaskDataToStore(i, store, "SUCCESS")
        }
      }

      // Running tasks metrics (-1 = no metrics reported, positive = metrics have been reported)
      Seq(-1, 6).foreach { metric =>
        writeTaskDataToStore(metric, store, "RUNNING")
      }

      /**
       * Following are the tasks metrics,
       * 1, 3, 5 => Success
       * 0, 2, 4 => Failed
       * -1, 6 => Running
       *
       * Task summary will consider (1, 3, 5) only
       */
      val summary = appStore.taskSummary(stageId, attemptId, uiQuantiles).get
      val successfulTasks = Array(getTaskMetrics(1), getTaskMetrics(3), getTaskMetrics(5))

      def assertQuantiles(metricGetter: TaskMetrics => Double,
        actualQuantiles: Seq[Double]): Unit = {
        val values = successfulTasks.map(metricGetter)
        val expectedQuantiles = new Distribution(values, 0, values.length)
          .getQuantiles(uiQuantiles.sorted)

        assert(actualQuantiles === expectedQuantiles)
      }

      assertQuantiles(_.executorDeserializeTime, summary.executorDeserializeTime)
      assertQuantiles(_.executorDeserializeCpuTime, summary.executorDeserializeCpuTime)
      assertQuantiles(_.executorRunTime, summary.executorRunTime)
      assertQuantiles(_.executorRunTime, summary.executorRunTime)
      assertQuantiles(_.executorCpuTime, summary.executorCpuTime)
      assertQuantiles(_.resultSize, summary.resultSize)
      assertQuantiles(_.jvmGCTime, summary.jvmGcTime)
      assertQuantiles(_.resultSerializationTime, summary.resultSerializationTime)
      assertQuantiles(_.memoryBytesSpilled, summary.memoryBytesSpilled)
      assertQuantiles(_.diskBytesSpilled, summary.diskBytesSpilled)
      assertQuantiles(_.peakExecutionMemory, summary.peakExecutionMemory)
      assertQuantiles(_.inputMetrics.bytesRead, summary.inputMetrics.bytesRead)
      assertQuantiles(_.inputMetrics.recordsRead, summary.inputMetrics.recordsRead)
      assertQuantiles(_.outputMetrics.bytesWritten, summary.outputMetrics.bytesWritten)
      assertQuantiles(_.outputMetrics.recordsWritten, summary.outputMetrics.recordsWritten)
      assertQuantiles(_.shuffleReadMetrics.remoteBlocksFetched,
        summary.shuffleReadMetrics.remoteBlocksFetched)
      assertQuantiles(_.shuffleReadMetrics.localBlocksFetched,
        summary.shuffleReadMetrics.localBlocksFetched)
      assertQuantiles(_.shuffleReadMetrics.fetchWaitTime, summary.shuffleReadMetrics.fetchWaitTime)
      assertQuantiles(_.shuffleReadMetrics.remoteBytesRead,
        summary.shuffleReadMetrics.remoteBytesRead)
      assertQuantiles(_.shuffleReadMetrics.remoteBytesReadToDisk,
        summary.shuffleReadMetrics.remoteBytesReadToDisk)
      assertQuantiles(
        t => t.shuffleReadMetrics.localBytesRead + t.shuffleReadMetrics.remoteBytesRead,
        summary.shuffleReadMetrics.readBytes)
      assertQuantiles(
        t => t.shuffleReadMetrics.localBlocksFetched + t.shuffleReadMetrics.remoteBlocksFetched,
        summary.shuffleReadMetrics.totalBlocksFetched)
      assertQuantiles(_.shuffleWriteMetrics.bytesWritten, summary.shuffleWriteMetrics.writeBytes)
      assertQuantiles(_.shuffleWriteMetrics.writeTime, summary.shuffleWriteMetrics.writeTime)
      assertQuantiles(_.shuffleWriteMetrics.recordsWritten,
        summary.shuffleWriteMetrics.writeRecords)

      appStore.close()
    }
  }

  private def compareQuantiles(count: Int, quantiles: Array[Double]): Unit = {
    val store = new InMemoryStore()
    val values = (0 until count).map { i =>
      val task = newTaskData(i)
      store.write(task)
      i.toDouble
    }.toArray

    val summary = new AppStatusStore(store).taskSummary(stageId, attemptId, quantiles).get
    val dist = new Distribution(values, 0, values.length).getQuantiles(quantiles.sorted)

    dist.zip(summary.executorRunTime).foreach { case (expected, actual) =>
      assert(expected === actual)
    }
  }

  private def newTaskData(i: Int, status: String = "SUCCESS"): TaskDataWrapper = {
    new TaskDataWrapper(
      i.toLong, i, i, i, i, i, i.toString, i.toString, status, i.toString, false, Nil, None, true,
      i, i, i, i, i, i, i, i, i, i,
      i, i, i, i, i, i, i, i, i, i,
      i, i, i, i, stageId, attemptId)
  }

  private def writeTaskDataToStore(i: Int, store: KVStore, status: String): Unit = {
    val liveTask = new LiveTask(new TaskInfo( i.toLong, i, i, i.toLong, i.toString,
       i.toString, TaskLocality.ANY, false), stageId, attemptId, None)

    if (status == "SUCCESS") {
      liveTask.info.finishTime = 1L
    } else if (status == "FAILED") {
      liveTask.info.failed = true
      liveTask.info.finishTime = 1L
    }

    val taskMetrics = getTaskMetrics(i)
    liveTask.updateMetrics(taskMetrics)
    liveTask.write(store.asInstanceOf[ElementTrackingStore], 1L)
  }

  /**
   * Creates fake task metrics
   * @param seed The random seed. The output will be reproducible for a given seed.
   * @return The test metrics object with fake data
   */
  private def getTaskMetrics(seed: Int): TaskMetrics = {
    val random = new Random(seed)
    val randomMax = 1000
    def nextInt(): Int = random.nextInt(randomMax)

    val taskMetrics = new TaskMetrics()
    taskMetrics.setExecutorDeserializeTime(nextInt())
    taskMetrics.setExecutorDeserializeCpuTime(nextInt())
    taskMetrics.setExecutorRunTime(nextInt())
    taskMetrics.setExecutorCpuTime(nextInt())
    taskMetrics.setResultSize(nextInt())
    taskMetrics.setJvmGCTime(nextInt())
    taskMetrics.setResultSerializationTime(nextInt())
    taskMetrics.incMemoryBytesSpilled(nextInt())
    taskMetrics.incDiskBytesSpilled(nextInt())
    taskMetrics.incPeakExecutionMemory(nextInt())
    taskMetrics.inputMetrics.incBytesRead(nextInt())
    taskMetrics.inputMetrics.incRecordsRead(nextInt())
    taskMetrics.outputMetrics.setBytesWritten(nextInt())
    taskMetrics.outputMetrics.setRecordsWritten(nextInt())
    taskMetrics.shuffleReadMetrics.incRemoteBlocksFetched(nextInt())
    taskMetrics.shuffleReadMetrics.incLocalBlocksFetched(nextInt())
    taskMetrics.shuffleReadMetrics.incFetchWaitTime(nextInt())
    taskMetrics.shuffleReadMetrics.incRemoteBytesRead(nextInt())
    taskMetrics.shuffleReadMetrics.incRemoteBytesReadToDisk(nextInt())
    taskMetrics.shuffleReadMetrics.incLocalBytesRead(nextInt())
    taskMetrics.shuffleReadMetrics.incRecordsRead(nextInt())
    taskMetrics.shuffleWriteMetrics.incBytesWritten(nextInt())
    taskMetrics.shuffleWriteMetrics.incWriteTime(nextInt())
    taskMetrics.shuffleWriteMetrics.incRecordsWritten(nextInt())
    taskMetrics
  }
}
