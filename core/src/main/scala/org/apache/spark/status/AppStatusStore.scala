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

import java.io.File
import java.util.{Arrays, List => JList}

import scala.collection.JavaConverters._

import org.apache.spark.{JobExecutionStatus, SparkConf}
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.status.api.v1
import org.apache.spark.util.{Distribution, Utils}
import org.apache.spark.util.kvstore.{InMemoryStore, KVStore}

/**
 * A wrapper around a KVStore that provides methods for accessing the API data stored within.
 */
private[spark] class AppStatusStore(store: KVStore) {

  def applicationInfo(): v1.ApplicationInfo = {
    store.view(classOf[ApplicationInfoWrapper]).max(1).iterator().next().info
  }

  def environmentInfo(): v1.ApplicationEnvironmentInfo = {
    val klass = classOf[ApplicationEnvironmentInfoWrapper]
    store.read(klass, klass.getName()).info
  }

  def jobsList(statuses: JList[JobExecutionStatus]): Seq[v1.JobData] = {
    val it = store.view(classOf[JobDataWrapper]).asScala.map(_.info)
    if (!statuses.isEmpty()) {
      it.filter { job => statuses.contains(job.status) }.toSeq
    } else {
      it.toSeq
    }
  }

  def job(jobId: Int): v1.JobData = {
    store.read(classOf[JobDataWrapper], jobId).info
  }

  def executorList(activeOnly: Boolean): Seq[v1.ExecutorSummary] = {
    val base = store.view(classOf[ExecutorSummaryWrapper])
    val filtered = if (activeOnly) {
      base.index("active").reverse().first(true).last(true)
    } else {
      base
    }
    filtered.asScala.map(_.info).toSeq
  }

  def executorSummary(executorId: String): Option[v1.ExecutorSummary] = {
    try {
      Some(store.read(classOf[ExecutorSummaryWrapper], executorId).info)
    } catch {
      case _: NoSuchElementException =>
        None
    }
  }

  def stageList(statuses: JList[v1.StageStatus]): Seq[v1.StageData] = {
    val it = store.view(classOf[StageDataWrapper]).asScala.map(_.info)
    if (!statuses.isEmpty) {
      it.filter { s => statuses.contains(s.status) }.toSeq
    } else {
      it.toSeq
    }
  }

  def stageData(stageId: Int): Seq[v1.StageData] = {
    store.view(classOf[StageDataWrapper]).index("stageId").first(stageId).last(stageId)
      .asScala.map(_.info).toSeq
  }

  def stageAttempt(stageId: Int, stageAttemptId: Int): v1.StageData = {
    store.read(classOf[StageDataWrapper], Array(stageId, stageAttemptId)).info
  }

  def taskSummary(
      stageId: Int,
      stageAttemptId: Int,
      quantiles: Array[Double]): v1.TaskMetricDistributions = {

    val stage = Array(stageId, stageAttemptId)

    val rawMetrics = store.view(classOf[TaskDataWrapper])
      .index("stage")
      .first(stage)
      .last(stage)
      .asScala
      .flatMap(_.info.taskMetrics)
      .toList
      .view

    def metricQuantiles(f: v1.TaskMetrics => Double): IndexedSeq[Double] =
      Distribution(rawMetrics.map { d => f(d) }).get.getQuantiles(quantiles)

    // We need to do a lot of similar munging to nested metrics here.  For each one,
    // we want (a) extract the values for nested metrics (b) make a distribution for each metric
    // (c) shove the distribution into the right field in our return type and (d) only return
    // a result if the option is defined for any of the tasks.  MetricHelper is a little util
    // to make it a little easier to deal w/ all of the nested options.  Mostly it lets us just
    // implement one "build" method, which just builds the quantiles for each field.

    val inputMetrics =
      new MetricHelper[v1.InputMetrics, v1.InputMetricDistributions](rawMetrics, quantiles) {
        def getSubmetrics(raw: v1.TaskMetrics): v1.InputMetrics = raw.inputMetrics

        def build: v1.InputMetricDistributions = new v1.InputMetricDistributions(
          bytesRead = submetricQuantiles(_.bytesRead),
          recordsRead = submetricQuantiles(_.recordsRead)
        )
      }.build

    val outputMetrics =
      new MetricHelper[v1.OutputMetrics, v1.OutputMetricDistributions](rawMetrics, quantiles) {
        def getSubmetrics(raw: v1.TaskMetrics): v1.OutputMetrics = raw.outputMetrics

        def build: v1.OutputMetricDistributions = new v1.OutputMetricDistributions(
          bytesWritten = submetricQuantiles(_.bytesWritten),
          recordsWritten = submetricQuantiles(_.recordsWritten)
        )
      }.build

    val shuffleReadMetrics =
      new MetricHelper[v1.ShuffleReadMetrics, v1.ShuffleReadMetricDistributions](rawMetrics,
        quantiles) {
        def getSubmetrics(raw: v1.TaskMetrics): v1.ShuffleReadMetrics =
          raw.shuffleReadMetrics

        def build: v1.ShuffleReadMetricDistributions = new v1.ShuffleReadMetricDistributions(
          readBytes = submetricQuantiles { s => s.localBytesRead + s.remoteBytesRead },
          readRecords = submetricQuantiles(_.recordsRead),
          remoteBytesRead = submetricQuantiles(_.remoteBytesRead),
          remoteBytesReadToDisk = submetricQuantiles(_.remoteBytesReadToDisk),
          remoteBlocksFetched = submetricQuantiles(_.remoteBlocksFetched),
          localBlocksFetched = submetricQuantiles(_.localBlocksFetched),
          totalBlocksFetched = submetricQuantiles { s =>
            s.localBlocksFetched + s.remoteBlocksFetched
          },
          fetchWaitTime = submetricQuantiles(_.fetchWaitTime)
        )
      }.build

    val shuffleWriteMetrics =
      new MetricHelper[v1.ShuffleWriteMetrics, v1.ShuffleWriteMetricDistributions](rawMetrics,
        quantiles) {
        def getSubmetrics(raw: v1.TaskMetrics): v1.ShuffleWriteMetrics =
          raw.shuffleWriteMetrics

        def build: v1.ShuffleWriteMetricDistributions = new v1.ShuffleWriteMetricDistributions(
          writeBytes = submetricQuantiles(_.bytesWritten),
          writeRecords = submetricQuantiles(_.recordsWritten),
          writeTime = submetricQuantiles(_.writeTime)
        )
      }.build

    new v1.TaskMetricDistributions(
      quantiles = quantiles,
      executorDeserializeTime = metricQuantiles(_.executorDeserializeTime),
      executorDeserializeCpuTime = metricQuantiles(_.executorDeserializeCpuTime),
      executorRunTime = metricQuantiles(_.executorRunTime),
      executorCpuTime = metricQuantiles(_.executorCpuTime),
      resultSize = metricQuantiles(_.resultSize),
      jvmGcTime = metricQuantiles(_.jvmGcTime),
      resultSerializationTime = metricQuantiles(_.resultSerializationTime),
      memoryBytesSpilled = metricQuantiles(_.memoryBytesSpilled),
      diskBytesSpilled = metricQuantiles(_.diskBytesSpilled),
      inputMetrics = inputMetrics,
      outputMetrics = outputMetrics,
      shuffleReadMetrics = shuffleReadMetrics,
      shuffleWriteMetrics = shuffleWriteMetrics
    )
  }

  def taskList(
      stageId: Int,
      stageAttemptId: Int,
      offset: Int,
      length: Int,
      sortBy: v1.TaskSorting): Seq[v1.TaskData] = {
    val stageKey = Array(stageId, stageAttemptId)
    val base = store.view(classOf[TaskDataWrapper])
    val indexed = sortBy match {
      case v1.TaskSorting.ID =>
        base.index("stage").first(stageKey).last(stageKey)
      case v1.TaskSorting.INCREASING_RUNTIME =>
        base.index("runtime").first(stageKey ++ Array(-1L)).last(stageKey ++ Array(Long.MaxValue))
      case v1.TaskSorting.DECREASING_RUNTIME =>
        base.index("runtime").first(stageKey ++ Array(Long.MaxValue)).last(stageKey ++ Array(-1L))
          .reverse()
    }
    indexed.skip(offset).max(length).asScala.map(_.info).toSeq
  }

  def rddList(cachedOnly: Boolean = true): Seq[v1.RDDStorageInfo] = {
    store.view(classOf[RDDStorageInfoWrapper]).asScala.map(_.info).filter { rdd =>
      !cachedOnly || rdd.numCachedPartitions > 0
    }.toSeq
  }

  def rdd(rddId: Int): v1.RDDStorageInfo = {
    store.read(classOf[RDDStorageInfoWrapper], rddId).info
  }

  def streamBlocksList(): Seq[StreamBlockData] = {
    store.view(classOf[StreamBlockData]).asScala.toSeq
  }

  def close(): Unit = {
    store.close()
  }

}

private[spark] object AppStatusStore {

  val CURRENT_VERSION = 1L

  /**
   * Create an in-memory store for a live application.
   *
   * @param conf Configuration.
   * @param bus Where to attach the listener to populate the store.
   */
  def createLiveStore(conf: SparkConf, bus: LiveListenerBus): AppStatusStore = {
    val store = new InMemoryStore()
    val stateStore = new AppStatusStore(store)
    bus.addToStatusQueue(new AppStatusListener(store, conf, true))
    stateStore
  }

}

/**
 * Helper for getting distributions from nested metric types.
 */
private abstract class MetricHelper[I, O](
    rawMetrics: Seq[v1.TaskMetrics],
    quantiles: Array[Double]) {

  def getSubmetrics(raw: v1.TaskMetrics): I

  def build: O

  val data: Seq[I] = rawMetrics.map(getSubmetrics)

  /** applies the given function to all input metrics, and returns the quantiles */
  def submetricQuantiles(f: I => Double): IndexedSeq[Double] = {
    Distribution(data.map { d => f(d) }).get.getQuantiles(quantiles)
  }
}
