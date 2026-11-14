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

import java.util.{Date, UUID}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable

import org.apache.spark.{JobExecutionStatus, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Status._
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.status.protobuf.KVStoreProtobufSerializer
import org.apache.spark.util.MetricUtils
import org.apache.spark.util.kvstore.CompactInMemoryStore

class CompactSQLExecutionDataSuite extends SparkFunSuite {
  private class CountingSerializer extends KVStoreProtobufSerializer {
    var detailReads = 0
    var detailWrites = 0

    override def deserialize[T](bytes: Array[Byte], klass: Class[T]): T = {
      if (klass == classOf[SQLExecutionDetails]) detailReads += 1
      super.deserialize(bytes, klass)
    }

    override def serialize(value: Object): Array[Byte] = {
      if (value.isInstanceOf[SQLExecutionDetails]) detailWrites += 1
      super.serialize(value)
    }
  }

  private def execution(id: Long, root: Long): SQLExecutionUIData = {
    new SQLExecutionUIData(
      executionId = id,
      rootExecutionId = root,
      description = s"query $id",
      details = "execution details",
      physicalPlanDescription = "a large plan\n" * 1000,
      modifiedConfigs = Map("spark.sql.shuffle.partitions" -> "20"),
      metrics = Seq(SQLPlanMetric("rows", 7L, "sum")),
      submissionTime = 10L,
      completionTime = Some(new Date(20L)),
      errorMessage = Some("an error message with details"),
      jobs = Map(1 -> JobExecutionStatus.FAILED),
      stages = Set(1, 2),
      metricValues = Map(7L -> "17"),
      queryId = UUID.fromString("efe98ba7-1532-491e-9b4f-4be621cef37c"))
  }

  test("summary and child reads do not deserialize cold execution details") {
    val serializer = new CountingSerializer
    val store = new CompactInMemoryStore(serializer)
    try {
      Seq(execution(1L, 1L), execution(2L, 1L), execution(3L, 3L)).foreach { data =>
        store.write(CompactSQLExecutionData.details(data))
        store.write(CompactSQLExecutionData.summary(data))
      }
      val status = new SQLAppStatusStore(store)
      val summaries = status.executionSummariesList()
      assert(summaries.size == 3)
      assert(summaries.forall(_.physicalPlanDescription == null))
      assert(summaries.forall(_.metrics.isEmpty))
      assert(summaries.forall(_.executionStatus == "FAILED"))
      assert(status.subExecutionSummaries(1L).map(_.executionId) == Seq(2L))
      assert(serializer.detailReads == 0)

      val expected = execution(2L, 1L)
      val actual = status.execution(2L).get
      assert(actual.description == expected.description)
      assert(actual.details == expected.details)
      assert(actual.physicalPlanDescription == expected.physicalPlanDescription)
      assert(actual.modifiedConfigs == expected.modifiedConfigs)
      assert(actual.metrics == expected.metrics)
      assert(actual.submissionTime == expected.submissionTime)
      assert(actual.completionTime == expected.completionTime)
      assert(actual.errorMessage == expected.errorMessage)
      assert(actual.jobs == expected.jobs)
      assert(actual.stages == expected.stages)
      assert(actual.metricValues == expected.metricValues)
      assert(actual.queryId == expected.queryId)
      assert(serializer.detailReads == 1)
      assert(status.executionsList(1, 1).map(_.executionId) == Seq(2L))
      assert(status.executionMetrics(2L) == expected.metricValues)
    } finally {
      store.close()
    }
  }

  test("job updates do not encode unchanged SQL plans again") {
    val serializer = new CountingSerializer
    val conf = new SparkConf(false).set(ASYNC_TRACKING_ENABLED, false)
    val store = new ElementTrackingStore(new CompactInMemoryStore(serializer), conf)
    try {
      val data = new LiveExecutionData(1L, compactStore = true, compactMetrics = true)
      data.physicalPlanDescription = "plan\n" * 1000
      data.modifiedConfigs = Map.empty
      data.addMetrics(Seq(SQLPlanMetric("rows", 1L, "sum")))
      data.write(store, 1L)
      data.jobs = Map(1 -> JobExecutionStatus.RUNNING)
      data.write(store, 2L)
      assert(serializer.detailWrites == 1)
      data.metricsValues = Map(1L -> "3")
      data.write(store, 3L)
      assert(serializer.detailWrites == 2)
      // Retention can run before all late job-end events of a completed execution arrive.
      store.delete(classOf[SQLExecutionSummary], 1L)
      store.delete(classOf[SQLExecutionDetails], 1L)
      data.write(store, 4L)
      assert(serializer.detailWrites == 3)
      assert(new SQLAppStatusStore(store).execution(1L).get.metricValues == Map(1L -> "3"))
    } finally {
      store.close()
    }
  }

  test("closing the store waits for queued compact SQL writes without blocking them") {
    val closing = new CountDownLatch(1)
    val written = new CountDownLatch(1)
    val conf = new SparkConf(false).set(ASYNC_TRACKING_ENABLED, true)
    val underlying = new CompactInMemoryStore(new KVStoreProtobufSerializer)
    val store = new ElementTrackingStore(underlying, conf) {
      override def close(closeParent: Boolean): Unit = synchronized {
        closing.countDown()
        super.close(closeParent)
        // Check while still holding the close monitor so a blocked writer cannot race this check.
        assert(written.getCount == 0)
      }
    }
    try {
      val execution = new LiveExecutionData(1L, compactStore = true, compactMetrics = true)
      execution.modifiedConfigs = Map.empty
      execution.write(store, 1L)
      store.doAsync {
        assert(closing.await(10, TimeUnit.SECONDS))
        execution.metricsValues = Map(1L -> "final metrics")
        execution.write(store, 2L)
        written.countDown()
      }
      store.close(closeParent = false)
      assert(new SQLAppStatusStore(store).execution(1L).get.metricValues ==
        Map(1L -> "final metrics"))
    } finally {
      underlying.close()
    }
  }

  test("AQE deduplicates metric metadata while preserving historical metric IDs") {
    val execution = new LiveExecutionData(1L, compactStore = true, compactMetrics = true)
    val original = SQLPlanMetric("rows", 1L, "sum")
    val updated = SQLPlanMetric("updated rows", 1L, "sum")
    val next = SQLPlanMetric("bytes", 2L, "size")
    execution.addMetrics(Seq(original))
    execution.addMetrics(Seq(updated, next))
    execution.addMetrics(Seq(next))
    assert(execution.metrics == Seq(updated, next))
  }

  test("compact metric values preserve retries, speculation and exact custom values") {
    val types = mutable.Map(1L -> "sum", 2L -> "size", 3L -> "average", 4L -> "custom")
    val regular = new LiveStageMetrics(1, 0, 600, types)
    val compact = new LiveStageMetrics(1, 0, 600, types, compactMetrics = true)

    def update(index: Int, finished: Boolean, values: Seq[Long], attempt: Int = 0): Unit = {
      val accums = values.zipWithIndex.map { case (value, metric) =>
        AccumulableInfo(metric + 1L, None, Some(value), None, false, false)
      }
      val taskId = index.toLong + attempt * 1000L
      regular.updateTaskMetrics(taskId, index, finished, accums)
      compact.updateTaskMetrics(taskId, index, finished, accums)
    }

    def compare(): Unit = {
      assert(compact.metricIds().toSet == regular.metricIds().toSet)
      types.foreach { case (id, metricType) =>
        val expected = regular.metricValues(id).get
        val actual = compact.metricValues(id).get
        if (metricType == "sum") {
          assert(actual.sum == expected.sum)
        } else {
          assert(actual.toSeq == expected.toSeq)
          assert(MetricUtils.stringValue(metricType, actual, Array.emptyLongArray) ==
            MetricUtils.stringValue(metricType, expected, Array.emptyLongArray))
        }
      }
      assert(compact.maxMetricValues().toSet == regular.maxMetricValues().toSet)
    }

    for (index <- 0 until 600) {
      val value = index.toLong % 23 - 2
      update(index, finished = false, Seq(value, value, value, value))
      // Task-end metrics may omit a metric that was already reported by a heartbeat.
      update(index, finished = true, Seq(value + 1))
      update(index, finished = true, Seq(999L, 999L, 999L, 999L), attempt = 1)
      if (index == 255 || index == 511) compare()
    }
    compact.compact()
    compare()
  }
}
