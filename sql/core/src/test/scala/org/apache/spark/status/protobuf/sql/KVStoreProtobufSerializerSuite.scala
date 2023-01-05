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

package org.apache.spark.status.protobuf.sql

import java.lang.{Long => JLong}
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.ui._
import org.apache.spark.sql.streaming.{SinkProgress, SourceProgress, StateOperatorProgress, StreamingQueryProgress}
import org.apache.spark.sql.streaming.ui.{StreamingQueryData, StreamingQueryProgressWrapper}
import org.apache.spark.sql.types.StructType
import org.apache.spark.status.api.v1.sql.SqlResourceSuite
import org.apache.spark.status.protobuf.KVStoreProtobufSerializer

class KVStoreProtobufSerializerSuite extends SparkFunSuite {

  private val serializer = new KVStoreProtobufSerializer()

  test("SQLExecutionUIData") {
    val input = SqlResourceSuite.sqlExecutionUIData
    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[SQLExecutionUIData])
    assert(result.executionId == input.executionId)
    assert(result.description == input.description)
    assert(result.details == input.details)
    assert(result.physicalPlanDescription == input.physicalPlanDescription)
    assert(result.modifiedConfigs == input.modifiedConfigs)
    assert(result.metrics == input.metrics)
    assert(result.submissionTime == input.submissionTime)
    assert(result.completionTime == input.completionTime)
    assert(result.errorMessage == input.errorMessage)
    assert(result.jobs == input.jobs)
    assert(result.stages == input.stages)
    assert(result.metricValues == input.metricValues)
  }

  test("SQLExecutionUIData with metricValues is empty map and null") {
    val templateData = SqlResourceSuite.sqlExecutionUIData

    val input1 = new SQLExecutionUIData(
      executionId = templateData.executionId,
      description = templateData.description,
      details = templateData.details,
      physicalPlanDescription = templateData.physicalPlanDescription,
      modifiedConfigs = templateData.modifiedConfigs,
      metrics = templateData.metrics,
      submissionTime = templateData.submissionTime,
      completionTime = templateData.completionTime,
      errorMessage = templateData.errorMessage,
      jobs = templateData.jobs,
      stages = templateData.stages,
      metricValues = Map.empty
    )
    val bytes1 = serializer.serialize(input1)
    val result1 = serializer.deserialize(bytes1, classOf[SQLExecutionUIData])
    // input.metricValues is empty map, result.metricValues is empty map.
    assert(result1.metricValues.isEmpty)

    val input2 = new SQLExecutionUIData(
      executionId = templateData.executionId,
      description = templateData.description,
      details = templateData.details,
      physicalPlanDescription = templateData.physicalPlanDescription,
      modifiedConfigs = templateData.modifiedConfigs,
      metrics = templateData.metrics,
      submissionTime = templateData.submissionTime,
      completionTime = templateData.completionTime,
      errorMessage = templateData.errorMessage,
      jobs = templateData.jobs,
      stages = templateData.stages,
      metricValues = null
    )
    val bytes2 = serializer.serialize(input2)
    val result2 = serializer.deserialize(bytes2, classOf[SQLExecutionUIData])
    // input.metricValues is null, result.metricValues is null.
    assert(result2.metricValues == null)
  }

  test("Spark Plan Graph") {
    val cluster = new SparkPlanGraphClusterWrapper(
      id = 5,
      name = "name_5",
      desc = "desc_5",
      nodes = Seq(new SparkPlanGraphNodeWrapper(
        node = new SparkPlanGraphNode(
          id = 12,
          name = "name_12",
          desc = "desc_12",
          metrics = Seq(
            SQLPlanMetric(
              name = "name_13",
              accumulatorId = 13,
              metricType = "metric_13"
            ),
            SQLPlanMetric(
              name = "name_14",
              accumulatorId = 14,
              metricType = "metric_14"
            )
          )
        ),
        cluster = new SparkPlanGraphClusterWrapper(
          id = 15,
          name = "name_15",
          desc = "desc_15",
          nodes = Seq(),
          metrics = Seq(
            SQLPlanMetric(
              name = "name_16",
              accumulatorId = 16,
              metricType = "metric_16"
            ),
            SQLPlanMetric(
              name = "name_17",
              accumulatorId = 17,
              metricType = "metric_17"
            )
          )
        )
      )),
      metrics = Seq(
        SQLPlanMetric(
          name = "name_6",
          accumulatorId = 6,
          metricType = "metric_6"
        ),
        SQLPlanMetric(
          name = "name_7 d",
          accumulatorId = 7,
          metricType = "metric_7"
        )
      )
    )
    val node = new SparkPlanGraphNodeWrapper(
      node = new SparkPlanGraphNode(
        id = 2,
        name = "name_1",
        desc = "desc_1",
        metrics = Seq(
          SQLPlanMetric(
            name = "name_2",
            accumulatorId = 3,
            metricType = "metric_1"
          ),
          SQLPlanMetric(
            name = "name_3",
            accumulatorId = 4,
            metricType = "metric_2"
          )
        )
      ),
      cluster = cluster
    )
    val input = new SparkPlanGraphWrapper(
      executionId = 1,
      nodes = Seq(node),
      edges = Seq(
        SparkPlanGraphEdge(8, 9),
        SparkPlanGraphEdge(10, 11)
      )
    )

    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[SparkPlanGraphWrapper])
    assert(result.executionId == input.executionId)
    assert(result.nodes.size == input.nodes.size)

    def compareNodes(n1: SparkPlanGraphNodeWrapper, n2: SparkPlanGraphNodeWrapper): Unit = {
      assert(n1.node.id == n2.node.id)
      assert(n1.node.name == n2.node.name)
      assert(n1.node.desc == n2.node.desc)

      assert(n1.node.metrics.size == n2.node.metrics.size)
      n1.node.metrics.zip(n2.node.metrics).foreach { case (m1, m2) =>
        assert(m1.name == m2.name)
        assert(m1.accumulatorId == m2.accumulatorId)
        assert(m1.metricType == m2.metricType)
      }

      assert(n1.cluster.id == n2.cluster.id)
      assert(n1.cluster.name == n2.cluster.name)
      assert(n1.cluster.desc == n2.cluster.desc)
      assert(n1.cluster.nodes.size == n2.cluster.nodes.size)
      n1.cluster.nodes.zip(n2.cluster.nodes).foreach { case (n3, n4) =>
        compareNodes(n3, n4)
      }
      n1.cluster.metrics.zip(n2.cluster.metrics).foreach { case (m1, m2) =>
        assert(m1.name == m2.name)
        assert(m1.accumulatorId == m2.accumulatorId)
        assert(m1.metricType == m2.metricType)
      }
    }

    result.nodes.zip(input.nodes).foreach { case (n1, n2) =>
      compareNodes(n1, n2)
    }

    assert(result.edges.size == input.edges.size)
    result.edges.zip(input.edges).foreach { case (e1, e2) =>
      assert(e1.fromId == e2.fromId)
      assert(e1.toId == e2.toId)
    }
  }

  test("StreamingQueryData") {
    val id = UUID.randomUUID()
    val input = new StreamingQueryData(
      name = "some-query",
      id = id,
      runId = s"run-id-$id",
      isActive = false,
      exception = Some("Some Exception"),
      startTimestamp = 1L,
      endTimestamp = Some(2L)
    )
    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[StreamingQueryData])
    assert(result.name == input.name)
    assert(result.id == input.id)
    assert(result.runId == input.runId)
    assert(result.isActive == input.isActive)
    assert(result.exception == input.exception)
    assert(result.startTimestamp == input.startTimestamp)
    assert(result.endTimestamp == input.endTimestamp)
  }

  test("StreamingQueryProgressWrapper") {
    // Generate input data
    val stateOperatorProgress0 = new StateOperatorProgress(
      operatorName = "op-0",
      numRowsTotal = 1L,
      numRowsUpdated = 2L,
      allUpdatesTimeMs = 3L,
      numRowsRemoved = 4L,
      allRemovalsTimeMs = 5L,
      commitTimeMs = 6L,
      memoryUsedBytes = 7L,
      numRowsDroppedByWatermark = 8L,
      numShufflePartitions = 9L,
      numStateStoreInstances = 10L,
      customMetrics = Map(
        "custom-metrics-00" -> JLong.valueOf("10"),
        "custom-metrics-01" -> JLong.valueOf("11")).asJava
    )

    val stateOperatorProgress1 = new StateOperatorProgress(
      operatorName = "op-1",
      numRowsTotal = 11L,
      numRowsUpdated = 12L,
      allUpdatesTimeMs = 13L,
      numRowsRemoved = 14L,
      allRemovalsTimeMs = 15L,
      commitTimeMs = 16L,
      memoryUsedBytes = 17L,
      numRowsDroppedByWatermark = 18L,
      numShufflePartitions = 19L,
      numStateStoreInstances = 20L,
      customMetrics = Map(
        "custom-metrics-10" -> JLong.valueOf("20"),
        "custom-metrics-11" -> JLong.valueOf("21")).asJava
    )

    val source0 = new SourceProgress(
      description = "description-0",
      startOffset = "startOffset-0",
      endOffset = "endOffset-0",
      latestOffset = "latestOffset-0",
      numInputRows = 10L,
      inputRowsPerSecond = 11.0,
      processedRowsPerSecond = 12.0,
      metrics = Map(
        "metrics-00" -> "10",
        "metrics-01" -> "11").asJava
    )

    val source1 = new SourceProgress(
      description = "description-",
      startOffset = "startOffset-1",
      endOffset = "endOffset-1",
      latestOffset = "latestOffset-1",
      numInputRows = 20L,
      inputRowsPerSecond = 21.0,
      processedRowsPerSecond = 22.0,
      metrics = Map(
        "metrics-10" -> "20",
        "metrics-11" -> "21").asJava
    )

    val sink = new SinkProgress(
      description = "sink-0",
      numOutputRows = 30,
      metrics = Map(
        "metrics-20" -> "30",
        "metrics-21" -> "31").asJava
    )

    val schema1 = new StructType()
      .add("c1", "long")
      .add("c2", "double")
    val schema2 = new StructType()
      .add("rc", "long")
      .add("min_q", "string")
      .add("max_q", "string")

    val observedMetrics = Map[String, Row](
      "event1" -> new GenericRowWithSchema(Array(1L, 3.0d), schema1),
      "event2" -> new GenericRowWithSchema(Array(1L, "hello", "world"), schema2)
    ).asJava

    val progress = new StreamingQueryProgress(
      id = UUID.randomUUID(),
      runId = UUID.randomUUID(),
      name = "name-1",
      timestamp = "2023-01-03T09:14:04.175Z",
      batchId = 1L,
      batchDuration = 2L,
      durationMs = Map(
        "duration-0" -> JLong.valueOf("10"),
        "duration-1" -> JLong.valueOf("11")).asJava,
      eventTime = Map(
        "eventTime-0" -> "20",
        "eventTime-1" -> "21").asJava,
      stateOperators = Array(stateOperatorProgress0, stateOperatorProgress1),
      sources = Array(source0, source1),
      sink = sink,
      observedMetrics = observedMetrics
    )
    val input = new StreamingQueryProgressWrapper(progress)

    // Do serialization and deserialization
    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[StreamingQueryProgressWrapper])

    // Assertion results
    val resultProcess = result.progress
    assert(progress.id == resultProcess.id)
    assert(progress.runId == resultProcess.runId)
    assert(progress.name == resultProcess.name)
    assert(progress.timestamp == resultProcess.timestamp)
    assert(progress.batchId == resultProcess.batchId)
    assert(progress.batchDuration == resultProcess.batchDuration)
    assert(progress.durationMs == resultProcess.durationMs)
    assert(progress.eventTime == resultProcess.eventTime)

    progress.stateOperators.zip(resultProcess.stateOperators).foreach {
      case (o1, o2) =>
        assert(o1.operatorName == o2.operatorName)
        assert(o1.numRowsTotal == o2.numRowsTotal)
        assert(o1.numRowsUpdated == o2.numRowsUpdated)
        assert(o1.allUpdatesTimeMs == o2.allUpdatesTimeMs)
        assert(o1.numRowsRemoved == o2.numRowsRemoved)
        assert(o1.allRemovalsTimeMs == o2.allRemovalsTimeMs)
        assert(o1.commitTimeMs == o2.commitTimeMs)
        assert(o1.memoryUsedBytes == o2.memoryUsedBytes)
        assert(o1.numRowsDroppedByWatermark == o2.numRowsDroppedByWatermark)
        assert(o1.numShufflePartitions == o2.numShufflePartitions)
        assert(o1.numStateStoreInstances == o2.numStateStoreInstances)
        assert(o1.customMetrics == o2.customMetrics)
    }

    progress.sources.zip(resultProcess.sources).foreach {
      case (s1, s2) =>
        assert(s1.description == s2.description)
        assert(s1.startOffset == s2.startOffset)
        assert(s1.endOffset == s2.endOffset)
        assert(s1.latestOffset == s2.latestOffset)
        assert(s1.numInputRows == s2.numInputRows)
        assert(s1.inputRowsPerSecond == s2.inputRowsPerSecond)
        assert(s1.processedRowsPerSecond == s2.processedRowsPerSecond)
        assert(s1.metrics == s2.metrics)
    }

    Seq(progress.sink).zip(Seq(resultProcess.sink)).foreach {
      case (s1, s2) =>
        assert(s1.description == s2.description)
        assert(s1.numOutputRows == s2.numOutputRows)
        assert(s1.metrics == s2.metrics)
    }

    val resultObservedMetrics = resultProcess.observedMetrics
    assert(progress.observedMetrics.size() == resultObservedMetrics.size())
    assert(progress.observedMetrics.keySet() == resultObservedMetrics.keySet())
    progress.observedMetrics.entrySet().forEach { e =>
      assert(e.getValue == resultObservedMetrics.get(e.getKey))
    }
  }
}
