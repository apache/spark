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

package org.apache.spark.shuffle.sort

import java.io.File
import java.util.Optional

import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter, ShufflePartitionWriter, SingleSpillShuffleMapOutputWriter}
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage
import org.apache.spark.shuffle.api.metric.CustomShuffleTaskMetric

/**
 * A [[CustomShuffleTaskMetric]] with a fixed name and value, for tests.
 */
private[spark] case class TestCustomShuffleTaskMetric(metricName: String, metricValue: Long)
  extends CustomShuffleTaskMetric {
  override def name(): String = metricName
  override def value(): Long = metricValue
}

/**
 * Wraps a [[ShuffleExecutorComponents]] so every [[ShuffleMapOutputWriter]] it creates reports the
 * given custom metric values from `currentMetricsValues()`, delegating all real writing.
 */
private[spark] class CustomMetricReportingExecutorComponents(
    delegate: ShuffleExecutorComponents,
    reportedMetrics: Array[CustomShuffleTaskMetric])
  extends ShuffleExecutorComponents {

  override def initializeExecutor(
      appId: String, execId: String, extraConfigs: java.util.Map[String, String]): Unit =
    delegate.initializeExecutor(appId, execId, extraConfigs)

  override def createMapOutputWriter(
      shuffleId: Int, mapTaskId: Long, numPartitions: Int): ShuffleMapOutputWriter =
    new CustomMetricReportingMapOutputWriter(
      delegate.createMapOutputWriter(shuffleId, mapTaskId, numPartitions), reportedMetrics)

  override def createSingleFileMapOutputWriter(
      shuffleId: Int, mapId: Long): Optional[SingleSpillShuffleMapOutputWriter] =
    delegate.createSingleFileMapOutputWriter(shuffleId, mapId)
      .map[SingleSpillShuffleMapOutputWriter] { writer =>
        new CustomMetricReportingSingleSpillMapOutputWriter(writer, reportedMetrics)
      }
}

private[spark] class CustomMetricReportingSingleSpillMapOutputWriter(
    delegate: SingleSpillShuffleMapOutputWriter,
    reportedMetrics: Array[CustomShuffleTaskMetric])
  extends SingleSpillShuffleMapOutputWriter {

  override def transferMapSpillFile(
      mapSpillFile: File, partitionLengths: Array[Long], checksums: Array[Long]): Unit =
    delegate.transferMapSpillFile(mapSpillFile, partitionLengths, checksums)

  override def currentMetricsValues(): Array[CustomShuffleTaskMetric] = reportedMetrics
}

private[spark] class CustomMetricReportingMapOutputWriter(
    delegate: ShuffleMapOutputWriter,
    reportedMetrics: Array[CustomShuffleTaskMetric])
  extends ShuffleMapOutputWriter {

  override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter =
    delegate.getPartitionWriter(reducePartitionId)

  override def commitAllPartitions(checksums: Array[Long]): MapOutputCommitMessage =
    delegate.commitAllPartitions(checksums)

  override def abort(error: Throwable): Unit = delegate.abort(error)

  override def currentMetricsValues(): Array[CustomShuffleTaskMetric] = reportedMetrics
}
