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

import java.util.Date

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.execution.ui.SQLExecutionUIData
import org.apache.spark.status.protobuf.{JobExecutionStatusSerializer, ProtobufSerDe, StoreTypes}
import org.apache.spark.status.protobuf.Utils._

private[protobuf] class SQLExecutionUIDataSerializer extends ProtobufSerDe[SQLExecutionUIData] {

  override def serialize(ui: SQLExecutionUIData): Array[Byte] = {
    val builder = StoreTypes.SQLExecutionUIData.newBuilder()
    builder.setExecutionId(ui.executionId)
    builder.setRootExecutionId(ui.rootExecutionId)
    setStringField(ui.description, builder.setDescription)
    setStringField(ui.details, builder.setDetails)
    setStringField(ui.physicalPlanDescription, builder.setPhysicalPlanDescription)
    if (ui.modifiedConfigs != null) {
      ui.modifiedConfigs.foreach {
        case (k, v) => builder.putModifiedConfigs(k, v)
      }
    }
    ui.metrics.foreach(m => builder.addMetrics(SQLPlanMetricSerializer.serialize(m)))
    builder.setSubmissionTime(ui.submissionTime)
    ui.completionTime.foreach(ct => builder.setCompletionTime(ct.getTime))
    ui.errorMessage.foreach(builder.setErrorMessage)
    ui.jobs.foreach {
      case (id, status) =>
        builder.putJobs(id.toLong, JobExecutionStatusSerializer.serialize(status))
    }
    ui.stages.foreach(stageId => builder.addStages(stageId.toLong))
    val metricValues = ui.metricValues
    if (metricValues == null) {
      builder.setMetricValuesIsNull(true)
    } else {
      builder.setMetricValuesIsNull(false)
      metricValues.foreach {
        case (k, v) => builder.putMetricValues(k, v)
      }
    }
    builder.build().toByteArray
  }

  override def deserialize(bytes: Array[Byte]): SQLExecutionUIData = {
    val ui = StoreTypes.SQLExecutionUIData.parseFrom(bytes)
    val completionTime =
      getOptional(ui.hasCompletionTime, () => new Date(ui.getCompletionTime))
    val errorMessage = getOptional(ui.hasErrorMessage, () => ui.getErrorMessage)
    val metrics =
      ui.getMetricsList.asScala.map(m => SQLPlanMetricSerializer.deserialize(m))
    val jobs = ui.getJobsMap.asScala.map {
      case (jobId, status) => jobId.toInt -> JobExecutionStatusSerializer.deserialize(status)
    }.toMap
    val metricValues = if (ui.getMetricValuesIsNull) {
      null
    } else {
      ui.getMetricValuesMap.asScala.map {
        case (k, v) => k.toLong -> v
      }.toMap
    }

    new SQLExecutionUIData(
      executionId = ui.getExecutionId,
      rootExecutionId = ui.getRootExecutionId,
      description = getStringField(ui.hasDescription, () => ui.getDescription),
      details = getStringField(ui.hasDetails, () => ui.getDetails),
      physicalPlanDescription =
        getStringField(ui.hasPhysicalPlanDescription, () => ui.getPhysicalPlanDescription),
      modifiedConfigs = ui.getModifiedConfigsMap.asScala.toMap,
      metrics = metrics,
      submissionTime = ui.getSubmissionTime,
      completionTime = completionTime,
      errorMessage = errorMessage,
      jobs = jobs,
      stages = ui.getStagesList.asScala.map(_.toInt).toSet,
      metricValues = metricValues
    )
  }
}
