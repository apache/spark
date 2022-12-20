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

import collection.JavaConverters._

import org.apache.spark.JobExecutionStatus
import org.apache.spark.sql.execution.ui.SQLExecutionUIData
import org.apache.spark.status.protobuf.{ExtendedSerializer, StoreTypes}
import org.apache.spark.status.protobuf.Utils.getOptional

class SQLExecutionUIDataSerializer extends ExtendedSerializer {

  override def serialize(ui: Any): Array[Byte] = serialize(ui.asInstanceOf[SQLExecutionUIData])
  private def serialize(ui: SQLExecutionUIData): Array[Byte] = {
    val builder = StoreTypes.SQLExecutionUIData.newBuilder()
    builder.setExecutionId(ui.executionId)
    builder.setDescription(ui.description)
    builder.setDetails(ui.details)
    builder.setPhysicalPlanDescription(ui.physicalPlanDescription)
    ui.modifiedConfigs.foreach {
      case (k, v) => builder.putModifiedConfigs(k, v)
    }
    ui.metrics.foreach(m => builder.addMetrics(SQLPlanMetricSerializer.serialize(m)))
    builder.setSubmissionTime(ui.submissionTime)
    if (ui.completionTime.isDefined) {
      builder.setCompletionTime(ui.completionTime.get.getTime)
    }
    if (ui.errorMessage.isDefined) {
      builder.setErrorMessage(ui.errorMessage.get)
    }
    ui.jobs.foreach {
      case (id, status) =>
        builder.putJobs(id.toLong, StoreTypes.JobExecutionStatus.valueOf(status.toString))
    }
    ui.stages.foreach(stageId => builder.addStages(stageId.toLong))
    ui.metricValues.foreach {
      case (k, v) => builder.putMetricValues(k, v)
    }
    builder.build().toByteArray
  }

  override def deserialize(bytes: Array[Byte]): SQLExecutionUIData = {
    val binary = StoreTypes.SQLExecutionUIData.parseFrom(bytes)
    val completionTime =
      getOptional(binary.hasCompletionTime, () => new Date(binary.getCompletionTime))
    val errorMessage = getOptional(binary.hasErrorMessage, () => binary.getErrorMessage)
    val metrics =
      binary.getMetricsList.asScala.map(m => SQLPlanMetricSerializer.deserialize(m)).toSeq
    val jobs = binary.getJobsMap.asScala.map {
      case (jobId, status) => jobId.toInt -> JobExecutionStatus.valueOf(status.toString)
    }.toMap
    val metricValues = binary.getMetricValuesMap.asScala.map {
      case (k, v) => k.toLong -> v
    }.toMap

    new SQLExecutionUIData(
      executionId = binary.getExecutionId,
      description = binary.getDescription,
      details = binary.getDetails,
      physicalPlanDescription = binary.getPhysicalPlanDescription,
      modifiedConfigs = binary.getModifiedConfigsMap.asScala.toMap,
      metrics = metrics,
      submissionTime = binary.getSubmissionTime,
      completionTime = completionTime,
      errorMessage = errorMessage,
      jobs = jobs,
      stages = binary.getStagesList.asScala.map(_.toInt).toSet,
      metricValues = metricValues
    )
  }
}
