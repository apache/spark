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

import com.fasterxml.jackson.annotation.JsonIgnore

import org.apache.spark.util.kvstore.KVIndex

/** List fields stored independently of the execution's plan and metric payload. */
private[spark] class SQLExecutionSummary(val info: SQLExecutionUIData) {
  @JsonIgnore @KVIndex
  def executionId: Long = info.executionId

  @JsonIgnore @KVIndex("completionTime")
  def completionTime: Long = info.completionTime.map(_.getTime).getOrElse(-1L)

  @JsonIgnore @KVIndex("rootExecutionId")
  def rootExecutionId: Long = info.rootExecutionId
}

/** The compact backend encodes this payload without retaining its object graph. */
private[spark] class SQLExecutionDetails(val info: SQLExecutionUIData) {
  @JsonIgnore @KVIndex
  def executionId: Long = info.executionId
}

private[spark] object CompactSQLExecutionData {
  def summary(data: SQLExecutionUIData): SQLExecutionSummary = {
    new SQLExecutionSummary(new SQLExecutionUIData(
      executionId = data.executionId,
      rootExecutionId = data.rootExecutionId,
      description = data.description,
      details = null,
      physicalPlanDescription = null,
      modifiedConfigs = Map.empty,
      metrics = Nil,
      submissionTime = data.submissionTime,
      completionTime = data.completionTime,
      // The summary only needs the distinction between success, failure and unknown status.
      errorMessage = data.errorMessage.map(e => if (e.isEmpty) "" else "failed"),
      jobs = data.jobs,
      stages = data.stages,
      metricValues = null,
      queryId = data.queryId))
  }

  def details(data: SQLExecutionUIData): SQLExecutionDetails = {
    new SQLExecutionDetails(new SQLExecutionUIData(
      executionId = data.executionId,
      rootExecutionId = data.rootExecutionId,
      description = null,
      details = data.details,
      physicalPlanDescription = data.physicalPlanDescription,
      modifiedConfigs = data.modifiedConfigs,
      metrics = data.metrics,
      submissionTime = -1L,
      completionTime = None,
      errorMessage = data.errorMessage,
      jobs = Map.empty,
      stages = Set.empty,
      metricValues = data.metricValues))
  }

  def combine(summary: SQLExecutionUIData, details: SQLExecutionUIData): SQLExecutionUIData = {
    new SQLExecutionUIData(
      executionId = summary.executionId,
      rootExecutionId = summary.rootExecutionId,
      description = summary.description,
      details = details.details,
      physicalPlanDescription = details.physicalPlanDescription,
      modifiedConfigs = details.modifiedConfigs,
      metrics = details.metrics,
      submissionTime = summary.submissionTime,
      completionTime = summary.completionTime,
      errorMessage = details.errorMessage,
      jobs = summary.jobs,
      stages = summary.stages,
      metricValues = details.metricValues,
      queryId = summary.queryId)
  }
}
