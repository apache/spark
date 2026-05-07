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

package org.apache.spark.deploy.history

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorLogUrlHandler
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.History._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.util.kvstore.KVStore

private[spark] class HistoryAppStatusStore(
    conf: SparkConf,
    store: KVStore)
  extends AppStatusStore(store, None) with Logging {

  private val logUrlPattern: Option[String] = {
    val appInfo = super.applicationInfo()
    val applicationCompleted = appInfo.attempts.nonEmpty && appInfo.attempts.head.completed
    if (applicationCompleted || conf.get(APPLY_CUSTOM_EXECUTOR_LOG_URL_TO_INCOMPLETE_APP)) {
      conf.get(CUSTOM_EXECUTOR_LOG_URL)
    } else {
      None
    }
  }

  private val logUrlHandler = new ExecutorLogUrlHandler(logUrlPattern)

  override def executorList(activeOnly: Boolean): Seq[v1.ExecutorSummary] = {
    val execList = super.executorList(activeOnly)
    if (logUrlPattern.nonEmpty) {
      execList.map(replaceLogUrls)
    } else {
      execList
    }
  }

  override def executorSummary(executorId: String): v1.ExecutorSummary = {
    val execSummary = super.executorSummary(executorId)
    if (logUrlPattern.nonEmpty) {
      replaceLogUrls(execSummary)
    } else {
      execSummary
    }
  }

  private def replaceLogUrls(exec: v1.ExecutorSummary): v1.ExecutorSummary = {
    val newLogUrlMap = logUrlHandler.applyPattern(exec.executorLogs, exec.attributes)
    replaceExecutorLogs(exec, newLogUrlMap)
  }

  private def replaceExecutorLogs(
      source: v1.ExecutorSummary,
      newExecutorLogs: Map[String, String]): v1.ExecutorSummary = {
    new v1.ExecutorSummary(source.id, source.hostPort, source.isActive, source.rddBlocks,
      source.memoryUsed, source.diskUsed, source.totalCores, source.maxTasks, source.activeTasks,
      source.failedTasks, source.completedTasks, source.totalTasks, source.totalDuration,
      source.totalGCTime, source.totalInputBytes, source.totalShuffleRead,
      source.totalShuffleWrite, source.isBlacklisted, source.maxMemory, source.addTime,
      source.removeTime, source.removeReason, newExecutorLogs, source.memoryMetrics,
      source.blacklistedInStages, source.peakMemoryMetrics, source.attributes, source.resources,
      source.resourceProfileId, source.isExcluded, source.excludedInStages)
  }

}
