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

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.matching.Regex

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.History._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.util.kvstore.KVStore

private[spark] class HistoryAppStatusStore(
    conf: SparkConf,
    store: KVStore)
  extends AppStatusStore(store, None) with Logging {

  import HistoryAppStatusStore._

  private val logUrlPattern: Option[String] = {
    val appInfo = super.applicationInfo()
    val applicationCompleted = appInfo.attempts.nonEmpty && appInfo.attempts.head.completed
    if (applicationCompleted || conf.get(APPLY_CUSTOM_EXECUTOR_LOG_URL_TO_INCOMPLETE_APP)) {
      conf.get(CUSTOM_EXECUTOR_LOG_URL)
    } else {
      None
    }
  }

  private val informedForMissingAttributes = new AtomicBoolean(false)

  override def executorList(activeOnly: Boolean): Seq[v1.ExecutorSummary] = {
    val execList = super.executorList(activeOnly)
    logUrlPattern match {
      case Some(pattern) => execList.map(replaceLogUrls(_, pattern))
      case None => execList
    }
  }

  override def executorSummary(executorId: String): v1.ExecutorSummary = {
    val execSummary = super.executorSummary(executorId)
    logUrlPattern match {
      case Some(pattern) => replaceLogUrls(execSummary, pattern)
      case None => execSummary
    }
  }

  private def replaceLogUrls(exec: v1.ExecutorSummary, urlPattern: String): v1.ExecutorSummary = {
    val attributes = exec.attributes

    // Relation between pattern {{FILE_NAME}} and attribute {{LOG_FILES}}
    // Given that HistoryAppStatusStore don't know which types of log files can be provided
    // from resource manager, we require resource manager to provide available types of log
    // files, which are encouraged to be same as types of log files provided in original log URLs.
    // Once we get the list of log files, we need to expose them to end users as a pattern
    // so that end users can compose custom log URL(s) including log file name(s).
    val allPatterns = CUSTOM_URL_PATTERN_REGEX.findAllMatchIn(urlPattern).map(_.group(1)).toSet
    val allPatternsExceptFileName = allPatterns.filter(_ != "FILE_NAME")
    val allAttributeKeys = attributes.keySet
    val allAttributeKeysExceptLogFiles = allAttributeKeys.filter(_ != "LOG_FILES")

    if (allPatternsExceptFileName.diff(allAttributeKeysExceptLogFiles).nonEmpty) {
      logFailToRenewLogUrls("some of required attributes are missing in app's event log.",
        allPatternsExceptFileName, allAttributeKeys)
      return exec
    } else if (allPatterns.contains("FILE_NAME") && !allAttributeKeys.contains("LOG_FILES")) {
      logFailToRenewLogUrls("'FILE_NAME' parameter is provided, but file information is " +
        "missing in app's event log.", allPatternsExceptFileName, allAttributeKeys)
      return exec
    }

    val updatedUrl = allPatternsExceptFileName.foldLeft(urlPattern) { case (orig, patt) =>
      // we already checked the existence of attribute when comparing keys
      orig.replace(s"{{$patt}}", attributes(patt))
    }

    val newLogUrlMap = if (allPatterns.contains("FILE_NAME")) {
      // allAttributeKeys should contain "LOG_FILES"
      attributes("LOG_FILES").split(",").map { file =>
        file -> updatedUrl.replace("{{FILE_NAME}}", file)
      }.toMap
    } else {
      Map("log" -> updatedUrl)
    }

    replaceExecutorLogs(exec, newLogUrlMap)
  }

  private def logFailToRenewLogUrls(
      reason: String,
      allPatterns: Set[String],
      allAttributes: Set[String]): Unit = {
    if (informedForMissingAttributes.compareAndSet(false, true)) {
      logInfo(s"Fail to renew executor log urls: $reason. Required: $allPatterns / " +
        s"available: $allAttributes. Failing back to show app's original log urls.")
    }
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
      source.blacklistedInStages, source.peakMemoryMetrics, source.attributes)
  }

}

private[spark] object HistoryAppStatusStore {
  val CUSTOM_URL_PATTERN_REGEX: Regex = "\\{\\{([A-Za-z0-9_\\-]+)\\}\\}".r
}
