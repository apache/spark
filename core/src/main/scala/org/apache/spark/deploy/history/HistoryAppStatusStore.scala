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

import scala.util.matching.Regex

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.History.CUSTOM_EXECUTOR_LOG_URL
import org.apache.spark.status.{AppStatusListener, AppStatusStore}
import org.apache.spark.status.api.v1
import org.apache.spark.status.api.v1.ExecutorSummary
import org.apache.spark.util.kvstore.KVStore

private[spark] class HistoryAppStatusStore(
    conf: SparkConf,
    store: KVStore,
    listener: Option[AppStatusListener] = None)
  extends AppStatusStore(store, listener) with Logging {

  import HistoryAppStatusStore._

  private val logUrlPattern: Option[String] = conf.get(CUSTOM_EXECUTOR_LOG_URL)

  override def executorList(activeOnly: Boolean): Seq[v1.ExecutorSummary] = {
    logUrlPattern match {
      case Some(pattern) => super.executorList(activeOnly).map(replaceLogUrls(_, pattern))
      case None => super.executorList(activeOnly)
    }
  }

  override def executorSummary(executorId: String): v1.ExecutorSummary = {
    logUrlPattern match {
      case Some(pattern) => replaceLogUrls(super.executorSummary(executorId), pattern)
      case None => super.executorSummary(executorId)
    }
  }

  private def replaceLogUrls(exec: ExecutorSummary, urlPattern: String): ExecutorSummary = {
    val attributes = exec.attributes

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

    val updatedUrl = allPatternsExceptFileName.foldLeft(urlPattern) { case(orig, patt) =>
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

    exec.replaceExecutorLogs(newLogUrlMap)
  }

  private def logFailToRenewLogUrls(
      reason: String,
      allPatterns: Set[String],
      allAttributes: Set[String]): Unit = {

    logWarning(s"Fail to renew executor log urls: $reason. Required: $allPatterns / " +
      s"available: $allAttributes. Failing back to show app's origin log urls.")
  }

}

object HistoryAppStatusStore {
  val CUSTOM_URL_PATTERN_REGEX: Regex = "\\{\\{([A-Za-z0-9_\\-]+)\\}\\}".r
}
