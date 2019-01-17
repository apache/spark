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
import org.apache.spark.internal.config.History._
import org.apache.spark.scheduler.SparkListenerExecutorAdded
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.status.{AppStatusListener, AppStatusSource, ElementTrackingStore}

private[spark] class HistoryAppStatusListener(
    kvstore: ElementTrackingStore,
    conf: SparkConf,
    live: Boolean,
    appStatusSource: Option[AppStatusSource] = None,
    lastUpdateTime: Option[Long] = None)
  extends AppStatusListener(kvstore, conf, live, appStatusSource, lastUpdateTime) {

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    val execInfo = event.executorInfo
    val newExecInfo = new ExecutorInfo(execInfo.executorHost, execInfo.totalCores,
      replaceLogUrls(execInfo), execInfo.attributes)

    super.onExecutorAdded(event.copy(executorInfo = newExecInfo))
  }

  def replaceLogUrls(execInfo: ExecutorInfo): Map[String, String] = {
    val oldLogUrlMap = execInfo.logUrlMap
    val attributes = execInfo.attributes

    conf.get(CUSTOM_EXECUTOR_LOG_URL) match {
      case Some(logUrlPattern) =>
        val pattern = "\\{\\{([A-Za-z0-9_\\-]+)\\}\\}".r

        val allPatterns = pattern.findAllMatchIn(logUrlPattern).map(_.group(1)).toSet
        val allPatternsExceptFileName = allPatterns.filter(_ != "FILE_NAME")
        val allAttributeKeys = attributes.keySet
        val allAttributeKeysExceptLogFiles = allAttributeKeys.filter(_ != "LOG_FILES")

        if (allPatternsExceptFileName.diff(allAttributeKeysExceptLogFiles).nonEmpty) {
          logFailToRenewLogUrls("some of required attributes are missing in app's event log.",
            allPatternsExceptFileName, allAttributeKeys)
          return oldLogUrlMap
        } else if (allPatterns.contains("FILE_NAME") && !allAttributeKeys.contains("LOG_FILES")) {
          logFailToRenewLogUrls("'FILE_NAME' parameter is provided, but file information is " +
            "missing in app's event log.", allPatternsExceptFileName, allAttributeKeys)
          return oldLogUrlMap
        }

        val updatedUrl = allPatternsExceptFileName.foldLeft(logUrlPattern) { case( orig, patt) =>
          // we already checked the existence of attribute when comparing keys
          orig.replace(s"{{$patt}}", attributes(patt))
        }

        if (allPatterns.contains("FILE_NAME")) {
          // allAttributeKeys should contain "LOG_FILES"
          attributes("LOG_FILES").split(",").map { file =>
            file -> updatedUrl.replace("{{FILE_NAME}}", file)
          }.toMap
        } else {
          Map("log" -> updatedUrl)
        }

      case None => oldLogUrlMap
    }
  }

  private def logFailToRenewLogUrls(
      reason: String,
      allPatterns: Set[String],
      allAttributes: Set[String]): Unit = {

    logWarning(s"Fail to renew executor log urls: $reason. Required: $allPatterns / " +
      s"available: $allAttributes. Failing back to show app's origin log urls.")
  }
}
