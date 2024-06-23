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

package org.apache.spark.executor

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.matching.Regex

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys

private[spark] class ExecutorLogUrlHandler(logUrlPattern: Option[String]) extends Logging {
  import ExecutorLogUrlHandler._

  private val informedForMissingAttributes = new AtomicBoolean(false)

  def applyPattern(
      logUrls: Map[String, String],
      attributes: Map[String, String]): Map[String, String] = {
    logUrlPattern match {
      case Some(pattern) => doApplyPattern(logUrls, attributes, pattern)
      case None => logUrls
    }
  }

  private def doApplyPattern(
      logUrls: Map[String, String],
      attributes: Map[String, String],
      urlPattern: String): Map[String, String] = {
    // Relation between pattern {{FILE_NAME}} and attribute {{LOG_FILES}}
    // Given that this class don't know which types of log files can be provided
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
      logUrls
    } else if (allPatterns.contains("FILE_NAME") && !allAttributeKeys.contains("LOG_FILES")) {
      logFailToRenewLogUrls("'FILE_NAME' parameter is provided, but file information is " +
        "missing in app's event log.", allPatternsExceptFileName, allAttributeKeys)
      logUrls
    } else {
      val updatedUrl = allPatternsExceptFileName.foldLeft(urlPattern) { case (orig, patt) =>
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
    }
  }

  private def logFailToRenewLogUrls(
      reason: String,
      allPatterns: Set[String],
      allAttributes: Set[String]): Unit = {
    if (informedForMissingAttributes.compareAndSet(false, true)) {
      logInfo(log"Fail to renew executor log urls: ${MDC(LogKeys.REASON, reason)}." +
        log" Required: ${MDC(LogKeys.REGEX, allPatterns)} / " +
        log"available: ${MDC(LogKeys.ATTRIBUTE_MAP, allAttributes)}." +
        log" Falling back to show app's original log urls.")
    }
  }
}

private[spark] object ExecutorLogUrlHandler {
  val CUSTOM_URL_PATTERN_REGEX: Regex = "\\{\\{([A-Za-z0-9_\\-]+)\\}\\}".r
}
