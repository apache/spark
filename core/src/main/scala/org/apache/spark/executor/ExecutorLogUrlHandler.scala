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

import org.apache.spark.internal.Logging

private[spark] class ExecutorLogUrlHandler(logUrlPattern: Option[String]) extends Logging {
  import ExecutorLogUrlHandler._

  private val informedForMissingAttributes = new AtomicBoolean(false)

  def applyPattern(
      logUrls: Map[String, String],
      attributes: Map[String, String]): Map[String, String] = {
    logUrlPattern match {
      case None => applySelfPattern(logUrls, attributes)
      case Some(pattern) => applyExternalPattern(logUrls, attributes, pattern)
    }
  }

  private def applySelfPattern(
      logUrls: Map[String, String],
      attributes: Map[String, String]): Map[String, String] = {
    val allAttributeKeys = attributes.keySet
    logUrls.map { case (logFile, logUrl) =>
      val allPatterns = extractPatterns(logUrl)
      if (allPatterns.diff(allAttributeKeys).nonEmpty) {
        logFailToRenewLogUrls("some of required attributes are missing.",
          allPatterns, allAttributeKeys)
        (logFile, logUrl)
      } else {
        val updatedUrl = replacePatterns(logUrl, allPatterns, attributes)
        (logFile, updatedUrl)
      }
    }
  }

  private def applyExternalPattern(
      logUrls: Map[String, String],
      attributes: Map[String, String],
      urlPattern: String): Map[String, String] = {
    // Relation between pattern {{FILE_NAME}} and attribute {{LOG_FILES}}
    // Given that this class don't know which types of log files can be provided
    // from resource manager, we require resource manager to provide available types of log
    // files, which are encouraged to be same as types of log files provided in original log URLs.
    // Once we get the list of log files, we need to expose them to end users as a pattern
    // so that end users can compose custom log URL(s) including log file name(s).
    val allPatterns = extractPatterns(urlPattern)
    val allPatternsExceptFileName = allPatterns.filter(_ != "FILE_NAME")
    val allAttributeKeys = attributes.keySet
    val allAttributeKeysExceptLogFiles = allAttributeKeys.filter(_ != "LOG_FILES")

    if (allPatternsExceptFileName.diff(allAttributeKeysExceptLogFiles).nonEmpty) {
      logFailToRenewLogUrls("some of required attributes are missing.",
        allPatternsExceptFileName, allAttributeKeys)
      logUrls
    } else if (allPatterns.contains("FILE_NAME") && !allAttributeKeys.contains("LOG_FILES")) {
      logFailToRenewLogUrls("'FILE_NAME' parameter is provided, but file information is " +
        "missing.", allPatternsExceptFileName, allAttributeKeys)
      logUrls
    } else {
      val updatedUrl = replacePatterns(urlPattern, allPatternsExceptFileName, attributes)
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

  private def extractPatterns(urlPattern: String): Set[String] = {
    CUSTOM_URL_PATTERN_REGEX.findAllMatchIn(urlPattern).map(_.group(1)).toSet
  }

  private def replacePatterns(
      logUrl: String,
      patterns: Set[String],
      attributes: Map[String, String]): String = {
    patterns.foldLeft(logUrl) { case (orig, patt) =>
      // we already checked the existence of attribute when comparing keys
      orig.replace(s"{{$patt}}", attributes(patt))
    }
  }

  private def logFailToRenewLogUrls(
      reason: String,
      allPatterns: Set[String],
      allAttributes: Set[String]): Unit = {
    if (informedForMissingAttributes.compareAndSet(false, true)) {
      logInfo(s"Fail to renew executor log urls: $reason. Required: $allPatterns / " +
        s"available: $allAttributes. Falling back to show app's original log urls.")
    }
  }
}

private[spark] object ExecutorLogUrlHandler {
  val CUSTOM_URL_PATTERN_REGEX: Regex = "\\{\\{([A-Za-z0-9_\\-]+)\\}\\}".r
}
