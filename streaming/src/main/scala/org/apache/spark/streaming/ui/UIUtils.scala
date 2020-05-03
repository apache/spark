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

package org.apache.spark.streaming.ui

import java.util.concurrent.TimeUnit

import scala.xml.Node

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.ui.{ UIUtils => SparkUIUtils }

private[streaming] object UIUtils {

  /**
   * Return the short string for a `TimeUnit`.
   */
  def shortTimeUnitString(unit: TimeUnit): String = unit match {
    case TimeUnit.NANOSECONDS => "ns"
    case TimeUnit.MICROSECONDS => "us"
    case TimeUnit.MILLISECONDS => "ms"
    case TimeUnit.SECONDS => "sec"
    case TimeUnit.MINUTES => "min"
    case TimeUnit.HOURS => "hrs"
    case TimeUnit.DAYS => "days"
  }

  /**
   * Find the best `TimeUnit` for converting milliseconds to a friendly string. Return the value
   * after converting, also with its TimeUnit.
   */
  def normalizeDuration(milliseconds: Long): (Double, TimeUnit) = {
    if (milliseconds < 1000) {
      return (milliseconds, TimeUnit.MILLISECONDS)
    }
    val seconds = milliseconds.toDouble / 1000
    if (seconds < 60) {
      return (seconds, TimeUnit.SECONDS)
    }
    val minutes = seconds / 60
    if (minutes < 60) {
      return (minutes, TimeUnit.MINUTES)
    }
    val hours = minutes / 60
    if (hours < 24) {
      return (hours, TimeUnit.HOURS)
    }
    val days = hours / 24
    (days, TimeUnit.DAYS)
  }

  /**
   * Convert `milliseconds` to the specified `unit`. We cannot use `TimeUnit.convert` because it
   * will discard the fractional part.
   */
  def convertToTimeUnit(milliseconds: Long, unit: TimeUnit): Double = unit match {
    case TimeUnit.NANOSECONDS => milliseconds * 1000 * 1000
    case TimeUnit.MICROSECONDS => milliseconds * 1000
    case TimeUnit.MILLISECONDS => milliseconds
    case TimeUnit.SECONDS => milliseconds / 1000.0
    case TimeUnit.MINUTES => milliseconds / 1000.0 / 60.0
    case TimeUnit.HOURS => milliseconds / 1000.0 / 60.0 / 60.0
    case TimeUnit.DAYS => milliseconds / 1000.0 / 60.0 / 60.0 / 24.0
  }

  def createOutputOperationFailureForUI(failure: String): String = {
    if (failure.startsWith("org.apache.spark.Spark")) {
      // SparkException or SparkDriverExecutionException
      "Failed due to Spark job error\n" + failure
    } else {
      var nextLineIndex = failure.indexOf("\n")
      if (nextLineIndex < 0) {
        nextLineIndex = failure.length
      }
      val firstLine = failure.substring(0, nextLineIndex)
      s"Failed due to error: $firstLine\n$failure"
    }
  }

  def failureReasonCell(
      failureReason: String,
      rowspan: Int = 1,
      includeFirstLineInExpandDetails: Boolean = true): Seq[Node] = {
    val isMultiline = failureReason.indexOf('\n') >= 0
    // Display the first line by default
    val failureReasonSummary = StringEscapeUtils.escapeHtml4(
      if (isMultiline) {
        failureReason.substring(0, failureReason.indexOf('\n'))
      } else {
        failureReason
      })
    val failureDetails =
      if (isMultiline && !includeFirstLineInExpandDetails) {
        // Skip the first line
        failureReason.substring(failureReason.indexOf('\n') + 1)
      } else {
        failureReason
      }
    val details = SparkUIUtils.detailsUINode(isMultiline, failureDetails)

    if (rowspan == 1) {
      <td valign="middle" style="max-width: 300px">{failureReasonSummary}{details}</td>
    } else {
      <td valign="middle" style="max-width: 300px" rowspan={rowspan.toString}>
        {failureReasonSummary}{details}
      </td>
    }
  }
}
