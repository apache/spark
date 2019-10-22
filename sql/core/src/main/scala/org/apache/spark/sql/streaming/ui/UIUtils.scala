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

package org.apache.spark.sql.streaming.ui

import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone}

import org.apache.commons.lang3.StringEscapeUtils

private[sql] object UIUtils {
  // SimpleDateFormat is not thread-safe. Don't expose it to avoid improper use.
  private val batchTimeFormat = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat =
      new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.ROOT)
  }

  private val batchTimeFormatWithMilliseconds = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat =
      new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS", Locale.ROOT)
  }

  /**
   * If `batchInterval` is less than 1 second, format `batchTime` with milliseconds. Otherwise,
   * format `batchTime` without milliseconds.
   *
   * @param batchTime the batch time to be formatted
   * @param batchInterval the batch interval
   * @param showYYYYMMSS if showing the `yyyy/MM/dd` part. If it's false, the return value wll be
   *                     only `HH:mm:ss` or `HH:mm:ss.SSS` depending on `batchInterval`
   * @param timezone only for test
   */
  def formatBatchTime(
      batchTime: Long,
      batchInterval: Long,
      showYYYYMMSS: Boolean = true,
      timezone: TimeZone = null): String = {
    val oldTimezones =
      (batchTimeFormat.get.getTimeZone, batchTimeFormatWithMilliseconds.get.getTimeZone)
    if (timezone != null) {
      batchTimeFormat.get.setTimeZone(timezone)
      batchTimeFormatWithMilliseconds.get.setTimeZone(timezone)
    }
    try {
      val formattedBatchTime =
        if (batchInterval < 1000) {
          batchTimeFormatWithMilliseconds.get.format(batchTime)
        } else {
          // If batchInterval >= 1 second, don't show milliseconds
          batchTimeFormat.get.format(batchTime)
        }
      if (showYYYYMMSS) {
        formattedBatchTime
      } else {
        formattedBatchTime.substring(formattedBatchTime.indexOf(' ') + 1)
      }
    } finally {
      if (timezone != null) {
        batchTimeFormat.get.setTimeZone(oldTimezones._1)
        batchTimeFormatWithMilliseconds.get.setTimeZone(oldTimezones._2)
      }
    }
  }

  /**
   * Remove suspicious characters of user input to prevent Cross-Site scripting (XSS) attacks
   *
   * For more information about XSS testing:
   * https://www.owasp.org/index.php/XSS_Filter_Evasion_Cheat_Sheet and
   * https://www.owasp.org/index.php/Testing_for_Reflected_Cross_site_scripting_(OTG-INPVAL-001)
   */
  def stripXSS(requestParameter: String): String = {
    val NEWLINE_AND_SINGLE_QUOTE_REGEX = raw"(?i)(\r\n|\n|\r|%0D%0A|%0A|%0D|'|%27)".r
    if (requestParameter == null) {
      null
    } else {
      // Remove new lines and single quotes, followed by escaping HTML version 4.0
      StringEscapeUtils.escapeHtml4(
        NEWLINE_AND_SINGLE_QUOTE_REGEX.replaceAllIn(requestParameter, ""))
    }
  }
}
