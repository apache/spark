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

import org.apache.spark.sql.catalyst.util.DateTimeUtils.getTimeZone

private[ui] object UIUtils {

  /**
   * Check whether `number` is valid, if not return 0.0d
   */
  def withNumberInvalid(number: => Double): Double = {
    if (number.isNaN || number.isInfinite) {
      0.0d
    } else {
      number
    }
  }

  /**
   * Execute a block of code when there is already one completed batch in streaming query,
   * otherwise return `default` value.
   */
  def withNoProgress[T](query: StreamingQueryUIData, body: => T, default: T): T = {
    if (query.lastProgress != null) {
      body
    } else {
      default
    }
  }

  def getQueryName(uiData: StreamingQueryUIData): String = {
    if (uiData.summary.name == null || uiData.summary.name.isEmpty) {
      "<no name>"
    } else {
      uiData.summary.name
    }
  }

  def getQueryStatus(uiData: StreamingQueryUIData): String = {
    if (uiData.summary.isActive) {
      "RUNNING"
    } else {
      uiData.summary.exception.map(_ => "FAILED").getOrElse("FINISHED")
    }
  }

  private val progressTimestampFormat = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat = {
      val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
      format.setTimeZone(getTimeZone("UTC"))
      format
    }
  }

  def parseProgressTimestamp(timestamp: String): Long = {
    progressTimestampFormat.get.parse(timestamp).getTime
  }
}
