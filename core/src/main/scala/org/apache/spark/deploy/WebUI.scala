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

package org.apache.spark.deploy

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Utilities used throughout the web UI.
 */
private[spark] object DeployWebUI {
  val DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

  def formatDate(date: Date): String = DATE_FORMAT.format(date)

  def formatDate(timestamp: Long): String = DATE_FORMAT.format(new Date(timestamp))

  def formatDuration(milliseconds: Long): String = {
    val seconds = milliseconds.toDouble / 1000
    if (seconds < 60) {
      return "%.0f s".format(seconds)
    }
    val minutes = seconds / 60
    if (minutes < 10) {
      return "%.1f min".format(minutes)
    } else if (minutes < 60) {
      return "%.0f min".format(minutes)
    }
    val hours = minutes / 60
    return "%.1f h".format(hours)
  }
}
