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

package org.apache.spark.ui

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.Lifecycle

private[spark] abstract class WebUI(name: String) extends Lifecycle {
  protected var serverInfo: Option[ServerInfo] = None

  /**
   * Bind to the HTTP server behind this web interface.
   * Overridden implementation should set serverInfo.
   */
  def bind() { }

  /** Return the actual port to which this server is bound. Only valid after bind(). */
  def boundPort: Int = serverInfo.map(_.boundPort).getOrElse(-1)

  /** Stop the server behind this web interface. Only valid after bind(). */
  protected override def doStop() {
    assert(serverInfo.isDefined, "Attempted to stop %s before binding to a server!".format(name))
    serverInfo.get.server.stop()
  }
  protected override  def doStart() { }
}

/**
 * Utilities used throughout the web UI.
 */
private[spark] object WebUI {
  // SimpleDateFormat is not thread-safe. Don't expose it to avoid improper use.
  private val dateFormat = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  }

  def formatDate(date: Date): String = dateFormat.get.format(date)

  def formatDate(timestamp: Long): String = dateFormat.get.format(new Date(timestamp))

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
    "%.1f h".format(hours)
  }
}
