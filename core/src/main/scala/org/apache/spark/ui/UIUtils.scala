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
import java.util.{Locale, Date}

import scala.xml.Node
import org.apache.spark.Logging

/** Utility functions for generating XML pages with spark content. */
private[spark] object UIUtils extends Logging {

  // SimpleDateFormat is not thread-safe. Don't expose it to avoid improper use.
  private val dateFormat = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  }

  def formatDate(date: Date): String = dateFormat.get.format(date)

  def formatDate(timestamp: Long): String = dateFormat.get.format(new Date(timestamp))

  def formatDuration(milliseconds: Long): String = {
    if (milliseconds < 100) {
      return "%d ms".format(milliseconds)
    }
    val seconds = milliseconds.toDouble / 1000
    if (seconds < 1) {
      return "%.1f s".format(seconds)
    }
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

  /** Generate a verbose human-readable string representing a duration such as "5 second 35 ms" */
  def formatDurationVerbose(ms: Long): String = {
    try {
      val second = 1000L
      val minute = 60 * second
      val hour = 60 * minute
      val day = 24 * hour
      val week = 7 * day
      val year = 365 * day

      def toString(num: Long, unit: String): String = {
        if (num == 0) {
          ""
        } else if (num == 1) {
          s"$num $unit"
        } else {
          s"$num ${unit}s"
        }
      }

      val millisecondsString = if (ms >= second && ms % second == 0) "" else s"${ms % second} ms"
      val secondString = toString((ms % minute) / second, "second")
      val minuteString = toString((ms % hour) / minute, "minute")
      val hourString = toString((ms % day) / hour, "hour")
      val dayString = toString((ms % week) / day, "day")
      val weekString = toString((ms % year) / week, "week")
      val yearString = toString(ms / year, "year")

      Seq(
        second -> millisecondsString,
        minute -> s"$secondString $millisecondsString",
        hour -> s"$minuteString $secondString",
        day -> s"$hourString $minuteString $secondString",
        week -> s"$dayString $hourString $minuteString",
        year -> s"$weekString $dayString $hourString"
      ).foreach { case (durationLimit, durationString) =>
        if (ms < durationLimit) {
          // if time is less than the limit (upto year)
          return durationString
        }
      }
      // if time is more than a year
      return s"$yearString $weekString $dayString"
    } catch {
      case e: Exception =>
        logError("Error converting time to string", e)
        // if there is some error, return blank string
        return ""
    }
  }

  /** Generate a human-readable string representing a number (e.g. 100 K) */
  def formatNumber(records: Double): String = {
    val trillion = 1e12
    val billion = 1e9
    val million = 1e6
    val thousand = 1e3

    val (value, unit) = {
      if (records >= 2*trillion) {
        (records / trillion, " T")
      } else if (records >= 2*billion) {
        (records / billion, " B")
      } else if (records >= 2*million) {
        (records / million, " M")
      } else if (records >= 2*thousand) {
        (records / thousand, " K")
      } else {
        (records, "")
      }
    }
    if (unit.isEmpty) {
      "%d".formatLocal(Locale.US, value.toInt)
    } else {
      "%.1f%s".formatLocal(Locale.US, value, unit)
    }
  }

  // Yarn has to go through a proxy so the base uri is provided and has to be on all links
  val uiRoot : String = Option(System.getenv("APPLICATION_WEB_PROXY_BASE")).getOrElse("")

  def prependBaseUri(basePath: String = "", resource: String = "") = uiRoot + basePath + resource

  /** Returns a spark page with correctly formatted headers */
  def headerSparkPage(
      content: => Seq[Node],
      basePath: String,
      appName: String,
      title: String,
      tabs: Seq[WebUITab],
      activeTab: WebUITab,
      refreshInterval: Option[Int] = None): Seq[Node] = {

    val header = tabs.map { tab =>
      <li class={if (tab == activeTab) "active" else ""}>
        <a href={prependBaseUri(basePath, "/" + tab.prefix)}>{tab.name}</a>
      </li>
    }

    <html>
      <head>
        <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
        <link rel="stylesheet" href={prependBaseUri("/static/bootstrap.min.css")}
              type="text/css" />
        <link rel="stylesheet" href={prependBaseUri("/static/webui.css")}
              type="text/css" />
        <script src={prependBaseUri("/static/sorttable.js")} ></script>
        <title>{appName} - {title}</title>
      </head>
      <body>
        <div class="navbar navbar-static-top">
          <div class="navbar-inner">
            <a href={prependBaseUri("/")} class="brand">
              <img src={prependBaseUri("/static/spark-logo-77x50px-hd.png")} />
            </a>
            <ul class="nav">{header}</ul>
            <p class="navbar-text pull-right"><strong>{appName}</strong> application UI</p>
          </div>
        </div>
        <div class="container-fluid">
          <div class="row-fluid">
            <div class="span12">
              <h3 style="vertical-align: bottom; display: inline-block;">
                {title}
              </h3>
            </div>
          </div>
          {content}
        </div>
      </body>
    </html>
  }

  /** Returns a page with the spark css/js and a simple format. Used for scheduler UI. */
  def basicSparkPage(content: => Seq[Node], title: String): Seq[Node] = {
    <html>
      <head>
        <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
        <link rel="stylesheet" href={prependBaseUri("/static/bootstrap.min.css")}
              type="text/css" />
        <link rel="stylesheet" href={prependBaseUri("/static/webui.css")}  type="text/css" />
        <script src={prependBaseUri("/static/sorttable.js")} ></script>
        <title>{title}</title>
      </head>
      <body>
        <div class="container-fluid">
          <div class="row-fluid">
            <div class="span12">
              <h3 style="vertical-align: middle; display: inline-block;">
                <img src={prependBaseUri("/static/spark-logo-77x50px-hd.png")}
                     style="margin-right: 15px;" />
                {title}
              </h3>
            </div>
          </div>
          {content}
        </div>
      </body>
    </html>
  }

  /** Returns an HTML table constructed by generating a row for each object in a sequence. */
  def listingTable[T](
      headers: Seq[String],
      generateDataRow: T => Seq[Node],
      data: Seq[T],
      fixedWidth: Boolean = false): Seq[Node] = {

    var tableClass = "table table-bordered table-striped table-condensed sortable"
    if (fixedWidth) {
      tableClass += " table-fixed"
    }
    val colWidth = 100.toDouble / headers.size
    val colWidthAttr = if (fixedWidth) colWidth + "%" else ""
    val headerRow: Seq[Node] = {
      // if none of the headers have "\n" in them
      if (headers.forall(!_.contains("\n"))) {
        // represent header as simple text
        headers.map(h => <th width={colWidthAttr}>{h}</th>)
      } else {
        // represent header text as list while respecting "\n"
        headers.map { case h =>
          <th width={colWidthAttr}>
            <ul class ="unstyled">
              { h.split("\n").map { case t => <li> {t} </li> } }
            </ul>
          </th>
        }
      }
    }
    <table class={tableClass}>
      <thead>{headerRow}</thead>
      <tbody>
        {data.map(r => generateDataRow(r))}
      </tbody>
    </table>
  }
}
