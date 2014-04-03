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

import scala.xml.Node

/** Utility functions for generating XML pages with spark content. */
private[spark] object UIUtils {

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

  // Yarn has to go through a proxy so the base uri is provided and has to be on all links
  val uiRoot : String = Option(System.getenv("APPLICATION_WEB_PROXY_BASE")).getOrElse("")

  def prependBaseUri(basePath: String = "", resource: String = "") = uiRoot + basePath + resource

  /** Returns a spark page with correctly formatted headers */
  def headerSparkPage(
      content: => Seq[Node],
      basePath: String,
      appName: String,
      title: String,
      tabs: Seq[UITab],
      activeTab: UITab) : Seq[Node] = {

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
            <a href={prependBaseUri(basePath, "/")} class="brand">
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
      makeRow: T => Seq[Node],
      rows: Seq[T],
      fixedWidth: Boolean = false): Seq[Node] = {

    val colWidth = 100.toDouble / headers.size
    val colWidthAttr = if (fixedWidth) colWidth + "%" else ""
    var tableClass = "table table-bordered table-striped table-condensed sortable"
    if (fixedWidth) {
      tableClass += " table-fixed"
    }

    <table class={tableClass}>
      <thead>{headers.map(h => <th width={colWidthAttr}>{h}</th>)}</thead>
      <tbody>
        {rows.map(r => makeRow(r))}
      </tbody>
    </table>
  }
}
