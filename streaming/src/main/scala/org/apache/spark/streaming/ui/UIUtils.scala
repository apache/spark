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

import scala.xml.Node

private[spark] object UIUtils {

  import org.apache.spark.ui.UIUtils.prependBaseUri

  def headerStreamingPage(
      content: => Seq[Node],
      basePath: String,
      appName: String,
      title: String): Seq[Node] = {
    val overview = {
      <li><a href={prependBaseUri(basePath)}>Overview</a></li>
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
        <script type="text/JavaScript">
          <!--
          function timedRefresh(timeoutPeriod) {
            setTimeout("location.reload(true);",timeoutPeriod);
          }
          //   -->
        </script>
      </head>
      <body onload="JavaScript:timedRefresh(1000);">
        <div class="navbar navbar-static-top">
          <div class="navbar-inner">
            <a href={prependBaseUri(basePath, "/")} class="brand">
              <img src={prependBaseUri("/static/spark-logo-77x50px-hd.png")} />
            </a>
            <ul class="nav">
              {overview}
            </ul>
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

  def listingTable[T](
      headers: Seq[String],
      makeRow: T => Seq[Node],
      rows: Seq[T],
      fixedWidth: Boolean = false): Seq[Node] = {
    org.apache.spark.ui.UIUtils.listingTable(headers, makeRow, rows, fixedWidth)
  }

  def listingTable[T](
      headers: Seq[String],
      rows: Seq[Seq[String]],
      fixedWidth: Boolean = false
    ): Seq[Node] = {
    def makeRow(data: Seq[String]): Seq[Node] = <tr> {data.map(d => <td>{d}</td>)} </tr>
    org.apache.spark.ui.UIUtils.listingTable(headers, makeRow, rows, fixedWidth)
  }
}
