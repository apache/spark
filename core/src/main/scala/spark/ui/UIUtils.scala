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

package spark.ui

import scala.xml.Node

import spark.SparkContext

/** Utility functions for generating XML pages with spark content. */
private[spark] object UIUtils {
  import Page._

  // Yarn has to go through a proxy so the base uri is provided and has to be on all links
  private[spark] val uiRoot : String = Option(System.getenv("APPLICATION_WEB_PROXY_BASE")).
                                         getOrElse("")

  def addBaseUri(resource: String = ""): String = {
    return uiRoot + resource
  }

  private[spark] val storageStr = addBaseUri("/storage")
  private[spark] val stagesStr = addBaseUri("/stages")
  private[spark] val envStr = addBaseUri("/environment")
  private[spark] val executorsStr = addBaseUri("/executors")
  private[spark] val bootstrapMinCssStr = addBaseUri("/static/bootstrap.min.css")
  private[spark] val webuiCssStr = addBaseUri("/static/webui.css")
  private[spark] val bootstrapResponsiveCssStr = addBaseUri("/static/bootstrap-responsive.min.css")
  private[spark] val sortTableStr = addBaseUri("/static/sorttable.js")
  private[spark] val sparkLogoHdStr = addBaseUri("/static/spark-logo-77x50px-hd.png")
  private[spark] val sparkLogoStr = addBaseUri("/static/spark_logo.png")


  /** Returns a spark page with correctly formatted headers */
  def headerSparkPage(content: => Seq[Node], sc: SparkContext, title: String, page: Page.Value)
  : Seq[Node] = {
    val storage = page match {
      case Storage => <li class="active"><a href={storageStr}>Storage</a></li>
      case _ => <li><a href={storageStr}>Storage</a></li>
    }
    val jobs = page match {
      case Jobs => <li class="active"><a href={stagesStr}>Jobs</a></li>
      case _ => <li><a href={stagesStr}>Jobs</a></li>
    }
    val environment = page match {
      case Environment => <li class="active"><a href={envStr}>Environment</a></li>
      case _ => <li><a href={envStr}>Environment</a></li>
    }
    val executors = page match {
      case Executors => <li class="active"><a href={executorsStr}>Executors</a></li>
      case _ => <li><a href={executorsStr}>Executors</a></li>
    }

    <html>
      <head>
        <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
        <link rel="stylesheet" href={bootstrapMinCssStr} type="text/css" />
        <link rel="stylesheet" href={webuiCssStr} type="text/css" />
        <link rel="stylesheet" href={bootstrapResponsiveCssStr} type="text/css" />
        <script src={sortTableStr}></script>
        <title>{sc.appName} - {title}</title>
        <style type="text/css">
          table.sortable thead {{ cursor: pointer; }}
        </style>
      </head>
      <body>
        <div class="container">

          <div class="row">
            <div class="span12">
              <div class="navbar">
                <div class="navbar-inner">
                  <div class="container">
                    <a href="/" class="brand"><img src={sparkLogoHdStr} /></a>
                    <ul class="nav nav-pills">
                      {jobs}
                      {storage}
                      {environment}
                      {executors}
                    </ul>
                    <p class="navbar-text pull-right">Application: <strong>{sc.appName}</strong></p>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div class="row" style="padding-top: 5px;">
            <div class="span12">
              <h3 style="vertical-align: bottom; display: inline-block;">
                {title}
              </h3>
            </div>
          </div>
          <hr/>
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
        <link rel="stylesheet" href={bootstrapMinCssStr} type="text/css" />
        <link rel="stylesheet" href={bootstrapResponsiveCssStr} type="text/css" />
        <script src={sortTableStr}></script>
        <title>{title}</title>
        <style type="text/css">
          table.sortable thead {{ cursor: pointer; }}
        </style>
      </head>
      <body>
        <div class="container">
          <div class="row">
            <div class="span2">
              <img src={sparkLogoStr} />
            </div>
            <div class="span10">
              <h3 style="vertical-align: bottom; margin-top: 40px; display: inline-block;">
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
