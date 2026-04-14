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

import java.{util => ju}
import java.lang.{Long => JLong}
import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale, TimeZone}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.xml._
import scala.xml.transform.{RewriteRule, RuleTransformer}

import jakarta.servlet.http.HttpServletRequest
import jakarta.ws.rs.core.{MediaType, MultivaluedMap, Response}
import org.eclipse.jetty.server.Request
import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.ui.scope.RDDOperationGraph

/** Utility functions for generating XML pages with spark content. */
private[spark] object UIUtils extends Logging {
  val TABLE_CLASS_NOT_STRIPED = "table table-bordered table-hover table-sm"
  val TABLE_CLASS_STRIPED = TABLE_CLASS_NOT_STRIPED + " table-striped"
  val TABLE_CLASS_STRIPED_SORTABLE = TABLE_CLASS_STRIPED + " sortable"

  private val dateTimeFormatter = DateTimeFormatter
    .ofPattern("yyyy/MM/dd HH:mm:ss", Locale.US)
    .withZone(ZoneId.systemDefault())

  def formatDate(date: Date): String = dateTimeFormatter.format(date.toInstant)

  def formatDate(timestamp: Long): String =
    dateTimeFormatter.format(Instant.ofEpochMilli(timestamp))

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
      s"$yearString $weekString $dayString"
    } catch {
      case e: Exception =>
        logError("Error converting time to string", e)
        // if there is some error, return blank string
        ""
    }
  }

  private val batchTimeFormat = DateTimeFormatter
    .ofPattern("yyyy/MM/dd HH:mm:ss", Locale.US)
    .withZone(ZoneId.systemDefault())

  private val batchTimeFormatWithMilliseconds = DateTimeFormatter
    .ofPattern("yyyy/MM/dd HH:mm:ss.SSS", Locale.US)
    .withZone(ZoneId.systemDefault())

  /**
   * If `batchInterval` is less than 1 second, format `batchTime` with milliseconds. Otherwise,
   * format `batchTime` without milliseconds.
   *
   * @param batchTime the batch time to be formatted
   * @param batchInterval the batch interval
   * @param showYYYYMMSS if showing the `yyyy/MM/dd` part. If it's false, the return value will be
   *                     only `HH:mm:ss` or `HH:mm:ss.SSS` depending on `batchInterval`
   * @param timezone only for test
   */
  def formatBatchTime(
      batchTime: Long,
      batchInterval: Long,
      showYYYYMMSS: Boolean = true,
      timezone: TimeZone = null): String = {
    // If batchInterval >= 1 second, don't show milliseconds
    val format = if (batchInterval < 1000) batchTimeFormatWithMilliseconds else batchTimeFormat
    val formatWithZone = if (timezone == null) format else format.withZone(timezone.toZoneId)
    val formattedBatchTime = formatWithZone.format(Instant.ofEpochMilli(batchTime))
    if (showYYYYMMSS) {
      formattedBatchTime
    } else {
      formattedBatchTime.substring(formattedBatchTime.indexOf(' ') + 1)
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
  def uiRoot(knoxBasePathGetter: String => String): String = {
    // Knox uses X-Forwarded-Context to notify the application the base path
    val knoxBasePath = Option(knoxBasePathGetter("X-Forwarded-Context"))
    // SPARK-11484 - Use the proxyBase set by the AM, if not found then use env.
    sys.props.get("spark.ui.proxyBase")
      .orElse(sys.env.get("APPLICATION_WEB_PROXY_BASE"))
      .orElse(knoxBasePath)
      .getOrElse("")
  }

  def uiRoot(request: HttpServletRequest): String = {
    uiRoot(request.getHeader _)
  }

  def uiRoot(request: Request): String = {
    uiRoot(request.getHeaders.get: String => String)
  }

  def prependBaseUri(
      request: HttpServletRequest,
      basePath: String = "",
      resource: String = ""): String = {
    uiRoot(request) + basePath + resource
  }

  def commonHeaderNodes(request: HttpServletRequest): Seq[Node] = {
    <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/bootstrap.min.css")} type="text/css"/>
    <link rel="stylesheet" href={prependBaseUri(request, "/static/webui.css")} type="text/css"/>
    <script src={prependBaseUri(request, "/static/sorttable.js")} ></script>
    <script src={prependBaseUri(request, "/static/jquery.min.js")}></script>
    <script src={prependBaseUri(request, "/static/bootstrap.bundle.min.js")}></script>
    <script src={prependBaseUri(request, "/static/initialize-tooltips.js")}></script>
    <script src={prependBaseUri(request, "/static/table.js")}></script>
    <script src={prependBaseUri(request, "/static/log-view.js")}></script>
    <script src={prependBaseUri(request, "/static/webui.js")}></script>
    <script src={prependBaseUri(request, "/static/scroll-button.js")} type="module"></script>
    <script nonce={CspNonce.get}>setUIRoot('{UIUtils.uiRoot(request)}')</script>
  }

  def timelineHeaderNodes(request: HttpServletRequest): Seq[Node] = {
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/vis-timeline-graph2d.min.css")} type="text/css"/>
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/timeline-view.css")} type="text/css"/>
    <script src={prependBaseUri(request, "/static/vis-timeline-graph2d.min.js")}></script>
    <script src={prependBaseUri(request, "/static/timeline-view.js")}></script>
  }

  def vizHeaderNodes(request: HttpServletRequest): Seq[Node] = {
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/spark-dag-viz.css")} type="text/css" />
    <script src={prependBaseUri(request, "/static/d3.min.js")}></script>
    <script src={prependBaseUri(request, "/static/dagre-d3.min.js")}></script>
    <script src={prependBaseUri(request, "/static/graphlib-dot.min.js")}></script>
    <script src={prependBaseUri(request, "/static/spark-dag-viz.js")}></script>
  }

  def dataTablesHeaderNodes(request: HttpServletRequest): Seq[Node] = {
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/dataTables.bootstrap5.min.css")}
          type="text/css"/>
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/jquery.dataTables.min.css")}
          type="text/css"/>
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/webui-dataTables.css")} type="text/css"/>
    <script src={prependBaseUri(request, "/static/jquery.dataTables.min.js")}></script>
    <script src={prependBaseUri(request, "/static/dataTables.bootstrap5.min.js")}></script>
  }

  /** Returns a spark page with correctly formatted headers */
  def headerSparkPage(
      request: HttpServletRequest,
      title: String,
      content: => Seq[Node],
      activeTab: SparkUITab,
      helpText: Option[String] = None,
      showVisualization: Boolean = false,
      useDataTables: Boolean = false,
      useTimeline: Boolean = false): Seq[Node] = {

    val appName = activeTab.appName
    val shortAppName = if (appName.length < 36) appName else appName.take(32) + "..."
    val header = activeTab.headerTabs.map { tab =>
      <li class={if (tab == activeTab) "nav-item active" else "nav-item"}>
        <a class="nav-link"
           href={prependBaseUri(request, activeTab.basePath, "/" + tab.prefix + "/")}>{tab.name}</a>
      </li>
    }
    val helpButton: Seq[Node] = helpText.map(tooltip(_, "top")).getOrElse(Seq.empty)

    <html data-bs-theme="light">
      <head>
        {commonHeaderNodes(request)}
        <script nonce={CspNonce.get}>{Unparsed(
          "document.documentElement.setAttribute('data-bs-theme'," +
          "localStorage.getItem('spark-theme')||" +
          "(matchMedia('(prefers-color-scheme:dark)').matches?'dark':'light'))")}</script>
        <script nonce={CspNonce.get}>setAppBasePath('{activeTab.basePath}')</script>
        {if (showVisualization) vizHeaderNodes(request) else Seq.empty}
        {if (useDataTables) dataTablesHeaderNodes(request) else Seq.empty}
        {if (useTimeline) timelineHeaderNodes(request) else Seq.empty}
        <link rel="shortcut icon"
              href={prependBaseUri(request, "/static/spark-logo.svg")}></link>
        <title>{appName} - {title}</title>
      </head>
      <body class="d-flex flex-column min-vh-100">
        <div id="loading-overlay"
             class={"position-fixed top-0 start-0 w-100 h-100" +
               " d-flex justify-content-center align-items-center d-none"}>
          <div class="text-center">
            <div class="spinner-border text-primary" role="status">
              <span class="visually-hidden">Loading...</span>
            </div>
            <h3 class="mt-2">Loading...</h3>
          </div>
        </div>
        <nav class="navbar navbar-expand-md navbar-light bg-light mb-4">
          <div class="navbar-header">
            <div class="navbar-brand">
              <a href={prependBaseUri(request, "/")}>
                <img class="spark-logo" src={prependBaseUri(request, "/static/spark-logo.svg")}
                     alt="Spark Logo" height="36" />
              </a>
            </div>
          </div>
          <button class="navbar-toggler" type="button" data-bs-toggle="collapse"
                  data-bs-target="#navbarCollapse" aria-controls="navbarCollapse"
                  aria-expanded="false" aria-label="Toggle navigation">
            <span class="navbar-toggler-icon"></span>
          </button>
          <div class="collapse navbar-collapse" id="navbarCollapse">
            <ul class="navbar-nav me-auto">{header}</ul>
            <span class="navbar-text navbar-right d-none d-md-block">
              <strong title={appName} class="text-nowrap">{shortAppName}</strong>
              <span class="text-nowrap">application UI</span>
            </span>
            <button id="theme-toggle" class="btn btn-sm btn-link text-decoration-none fs-5 ms-2 p-0"
                    type="button" title="Toggle dark mode"></button>
          </div>
        </nav>
        <div class="container-fluid flex-fill">
          <div class="row">
            <div class="col-12">
              <h3 class="align-bottom text-nowrap overflow-hidden text-truncate">
                {title}
                {helpButton}
              </h3>
            </div>
          </div>
          <div class="row">
            <div class="col-12">
              {content}
            </div>
          </div>
        </div>
        {sparkFooter(Some(activeTab))}
      </body>
    </html>
  }

  /** Returns a page with the spark css/js and a simple format. Used for scheduler UI. */
  def basicSparkPage(
      request: HttpServletRequest,
      content: => Seq[Node],
      title: String,
      useDataTables: Boolean = false,
      useTimeline: Boolean = false): Seq[Node] = {
    <html data-bs-theme="light">
      <head>
        {commonHeaderNodes(request)}
        <script nonce={CspNonce.get}>{Unparsed(
          "document.documentElement.setAttribute('data-bs-theme'," +
          "localStorage.getItem('spark-theme')||" +
          "(matchMedia('(prefers-color-scheme:dark)').matches?'dark':'light'))")}</script>
        {if (useDataTables) dataTablesHeaderNodes(request) else Seq.empty}
        {if (useTimeline) timelineHeaderNodes(request) else Seq.empty}
        <link rel="shortcut icon"
              href={prependBaseUri(request, "/static/spark-logo.svg")}></link>
        <title>{title}</title>
      </head>
      <body class="d-flex flex-column min-vh-100">
        <div id="loading-overlay"
             class={"position-fixed top-0 start-0 w-100 h-100" +
               " d-flex justify-content-center align-items-center d-none"}>
          <div class="text-center">
            <div class="spinner-border text-primary" role="status">
              <span class="visually-hidden">Loading...</span>
            </div>
            <h3 class="mt-2">Loading...</h3>
          </div>
        </div>
        <div class="container-fluid flex-fill">
          <div class="row">
            <div class="col-12">
              <h3 class="align-middle d-inline-block">
                <a class="text-decoration-none" href={prependBaseUri(request, "/")}>
                  <img class="spark-logo" src={prependBaseUri(request, "/static/spark-logo.svg")}
                       alt="Spark Logo" height="36" />
                  <span class="version me-3">{org.apache.spark.SPARK_VERSION}</span>
                </a>
                {title}
                <button id="theme-toggle"
                        class="btn btn-sm btn-link text-decoration-none fs-5 ms-2 p-0 align-middle"
                        type="button" title="Toggle dark mode"></button>
              </h3>
            </div>
          </div>
          <div class="row">
            <div class="col-12">
              {content}
            </div>
          </div>
        </div>
        {sparkFooter()}
      </body>
    </html>
  }

  private def sparkFooter(tab: Option[SparkUITab] = None): Seq[Node] = {
    val user = tab.map(_.sparkUser).getOrElse(System.getProperty("user.name", ""))
    val version = tab.map(_.appSparkVersion).getOrElse(org.apache.spark.SPARK_VERSION)
    val startTimeOpt = tab.map(_.appStartTime).orElse(SparkContext.getActive.map(_.startTime))

    // scalastyle:off
    <footer class="footer mt-auto py-2 bg-body-tertiary border-top">
      <div class="container-fluid">
        <div class="d-flex justify-content-between align-items-center small text-body-secondary">
          <span>{version}</span>
          {startTimeOpt.map { t =>
            <span>Started: {formatDate(new Date(t))}</span>
            <span id="footer-uptime" data-start-time={t.toString}></span>
          }.getOrElse(Seq.empty)}
          <span>{user}</span>
        </div>
      </div>
    </footer>
    // scalastyle:on
  }

  /** Returns an HTML table constructed by generating a row for each object in a sequence. */
  def listingTable[T](
      headers: Seq[String],
      generateDataRow: T => Seq[Node],
      data: Iterable[T],
      fixedWidth: Boolean = false,
      id: Option[String] = None,
      // When headerClasses is not empty, it should have the same length as headers parameter
      headerClasses: Seq[String] = Seq.empty,
      stripeRowsWithCss: Boolean = true,
      sortable: Boolean = true,
      // The tooltip information could be None, which indicates header does not have a tooltip.
      // When tooltipHeaders is not empty, it should have the same length as headers parameter
      tooltipHeaders: Seq[Option[String]] = Seq.empty): Seq[Node] = {

    val listingTableClass = {
      val _tableClass = if (stripeRowsWithCss) TABLE_CLASS_STRIPED else TABLE_CLASS_NOT_STRIPED
      if (sortable) {
        _tableClass + " sortable"
      } else {
        _tableClass
      }
    }
    val colWidth = 100.toDouble / headers.size
    val colWidthAttr = if (fixedWidth) s"$colWidth%" else ""

    def getClass(index: Int): String = {
      if (index < headerClasses.size) {
        headerClasses(index)
      } else {
        ""
      }
    }

    def getTooltip(index: Int): Option[String] = {
      if (index < tooltipHeaders.size) {
        tooltipHeaders(index)
      } else {
        None
      }
    }

    val newlinesInHeader = headers.exists(_.contains("\n"))
    def getHeaderContent(header: String): Seq[Node] = {
      if (newlinesInHeader) {
        <ul class="list-unstyled">
          { header.split("\n").map(t => <li> {t} </li>) }
        </ul>
      } else {
        Text(header)
      }
    }

    val headerRow: Seq[Node] = {
      headers.to(LazyList).zipWithIndex.map { x =>
        getTooltip(x._2) match {
          case Some(tooltip) =>
            <th width={colWidthAttr} class={getClass(x._2)}>
              {tooltipSpan(getHeaderContent(x._1), tooltip)}
            </th>
          case None => <th width={colWidthAttr} class={getClass(x._2)}>{getHeaderContent(x._1)}</th>
        }
      }
    }
    <div class="table-responsive">
    <table class={listingTableClass} id={id.map(Text.apply)}>
      <thead>{headerRow}</thead>
      <tbody>
        {data.map(r => generateDataRow(r))}
      </tbody>
    </table>
    </div>
  }

  def makeProgressBar(
      started: Int,
      completed: Int,
      failed: Int,
      skipped: Int,
      reasonToNumKilled: Map[String, Int],
      total: Int): Seq[Node] = {
    val ratio = if (total == 0) 100.0 else (completed.toDouble / total) * 100
    val completeWidth = "width: %s%%".format(ratio)
    // started + completed can be > total when there are speculative tasks
    val boundedStarted = math.min(started, total - completed)
    val startRatio = if (total == 0) 0.0 else (boundedStarted.toDouble / total) * 100
    val startWidth = "width: %s%%".format(startRatio)

    val totalKilled = reasonToNumKilled.values.sum
    val killReasonTitle = reasonToNumKilled.toSeq.sortBy(-_._2).map {
        case (reason, count) =>
          val truncated = if (reason.length > 120) reason.take(120) + "..." else reason
          s" ($count killed: $truncated)"
      }.mkString
    val progressTitle = s"$completed/$total" + {
      if (started > 0) s" ($started running)" else ""
    } + {
      if (failed > 0) s" ($failed failed)" else ""
    } + {
      if (skipped > 0) s" ($skipped skipped)" else ""
    } + killReasonTitle

    val progressLabel = s"$completed/$total" +
      (if (failed == 0 && skipped == 0 && started > 0) s" ($started running)" else "") +
      (if (failed > 0) s" ($failed failed)" else "") +
      (if (skipped > 0) s" ($skipped skipped)" else "") +
      (if (totalKilled > 0) s" ($totalKilled killed)" else "")

    // scalastyle:off line.size.limit
    <div class="progress-stacked" title={progressTitle}>
      <span class="position-absolute w-100 h-100 d-flex align-items-center justify-content-center">
        {progressLabel}
      </span>
      <div class="progress" role="progressbar" aria-label="Completed" aria-valuenow={ratio.toInt.toString} aria-valuemin="0" aria-valuemax="100" style={completeWidth}>
        <div class="progress-bar progress-completed"></div>
      </div>
      <div class="progress" role="progressbar" aria-label="Running" aria-valuenow={startRatio.toInt.toString} aria-valuemin="0" aria-valuemax="100" style={startWidth}>
        <div class="progress-bar progress-started"></div>
      </div>
    </div>
    // scalastyle:on line.size.limit
  }

  /** Return a "DAG visualization" DOM element that expands into a visualization for a stage. */
  def showDagVizForStage(stageId: Int, graph: Option[RDDOperationGraph]): Seq[Node] = {
    showDagViz(graph.toSeq, forJob = false)
  }

  /** Return a "DAG visualization" DOM element that expands into a visualization for a job. */
  def showDagVizForJob(jobId: Int,
      graphs: collection.Seq[RDDOperationGraph]): collection.Seq[Node] = {
    showDagViz(graphs, forJob = true)
  }

  /**
   * Return a "DAG visualization" DOM element that expands into a visualization on the UI.
   *
   * This populates metadata necessary for generating the visualization on the front-end in
   * a format that is expected by spark-dag-viz.js. Any changes in the format here must be
   * reflected there.
   */
  private def showDagViz(
      graphs: collection.Seq[RDDOperationGraph], forJob: Boolean): collection.Seq[Node] = {
    <div>
      <span id={if (forJob) "job-dag-viz" else "stage-dag-viz"}
            class="expand-dag-viz" data-forjob={forJob.toString}>
        <span class="expand-dag-viz-arrow arrow-closed"></span>
        {tooltipLink(<xml:group>DAG Visualization</xml:group>,
          if (forJob) ToolTips.JOB_DAG else ToolTips.STAGE_DAG)}
      </span>
      <div id="dag-viz-graph"></div>
      <div id="dag-viz-metadata" class="d-none">
        {
          graphs.map { g =>
            val stageId = g.rootCluster.id.replaceAll(RDDOperationGraph.STAGE_CLUSTER_PREFIX, "")
            val skipped = g.rootCluster.name.contains("skipped").toString
            <div class="stage-metadata" stage-id={stageId} skipped={skipped}>
              <div class="dot-file">{RDDOperationGraph.makeDotFile(g)}</div>
              { g.incomingEdges.map { e => <div class="incoming-edge">{e.fromId},{e.toId}</div> } }
              { g.outgoingEdges.map { e => <div class="outgoing-edge">{e.fromId},{e.toId}</div> } }
              {
                g.rootCluster.getCachedNodes.map { n =>
                  <div class="cached-rdd">{n.id}</div>
                } ++
                g.rootCluster.getBarrierClusters.map { c =>
                  <div class="barrier-rdd">{c.id}</div>
                } ++
                g.rootCluster.getIndeterminateNodes.map { n =>
                  <div class="indeterminate-rdd">{n.id}</div>
                }
              }
            </div>
          }
        }
      </div>
    </div>
  }

  def tooltip(text: String, position: String): Seq[Node] = {
    <sup>
      (<a data-bs-toggle="tooltip" title={text}>?</a>)
    </sup>
  }

  /** Wrap content in a span with a Bootstrap 5 tooltip. */
  def tooltipSpan(content: Seq[Node], text: String,
      placement: String = "top"): Seq[Node] = {
    val bsPlacement = if (placement == "top") null else placement
    <span data-bs-toggle="tooltip"
        data-bs-placement={bsPlacement} title={text}>
      {content}
    </span>
  }

  /** Create a link with a Bootstrap 5 tooltip. */
  def tooltipLink(content: Seq[Node], text: String,
      placement: String = "top"): Seq[Node] = {
    val bsPlacement = if (placement == "top") null else placement
    <a data-bs-toggle="tooltip"
        data-bs-placement={bsPlacement} title={text}>
      {content}
    </a>
  }

  /**
   * Returns HTML rendering of a job or stage description. It will try to parse the string as HTML
   * and make sure that it only contains anchors with root-relative links. Otherwise,
   * the whole string will rendered as a simple escaped text.
   *
   * Note: In terms of security, only anchor tags with root relative links are supported. So any
   * attempts to embed links outside Spark UI, other tags like &lt;script&gt;, or inline scripts
   * like `onclick` will cause in the whole description to be treated as plain text.
   *
   * @param desc        the original job or stage description string, which may contain html tags.
   * @param basePathUri with which to prepend the relative links; this is used when plainText is
   *                    false.
   * @param plainText   whether to keep only plain text (i.e. remove html tags) from the original
   *                    description string.
   * @return the HTML rendering of the job or stage description, which will be a Text when plainText
   *         is true, and an Elem otherwise.
   */
  def makeDescription(desc: String, basePathUri: String, plainText: Boolean = false): NodeSeq = {

    // If the description can be parsed as HTML and has only relative links, then render
    // as HTML, otherwise render as escaped string
    try {
      // Try to load the description as unescaped HTML
      val xml = XML.loadString(s"""<span class="description-input">$desc</span>""")

      // Verify that this has only anchors and span (we are wrapping in span)
      val allowedNodeLabels = Set("a", "span", "br")
      val allowedAttributes = Set("class", "href")
      val illegalNodes =
        (xml \\ "_").filterNot { node =>
          allowedNodeLabels.contains(node.label) &&
            // Verify we only have href attributes
            node.attributes.map(_.key).forall(allowedAttributes.contains)
        }
      if (illegalNodes.nonEmpty) {
        throw new IllegalArgumentException(
          "Only HTML anchors allowed in job descriptions\n" +
            illegalNodes.map { n => s"${n.label} in $n"}.mkString("\n\t"))
      }

      // Verify that all links are relative links starting with "/"
      val allLinks =
        xml \\ "a" flatMap { _.attributes } filter { _.key == "href" } map { _.value.toString }
      if (allLinks.exists { ! _.startsWith ("/") }) {
        throw new IllegalArgumentException(
          "Links in job descriptions must be root-relative:\n" + allLinks.mkString("\n\t"))
      }

      val rule =
        if (plainText) {
          // Remove all tags, retaining only their texts
          new RewriteRule() {
            override def transform(n: Node): Seq[Node] = {
              n match {
                case e: Elem if e.child.isEmpty => Text(e.text)
                case e: Elem => Text(e.child.flatMap(transform).text)
                case _ => n
              }
            }
          }
        }
        else {
          // Prepend the relative links with basePathUri
          new RewriteRule() {
            override def transform(n: Node): Seq[Node] = {
              n match {
                case e: Elem if (e \ "@href").nonEmpty =>
                  val relativePath = e.attribute("href").get.toString
                  val fullUri = s"${basePathUri.stripSuffix("/")}/${relativePath.stripPrefix("/")}"
                  e % Attribute(null, "href", fullUri, Null)
                case _ => n
              }
            }
          }
        }
      new RuleTransformer(rule).transform(xml)
    } catch {
      case NonFatal(e) =>
        if (plainText) Text(desc) else <span class="description-input">{desc}</span>
    }
  }

  /**
   * Decode URLParameter if URL is encoded by YARN-WebAppProxyServlet.
   * Due to YARN-2844: WebAppProxyServlet cannot handle urls which contain encoded characters
   * Therefore we need to decode it until we get the real URLParameter.
   */
  def decodeURLParameter(urlParam: String): String = {
    var param = urlParam
    var decodedParam = URLDecoder.decode(param, UTF_8.name())
    while (param != decodedParam) {
      param = decodedParam
      decodedParam = URLDecoder.decode(param, UTF_8.name())
    }
    param
  }

  /**
   * Decode URLParameter if URL is encoded by YARN-WebAppProxyServlet.
   */
  def decodeURLParameter(params: MultivaluedMap[String, String]): MultivaluedStringMap = {
    val decodedParameters = new MultivaluedStringMap
    params.forEach((encodeKey, encodeValues) => {
      val decodeKey = decodeURLParameter(encodeKey)
      val decodeValues = new java.util.LinkedList[String]
      encodeValues.forEach(v => {
        decodeValues.add(decodeURLParameter(v))
      })
      decodedParameters.addAll(decodeKey, decodeValues)
    })
    decodedParameters
  }

  def getTimeZoneOffset() : Int =
    TimeZone.getDefault().getOffset(System.currentTimeMillis()) / 1000 / 60

  /**
  * Return the correct Href after checking if master is running in the
  * reverse proxy mode or not.
  */
  def makeHref(proxy: Boolean, id: String, origHref: String): String = {
    if (proxy) {
      val proxyPrefix = sys.props.getOrElse("spark.ui.proxyBase", "")
      proxyPrefix + "/proxy/" + id
    } else {
      origHref
    }
  }

  def buildErrorResponse(status: Response.Status, msg: String): Response = {
    Response.status(status).entity(msg).`type`(MediaType.TEXT_PLAIN).build()
  }

  /**
   * There may be different duration labels in each batch. So we need to
   * mark those missing duration label as '0d' to avoid UI rending error.
   */
  def durationDataPadding(
      values: Array[(Long, ju.Map[String, JLong])]): Array[(Long, Map[String, Double])] = {
    val operationLabels = values.flatMap(_._2.keySet().asScala).toSet
    values.map { case (xValue, yValue) =>
      val dataPadding = operationLabels.map { opLabel =>
        if (yValue.containsKey(opLabel)) {
          (opLabel, yValue.get(opLabel).toDouble)
        } else {
          (opLabel, 0d)
        }
      }
      (xValue, dataPadding.toMap)
    }
  }

  def detailsUINode(isMultiline: Boolean, message: String): Seq[Node] = {
    if (isMultiline) {
      // scalastyle:off
      <span data-toggle-details=".stacktrace-details"
            class="expand-details float-end">
        +details
      </span> ++
        <div class="stacktrace-details collapsed">
          <pre>{message}</pre>
        </div>
      // scalastyle:on
    } else {
      Seq.empty[Node]
    }
  }

  private final val ERROR_CLASS_REGEX = """\[(?<errorClass>[A-Z][A-Z_.]+[A-Z])]""".r

  /**
   * This function works exactly the same as utils.errorSummary(javascript), it shall be
   * remained the same whichever changed */
  def errorSummary(errorMessage: String): (String, Boolean) = {
    var isMultiline = true
    val maybeErrorClass =
      ERROR_CLASS_REGEX.findFirstMatchIn(errorMessage).map(_.group("errorClass"))
    val errorClassOrBrief = if (maybeErrorClass.nonEmpty && maybeErrorClass.get.nonEmpty) {
      maybeErrorClass.get
    } else if (errorMessage.indexOf('\n') >= 0) {
      errorMessage.substring(0, errorMessage.indexOf('\n'))
    } else if (errorMessage.indexOf(":") >= 0) {
      errorMessage.substring(0, errorMessage.indexOf(":"))
    } else {
      isMultiline = false
      errorMessage
    }

    (errorClassOrBrief, isMultiline)
  }

  def errorMessageCell(errorMessage: String): Seq[Node] = {
    val (summary, isMultiline) = errorSummary(errorMessage)
    val details = detailsUINode(isMultiline, errorMessage)
    <td>{summary}{details}</td>
  }

  def formatImportJavaScript(
      request: HttpServletRequest,
      sourceFile: String,
      methods: String*): String = {
    val methodsStr = methods.mkString("{", ", ", "}")
    val sourceFileStr = prependBaseUri(request, sourceFile)
    s"""import $methodsStr from "$sourceFileStr";"""
  }
}
