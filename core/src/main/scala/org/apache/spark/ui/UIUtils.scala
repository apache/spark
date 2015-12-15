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
import java.util.{Date, Locale}

import scala.util.control.NonFatal
import scala.xml._
import scala.xml.transform.{RewriteRule, RuleTransformer}

import org.apache.spark.Logging
import org.apache.spark.ui.scope.RDDOperationGraph

/** Utility functions for generating XML pages with spark content. */
private[spark] object UIUtils extends Logging {
  val TABLE_CLASS_NOT_STRIPED = "table table-bordered table-condensed"
  val TABLE_CLASS_STRIPED = TABLE_CLASS_NOT_STRIPED + " table-striped"
  val TABLE_CLASS_STRIPED_SORTABLE = TABLE_CLASS_STRIPED + " sortable"

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
  def uiRoot: String = {
    // SPARK-11484 - Use the proxyBase set by the AM, if not found then use env.
    sys.props.get("spark.ui.proxyBase")
      .orElse(sys.env.get("APPLICATION_WEB_PROXY_BASE"))
      .getOrElse("")
  }

  def prependBaseUri(basePath: String = "", resource: String = ""): String = {
    uiRoot + basePath + resource
  }

  def commonHeaderNodes: Seq[Node] = {
    <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
    <link rel="stylesheet" href={prependBaseUri("/static/bootstrap.min.css")} type="text/css"/>
    <link rel="stylesheet" href={prependBaseUri("/static/vis.min.css")} type="text/css"/>
    <link rel="stylesheet" href={prependBaseUri("/static/webui.css")} type="text/css"/>
    <link rel="stylesheet" href={prependBaseUri("/static/timeline-view.css")} type="text/css"/>
    <script src={prependBaseUri("/static/sorttable.js")} ></script>
    <script src={prependBaseUri("/static/jquery-1.11.1.min.js")}></script>
    <script src={prependBaseUri("/static/vis.min.js")}></script>
    <script src={prependBaseUri("/static/bootstrap-tooltip.js")}></script>
    <script src={prependBaseUri("/static/initialize-tooltips.js")}></script>
    <script src={prependBaseUri("/static/table.js")}></script>
    <script src={prependBaseUri("/static/additional-metrics.js")}></script>
    <script src={prependBaseUri("/static/timeline-view.js")}></script>
  }

  def vizHeaderNodes: Seq[Node] = {
    <link rel="stylesheet" href={prependBaseUri("/static/spark-dag-viz.css")} type="text/css" />
    <script src={prependBaseUri("/static/d3.min.js")}></script>
    <script src={prependBaseUri("/static/dagre-d3.min.js")}></script>
    <script src={prependBaseUri("/static/graphlib-dot.min.js")}></script>
    <script src={prependBaseUri("/static/spark-dag-viz.js")}></script>
  }

  /** Returns a spark page with correctly formatted headers */
  def headerSparkPage(
      title: String,
      content: => Seq[Node],
      activeTab: SparkUITab,
      refreshInterval: Option[Int] = None,
      helpText: Option[String] = None,
      showVisualization: Boolean = false): Seq[Node] = {

    val appName = activeTab.appName
    val shortAppName = if (appName.length < 36) appName else appName.take(32) + "..."
    val header = activeTab.headerTabs.map { tab =>
      <li class={if (tab == activeTab) "active" else ""}>
        <a href={prependBaseUri(activeTab.basePath, "/" + tab.prefix + "/")}>{tab.name}</a>
      </li>
    }
    val helpButton: Seq[Node] = helpText.map(tooltip(_, "bottom")).getOrElse(Seq.empty)

    <html>
      <head>
        {commonHeaderNodes}
        {if (showVisualization) vizHeaderNodes else Seq.empty}
        <title>{appName} - {title}</title>
      </head>
      <body>
        <div class="navbar navbar-static-top">
          <div class="navbar-inner">
            <div class="brand">
              <a href={prependBaseUri("/")} class="brand">
                <img src={prependBaseUri("/static/spark-logo-77x50px-hd.png")} />
                <span class="version">{org.apache.spark.SPARK_VERSION}</span>
              </a>
            </div>
            <p class="navbar-text pull-right">
              <strong title={appName}>{shortAppName}</strong> application UI
            </p>
            <ul class="nav">{header}</ul>
          </div>
        </div>
        <div class="container-fluid">
          <div class="row-fluid">
            <div class="span12">
              <h3 style="vertical-align: bottom; display: inline-block;">
                {title}
                {helpButton}
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
        {commonHeaderNodes}
        <title>{title}</title>
      </head>
      <body>
        <div class="container-fluid">
          <div class="row-fluid">
            <div class="span12">
              <h3 style="vertical-align: middle; display: inline-block;">
                <a style="text-decoration: none" href={prependBaseUri("/")}>
                  <img src={prependBaseUri("/static/spark-logo-77x50px-hd.png")} />
                  <span class="version"
                        style="margin-right: 15px;">{org.apache.spark.SPARK_VERSION}</span>
                </a>
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
      data: Iterable[T],
      fixedWidth: Boolean = false,
      id: Option[String] = None,
      headerClasses: Seq[String] = Seq.empty,
      stripeRowsWithCss: Boolean = true,
      sortable: Boolean = true): Seq[Node] = {

    val listingTableClass = {
      val _tableClass = if (stripeRowsWithCss) TABLE_CLASS_STRIPED else TABLE_CLASS_NOT_STRIPED
      if (sortable) {
        _tableClass + " sortable"
      } else {
        _tableClass
      }
    }
    val colWidth = 100.toDouble / headers.size
    val colWidthAttr = if (fixedWidth) colWidth + "%" else ""

    def getClass(index: Int): String = {
      if (index < headerClasses.size) {
        headerClasses(index)
      } else {
        ""
      }
    }

    val newlinesInHeader = headers.exists(_.contains("\n"))
    def getHeaderContent(header: String): Seq[Node] = {
      if (newlinesInHeader) {
        <ul class="unstyled">
          { header.split("\n").map { case t => <li> {t} </li> } }
        </ul>
      } else {
        Text(header)
      }
    }

    val headerRow: Seq[Node] = {
      headers.view.zipWithIndex.map { x =>
        <th width={colWidthAttr} class={getClass(x._2)}>{getHeaderContent(x._1)}</th>
      }
    }
    <table class={listingTableClass} id={id.map(Text.apply)}>
      <thead>{headerRow}</thead>
      <tbody>
        {data.map(r => generateDataRow(r))}
      </tbody>
    </table>
  }

  def makeProgressBar(
      started: Int,
      completed: Int,
      failed: Int,
      skipped: Int,
      total: Int): Seq[Node] = {
    val completeWidth = "width: %s%%".format((completed.toDouble/total)*100)
    // started + completed can be > total when there are speculative tasks
    val boundedStarted = math.min(started, total - completed)
    val startWidth = "width: %s%%".format((boundedStarted.toDouble/total)*100)

    <div class="progress">
      <span style="text-align:center; position:absolute; width:100%; left:0;">
        {completed}/{total}
        { if (failed > 0) s"($failed failed)" }
        { if (skipped > 0) s"($skipped skipped)" }
      </span>
      <div class="bar bar-completed" style={completeWidth}></div>
      <div class="bar bar-running" style={startWidth}></div>
    </div>
  }

  /** Return a "DAG visualization" DOM element that expands into a visualization for a stage. */
  def showDagVizForStage(stageId: Int, graph: Option[RDDOperationGraph]): Seq[Node] = {
    showDagViz(graph.toSeq, forJob = false)
  }

  /** Return a "DAG visualization" DOM element that expands into a visualization for a job. */
  def showDagVizForJob(jobId: Int, graphs: Seq[RDDOperationGraph]): Seq[Node] = {
    showDagViz(graphs, forJob = true)
  }

  /**
   * Return a "DAG visualization" DOM element that expands into a visualization on the UI.
   *
   * This populates metadata necessary for generating the visualization on the front-end in
   * a format that is expected by spark-dag-viz.js. Any changes in the format here must be
   * reflected there.
   */
  private def showDagViz(graphs: Seq[RDDOperationGraph], forJob: Boolean): Seq[Node] = {
    <div>
      <span id={if (forJob) "job-dag-viz" else "stage-dag-viz"}
            class="expand-dag-viz" onclick={s"toggleDagViz($forJob);"}>
        <span class="expand-dag-viz-arrow arrow-closed"></span>
        <a data-toggle="tooltip" title={if (forJob) ToolTips.JOB_DAG else ToolTips.STAGE_DAG}
           data-placement="right">
          DAG Visualization
        </a>
      </span>
      <div id="dag-viz-graph"></div>
      <div id="dag-viz-metadata" style="display:none">
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
      (<a data-toggle="tooltip" data-placement={position} title={text}>?</a>)
    </sup>
  }

  /** Return a script element that automatically expands the DAG visualization on page load. */
  def expandDagVizOnLoad(forJob: Boolean): Seq[Node] = {
    <script type="text/javascript">
      {Unparsed("$(document).ready(function() { toggleDagViz(" + forJob + ") });")}
    </script>
  }

  /**
   * Returns HTML rendering of a job or stage description. It will try to parse the string as HTML
   * and make sure that it only contains anchors with root-relative links. Otherwise,
   * the whole string will rendered as a simple escaped text.
   *
   * Note: In terms of security, only anchor tags with root relative links are supported. So any
   * attempts to embed links outside Spark UI, or other tags like <script> will cause in the whole
   * description to be treated as plain text.
   */
  def makeDescription(desc: String, basePathUri: String): NodeSeq = {
    import scala.language.postfixOps

    // If the description can be parsed as HTML and has only relative links, then render
    // as HTML, otherwise render as escaped string
    try {
      // Try to load the description as unescaped HTML
      val xml = XML.loadString(s"""<span class="description-input">$desc</span>""")

      // Verify that this has only anchors and span (we are wrapping in span)
      val allowedNodeLabels = Set("a", "span")
      val illegalNodes = xml \\ "_"  filterNot { case node: Node =>
        allowedNodeLabels.contains(node.label)
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

      // Prepend the relative links with basePathUri
      val rule = new RewriteRule() {
        override def transform(n: Node): Seq[Node] = {
          n match {
            case e: Elem if e \ "@href" nonEmpty =>
              val relativePath = e.attribute("href").get.toString
              val fullUri = s"${basePathUri.stripSuffix("/")}/${relativePath.stripPrefix("/")}"
              e % Attribute(null, "href", fullUri, Null)
            case _ => n
          }
        }
      }
      new RuleTransformer(rule).transform(xml)
    } catch {
      case NonFatal(e) =>
        <span class="description-input">{desc}</span>
    }
  }
}
