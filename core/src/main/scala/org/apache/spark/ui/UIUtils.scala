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

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.{MediaType, Response}

import scala.util.control.NonFatal
import scala.xml._
import scala.xml.transform.{RewriteRule, RuleTransformer}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.internal.Logging
import org.apache.spark.ui.scope.RDDOperationGraph

/** Utility functions for generating XML pages with spark content. */
private[spark] object UIUtils extends Logging {
  val TABLE_CLASS_NOT_STRIPED = "table table-bordered table-condensed"
  val TABLE_CLASS_STRIPED = TABLE_CLASS_NOT_STRIPED + " table-striped"
  val TABLE_CLASS_STRIPED_SORTABLE = TABLE_CLASS_STRIPED + " sortable"

  private val NEWLINE_AND_SINGLE_QUOTE_REGEX = raw"(?i)(\r\n|\n|\r|%0D%0A|%0A|%0D|'|%27)".r

  // SimpleDateFormat is not thread-safe. Don't expose it to avoid improper use.
  private val dateFormat = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat =
      new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.US)
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
  def uiRoot(request: HttpServletRequest): String = {
    // Knox uses X-Forwarded-Context to notify the application the base path
    val knoxBasePath = Option(request.getHeader("X-Forwarded-Context"))
    // SPARK-11484 - Use the proxyBase set by the AM, if not found then use env.
    sys.props.get("spark.ui.proxyBase")
      .orElse(sys.env.get("APPLICATION_WEB_PROXY_BASE"))
      .orElse(knoxBasePath)
      .getOrElse("")
  }

  def prependBaseUri(
      request: HttpServletRequest,
      basePath: String = "",
      resource: String = ""): String = {
    uiRoot(request) + basePath + resource
  }

  def commonHeaderNodes(request: HttpServletRequest): Seq[Node] = {
    <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/bootstrap.min.css")} type="text/css"/>
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/vis-timeline-graph2d.min.css")} type="text/css"/>
    <link rel="stylesheet" href={prependBaseUri(request, "/static/webui.css")} type="text/css"/>
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/timeline-view.css")} type="text/css"/>
    <script src={prependBaseUri(request, "/static/sorttable.js")} ></script>
    <script src={prependBaseUri(request, "/static/jquery-1.12.4.min.js")}></script>
    <script src={prependBaseUri(request, "/static/vis-timeline-graph2d.min.js")}></script>
    <script src={prependBaseUri(request, "/static/bootstrap-tooltip.js")}></script>
    <script src={prependBaseUri(request, "/static/initialize-tooltips.js")}></script>
    <script src={prependBaseUri(request, "/static/table.js")}></script>
    <script src={prependBaseUri(request, "/static/additional-metrics.js")}></script>
    <script src={prependBaseUri(request, "/static/timeline-view.js")}></script>
    <script src={prependBaseUri(request, "/static/log-view.js")}></script>
    <script src={prependBaseUri(request, "/static/webui.js")}></script>
    <script>setUIRoot('{UIUtils.uiRoot(request)}')</script>
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
    <link rel="stylesheet" href={prependBaseUri(request,
      "/static/jquery.dataTables.1.10.18.min.css")} type="text/css"/>
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/dataTables.bootstrap.css")} type="text/css"/>
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/jsonFormatter.min.css")} type="text/css"/>
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/webui-dataTables.css")} type="text/css"/>
    <script src={prependBaseUri(request, "/static/jquery.dataTables.1.10.18.min.js")}></script>
    <script src={prependBaseUri(request, "/static/jquery.cookies.2.2.0.min.js")}></script>
    <script src={prependBaseUri(request, "/static/jquery.blockUI.min.js")}></script>
    <script src={prependBaseUri(request, "/static/dataTables.bootstrap.min.js")}></script>
    <script src={prependBaseUri(request, "/static/jsonFormatter.min.js")}></script>
    <script src={prependBaseUri(request, "/static/jquery.mustache.js")}></script>
  }

  /** Returns a spark page with correctly formatted headers */
  def headerSparkPage(
      request: HttpServletRequest,
      title: String,
      content: => Seq[Node],
      activeTab: SparkUITab,
      refreshInterval: Option[Int] = None,
      helpText: Option[String] = None,
      showVisualization: Boolean = false,
      useDataTables: Boolean = false): Seq[Node] = {

    val appName = activeTab.appName
    val shortAppName = if (appName.length < 36) appName else appName.take(32) + "..."
    val header = activeTab.headerTabs.map { tab =>
      <li class={if (tab == activeTab) "active" else ""}>
        <a href={prependBaseUri(request, activeTab.basePath, "/" + tab.prefix + "/")}>{tab.name}</a>
      </li>
    }
    val helpButton: Seq[Node] = helpText.map(tooltip(_, "bottom")).getOrElse(Seq.empty)

    <html>
      <head>
        {commonHeaderNodes(request)}
        <script>setAppBasePath('{activeTab.basePath}')</script>
        {if (showVisualization) vizHeaderNodes(request) else Seq.empty}
        {if (useDataTables) dataTablesHeaderNodes(request) else Seq.empty}
        <link rel="shortcut icon"
              href={prependBaseUri(request, "/static/spark-logo-77x50px-hd.png")}></link>
        <title>{appName} - {title}</title>
      </head>
      <body>
        <div class="navbar navbar-static-top">
          <div class="navbar-inner">
            <div class="brand">
              <a href={prependBaseUri(request, "/")} class="brand">
                <img src={prependBaseUri(request, "/static/spark-logo-77x50px-hd.png")} />
                <span class="version">{activeTab.appSparkVersion}</span>
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
  def basicSparkPage(
      request: HttpServletRequest,
      content: => Seq[Node],
      title: String,
      useDataTables: Boolean = false): Seq[Node] = {
    <html>
      <head>
        {commonHeaderNodes(request)}
        {if (useDataTables) dataTablesHeaderNodes(request) else Seq.empty}
        <link rel="shortcut icon"
              href={prependBaseUri(request, "/static/spark-logo-77x50px-hd.png")}></link>
        <title>{title}</title>
      </head>
      <body>
        <div class="container-fluid">
          <div class="row-fluid">
            <div class="span12">
              <h3 style="vertical-align: middle; display: inline-block;">
                <a style="text-decoration: none" href={prependBaseUri(request, "/")}>
                  <img src={prependBaseUri(request, "/static/spark-logo-77x50px-hd.png")} />
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
      reasonToNumKilled: Map[String, Int],
      total: Int): Seq[Node] = {
    val completeWidth = "width: %s%%".format((completed.toDouble/total)*100)
    // started + completed can be > total when there are speculative tasks
    val boundedStarted = math.min(started, total - completed)
    val startWidth = "width: %s%%".format((boundedStarted.toDouble/total)*100)

    <div class="progress">
      <span style="text-align:center; position:absolute; width:100%; left:0;">
        {completed}/{total}
        { if (failed == 0 && skipped == 0 && started > 0) s"($started running)" }
        { if (failed > 0) s"($failed failed)" }
        { if (skipped > 0) s"($skipped skipped)" }
        { reasonToNumKilled.toSeq.sortBy(-_._2).map {
            case (reason, count) => s"($count killed: $reason)"
          }
        }
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

  /**
   * Returns HTML rendering of a job or stage description. It will try to parse the string as HTML
   * and make sure that it only contains anchors with root-relative links. Otherwise,
   * the whole string will rendered as a simple escaped text.
   *
   * Note: In terms of security, only anchor tags with root relative links are supported. So any
   * attempts to embed links outside Spark UI, or other tags like {@code <script>} will cause in
   * the whole description to be treated as plain text.
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
    import scala.language.postfixOps

    // If the description can be parsed as HTML and has only relative links, then render
    // as HTML, otherwise render as escaped string
    try {
      // Try to load the description as unescaped HTML
      val xml = XML.loadString(s"""<span class="description-input">$desc</span>""")

      // Verify that this has only anchors and span (we are wrapping in span)
      val allowedNodeLabels = Set("a", "span", "br")
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

      val rule =
        if (plainText) {
          // Remove all tags, retaining only their texts
          new RewriteRule() {
            override def transform(n: Node): Seq[Node] = {
              n match {
                case e: Elem if e.child isEmpty => Text(e.text)
                case e: Elem if e.child nonEmpty => Text(e.child.flatMap(transform).text)
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
                case e: Elem if e \ "@href" nonEmpty =>
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
    var decodedParam = URLDecoder.decode(param, "UTF-8")
    while (param != decodedParam) {
      param = decodedParam
      decodedParam = URLDecoder.decode(param, "UTF-8")
    }
    param
  }

  def getTimeZoneOffset() : Int =
    TimeZone.getDefault().getOffset(System.currentTimeMillis()) / 1000 / 60

  /**
  * Return the correct Href after checking if master is running in the
  * reverse proxy mode or not.
  */
  def makeHref(proxy: Boolean, id: String, origHref: String): String = {
    if (proxy) {
      s"/proxy/$id"
    } else {
      origHref
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
    if (requestParameter == null) {
      null
    } else {
      // Remove new lines and single quotes, followed by escaping HTML version 4.0
      StringEscapeUtils.escapeHtml4(
        NEWLINE_AND_SINGLE_QUOTE_REGEX.replaceAllIn(requestParameter, ""))
    }
  }

  def buildErrorResponse(status: Response.Status, msg: String): Response = {
    Response.status(status).entity(msg).`type`(MediaType.TEXT_PLAIN).build()
  }
}
