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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.xml.{Node, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.ui.UIUtils.formatImportJavaScript

/**
 * A helper class to generate JavaScript and HTML for both timeline and histogram graphs.
 *
 * @param timelineDivId the timeline `id` used in the html `div` tag
 * @param histogramDivId the timeline `id` used in the html `div` tag
 * @param data the data for the graph
 * @param minX the min value of X axis
 * @param maxX the max value of X axis
 * @param minY the min value of Y axis
 * @param maxY the max value of Y axis
 * @param unitY the unit of Y axis
 * @param batchInterval if `batchInterval` is not None, we will draw a line for `batchInterval` in
 *                      the graph
 */
private[spark] class GraphUIData(
    timelineDivId: String,
    histogramDivId: String,
    data: Seq[(Long, Double)],
    minX: Long,
    maxX: Long,
    minY: Double,
    maxY: Double,
    unitY: String,
    batchInterval: Option[Double] = None) {

  private var dataJavaScriptName: String = _

  def generateDataJs(jsCollector: JsCollector): Unit = {
    val jsForData = data.map { case (x, y) =>
      s"""{"x": $x, "y": $y}"""
    }.mkString("[", ",", "]")
    dataJavaScriptName = jsCollector.nextVariableName
    jsCollector.addPreparedStatement(s"var $dataJavaScriptName = $jsForData;")
  }

  def generateTimelineHtml(jsCollector: JsCollector): Seq[Node] = {
    jsCollector.addImports("/static/streaming-page.js", "registerTimeline")
    jsCollector.addPreparedStatement(s"registerTimeline($minY, $maxY);")
    jsCollector.addImports("/static/streaming-page.js", "drawTimeline")
    if (batchInterval.isDefined) {
      jsCollector.addStatement(
        "drawTimeline(" +
          s"'#$timelineDivId', $dataJavaScriptName, $minX, $maxX, $minY, $maxY, '$unitY'," +
          s" ${batchInterval.get}" +
          ");")
    } else {
      jsCollector.addStatement(
        s"drawTimeline('#$timelineDivId', $dataJavaScriptName, $minX, $maxX, $minY, $maxY," +
          s" '$unitY');")
    }
    <div id={timelineDivId}></div>
  }

  def generateHistogramHtml(jsCollector: JsCollector): Seq[Node] = {
    val histogramData = s"$dataJavaScriptName.map(function(d) { return d.y; })"
    jsCollector.addImports("/static/streaming-page.js", "registerHistogram")
    jsCollector.addPreparedStatement(s"registerHistogram($histogramData, $minY, $maxY);")
    jsCollector.addImports("/static/streaming-page.js", "drawHistogram")
    if (batchInterval.isDefined) {
      jsCollector.addStatement(
        "drawHistogram(" +
          s"'#$histogramDivId', $histogramData, $minY, $maxY, '$unitY', ${batchInterval.get}" +
          ");")
    } else {
      jsCollector.addStatement(
        s"drawHistogram('#$histogramDivId', $histogramData, $minY, $maxY, '$unitY');")
    }
    <div id={histogramDivId}></div>
  }

  def generateAreaStackHtmlWithData(
      jsCollector: JsCollector,
      values: Array[(Long, ju.Map[String, JLong])]): Seq[Node] = {
    val operationLabels = values.flatMap(_._2.keySet().asScala).toSet
    val durationDataPadding = UIUtils.durationDataPadding(values)
    val jsForData = durationDataPadding.map { case (x, y) =>
      val s = y.toSeq.sortBy(_._1).map(e => s""""${e._1}": "${e._2}"""").mkString(",")
      s"""{x: "${UIUtils.formatBatchTime(x, 1, showYYYYMMSS = false)}", $s}"""
    }.mkString("[", ",", "]")
    val jsForLabels = operationLabels.toSeq.sorted.mkString("[\"", "\",\"", "\"]")

    dataJavaScriptName = jsCollector.nextVariableName
    jsCollector.addPreparedStatement(s"var $dataJavaScriptName = $jsForData;")
    val labels = jsCollector.nextVariableName
    jsCollector.addPreparedStatement(s"var $labels = $jsForLabels;")
    jsCollector.addImports("/static/structured-streaming-page.js", "drawAreaStack")
    jsCollector.addStatement(
      s"drawAreaStack('#$timelineDivId', $labels, $dataJavaScriptName)")
    <div id={timelineDivId}></div>
  }
}

/**
 * A helper class that allows the user to add JavaScript statements which will be executed when the
 * DOM has finished loading.
 */
private[spark] class JsCollector(req: HttpServletRequest) {

  private var variableId = 0

  /**
   * Return the next unused JavaScript variable name
   */
  def nextVariableName: String = {
    variableId += 1
    "v" + variableId
  }

  /**
   * JavaScript statements that will execute before `statements`
   */
  private val preparedStatements = ArrayBuffer[String]()

  /**
   * JavaScript statements that will execute after `preparedStatements`
   */
  private val statements = ArrayBuffer[String]()

  private val imports = mutable.Set[String]()

  def addPreparedStatement(js: String): Unit = {
    preparedStatements += js
  }

  def addStatement(js: String): Unit = {
    statements += js
  }

  def addImports(sourceFile: String, functions: String*): Unit = {
    imports.add(formatImportJavaScript(req, sourceFile, functions: _*))
  }
  def addImports(js: String): Unit = {
    imports.add(js)
  }

  /**
   * Generate a html snippet that will execute all scripts when the DOM has finished loading.
   */
  def toHtml: Seq[Node] = {
    val js =
      s"""
         |${imports.mkString("\n")}
         |
         |$$(document).ready(function() {
         |    ${preparedStatements.mkString("\n")}
         |    ${statements.mkString("\n")}
         |});""".stripMargin

    <script type="module">{Unparsed(js)}</script>
  }
}
