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
package org.apache.spark.ui.jobs

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable
import scala.xml.Node

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.History.TASK_METRICS_AGGREGATION_PERIOD
import org.apache.spark.status.{ApplicationStatisticsData, AppStatusStore}
import org.apache.spark.ui.{GraphUIData, JsCollector, SparkUITab, UIUtils, WebUIPage}

private[ui] class JobStatisticsPage(
    parent: SparkUITab,
    store: AppStatusStore,
    conf: SparkConf) extends WebUIPage("statistics") {
  private val taskMetricsAggregationPeriod = conf.get(TASK_METRICS_AGGREGATION_PERIOD)

  def generateLoadResources(request: HttpServletRequest): Seq[Node] = {
    // scalastyle:off
    <script src={UIUtils.prependBaseUri(request, "/static/d3.min.js")}></script>
        <link rel="stylesheet" href={UIUtils.prependBaseUri(request, "/static/statisticspage.css")} type="text/css"/>
      <script src={UIUtils.prependBaseUri(request, "/static/streaming-page.js")}></script>
      <script src={UIUtils.prependBaseUri(request, "/static/statisticspage.js")}></script>
    // scalastyle:on
  }

  def generateBasicInfo(
    appStartTime: Long,
    appEndTime: Long,
    appStatisticsData: Array[ApplicationStatisticsData]): Seq[Node] = {
    // scalastyle:off
    <div>
      <strong>Total Uptime: </strong>{if (appEndTime < 0) {
      UIUtils.formatDuration(System.currentTimeMillis() - appStartTime)
    } else if (appEndTime > 0) {
      UIUtils.formatDuration(appEndTime - appStartTime)
    }} <strong> from </strong> {UIUtils.formatDate(appStartTime)}
    </div>
    <div>
      <strong>Completed Jobs: </strong>{appStatisticsData.foldLeft(0L) {(r, d) => r + d.jobCount}}
    </div>
      <div>
        <strong>Completed Stages: </strong>{appStatisticsData.foldLeft(0L) {(r, d) => r + d.stageCount}}
      </div>
    <div>
      <strong>Completed Tasks: </strong>{appStatisticsData.foldLeft(0L) {(r, d) => r + d.taskCount}}
    </div>
        <br/>
    // scalastyle:on
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val app = store.applicationInfo()
    val appStatisticsData = store.applicationStatistics()
    val basicInfoContent = generateLoadResources(request) ++
      generateBasicInfo(
        app.attempts.head.getStartTimeEpoch,
        app.attempts.head.getEndTimeEpoch,
        appStatisticsData)
    val tableContent = if (appStatisticsData.length == 0) {
      <div id="empty-information-message">
        <b>No visualization information available.</b>
      </div>
    } else {
      generateStatTable(
        app.attempts.head.getStartTimeEpoch,
        app.attempts.head.getEndTimeEpoch,
        appStatisticsData)
    }

    UIUtils.headerSparkPage(
      request,
      s"Statistics for application",
      basicInfoContent ++ tableContent,
      parent)
  }

  def generateStatTable(
    appStartTime: Long,
    appEndTime: Long,
    appStatisticsData: Array[ApplicationStatisticsData]): Seq[Node] = {
    val jsCollector = new JsCollector
    val minStatisticsTime = appStartTime
    val maxStatisticsTime = appEndTime
    val stageIdJSData = appStatisticsData
      .map {d => s"""{x:${d.endTimeStamp},id:'${d.associatedStageIds}'}"""}
      .mkString("[", ",", "]")
    val stageIdJSName = jsCollector.nextVariableName
    jsCollector.addPreparedStatement(s"var $stageIdJSName = $stageIdJSData;")
    val tempData = (new collection.mutable.ArrayBuffer()) ++ appStatisticsData
    val keySet = tempData.map(_.endTimeStamp).toSet
    for (i <- minStatisticsTime to maxStatisticsTime
      by taskMetricsAggregationPeriod) {
      val tempTimeStamp = UIUtils.timePointDiscretized(i, taskMetricsAggregationPeriod)
      if (!keySet.exists(_ == tempTimeStamp)) {
        tempData += new ApplicationStatisticsData(
          tempTimeStamp,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0.0,
          0.0,
        "")
      }
    }
    val data = tempData.sortBy(_.endTimeStamp).toSeq
    val clusterThroughPut = new mutable.HashMap[String, Seq[(Long, Double)]]()
    clusterThroughPut.update("Jobs", data.map{d => (d.endTimeStamp, d.jobCount.toDouble)})
    clusterThroughPut.update("Stages", data.map{d => (d.endTimeStamp, d.stageCount.toDouble)})
    clusterThroughPut.update("Tasks", data.map{d => (d.endTimeStamp, d.taskCount.toDouble)})
    val graphUIDataForThroughput =
      new GraphUIData(
        "cluster-through-put-multi-line",
        "",
        Seq.empty[(Long, Double)],
        0L,
        0L,
        0L,
        0L,
        "count")

    val taskDurations = new mutable.HashMap[String, Seq[(Long, Double)]]()
    taskDurations.update("P50", data.map{d => (d.endTimeStamp, d.durationP50)})
    taskDurations.update("P90", data.map{d => (d.endTimeStamp, d.durationP90)})
    val graphUIDataForTaskDuration =
      new GraphUIData(
        "task-duration-multi-line",
        "",
        Seq.empty[(Long, Double)],
        0L,
        0L,
        0L,
        0L,
        "ms")

    val ioData = new mutable.HashMap[String, Seq[(Long, Double)]]()
    ioData.update("ShuffleRead",
      data.map{d => (d.endTimeStamp, d.shuffleTotalReadBytesSum.toDouble)})
    ioData.update("ShuffleWrite",
      data.map{d => (d.endTimeStamp, d.shuffleWriteBytesSum.toDouble)})
    ioData.update("Read",
      data.map{d => (d.endTimeStamp, d.readBytesSum.toDouble)})
    val graphUIDataForIOData =
      new GraphUIData(
        "io-data-multi-line",
        "",
        Seq.empty[(Long, Double)],
        0L,
        0L,
        0L,
        0L,
        "bytes")

    val taskTimeData = new mutable.HashMap[String, Seq[(Long, Double)]]()
    taskTimeData.update("SchedulerDelay", data.map {d =>
      (d.endTimeStamp,
        if (d.durationSum > 0 && d.executorRunTimeSum > 0) {
          val temp = d.durationSum - d.executorRunTimeSum -
            d.executorDeserializeTimeSum - d.resultSerializationTimeSum - d.gettingResultTimeSum
          if (temp > 0) {
            100.0 * temp / d.durationSum
          } else 0.0

        } else 0.0)
    })
    taskTimeData.update("ComputingTime", data.map {d =>
      (d.endTimeStamp,
        if (d.durationSum > 0 && d.executorRunTimeSum > 0) {
          val temp = d.executorRunTimeSum - d.shuffleFetchWaitTimeSum - d.shuffleWriteTimeSum/1e6
          if (temp > 0) {
            100.0 * temp / d.durationSum
          } else 0.0
        } else 0.0)
    })
    taskTimeData.update("GetResultTime", data.map {d =>
      (d.endTimeStamp,
        if (d.durationSum > 0) {
          100.0 * d.gettingResultTimeSum / d.durationSum
        } else 0.0)
    })
    taskTimeData.update("TaskDeserialization", data.map {d =>
      (d.endTimeStamp,
        if (d.durationSum > 0) {
          100.0 * d.executorDeserializeTimeSum / d.durationSum
        } else 0.0)
    })
    taskTimeData.update("ResultSerialization", data.map {d =>
      (d.endTimeStamp,
        if (d.durationSum > 0) {
          100.0 * d.resultSerializationTimeSum / d.durationSum
        } else 0.0)
    })
    taskTimeData.update("ShuffleRead", data.map {d =>
      (d.endTimeStamp,
        if (d.durationSum > 0) {
          100.0 * d.shuffleFetchWaitTimeSum / d.durationSum
        } else 0.0)
    })
    taskTimeData.update("ShuffleWrite", data.map {d =>
      (d.endTimeStamp,
        if (d.durationSum > 0) {
          100.0 * d.shuffleWriteTimeSum / (d.durationSum  * 1e6)
        } else 0.0)
    })

    val graphUIDataForTaskTime =
      new GraphUIData(
        "task-time-multi-line",
        "",
        Seq.empty[(Long, Double)],
        0L,
        0L,
        0L,
        0L,
        "%")

    val taskGCData = new mutable.HashMap[String, Seq[(Long, Double)]]()

    taskGCData.update("GC%", data.map {d =>
      (d.endTimeStamp,
        if (d.executorRunTimeSum > 0) {
          100.0 * d.jvmGCTimeSum / d.durationSum
        } else 0.0)
    })

    val graphUIDataForTaskGC =
      new GraphUIData(
        "task-gc-multi-line",
        "",
        Seq.empty[(Long, Double)],
        0L,
        0L,
        0L,
        0L,
        "%")
    // scalastyle:off
    <table id="stat-table" class="table table-bordered" style="width: auto">
      <thead>
        <tr>
          <th style="width: 160px;"></th>
          <th style="width: 1000px;">Statistics</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div>
                <strong>Cluster Throughput
                  {UIUtils.tooltip("The sum of completed tasks, stages, and jobs per minute.", "right")}
                </strong>
              </div>
            </div>
          </td>
          <td class="timeline">
            {graphUIDataForThroughput.generateBrushMultiTimelineHtmlWithData(jsCollector, clusterThroughPut.toMap, stageIdJSName)}
          </td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div>
                <strong>IO (Shuffle and Read) Data
                  {UIUtils.tooltip("The sum of total shuffle read bytes, shuffle write bytes and read bytes per minute.", "right")}
                </strong>
              </div>
            </div>
          </td>
          <td class="timeline">
            {graphUIDataForIOData.generateBrushMultiTimelineHtmlWithData(jsCollector, ioData.toMap, stageIdJSName, true)}
          </td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div>
                <strong>Task Duration Time
                  {UIUtils.tooltip("The 50% and 90% percentile of task duration per minute.", "right")}
                </strong>
              </div>
            </div>
          </td>
          <td class="timeline">
            {graphUIDataForTaskDuration.generateBrushMultiTimelineHtmlWithData(jsCollector, taskDurations.toMap, stageIdJSName, true)}
          </td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div>
                <strong>Task Duration Time Component %
                  {UIUtils.tooltip("The percentage of scheduler delay, computing time, shuffle read, task deserialization, result serialization, shuffle write, get result time by task duration per minute. For task time details, please check stage page.", "right")}
                </strong>
              </div>
            </div>
          </td>
          <td class="timeline">
            {graphUIDataForTaskTime.generateBrushAreaStackHtmlWithData(jsCollector, taskTimeData.toMap, stageIdJSName, true)}
          </td>
        </tr>
        <tr>
          <td style="vertical-align: middle;">
            <div style="width: 160px;">
              <div>
                <strong>Task JVM GC Time %
                  {UIUtils.tooltip("The percentage of JVM GC time by task duration per minute.", "right")}
                </strong>
              </div>
            </div>
          </td>
          <td class="timeline">
            {graphUIDataForTaskGC.generateBrushMultiTimelineHtmlWithData(jsCollector, taskGCData.toMap, stageIdJSName)}
          </td>
        </tr>
      </tbody>
    </table> ++ jsCollector.toHtml
    // scalastyle:on
  }
}
