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

package org.apache.spark.sql.execution.ui

import scala.xml.{Node, Unparsed, Utility}

import jakarta.servlet.http.HttpServletRequest
import org.apache.commons.text.StringEscapeUtils
import org.json4s.JNull
import org.json4s.JsonAST.{JBool, JString}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.UI.{UI_SQL_GROUP_SUB_EXECUTION_ENABLED, UI_TIMELINE_ENABLED, UI_TIMELINE_JOBS_MAXIMUM}
import org.apache.spark.ui.{CspNonce, UIUtils, WebUIPage}
import org.apache.spark.ui.jobs.ApiHelper

class ExecutionPage(parent: SQLTab) extends WebUIPage("execution") with Logging {

  private val pandasOnSparkConfPrefix = "pandas_on_Spark."

  private val sqlStore = parent.sqlStore
  private val groupSubExecutionEnabled = parent.conf.get(UI_SQL_GROUP_SUB_EXECUTION_ENABLED)


  override def render(request: HttpServletRequest): Seq[Node] = {
    val parameterExecutionId = request.getParameter("id")
    require(parameterExecutionId != null && parameterExecutionId.nonEmpty,
      "Missing execution id parameter")

    val executionId = parameterExecutionId.toLong
    val content = sqlStore.execution(executionId).map { executionUIData =>
      val currentTime = System.currentTimeMillis()
      val duration = executionUIData.completionTime.map(_.getTime()).getOrElse(currentTime) -
        executionUIData.submissionTime

      val summary =
        <div>
          <ul class="list-unstyled">
            <li>
              <strong>Submitted Time: </strong>{UIUtils.formatDate(executionUIData.submissionTime)}
            </li>
            <li>
              <strong>Duration: </strong>{UIUtils.formatDuration(duration)}
            </li>
            {
              Option(executionUIData.queryId).map { qId =>
                <li>
                  <strong>Query ID: </strong>{qId}
                </li>
              }.getOrElse(Seq.empty)
            }
            {
              if (executionUIData.rootExecutionId != executionId) {
                <li>
                  <strong>Parent Execution: </strong>
                  <a href={"?id=" + executionUIData.rootExecutionId}>
                    {executionUIData.rootExecutionId}
                  </a>
                </li>
              }
            }
            {
              if (groupSubExecutionEnabled) {
                val subExecutions = sqlStore.executionsList()
                  .filter(e => e.rootExecutionId == executionId && e.executionId != executionId)
                if (subExecutions.nonEmpty) {
                  <li>
                    <strong>Sub Executions: </strong>
                    {
                      subExecutions.map { e =>
                        <a href={"?id=" + e.executionId}>{e.executionId}</a><span>&nbsp;</span>
                      }
                    }
                  </li>
                }
              }
            }
          </ul>
          <div id="plan-viz-download-btn-container">
            <select id="plan-viz-format-select">
              <option value="svg">SVG</option>
              <option value="dot">DOT</option>
              <option value="txt">TXT</option>
            </select>
            <label for="plan-viz-format-select">
              <a id="plan-viz-download-btn" class="downloadbutton">Download</a>
            </label>
            <button id="copy-plan-btn" class="btn btn-sm btn-outline-secondary ms-2"
                    type="button" title="Copy physical plan to clipboard">
              &#x1f4cb; Copy Plan</button>
            <button id="copy-link-btn" class="btn btn-sm btn-outline-secondary ms-1"
                    type="button" title="Copy shareable link to this execution">
              &#x1f517; Copy Link</button>
          </div>
        </div>

      val metrics = sqlStore.executionMetrics(executionId)
      val graph = sqlStore.planGraph(executionId)
      val configs = Option(executionUIData.modifiedConfigs).getOrElse(Map.empty)

      summary ++
        planVisualization(request, metrics, graph) ++
        physicalPlanDescription(executionUIData.physicalPlanDescription) ++
        jobsTable(request, executionUIData) ++
        modifiedConfigs(configs.filter { case (k, _) => !k.startsWith(pandasOnSparkConfPrefix) }) ++
        modifiedPandasOnSparkConfigs(
          configs.filter { case (k, _) => k.startsWith(pandasOnSparkConfPrefix) }) ++
        <br/>
    }.getOrElse {
      <div>No information to display for query {executionId}</div>
    }

    UIUtils.headerSparkPage(
      request, s"Details for Query $executionId", content, parent, useTimeline = true)
  }


  private def planVisualizationResources(request: HttpServletRequest): Seq[Node] = {
    // scalastyle:off
    <link rel="stylesheet" href={UIUtils.prependBaseUri(request, "/static/sql/spark-sql-viz.css")} type="text/css"/>
    <script src={UIUtils.prependBaseUri(request, "/static/d3.min.js")}></script>
    <script src={UIUtils.prependBaseUri(request, "/static/dagre-d3.min.js")}></script>
    <script src={UIUtils.prependBaseUri(request, "/static/graphlib-dot.min.js")}></script>
    <script src={UIUtils.prependBaseUri(request, "/static/sql/spark-sql-viz.js")}></script>
    // scalastyle:on
  }

  private def planVisualization(
      request: HttpServletRequest,
      metrics: Map[Long, String],
      graph: SparkPlanGraph): Seq[Node] = {

    <div>
      <div>
        <span data-action="togglePlanViz">
          <h4>
            <span id="plan-viz-graph-arrow" class="arrow-open"></span>
            <a>Plan Visualization</a>
          </h4>
        </span>
      </div>

      <div id="plan-viz-content" class="row">
        <div id="plan-viz-graph-col" class="col-12">
          <div id="plan-viz-graph">
            <div>
              <input type="checkbox" id="stageId-and-taskId-checkbox"></input>
              <span>Show the Stage ID and Task ID that corresponds to the max metric</span>
            </div>
            <div>
              <input type="checkbox" id="detailed-labels-checkbox"></input>
              <span>Show metrics in graph nodes (detailed mode)</span>
            </div>
          </div>
        </div>
        <div id="plan-viz-details-col" class="col-4 d-none">
          <div id="plan-viz-details-panel" class="sticky-top" style="top: 4rem; z-index: 1;">
            <div class="card">
              <div class="card-header d-flex justify-content-between align-items-center">
                <span class="fw-bold" id="plan-viz-details-title">Details</span>
                <button id="plan-viz-panel-close" class="btn btn-sm btn-close"
                        type="button" title="Close panel"></button>
              </div>
              <div class="card-body" id="plan-viz-details-body">
                <p class="text-muted mb-0">Click a node to view details</p>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div id="plan-viz-metadata" style="display:none">
        <div class="dot-file">
          {graph.makeDotFile(metrics)}
        </div>
        <div class="node-details">
          {graph.makeNodeDetailsJson(metrics)}
        </div>
      </div>
      {planVisualizationResources(request)}
    </div>
  }

  private def jobURL(request: HttpServletRequest, jobId: Long): String =
    "%s/jobs/job/?id=%s".format(UIUtils.prependBaseUri(request, parent.basePath), jobId)

  /** Render a vis-timeline showing job start/end times for this SQL execution. */
  private def jobTimeline(executionUIData: SQLExecutionUIData): Seq[Node] = {
    if (!parent.conf.get(UI_TIMELINE_ENABLED)) return Nil

    val jobIds = executionUIData.jobs.keys.toSeq.sorted
    if (jobIds.isEmpty) return Nil

    val store = parent.parent.store
    val now = System.currentTimeMillis()
    val startTime = executionUIData.submissionTime
    val maxJobs = parent.conf.get(UI_TIMELINE_JOBS_MAXIMUM)

    val jobEvents = jobIds.takeRight(maxJobs).flatMap { jobId =>
      try {
        val job = store.job(jobId)
        if (job.submissionTime.isEmpty) None
        else {
          val (_, lastStageDesc) = ApiHelper.lastStageNameAndDescription(store, job)
          val desc = job.description.getOrElse(lastStageDesc)
          val escapedDesc = Utility.escape(desc)
          val jsDesc = StringEscapeUtils.escapeEcmaScript(Utility.escape(escapedDesc))
          val jsLabel = StringEscapeUtils.escapeEcmaScript(escapedDesc)
          val subTime = job.submissionTime.get.getTime
          val endTime = job.completionTime.map(_.getTime).getOrElse(now)
          val cssClass = job.status match {
            case JobExecutionStatus.SUCCEEDED => "succeeded"
            case JobExecutionStatus.FAILED => "failed"
            case JobExecutionStatus.RUNNING => "running"
            case _ => "unknown"
          }
          Some(
            s"""{
               |  'className': 'job application-timeline-object $cssClass',
               |  'group': 'jobs',
               |  'start': new Date($subTime),
               |  'end': new Date($endTime),
               |  'content': '<div class="application-timeline-content"' +
               |    'data-bs-html="true" data-bs-toggle="tooltip"' +
               |    'data-bs-title="$jsDesc (Job $jobId)<br>' +
               |    'Status: ${job.status}<br>' +
               |    'Duration: ${UIUtils.formatDuration(endTime - subTime)}">' +
               |    '$jsLabel (Job $jobId)</div>'
               |}""".stripMargin)
        }
      } catch {
        case _: NoSuchElementException => None
      }
    }

    if (jobEvents.isEmpty) return Nil

    val groupJson = """[{'id': 'jobs', 'content': '<div>Jobs</div>'}]"""
    val eventArrayJson = jobEvents.mkString("[", ",", "]")

    // scalastyle:off line.size.limit
    <div>
      <span class="expand-application-timeline">
        <span class="expand-application-timeline-arrow arrow-closed"></span>
        <a>Job Timeline</a>
      </span>
      <div id="application-timeline" class="collapsed">
        <div class="control-panel">
          <div id="application-timeline-zoom-lock">
            <input type="checkbox"></input>
            <span>Enable zooming</span>
          </div>
        </div>
      </div>
      <script type="text/javascript" nonce={CspNonce.get}>
        {Unparsed(s"drawApplicationTimeline($groupJson, $eventArrayJson, $startTime, ${UIUtils.getTimeZoneOffset()});")}
      </script>
    </div>
    // scalastyle:on line.size.limit
  }

  private def physicalPlanDescription(physicalPlanDescription: String): Seq[Node] = {
    val (initialPlan, finalPlan) = extractInitialAndFinalPlans(physicalPlanDescription)
    val hasDiff = initialPlan.nonEmpty && finalPlan.nonEmpty

    // scalastyle:off line.size.limit
    <div>
      <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#physical-plan-details"
            aria-expanded="false" aria-controls="physical-plan-details"
            data-collapse-name="collapse-plan-details">
        <h4>
          <span class="collapse-table-arrow arrow-closed"></span>
          <a>Plan Details</a>
        </h4>
      </span>
      <div class="collapsible-table collapse" id="physical-plan-details">
        {if (hasDiff) {
          <div>
            <ul class="nav nav-pills nav-pills-sm mb-2" role="tablist">
              <li class="nav-item" role="presentation">
                <button class="nav-link active btn-sm" data-bs-toggle="pill"
                        data-bs-target="#plan-unified-tab" type="button" role="tab">Unified</button>
              </li>
              <li class="nav-item" role="presentation">
                <button class="nav-link btn-sm" data-bs-toggle="pill"
                        data-bs-target="#plan-split-tab" type="button" role="tab">Split</button>
              </li>
            </ul>
            <div class="tab-content">
              <div class="tab-pane fade show active" id="plan-unified-tab" role="tabpanel">
                <pre>{physicalPlanDescription}</pre>
              </div>
              <div class="tab-pane fade" id="plan-split-tab" role="tabpanel">
                <div class="row">
                  <div class="col-6">
                    <h6 class="fw-bold text-muted">Initial Plan</h6>
                    <pre class="border rounded p-2" style="font-size: 0.8rem; max-height: 600px; overflow: auto;">{initialPlan}</pre>
                  </div>
                  <div class="col-6">
                    <h6 class="fw-bold text-muted">Final Plan</h6>
                    <pre class="border rounded p-2" style="font-size: 0.8rem; max-height: 600px; overflow: auto;">{finalPlan}</pre>
                  </div>
                </div>
              </div>
            </div>
          </div>
        } else {
          <pre>{physicalPlanDescription}</pre>
        }}
      </div>
    </div>
    // scalastyle:on line.size.limit
  }

  /**
   * Extract Initial Plan and Final Plan tree sections from the physicalPlanDescription.
   * Returns (initialPlan, finalPlan). If the plan doesn't contain AQE sections, returns
   * empty strings.
   */
  private def extractInitialAndFinalPlans(
      description: String): (String, String) = {
    val lines = description.split("\n")
    var initialLines = Seq.empty[String]
    var finalLines = Seq.empty[String]
    var section = "" // "", "final", "initial"

    for (line <- lines) {
      val trimmed = line.trim
      if (trimmed.contains("== Final Plan ==")) {
        section = "final"
      } else if (trimmed.contains("== Initial Plan ==")) {
        section = "initial"
      } else if (section.nonEmpty && trimmed.startsWith("(") && trimmed.contains(")") &&
          !trimmed.startsWith("(+") && !trimmed.startsWith("(-")) {
        section = ""
      } else if (section == "final") {
        finalLines :+= line
      } else if (section == "initial") {
        initialLines :+= line
      }
    }
    (initialLines.mkString("\n").trim, finalLines.mkString("\n").trim)
  }

  private def jobsTable(
      request: HttpServletRequest,
      executionUIData: SQLExecutionUIData): Seq[Node] = {
    val jobIds = executionUIData.jobs.keys.toSeq.sorted.reverse
    if (jobIds.isEmpty) return Nil

    val store = parent.parent.store
    val basePath = UIUtils.prependBaseUri(request, parent.basePath)
    val rows = jobIds.flatMap { jobId =>
      try {
        val job = store.job(jobId)
        val submissionTimeMs = job.submissionTime.map(_.getTime).getOrElse(-1L)
        val formattedTime = job.submissionTime.map(UIUtils.formatDate).getOrElse("")
        val durationMs = (job.submissionTime, job.completionTime) match {
          case (Some(start), Some(end)) => end.getTime - start.getTime
          case (Some(start), None) => System.currentTimeMillis() - start.getTime
          case _ => -1L
        }
        val duration = if (durationMs >= 0) UIUtils.formatDuration(durationMs) else ""
        val (lastStageName, lastStageDesc) =
          ApiHelper.lastStageNameAndDescription(store, job)
        val jobDesc = UIUtils.makeDescription(
          job.description.getOrElse(lastStageDesc), basePath, plainText = false)
        val detailUrl = s"$basePath/jobs/job/?id=$jobId"
        val stagesInfo = {
          val completed = job.numCompletedStages
          val total = job.stageIds.size - job.numSkippedStages
          val extra = Seq(
            if (job.numFailedStages > 0) s"(${job.numFailedStages} failed)" else "",
            if (job.numSkippedStages > 0) s"(${job.numSkippedStages} skipped)" else ""
          ).filter(_.nonEmpty).mkString(" ")
          s"$completed/$total $extra"
        }
        Some(
          <tr id={"job-" + jobId}>
            <td><a href={jobURL(request, jobId)}>{jobId}</a></td>
            <td>
              {jobDesc}
              <a href={detailUrl} class="name-link">{lastStageName}</a>
            </td>
            <td sorttable_customkey={submissionTimeMs.toString}>{formattedTime}</td>
            <td sorttable_customkey={durationMs.toString}>{duration}</td>
            <td class="stage-progress-cell">{stagesInfo}</td>
            <td class="progress-cell">
              {UIUtils.makeProgressBar(started = job.numActiveTasks,
              completed = job.numCompletedIndices,
              failed = job.numFailedTasks, skipped = job.numSkippedTasks,
              reasonToNumKilled = job.killedTasksSummary,
              total = job.numTasks - job.numSkippedTasks)}
            </td>
          </tr>)
      } catch {
        case _: NoSuchElementException => None
      }
    }

    // scalastyle:off
    <div>
      <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#sql-jobs-table"
            aria-expanded="true" aria-controls="sql-jobs-table"
            data-collapse-name="collapse-sql-jobs">
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>Associated Jobs ({jobIds.size})</a>
        </h4>
      </span>
      <div class="collapsible-table collapse show" id="sql-jobs-table">
        {jobTimeline(executionUIData)}
        <div class="table-responsive">
        <table class="table table-bordered table-hover table-sm sortable">
          <thead>
            <tr>
              <th>Job ID</th>
              <th>Description</th>
              <th>Submitted</th>
              <th>Duration</th>
              <th class="sorttable_nosort">Stages: Succeeded/Total</th>
              <th class="sorttable_nosort">Tasks (for all stages): Succeeded/Total</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
        </div>
      </div>
    </div>
    // scalastyle:on
  }

  private def modifiedConfigs(modifiedConfigs: Map[String, String]): Seq[Node] = {
    if (Option(modifiedConfigs).exists(_.isEmpty)) return Nil

    val configs = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      Option(modifiedConfigs).getOrElse(Map.empty).toSeq.sorted,
      fixedWidth = true
    )

    <div>
      <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#sql-properties"
            aria-expanded="false" aria-controls="sql-properties"
            data-collapse-name="collapse-sql-properties">
        <h4>
          <span class="collapse-table-arrow arrow-closed"></span>
          <a>SQL / DataFrame Properties</a>
        </h4>
      </span>
      <div class="collapsible-table collapse" id="sql-properties">
        {configs}
      </div>
    </div>
  }

  private def modifiedPandasOnSparkConfigs(
      modifiedPandasOnSparkConfigs: Map[String, String]): Seq[Node] = {
    if (Option(modifiedPandasOnSparkConfigs).exists(_.isEmpty)) return Nil

    val modifiedOptions = modifiedPandasOnSparkConfigs.toSeq.map { case (k, v) =>
      // Remove prefix.
      val key = k.slice(pandasOnSparkConfPrefix.length, k.length)
      // The codes below is a simple version of Python's repr().
      // Pandas API on Spark does not support other types in the options yet.
      val pyValue = parse(v) match {
        case JNull => "None"
        case JBool(v) => v.toString.capitalize
        case JString(s) => s"'$s'"
        case _ => v
      }
      (key, pyValue)
    }

    val configs = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      modifiedOptions.sorted,
      fixedWidth = true
    )

    <div>
      <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#pandas-on-spark-properties"
            aria-expanded="false" aria-controls="pandas-on-spark-properties"
            data-collapse-name="collapse-pandas-on-spark-properties">
        <h4>
          <span class="collapse-table-arrow arrow-closed"></span>
          <a>Pandas API Properties</a>
        </h4>
      </span>
      <div class="collapsible-table collapse" id="pandas-on-spark-properties">
        {configs}
      </div>
    </div>
  }

  private def propertyHeader = Seq("Name", "Value")
  private def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
}
