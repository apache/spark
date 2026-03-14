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

import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest
import org.json4s.JNull
import org.json4s.JsonAST.{JBool, JString}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.UI.UI_SQL_GROUP_SUB_EXECUTION_ENABLED
import org.apache.spark.ui.{UIUtils, WebUIPage}

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

      def jobLinks(status: JobExecutionStatus, label: String): Seq[Node] = {
        val jobs = executionUIData.jobs.flatMap { case (jobId, jobStatus) =>
          if (jobStatus == status) Some(jobId) else None
        }
        if (jobs.nonEmpty) {
          <li class="job-url">
            <strong>{label} </strong>
            {jobs.toSeq.sorted.map { jobId =>
              <a href={jobURL(request, jobId.intValue())}>{jobId.toString}</a><span>&nbsp;</span>
            }}
          </li>
        } else {
          Nil
        }
      }


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
            {jobLinks(JobExecutionStatus.RUNNING, "Running Jobs:")}
            {jobLinks(JobExecutionStatus.SUCCEEDED, "Succeeded Jobs:")}
            {jobLinks(JobExecutionStatus.FAILED, "Failed Jobs:")}
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
          </div>
        </div>

      val metrics = sqlStore.executionMetrics(executionId)
      val graph = sqlStore.planGraph(executionId)
      val configs = Option(executionUIData.modifiedConfigs).getOrElse(Map.empty)

      summary ++
        planVisualization(request, metrics, graph) ++
        physicalPlanDescription(executionUIData.physicalPlanDescription) ++
        modifiedConfigs(configs.filter { case (k, _) => !k.startsWith(pandasOnSparkConfPrefix) }) ++
        modifiedPandasOnSparkConfigs(
          configs.filter { case (k, _) => k.startsWith(pandasOnSparkConfPrefix) }) ++
        <br/>
    }.getOrElse {
      <div>No information to display for query {executionId}</div>
    }

    UIUtils.headerSparkPage(
      request, s"Details for Query $executionId", content, parent)
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
            <div class="btn-group btn-group-sm mb-2" role="group">
              <input type="radio" class="btn-check" name="plan-diff-mode" id="plan-diff-unified"
                     data-bs-toggle="tab" data-bs-target="#plan-unified-tab" autocomplete="off" checked="checked"/>
              <label class="btn btn-outline-secondary" htmlFor="plan-diff-unified">Unified</label>
              <input type="radio" class="btn-check" name="plan-diff-mode" id="plan-diff-split"
                     data-bs-toggle="tab" data-bs-target="#plan-split-tab" autocomplete="off"/>
              <label class="btn btn-outline-secondary" htmlFor="plan-diff-split">Split</label>
            </div>
            <div class="tab-content">
              <div class="tab-pane fade show active" id="plan-unified-tab" role="tabpanel">
                {unifiedDiff(initialPlan, finalPlan)}
              </div>
              <div class="tab-pane fade" id="plan-split-tab" role="tabpanel">
                <div class="row">
                  <div class="col-6">
                    <h6 class="fw-bold text-muted">Initial Plan</h6>
                    <pre class="border rounded p-2 plan-diff-pre">{initialPlan}</pre>
                  </div>
                  <div class="col-6">
                    <h6 class="fw-bold text-muted">Final Plan</h6>
                    <pre class="border rounded p-2 plan-diff-pre">{finalPlan}</pre>
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

  /** Generate a unified diff view with colored +/- lines. */
  private def unifiedDiff(initial: String, final_ : String): Seq[Node] = {
    val initLines = initial.split("\n")
    val finalLines = final_.split("\n")

    // Simple LCS-based diff
    val lcs = longestCommonSubsequence(initLines, finalLines)
    val diffLines = new scala.collection.mutable.ArrayBuffer[(String, String)]() // (type, text)
    var i = 0
    var j = 0
    var k = 0
    while (k < lcs.length) {
      while (i < initLines.length && initLines(i) != lcs(k)) {
        diffLines += (("-", initLines(i)))
        i += 1
      }
      while (j < finalLines.length && finalLines(j) != lcs(k)) {
        diffLines += (("+", finalLines(j)))
        j += 1
      }
      diffLines += ((" ", lcs(k)))
      i += 1; j += 1; k += 1
    }
    while (i < initLines.length) { diffLines += (("-", initLines(i))); i += 1 }
    while (j < finalLines.length) { diffLines += (("+", finalLines(j))); j += 1 }

    val lines = diffLines.map { case (typ, text) =>
      val cssClass = typ match {
        case "-" => "diff-removed"
        case "+" => "diff-added"
        case _ => ""
      }
      val prefix = typ match { case " " => " "; case other => other }
      <div class={s"diff-line $cssClass"}><code>{s"$prefix $text"}</code></div>
    }

    // scalastyle:off line.size.limit
    <div class="border rounded p-2 plan-diff-pre" style="max-height: 600px; overflow: auto;">
      <style>{".diff-removed {{ background-color: rgba(var(--bs-danger-rgb), 0.15); }} .diff-added {{ background-color: rgba(var(--bs-success-rgb), 0.15); }} .diff-line code {{ font-size: 0.8rem; white-space: pre; }} .plan-diff-pre {{ font-size: 0.8rem; max-height: 600px; overflow: auto; }}"}</style>
      {lines}
    </div>
    // scalastyle:on line.size.limit
  }

  /** Compute LCS of two string arrays. */
  private def longestCommonSubsequence(a: Array[String], b: Array[String]): Array[String] = {
    val m = a.length
    val n = b.length
    val dp = Array.ofDim[Int](m + 1, n + 1)
    for (i <- 1 to m; j <- 1 to n) {
      dp(i)(j) = if (a(i - 1) == b(j - 1)) dp(i - 1)(j - 1) + 1
                 else math.max(dp(i - 1)(j), dp(i)(j - 1))
    }
    // Backtrack
    val result = new scala.collection.mutable.ArrayBuffer[String]()
    var (i, j) = (m, n)
    while (i > 0 && j > 0) {
      if (a(i - 1) == b(j - 1)) { result += a(i - 1); i -= 1; j -= 1 }
      else if (dp(i - 1)(j) > dp(i)(j - 1)) i -= 1
      else j -= 1
    }
    result.reverse.toArray
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
        // Node details section (e.g., "(1) Range [codegen id : 1]") — stop collecting
        section = ""
      } else if (section == "final") {
        finalLines :+= line
      } else if (section == "initial") {
        initialLines :+= line
      }
    }
    (initialLines.mkString("\n").trim, finalLines.mkString("\n").trim)
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
