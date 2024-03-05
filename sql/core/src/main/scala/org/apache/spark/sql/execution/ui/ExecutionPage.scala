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
import org.apache.spark.ui.{UIUtils, WebUIPage}

class ExecutionPage(parent: SQLTab) extends WebUIPage("execution") with Logging {

  private val pandasOnSparkConfPrefix = "pandas_on_Spark."

  private val sqlStore = parent.sqlStore

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
            {jobLinks(JobExecutionStatus.RUNNING, "Running Jobs:")}
            {jobLinks(JobExecutionStatus.SUCCEEDED, "Succeeded Jobs:")}
            {jobLinks(JobExecutionStatus.FAILED, "Failed Jobs:")}
          </ul>
        </div>

      val metrics = sqlStore.executionMetrics(executionId)
      val graph = sqlStore.planGraph(executionId)
      val configs = Option(executionUIData.modifiedConfigs).getOrElse(Map.empty)

      summary ++
        planVisualization(request, metrics, graph) ++
        physicalPlanDescription(executionUIData.physicalPlanDescription) ++
        modifiedConfigs(configs.filter { case (k, _) => !k.startsWith(pandasOnSparkConfPrefix) }) ++
        modifiedPandasOnSparkConfigs(
          configs.filter { case (k, _) => k.startsWith(pandasOnSparkConfPrefix) })
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
        <span style="cursor: pointer;" onclick="togglePlanViz();">
          <span id="plan-viz-graph-arrow" class="arrow-open"></span>
          <a>Plan Visualization</a>
        </span>
      </div>

      <div id="plan-viz-graph">
        <div>
          <input type="checkbox" id="stageId-and-taskId-checkbox"></input>
          <span>Show the Stage ID and Task ID that corresponds to the max metric</span>
        </div>
      </div>
      <div id="plan-viz-metadata" style="display:none">
        <div class="dot-file">
          {graph.makeDotFile(metrics)}
        </div>
      </div>
      {planVisualizationResources(request)}
      <script>$(function() {{ if (shouldRenderPlanViz()) {{ renderPlanViz(); }} }})</script>
    </div>
  }

  private def jobURL(request: HttpServletRequest, jobId: Long): String =
    "%s/jobs/job/?id=%s".format(UIUtils.prependBaseUri(request, parent.basePath), jobId)

  private def physicalPlanDescription(physicalPlanDescription: String): Seq[Node] = {
    <div>
      <span style="cursor: pointer;" onclick="clickPhysicalPlanDetails();">
        <span id="physical-plan-details-arrow" class="arrow-closed"></span>
        <a>Details</a>
      </span>
    </div>
    <div id="physical-plan-details" style="display: none;">
      <pre>{physicalPlanDescription}</pre>
    </div>
    <script>
      function clickPhysicalPlanDetails() {{
        $('#physical-plan-details').toggle();
        $('#physical-plan-details-arrow').toggleClass('arrow-open').toggleClass('arrow-closed');
      }}
    </script>
    <br/>
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
      <span class="collapse-sql-properties collapse-table"
            onClick="collapseTable('collapse-sql-properties', 'sql-properties')">
        <span class="collapse-table-arrow arrow-closed"></span>
        <a>SQL / DataFrame Properties</a>
      </span>
      <div class="sql-properties collapsible-table collapsed">
        {configs}
      </div>
    </div>
    <br/>
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
      <span class="collapse-pandas-on-spark-properties collapse-table"
            onClick="collapseTable('collapse-pandas-on-spark-properties',
             'pandas-on-spark-properties')">
        <span class="collapse-table-arrow arrow-closed"></span>
        <a>Pandas API Properties</a>
      </span>
      <div class="pandas-on-spark-properties collapsible-table collapsed">
        {configs}
      </div>
    </div>
    <br/>
  }

  private def propertyHeader = Seq("Name", "Value")
  private def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
}
