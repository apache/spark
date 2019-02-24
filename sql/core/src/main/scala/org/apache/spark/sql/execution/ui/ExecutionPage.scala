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

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage}

class ExecutionPage(parent: SQLTab) extends WebUIPage("execution") with Logging {

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
          <li>
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
          <ul class="unstyled">
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

      summary ++
        planVisualization(request, metrics, graph) ++
        physicalPlanDescription(executionUIData.physicalPlanDescription)
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
    val metadata = graph.allNodes.flatMap { node =>
      val nodeId = s"plan-meta-data-${node.id}"
      <div id={nodeId}>{node.desc}</div>
    }

    <div>
      <div id="plan-viz-graph"></div>
      <div id="plan-viz-metadata" style="display:none">
        <div class="dot-file">
          {graph.makeDotFile(metrics)}
        </div>
        <div id="plan-viz-metadata-size">{graph.allNodes.size.toString}</div>
        {metadata}
      </div>
      {planVisualizationResources(request)}
      <script>$(function() {{ renderPlanViz(); }})</script>
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
}
