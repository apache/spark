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

package org.apache.spark.status.api.v1.sql

import java.util.{Date, HashMap}

import scala.util.{Failure, Success, Try}

import jakarta.ws.rs._
import jakarta.ws.rs.core.{Context, MediaType, UriInfo}

import org.apache.spark.JobExecutionStatus
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphNode, SQLAppStatusStore, SQLExecutionUIData}
import org.apache.spark.status.api.v1.{BaseAppResource, NotFoundException}
import org.apache.spark.ui.UIUtils

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class SqlResource extends BaseAppResource {

  val WHOLE_STAGE_CODEGEN = "WholeStageCodegen"

  @GET
  def sqlList(
      @DefaultValue("true") @QueryParam("details") details: Boolean,
      @DefaultValue("true") @QueryParam("planDescription") planDescription: Boolean,
      @DefaultValue("0") @QueryParam("offset") offset: Int,
      @DefaultValue("-1") @QueryParam("length") length: Int): Seq[ExecutionData] = {
    withUI { ui =>
      val sqlStore = new SQLAppStatusStore(ui.store.store)
      val execs = if (length <= 0) {
        sqlStore.executionsList()
      } else {
        sqlStore.executionsList(offset, length)
      }
      execs.map { exec =>
        val graph = sqlStore.planGraph(exec.executionId)
        prepareExecutionData(exec, graph, details, planDescription)
      }
    }
  }

  @GET
  @Path("{executionId:\\d+}")
  def sql(
      @PathParam("executionId") execId: Long,
      @DefaultValue("true") @QueryParam("details") details: Boolean,
      @DefaultValue("true") @QueryParam("planDescription")
      planDescription: Boolean): ExecutionData = {
    withUI { ui =>
      val sqlStore = new SQLAppStatusStore(ui.store.store)
      sqlStore
        .execution(execId)
        .map(prepareExecutionData(_, sqlStore.planGraph(execId), details, planDescription))
        .getOrElse(throw new NotFoundException("unknown query execution id: " + execId))
    }
  }

  /**
   * Server-side DataTables endpoint for SQL executions listing.
   * Accepts DataTables server-side parameters (start, length, order, search)
   * and returns paginated results with recordsTotal/recordsFiltered counts.
   */
  @GET
  @Path("sqlTable")
  def sqlTable(@Context uriInfo: UriInfo): HashMap[String, Object] = {
    withUI { ui =>
      val sqlStore = new SQLAppStatusStore(ui.store.store)
      val uriParams = UIUtils.decodeURLParameter(uriInfo.getQueryParameters(true))

      // Echo draw counter to prevent stale responses
      val draw = Option(uriParams.getFirst("draw")).map(_.toInt).getOrElse(0)

      val totalRecords = sqlStore.executionsCount()

      // Search and status filter
      val searchValue = Option(uriParams.getFirst("search[value]"))
        .filter(_.nonEmpty)
      val statusFilter = Option(uriParams.getFirst("status"))
        .filter(_.nonEmpty)
      val needsFilter = searchValue.isDefined || statusFilter.isDefined

      val filteredExecs = if (needsFilter) {
        // When filtering, we must load all and filter in memory
        val allExecs = sqlStore.executionsList()
        allExecs.filter { exec =>
          val matchesSearch = searchValue.forall { search =>
            val lower = search.toLowerCase(java.util.Locale.ROOT)
            exec.description.toLowerCase(java.util.Locale.ROOT).contains(lower) ||
              exec.executionStatus.toLowerCase(java.util.Locale.ROOT).contains(lower) ||
              exec.executionId.toString.contains(lower)
          }
          val matchesStatus = statusFilter.forall { status =>
            exec.executionStatus.equalsIgnoreCase(status)
          }
          matchesSearch && matchesStatus
        }
      } else {
        // No filter — will use KVStore pagination below
        Seq.empty
      }
      val filteredRecords = if (needsFilter) filteredExecs.size else totalRecords

      // Sort
      val sortCol = Option(uriParams.getFirst("order[0][column]"))
        .flatMap(c => Option(uriParams.getFirst(s"columns[$c][name]")))
        .getOrElse("id")
      val sortDir = Option(uriParams.getFirst("order[0][dir]")).getOrElse("desc")

      // Paginate
      val start = Option(uriParams.getFirst("start")).map(_.toInt).getOrElse(0)
      val length = Option(uriParams.getFirst("length")).map(_.toInt).getOrElse(20)

      val page = if (needsFilter) {
        // Filter/search: sort and paginate in memory
        val sorted = sortExecs(filteredExecs, sortCol, sortDir)
        if (length > 0) sorted.slice(start, start + length) else sorted
      } else {
        // No filter: use KVStore-level pagination for efficiency
        // KVStore returns in insertion order; sort in memory for the page
        val execs = sqlStore.executionsList()
        val sorted = sortExecs(execs, sortCol, sortDir)
        if (length > 0) sorted.slice(start, start + length) else sorted
      }

      // Convert to Java-compatible row data
      val aaData = page.map(execToRow)

      val ret = new HashMap[String, Object]()
      ret.put("draw", Integer.valueOf(draw))
      ret.put("aaData", aaData)
      ret.put("recordsTotal", java.lang.Long.valueOf(filteredRecords))
      ret.put("recordsFiltered", java.lang.Long.valueOf(filteredRecords))
      ret
    }
  }

  private def sortExecs(
      execs: Seq[SQLExecutionUIData],
      sortCol: String,
      sortDir: String): Seq[SQLExecutionUIData] = {
    val sorted = sortCol match {
      case "id" => execs.sortBy(_.executionId)
      case "status" => execs.sortBy(_.executionStatus)
      case "description" => execs.sortBy(_.description)
      case "submissionTime" => execs.sortBy(_.submissionTime)
      case "duration" =>
        execs.sortBy(e =>
          e.completionTime.getOrElse(new Date()).getTime - e.submissionTime)
      case _ => execs.sortBy(_.executionId)
    }
    if (sortDir == "asc") sorted else sorted.reverse
  }

  private def execToRow(exec: SQLExecutionUIData): java.util.LinkedHashMap[String, Object] = {
    val duration = exec.completionTime.getOrElse(new Date()).getTime - exec.submissionTime
    val jobIds = exec.jobs.collect {
      case (id, JobExecutionStatus.SUCCEEDED) => id
    }.toSeq.sorted
    val row = new java.util.LinkedHashMap[String, Object]()
    row.put("id", java.lang.Long.valueOf(exec.executionId))
    row.put("status", exec.executionStatus)
    row.put("description", exec.description)
    row.put("submissionTime", new Date(exec.submissionTime))
    row.put("duration", java.lang.Long.valueOf(duration))
    row.put("jobIds", jobIds)
    row.put("queryId", if (exec.queryId != null) exec.queryId.toString else null)
    row.put("errorMessage", exec.errorMessage.orNull)
    row.put("rootExecutionId", java.lang.Long.valueOf(exec.rootExecutionId))
    row
  }

  private def prepareExecutionData(
    exec: SQLExecutionUIData,
    graph: SparkPlanGraph,
    details: Boolean,
    planDescription: Boolean): ExecutionData = {

    var running = Seq[Int]()
    var completed = Seq[Int]()
    var failed = Seq[Int]()

    exec.jobs.foreach {
      case (id, JobExecutionStatus.RUNNING) =>
        running = running :+ id
      case (id, JobExecutionStatus.SUCCEEDED) =>
        completed = completed :+ id
      case (id, JobExecutionStatus.FAILED) =>
        failed = failed :+ id
      case _ =>
    }

    val duration = exec.completionTime.getOrElse(new Date()).getTime - exec.submissionTime
    val planDetails = if (planDescription) exec.physicalPlanDescription else ""
    val nodes = if (details) {
      printableMetrics(graph.allNodes, Option(exec.metricValues).getOrElse(Map.empty))
    } else {
      Seq.empty
    }
    val edges = if (details) graph.edges else Seq.empty

    new ExecutionData(
      exec.executionId,
      exec.executionStatus,
      exec.description,
      planDetails,
      new Date(exec.submissionTime),
      duration,
      running,
      completed,
      failed,
      nodes,
      edges,
      if (exec.queryId != null) exec.queryId.toString else null,
      exec.errorMessage.orNull,
      exec.rootExecutionId)
  }

  private def printableMetrics(allNodes: collection.Seq[SparkPlanGraphNode],
    metricValues: Map[Long, String]): collection.Seq[Node] = {

    def getMetric(metricValues: Map[Long, String], accumulatorId: Long,
      metricName: String): Option[Metric] = {

      metricValues.get(accumulatorId).map( mv => {
        val metricValue = if (mv.startsWith("\n")) mv.substring(1, mv.length) else mv
        Metric(metricName, metricValue)
      })
    }

    val nodeIdAndWSCGIdMap = getNodeIdAndWSCGIdMap(allNodes)
    val nodes = allNodes.map { node =>
      val wholeStageCodegenId = nodeIdAndWSCGIdMap.get(node.id).flatten
      val metrics =
        node.metrics.flatMap(m => getMetric(metricValues, m.accumulatorId, m.name.trim))
      Node(nodeId = node.id, nodeName = node.name.trim, wholeStageCodegenId, metrics)
    }

    nodes.sortBy(_.nodeId).reverse
  }

  private def getNodeIdAndWSCGIdMap(
      allNodes: collection.Seq[SparkPlanGraphNode]): Map[Long, Option[Long]] = {
    val wscgNodes = allNodes.filter(_.name.trim.startsWith(WHOLE_STAGE_CODEGEN))
    val nodeIdAndWSCGIdMap: Map[Long, Option[Long]] = wscgNodes.flatMap {
      _ match {
        case x: SparkPlanGraphCluster => x.nodes.map(_.id -> getWholeStageCodegenId(x.name.trim))
        case _ => Seq.empty
      }
    }.toMap

    nodeIdAndWSCGIdMap
  }

  private def getWholeStageCodegenId(wscgNodeName: String): Option[Long] = {
    Try(wscgNodeName.substring(
      s"$WHOLE_STAGE_CODEGEN (".length, wscgNodeName.length - 1).toLong) match {
      case Success(wscgId) => Some(wscgId)
      case Failure(t) => None
    }
  }

}
