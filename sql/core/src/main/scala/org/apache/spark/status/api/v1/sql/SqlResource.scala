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

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import jakarta.ws.rs._
import jakarta.ws.rs.core.{Context, MediaType, UriInfo}

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.config.UI.UI_SQL_GROUP_SUB_EXECUTION_ENABLED
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
   *
   * When `groupSubExecution=true` (default = `spark.ui.groupSQLSubExecutionEnabled`),
   * pagination is over root executions only and each row carries its sub-executions
   * inline as `subExecutions: [...]`. Sub-executions whose root is missing from the
   * filtered set (orphans) are surfaced as roots so they don't disappear.
   */
  @GET
  @Path("sqlTable")
  def sqlTable(@Context uriInfo: UriInfo): HashMap[String, Object] = {
    withUI { ui =>
      val sqlStore = new SQLAppStatusStore(ui.store.store)
      val uriParams = UIUtils.decodeURLParameter(uriInfo.getQueryParameters(true))

      // Echo draw counter to prevent stale responses
      val draw = Option(uriParams.getFirst("draw")).map(_.toInt).getOrElse(0)

      // Sub-execution grouping flag; default to the cluster config. Defensive
      // parse - bad values should not 500 the public REST endpoint.
      val groupSubExec = Option(uriParams.getFirst("groupSubExecution"))
        .flatMap(v => Try(v.toBoolean).toOption)
        .getOrElse(ui.conf.get(UI_SQL_GROUP_SUB_EXECUTION_ENABLED))

      // Search and status filter
      val searchValue = Option(uriParams.getFirst("search[value]"))
        .filter(_.nonEmpty)
      val statusFilter = Option(uriParams.getFirst("status"))
        .filter(_.nonEmpty)
      val needsFilter = searchValue.isDefined || statusFilter.isDefined

      // Always load all execs once. We need the full set to (a) identify orphan
      // sub-executions whose root is filtered out and (b) count root rows for
      // `recordsTotal`. `sqlStore.executionsList()` is already a full
      // materialization, so there is no separate "KVStore-pagination" path being
      // disabled here.
      val allExecs = sqlStore.executionsList()

      val filteredExecs = if (needsFilter) {
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
        allExecs
      }

      val (rootRows, subsByRoot) = if (groupSubExec) {
        SqlResource.partitionRoots(filteredExecs)
      } else {
        (filteredExecs, Map.empty[Long, Seq[SQLExecutionUIData]])
      }

      // Sort
      val sortCol = Option(uriParams.getFirst("order[0][column]"))
        .flatMap(c => Option(uriParams.getFirst(s"columns[$c][name]")))
        .getOrElse("id")
      val sortDir = Option(uriParams.getFirst("order[0][dir]")).getOrElse("desc")

      // Paginate
      val start = Option(uriParams.getFirst("start")).map(_.toInt).getOrElse(0)
      val length = Option(uriParams.getFirst("length")).map(_.toInt).getOrElse(20)

      val sortedRoots = sortExecs(rootRows, sortCol, sortDir)
      val page = if (length > 0) sortedRoots.slice(start, start + length) else sortedRoots

      // Convert to Java-compatible row data; embed sub-executions when grouping.
      // Always emit a `subExecutions` field (possibly empty) in grouped mode so
      // JSON consumers see a consistent schema; flat mode never includes it.
      val aaData = page.map { exec =>
        val row = execToRow(exec)
        if (groupSubExec) {
          val subs = subsByRoot.getOrElse(exec.executionId, Seq.empty)
          // Sort subs by id ascending so they appear in chronological order
          row.put("subExecutions", sortExecs(subs, "id", "asc").map(execToRow).asJava)
        }
        row
      }

      // Counts: grouped totals reflect root-only counts so DataTables shows
      // "Showing X to Y of Z entries" matching the rows the user actually sees.
      // Flat mode's recordsTotal is the unfiltered total (from the KVStore),
      // which lets DataTables show the "filtered from W total entries" suffix.
      val recordsTotal = if (groupSubExec) {
        if (needsFilter) {
          // Re-derive root rows from the unfiltered set using the same predicate
          SqlResource.partitionRoots(allExecs)._1.size
        } else {
          rootRows.size
        }
      } else {
        sqlStore.executionsCount()
      }
      val recordsFiltered = if (groupSubExec) rootRows.size else filteredExecs.size

      val ret = new HashMap[String, Object]()
      ret.put("draw", Integer.valueOf(draw))
      ret.put("aaData", aaData)
      ret.put("recordsTotal", java.lang.Long.valueOf(recordsTotal))
      ret.put("recordsFiltered", java.lang.Long.valueOf(recordsFiltered))
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

private[v1] object SqlResource {

  /**
   * Split a set of executions into root rows and a sub-execution map. A root row is
   * either an execution whose id equals its rootExecutionId, or an orphan sub whose
   * root parent is absent from the input set. Called on the filtered set (for paging)
   * and on the full set (for `recordsTotal`), so the predicate lives in one place
   * rather than being inlined twice.
   */
  def partitionRoots(execs: Seq[SQLExecutionUIData])
      : (Seq[SQLExecutionUIData], Map[Long, Seq[SQLExecutionUIData]]) = {
    val ids = execs.iterator.map(_.executionId).toSet
    val (roots, subs) = execs.partition { e =>
      e.executionId == e.rootExecutionId || !ids.contains(e.rootExecutionId)
    }
    (roots, subs.groupBy(_.rootExecutionId))
  }
}
