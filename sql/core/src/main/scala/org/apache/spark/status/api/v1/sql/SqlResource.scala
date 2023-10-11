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

import java.util.Date
import javax.ws.rs._
import javax.ws.rs.core.MediaType

import scala.util.{Failure, Success, Try}

import org.apache.spark.JobExecutionStatus
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphNode, SQLAppStatusStore, SQLExecutionUIData}
import org.apache.spark.status.api.v1.{BaseAppResource, NotFoundException}

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class SqlResource extends BaseAppResource {

  val WHOLE_STAGE_CODEGEN = "WholeStageCodegen"

  @GET
  def sqlList(
      @DefaultValue("true") @QueryParam("details") details: Boolean,
      @DefaultValue("true") @QueryParam("planDescription") planDescription: Boolean,
      @DefaultValue("0") @QueryParam("offset") offset: Int,
      @DefaultValue("20") @QueryParam("length") length: Int): Seq[ExecutionData] = {
    withUI { ui =>
      val sqlStore = new SQLAppStatusStore(ui.store.store)
      sqlStore.executionsList(offset, length).map { exec =>
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
      edges)
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
