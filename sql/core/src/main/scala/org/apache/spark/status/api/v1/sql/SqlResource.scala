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
import scala.util.control.NonFatal

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphNode, SQLAppStatusStore, SQLExecutionUIData}
import org.apache.spark.status.api.v1.{BaseAppResource, NotFoundException}

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class SqlResource extends BaseAppResource with Logging {

  val WHOLE_STAGE_CODEGEN = "WholeStageCodegen"
  val COMMA = ","

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
      val graph = sqlStore.planGraph(execId)
      sqlStore
        .execution(execId)
        .map(prepareExecutionData(_, graph, details, planDescription))
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

    val status = if (exec.jobs.size == completed.size) {
      "COMPLETED"
    } else if (failed.nonEmpty) {
      "FAILED"
    } else {
      "RUNNING"
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
      status,
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

  private def printableMetrics(allNodes: Seq[SparkPlanGraphNode],
    metricValues: Map[Long, String]): Seq[Node] = {

    def getMetric(metricValues: Map[Long, String], accumulatorId: Long,
      metricName: String): Option[Metric] = {

      metricValues.get(accumulatorId).map( mv => {
        val metricValue = if (mv.startsWith("\n")) mv.substring(1, mv.length) else mv
        val value = extractMetric(metricName, metricValue)
        Metric(metricName, value)
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

  private def getNodeIdAndWSCGIdMap(allNodes: Seq[SparkPlanGraphNode]): Map[Long, Option[Long]] = {
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

  private def extractMetric(metricName: String, metricValue: String): Value = {
    try {
      extractMetricValue(metricValue)
    } catch {
      case NonFatal(t) => logError("Unsupported Metric Format found. " +
          s"MetricName: $metricName - MetricValue: $metricValue - Cause: ${t.getMessage}")
        Value()
    }
  }

  private def extractMetricValue(metricValue: String): Value = {
    val trimmedMetricValue = metricValue.replace(COMMA, "").trim

    if (trimmedMetricValue.startsWith("(") && trimmedMetricValue.endsWith(")")) {
      val arr = getValueArray(trimmedMetricValue)
      Value(amount = None, min = Some(arr(0)), med = Some(arr(1)), max = Some(arr(2))
        , stageId = Some(arr(3)), taskId = Some(arr(4)))
    } else if (trimmedMetricValue.contains("driver")) {
      processDriverString(trimmedMetricValue)
    } else {
      processString(trimmedMetricValue)
    }
  }

  private def processDriverString(str: String): Value = {
    // Driver String looks like "total (min, med, max (stageId: taskId))
    // 194.0 B (97.0 B, 97.0 B, 97.0 B (driver))"
    val metricValues = str.split("\n").last.split("(?<!\\G\\S+)\\s")
    val cleanMetricValues = metricValues.map( x => x.replaceAll("[(]|[)]|[:]", ""))
    require(cleanMetricValues.size == 5,
      s"Array size should be 5 as total, min, med, max, driver +" +
        s" but found metric value: $str")
    logDebug("final values are:" + cleanMetricValues.mkString(",") + "\n")
    Value(amount = Some(cleanMetricValues(0)), min = Some(cleanMetricValues(1)),
      med = Some(cleanMetricValues(2)), max = Some(cleanMetricValues(3)))
  }

  private def processString(str: String): Value = {
    def processTotalString(totalStr: String): Value = {
      // totalStr looks like total (min med max (stageId: taskId))\n
      // 4 ms (2 ms 2 ms 2 ms (stage 6.0: task 17))
      // Using regex to first split second line based on every 2 spaces, then removing "(" "(" ":"
      // special characters
      val metricValues = totalStr.split("\n").last.split("(?<!\\G\\S+)\\s")
      val cleanMetricValues = metricValues.map( x => x.replaceAll("[(]|[)]|[:]", ""))
      require(cleanMetricValues.size == 6,
        s"Array size should be 6 as total, min, med, max, stageId, TaskId" +
          s" but found metric value: $totalStr")
      cleanMetricValues(4) = cleanMetricValues(4).replace("stage ", "")
      cleanMetricValues(5) = cleanMetricValues(5).replace("task ", "")
      logDebug("final values are:" + cleanMetricValues.mkString(",") + "\n")
      Value(amount = Some(cleanMetricValues(0)), min = Some(cleanMetricValues(1)),
        med = Some(cleanMetricValues(2)), max = Some(cleanMetricValues(3)),
        stageId = Some(cleanMetricValues(4)), taskId = Some(cleanMetricValues(5)))
    }
    val arr = str.trim.split(" ")
    val result = arr.length match {
      case 1 => Value(amount = Some(str)) // Singular numeric string like "1" with no units.
      case 2 => Value(amount = Some(str)) // Numeric string with units like "1 ms"
      case 17 => processTotalString(str) // Sample value in processTotalString commented lines.
      case _ => logError("Unsupported Metric Value found. " +
        s"MetricValue: $str - Cause: Different string length")
        Value(amount = Some(str))
      // Default when metric value is unexpected.
    }
    result
  }

  private def getValueArray(metricValue: String): Array[String] = {
    // metricValue looks like (min med max (stageId: taskId)):
    // (1 1 1 (stage 8.0: task 9)), so using regex to replace
    // everything that isn't part off a number with a space then splitting the string by
    // its spaces.
    val cleanMetricValue = metricValue.replaceAll("[^\\d.]", " ").trim
    val valueArray = cleanMetricValue.split("\\s+")
    require(valueArray.size == 5, s"Array size should be 5 as min, med, max, stageId, TaskId" +
      s" but found metric value: $metricValue")
    valueArray.map(_.trim)
  }

}
