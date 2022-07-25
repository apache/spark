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

package org.apache.spark.sql.sparkconnect.service

import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{Request, Response}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, QueryStageExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sparkconnect.command.SparkConnectCommandPlanner
import org.apache.spark.sql.sparkconnect.planner.SparkConnectPlanner
import org.apache.spark.sql.util.ArrowUtils

class SparkConnectStreamHandler(responseObserver: StreamObserver[Response]) extends Logging {

  def handle(v: Request): Unit = {
    // Preconditions.checkState(v.userContext.nonEmpty, "User Context must be present")
    val session = SparkConnectService.getOrCreateIsolatedSession(v.getUserContext.userId).session
    v.getPlan.opType match {
      case proto.Plan.OpType.Command(_) => handleCommand(session, v)
      case proto.Plan.OpType.Root(_) => handlePlan(session, v)
      case _ => throw new UnsupportedOperationException(s"${v.getPlan.opType} not supported.")
    }
  }

  def handlePlan(session: SparkSession, request: proto.Request): Unit = {
    // Extract the plan from the request and convert it to a logical plan
    val rows =
      Dataset.ofRows(session, SparkConnectPlanner(request.getPlan.getRoot, session).transform())
    processRows(request.clientId, rows)
  }

  private def processRows(clientId: String, rows: DataFrame) = {
    val timeZoneId = SQLConf.get.sessionLocalTimeZone
    val schema =
      ByteString.copyFrom(ArrowUtils.toArrowSchema(rows.schema, timeZoneId).toByteArray)

    val textSchema = rows.schema.fields.map(f => f.name).mkString("|")

    // TODO empty results (except limit 0) will not yield a schema.

    val data = rows.collect().map(x => x.toSeq.mkString("|")).mkString("\n")
    val bbb = proto.Response.CSVBatch(rowCount = -1, data = textSchema ++ "\n" ++ data)
    val response = proto.Response(
      clientId = clientId,
      resultType = proto.Response.ResultType.CsvBatch(bbb),
      // metrics = Some(MetricGenerator.buildMetrics(rows.queryExecution.executedPlan))
    )

    // Send all the data
    responseObserver.onNext(response)

    //    val batches = rows.collectToArrowBatches()
    //    batches.iterator.asScala.foreach(x => {
    //      responseObserver.onNext(
    //        Response(
    //          clientId = clientId,
    //          batch = Some(
    //            Response.ArrowBatch(
    //              x.rowCount,
    //              x.uncompressedBytes,
    //              x.compressedBytes,
    //              ByteString.copyFrom(x.batch),
    //              schema
    //            )
    //          )
    //        )
    //      )
    //    })
    responseObserver.onNext(sendMetricsToResponse(clientId, rows))
    responseObserver.onCompleted()
  }

  def sendMetricsToResponse(clientId: String, rows: DataFrame): Response = {
    // Send a last batch with the metrics
    Response(
      clientId = clientId,
      metrics = Some(MetricGenerator.buildMetrics(rows.queryExecution.executedPlan)))
  }

  def handleCommand(session: SparkSession, request: Request): Unit = {
    val command = request.getPlan.getCommand
    SparkConnectCommandPlanner(session, command).process()
    responseObserver.onCompleted()

  }

}

object MetricGenerator extends AdaptiveSparkPlanHelper {
  def buildMetrics(p: SparkPlan): Response.Metrics = {
    Response.Metrics(metrics = transformPlan(p, p.id))
  }

  def transformChildren(p: SparkPlan): Seq[Response.Metrics.MetricObject] = {
    allChildren(p).flatMap(c => transformPlan(c, p.id))
  }

  def allChildren(p: SparkPlan): Seq[SparkPlan] = p match {
    case a: AdaptiveSparkPlanExec => Seq(a.executedPlan)
    case s: QueryStageExec => Seq(s.plan)
    case _ => p.children
  }

  def transformPlan(p: SparkPlan, parentId: Int): Seq[Response.Metrics.MetricObject] = {
    val mv = p.metrics.map(m =>
      m._1 -> Response.Metrics.MetricValue(m._2.name.getOrElse(""), m._2.value, m._2.metricType))
    Seq(
      Response.Metrics.MetricObject(
        name = p.nodeName,
        planId = p.id,
        executionMetrics = mv,
        parent = parentId)) ++
      transformChildren(p)
  }

}
