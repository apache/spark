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

package org.apache.spark.sql.connect.service

import scala.collection.JavaConverters._

import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

import org.apache.spark.annotation.{Since, Unstable}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{Request, Response}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.connect.command.SparkConnectCommandPlanner
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, QueryStageExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.ArrowUtils

@Unstable
@Since("3.4.0")
class SparkConnectStreamHandler(responseObserver: StreamObserver[Response]) extends Logging {

  def handle(v: Request): Unit = {
    val session =
      SparkConnectService.getOrCreateIsolatedSession(v.getUserContext.getUserId).session
    v.getPlan.getOpTypeCase match {
      case proto.Plan.OpTypeCase.COMMAND => handleCommand(session, v)
      case proto.Plan.OpTypeCase.ROOT => handlePlan(session, v)
      case _ =>
        throw new UnsupportedOperationException(s"${v.getPlan.getOpTypeCase} not supported.")
    }
  }

  def handlePlan(session: SparkSession, request: proto.Request): Unit = {
    // Extract the plan from the request and convert it to a logical plan
    val planner = new SparkConnectPlanner(request.getPlan.getRoot, session)
    val rows =
      Dataset.ofRows(session, planner.transform())
    processRows(request.getClientId, rows)
  }

  private def processRows(clientId: String, rows: DataFrame) = {
    val timeZoneId = SQLConf.get.sessionLocalTimeZone
    val schema =
      ByteString.copyFrom(ArrowUtils.toArrowSchema(rows.schema, timeZoneId).toByteArray)

    val textSchema = rows.schema.fields.map(f => f.name).mkString("|")
    val data = rows.collect().map(x => x.toSeq.mkString("|")).mkString("\n")
    val bbb = proto.Response.CSVBatch.newBuilder
      .setRowCount(-1)
      .setData(textSchema ++ "\n" ++ data)
      .build()
    val response = proto.Response.newBuilder().setClientId(clientId).setCsvBatch(bbb).build()

    // Send all the data
    responseObserver.onNext(response)
    responseObserver.onNext(sendMetricsToResponse(clientId, rows))
    responseObserver.onCompleted()
  }

  def sendMetricsToResponse(clientId: String, rows: DataFrame): Response = {
    // Send a last batch with the metrics
    Response
      .newBuilder()
      .setClientId(clientId)
      .setMetrics(MetricGenerator.buildMetrics(rows.queryExecution.executedPlan))
      .build()
  }

  def handleCommand(session: SparkSession, request: Request): Unit = {
    val command = request.getPlan.getCommand
    val planner = new SparkConnectCommandPlanner(session, command)
    planner.process()
    responseObserver.onCompleted()
  }
}

object MetricGenerator extends AdaptiveSparkPlanHelper {
  def buildMetrics(p: SparkPlan): Response.Metrics = {
    val b = Response.Metrics.newBuilder
    b.addAllMetrics(transformPlan(p, p.id).asJava)
    b.build()
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
      m._1 -> Response.Metrics.MetricValue.newBuilder
        .setName(m._2.name.getOrElse(""))
        .setValue(m._2.value)
        .setMetricType(m._2.metricType)
        .build())
    val mo = Response.Metrics.MetricObject
      .newBuilder()
      .setName(p.nodeName)
      .setPlanId(p.id)
      .putAllExecutionMetrics(mv.asJava)
      .build()
    Seq(mo) ++ transformChildren(p)
  }

}
