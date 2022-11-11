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

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{Request, Response}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, QueryStageExec}
import org.apache.spark.sql.execution.arrow.ArrowConverters

class SparkConnectStreamHandler(responseObserver: StreamObserver[Response]) extends Logging {

  // The maximum batch size in bytes for a single batch of data to be returned via proto.
  private val MAX_BATCH_SIZE: Long = 4 * 1024 * 1024

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

  def handlePlan(session: SparkSession, request: Request): Unit = {
    // Extract the plan from the request and convert it to a logical plan
    val planner = new SparkConnectPlanner(session)
    val dataframe = Dataset.ofRows(session, planner.transformRelation(request.getPlan.getRoot))
    try {
      processAsArrowBatches(request.getClientId, dataframe)
    } catch {
      case e: Exception =>
        logWarning(e.getMessage)
        processAsJsonBatches(request.getClientId, dataframe)
    }
  }

  def processAsJsonBatches(clientId: String, dataframe: DataFrame): Unit = {
    // Only process up to 10MB of data.
    val sb = new StringBuilder
    var rowCount = 0
    dataframe.toJSON
      .collect()
      .foreach(row => {

        // There are a few cases to cover here.
        // 1. The aggregated buffer size is larger than the MAX_BATCH_SIZE
        //     -> send the current batch and reset.
        // 2. The aggregated buffer size is smaller than the MAX_BATCH_SIZE
        //     -> append the row to the buffer.
        // 3. The row in question is larger than the MAX_BATCH_SIZE
        //     -> fail the query.

        // Case 3. - Fail
        if (row.size > MAX_BATCH_SIZE) {
          throw SparkException.internalError(
            s"Serialized row is larger than MAX_BATCH_SIZE: ${row.size} > ${MAX_BATCH_SIZE}")
        }

        // Case 1 - FLush and send.
        if (sb.size + row.size > MAX_BATCH_SIZE) {
          val response = proto.Response.newBuilder().setClientId(clientId)
          val batch = proto.Response.JSONBatch
            .newBuilder()
            .setData(ByteString.copyFromUtf8(sb.toString()))
            .setRowCount(rowCount)
            .build()
          response.setJsonBatch(batch)
          responseObserver.onNext(response.build())
          sb.clear()
          sb.append(row)
          rowCount = 1
        } else {
          // Case 2 - Append.
          // Make sure to put the newline delimiters only between items and not at the end.
          if (rowCount > 0) {
            sb.append("\n")
          }
          sb.append(row)
          rowCount += 1
        }
      })

    // If the last batch is not empty, send out the data to the client.
    if (sb.size > 0) {
      val response = proto.Response.newBuilder().setClientId(clientId)
      val batch = proto.Response.JSONBatch
        .newBuilder()
        .setData(ByteString.copyFromUtf8(sb.toString()))
        .setRowCount(rowCount)
        .build()
      response.setJsonBatch(batch)
      responseObserver.onNext(response.build())
    }

    responseObserver.onNext(sendMetricsToResponse(clientId, dataframe))
    responseObserver.onCompleted()
  }

  def processAsArrowBatches(clientId: String, dataframe: DataFrame): Unit = {
    val spark = dataframe.sparkSession
    val schema = dataframe.schema
    val maxRecordsPerBatch = spark.sessionState.conf.arrowMaxRecordsPerBatch
    val timeZoneId = spark.sessionState.conf.sessionLocalTimeZone

    SQLExecution.withNewExecutionId(dataframe.queryExecution, Some("collectArrow")) {
      val rows = dataframe.queryExecution.executedPlan.execute()
      val numPartitions = rows.getNumPartitions
      // Conservatively sets it 70% because the size is not accurate but estimated.
      val maxBatchSize = (MAX_BATCH_SIZE * 0.7).toLong
      var numSent = 0

      if (numPartitions > 0) {
        type Batch = (Array[Byte], Long)

        val batches = rows.mapPartitionsInternal { iter =>
          val newIter = ArrowConverters
            .toBatchWithSchemaIterator(iter, schema, maxRecordsPerBatch, maxBatchSize, timeZoneId)
          newIter.map { batch: Array[Byte] => (batch, newIter.rowCountInLastBatch) }
        }

        // This callback is executed by the DAGScheduler thread.
        def writeBatches(arrowBatches: Array[Batch]): Unit = {
          for (arrowBatch <- arrowBatches) {
            val (bytes, count) = arrowBatch
            val response = proto.Response.newBuilder().setClientId(clientId)
            val batch = proto.Response.ArrowBatch
              .newBuilder()
              .setRowCount(count)
              .setData(ByteString.copyFrom(bytes))
              .build()
            response.setArrowBatch(batch)
            responseObserver.onNext(response.build())
            numSent += 1
          }
        }

        // Store collection results for worst case of 1 to N-1 partitions
        val results = new Array[Array[Batch]](numPartitions - 1)
        var lastIndex = -1 // index of last partition written

        // Handler to eagerly write partitions in order
        val resultHandler = (partitionId: Int, partition: Array[Batch]) => {
          // If result is from next partition in order
          if (partitionId - 1 == lastIndex) {
            writeBatches(partition)
            lastIndex += 1
            // Write stored partitions that come next in order
            while (lastIndex < results.length && results(lastIndex) != null) {
              writeBatches(results(lastIndex))
              results(lastIndex) = null
              lastIndex += 1
            }
          } else {
            // Store partitions received out of order
            results(partitionId - 1) = partition
          }
        }
        spark.sparkContext.runJob(batches, (iter: Iterator[Batch]) => iter.toArray, resultHandler)
      }

      // Make sure at least 1 batch will be sent.
      if (numSent == 0) {
        val bytes = ArrowConverters.createEmptyArrowBatch(schema, timeZoneId)
        val response = proto.Response.newBuilder().setClientId(clientId)
        val batch = proto.Response.ArrowBatch
          .newBuilder()
          .setRowCount(0L)
          .setData(ByteString.copyFrom(bytes))
          .build()
        response.setArrowBatch(batch)
        responseObserver.onNext(response.build())
      }

      responseObserver.onNext(sendMetricsToResponse(clientId, dataframe))
      responseObserver.onCompleted()
    }
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
    val planner = new SparkConnectPlanner(session)
    planner.process(command)
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
