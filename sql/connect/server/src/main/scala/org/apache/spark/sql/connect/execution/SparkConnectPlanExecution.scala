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

package org.apache.spark.sql.connect.execution

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

import org.apache.spark.SparkEnv
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic.{DataFrame, Dataset}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter.toLiteralProto
import org.apache.spark.sql.connect.config.Connect.CONNECT_GRPC_ARROW_MAX_BATCH_SIZE
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.ExecuteHolder
import org.apache.spark.sql.connect.utils.MetricGenerator
import org.apache.spark.sql.execution.{DoNotCleanup, LocalTableScanExec, RemoveShuffleFiles, SkipMigration, SQLExecution}
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils

/**
 * Handle ExecutePlanRequest where the operation to handle is of `Plan` type.
 * proto.Plan.OpTypeCase.ROOT
 * @param executeHolder
 */
private[execution] class SparkConnectPlanExecution(executeHolder: ExecuteHolder) {

  private val sessionHolder = executeHolder.sessionHolder
  private val session = executeHolder.session

  def handlePlan(responseObserver: ExecuteResponseObserver[proto.ExecutePlanResponse]): Unit = {
    val request = executeHolder.request
    if (request.getPlan.getOpTypeCase != proto.Plan.OpTypeCase.ROOT) {
      throw new IllegalStateException(
        s"Illegal operation type ${request.getPlan.getOpTypeCase} to be handled here.")
    }
    val planner = new SparkConnectPlanner(executeHolder)
    val tracker = executeHolder.eventsManager.createQueryPlanningTracker()
    val conf = session.sessionState.conf
    val shuffleCleanupMode =
      if (conf.getConf(SQLConf.SHUFFLE_DEPENDENCY_FILE_CLEANUP_ENABLED)) {
        RemoveShuffleFiles
      } else if (conf.getConf(SQLConf.SHUFFLE_DEPENDENCY_SKIP_MIGRATION_ENABLED)) {
        SkipMigration
      } else {
        DoNotCleanup
      }
    val dataframe =
      Dataset.ofRows(
        sessionHolder.session,
        planner.transformRelation(request.getPlan.getRoot, cachePlan = true),
        tracker,
        shuffleCleanupMode)
    responseObserver.onNext(createSchemaResponse(request.getSessionId, dataframe.schema))
    processAsArrowBatches(dataframe, responseObserver, executeHolder)
    responseObserver.onNext(MetricGenerator.createMetricsResponse(sessionHolder, dataframe))
    createObservedMetricsResponse(
      request.getSessionId,
      executeHolder.allObservationAndPlanIds,
      dataframe).foreach(responseObserver.onNext)
  }

  type Batch = (Array[Byte], Long)

  def rowToArrowConverter(
      schema: StructType,
      maxRecordsPerBatch: Int,
      maxBatchSize: Long,
      timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean): Iterator[InternalRow] => Iterator[Batch] = { rows =>
    val batches = ArrowConverters.toBatchWithSchemaIterator(
      rows,
      schema,
      maxRecordsPerBatch,
      maxBatchSize,
      timeZoneId,
      errorOnDuplicatedFieldNames)
    batches.map(b => b -> batches.rowCountInLastBatch)
  }

  def processAsArrowBatches(
      dataframe: DataFrame,
      responseObserver: StreamObserver[ExecutePlanResponse],
      executePlan: ExecuteHolder): Unit = {
    val sessionId = executePlan.sessionHolder.sessionId
    val spark = dataframe.sparkSession
    val schema = dataframe.schema
    val maxRecordsPerBatch = spark.sessionState.conf.arrowMaxRecordsPerBatch
    val timeZoneId = spark.sessionState.conf.sessionLocalTimeZone
    // Conservatively sets it 70% because the size is not accurate but estimated.
    val maxBatchSize = (SparkEnv.get.conf.get(CONNECT_GRPC_ARROW_MAX_BATCH_SIZE) * 0.7).toLong

    val converter = rowToArrowConverter(
      schema,
      maxRecordsPerBatch,
      maxBatchSize,
      timeZoneId,
      errorOnDuplicatedFieldNames = false)

    var numSent = 0
    def sendBatch(bytes: Array[Byte], count: Long, startOffset: Long): Unit = {
      val response = proto.ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setServerSideSessionId(sessionHolder.serverSessionId)

      val batch = proto.ExecutePlanResponse.ArrowBatch
        .newBuilder()
        .setRowCount(count)
        .setData(ByteString.copyFrom(bytes))
        .setStartOffset(startOffset)
        .build()
      response.setArrowBatch(batch)
      responseObserver.onNext(response.build())
      numSent += 1
    }

    dataframe.queryExecution.executedPlan match {
      case LocalTableScanExec(_, rows, _) =>
        executePlan.eventsManager.postFinished(Some(rows.length))
        var offset = 0L
        converter(rows.iterator).foreach { case (bytes, count) =>
          sendBatch(bytes, count, offset)
          offset += count
        }
      case _ =>
        SQLExecution.withNewExecutionId(dataframe.queryExecution, Some("collectArrow")) {
          val rows = dataframe.queryExecution.executedPlan.execute()
          val numPartitions = rows.getNumPartitions

          if (numPartitions > 0) {
            type Batch = (Array[Byte], Long)

            val batches = rows.mapPartitionsInternal(converter)

            val signal = new Object
            val partitions = new Array[Array[Batch]](numPartitions)
            var numFinishedPartitions = 0
            var totalNumRows: Long = 0
            var error: Option[Throwable] = None

            // This callback is executed by the DAGScheduler thread.
            // After fetching a partition, it inserts the partition into the Map, and then
            // wakes up the main thread.
            val resultHandler = (partitionId: Int, partition: Array[Batch]) => {
              signal.synchronized {
                partitions(partitionId) = partition
                totalNumRows += partition.map(_._2).sum
                numFinishedPartitions += 1
                if (numFinishedPartitions == numPartitions) {
                  // Execution is finished, when all partitions returned results.
                  executePlan.eventsManager.postFinished(Some(totalNumRows))
                }
                signal.notify()
              }
              ()
            }

            val future = spark.sparkContext
              .submitJob(
                rdd = batches,
                processPartition = (iter: Iterator[Batch]) => iter.toArray,
                partitions = Seq.range(0, numPartitions),
                resultHandler = resultHandler,
                resultFunc = () => ())
              // Collect errors and propagate them to the main thread.
              .andThen {
                case Success(_) => // do nothing
                case Failure(throwable) =>
                  signal.synchronized {
                    error = Some(throwable)
                    signal.notify()
                  }
              }(ThreadUtils.sameThread)

            // The main thread will wait until 0-th partition is available,
            // then send it to client and wait for the next partition.
            // Different from the implementation of [[Dataset#collectAsArrowToPython]], it sends
            // the arrow batches in main thread to avoid DAGScheduler thread been blocked for
            // tasks not related to scheduling. This is particularly important if there are
            // multiple users or clients running code at the same time.
            var currentPartitionId = 0
            var currentOffset = 0L
            while (currentPartitionId < numPartitions) {
              val partition = signal.synchronized {
                var part = partitions(currentPartitionId)
                while (part == null && error.isEmpty) {
                  signal.wait()
                  part = partitions(currentPartitionId)
                }
                partitions(currentPartitionId) = null

                error.foreach { other =>
                  throw other
                }
                part
              }

              partition.foreach { case (bytes, count) =>
                sendBatch(bytes, count, currentOffset)
                currentOffset += count
              }

              currentPartitionId += 1
            }
            ThreadUtils.awaitReady(future, Duration.Inf)
          } else {
            executePlan.eventsManager.postFinished(Some(0))
          }
        }
    }

    // Make sure at least 1 batch will be sent.
    if (numSent == 0) {
      sendBatch(
        ArrowConverters.createEmptyArrowBatch(
          schema,
          timeZoneId,
          errorOnDuplicatedFieldNames = false),
        0L,
        0L)
    }
  }

  private def createSchemaResponse(sessionId: String, schema: StructType): ExecutePlanResponse = {
    // Send the Spark data type
    ExecutePlanResponse
      .newBuilder()
      .setSessionId(sessionId)
      .setServerSideSessionId(sessionHolder.serverSessionId)
      .setSchema(DataTypeProtoConverter.toConnectProtoType(schema))
      .build()
  }

  private def createObservedMetricsResponse(
      sessionId: String,
      observationAndPlanIds: Map[String, Long],
      dataframe: DataFrame): Option[ExecutePlanResponse] = {
    val observedMetrics = dataframe.queryExecution.observedMetrics.collect {
      case (name, row) if !executeHolder.observations.contains(name) =>
        val values = (0 until row.length).map { i =>
          (if (row.schema != null) Some(row.schema.fieldNames(i)) else None, row(i))
        }
        name -> values
    }
    if (observedMetrics.nonEmpty) {
      Some(
        SparkConnectPlanExecution
          .createObservedMetricsResponse(
            sessionId,
            sessionHolder.serverSessionId,
            observationAndPlanIds,
            observedMetrics))
    } else None
  }
}

object SparkConnectPlanExecution {
  def createObservedMetricsResponse(
      sessionId: String,
      serverSessionId: String,
      observationAndPlanIds: Map[String, Long],
      metrics: Map[String, Seq[(Option[String], Any)]]): ExecutePlanResponse = {
    val observedMetrics = metrics.map { case (name, values) =>
      val metrics = ExecutePlanResponse.ObservedMetrics
        .newBuilder()
        .setName(name)
      values.foreach { case (key, value) =>
        metrics.addValues(toLiteralProto(value))
        key.foreach(metrics.addKeys)
      }
      observationAndPlanIds.get(name).foreach(metrics.setPlanId)
      metrics.build()
    }
    // Prepare a response with the observed metrics.
    ExecutePlanResponse
      .newBuilder()
      .setSessionId(sessionId)
      .setServerSideSessionId(serverSessionId)
      .addAllObservedMetrics(observedMetrics.asJava)
      .build()
  }
}
