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

package org.apache.spark.sql.execution.python

import java.io.DataOutputStream
import java.net.ServerSocket

import scala.concurrent.ExecutionContext

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions, PythonRDD}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.TransformWithStateInPandasPythonRunner._
import org.apache.spark.sql.execution.streaming.StatefulProcessorHandleImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils

/**
 * Python runner with no initial state in TransformWithStateInPandas.
 * Write input data as one single InternalRow in each row in arrow batch.
 */
class TransformWithStateInPandasPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    _schema: StructType,
    processorHandle: StatefulProcessorHandleImpl,
    _timeZoneId: String,
    initialWorkerConf: Map[String, String],
    override val pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    groupingKeySchema: StructType,
    hasInitialState: Boolean)
  extends TransformWithStateInPandasPythonBaseRunner[InType](
    funcs, evalType, argOffsets, _schema, processorHandle, _timeZoneId,
    initialWorkerConf, pythonMetrics, jobArtifactUUID, groupingKeySchema, hasInitialState)
  with PythonArrowInput[InType] {

  private var pandasWriter: BaseStreamingArrowWriter = _

  override protected def writeNextInputToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[InType]): Boolean = {
    if (pandasWriter == null) {
      pandasWriter = new BaseStreamingArrowWriter(root, writer, arrowMaxRecordsPerBatch)
    }

    if (inputIterator.hasNext) {
      val startData = dataOut.size()
      val next = inputIterator.next()
      val dataIter = next._2

      while (dataIter.hasNext) {
        val dataRow = dataIter.next()
        pandasWriter.writeRow(dataRow)
      }
      pandasWriter.finalizeCurrentArrowBatch()
      val deltaData = dataOut.size() - startData
      pythonMetrics("pythonDataSent") += deltaData
      true
    } else {
      super[PythonArrowInput].close()
      false
    }
  }
}

/**
 * Python runner with initial state in TransformWithStateInPandas.
 * Write input data as one InternalRow(inputRow, initialState) in each row in arrow batch.
 */
class TransformWithStateInPandasPythonInitialStateRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    dataSchema: StructType,
    initStateSchema: StructType,
    processorHandle: StatefulProcessorHandleImpl,
    _timeZoneId: String,
    initialWorkerConf: Map[String, String],
    override val pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    groupingKeySchema: StructType,
    hasInitialState: Boolean)
  extends TransformWithStateInPandasPythonBaseRunner[GroupedInType](
    funcs, evalType, argOffsets, dataSchema, processorHandle, _timeZoneId,
    initialWorkerConf, pythonMetrics, jobArtifactUUID, groupingKeySchema, hasInitialState)
  with PythonArrowInput[GroupedInType] {

  override protected lazy val schema: StructType = new StructType()
    .add("state", dataSchema)
    .add("initState", initStateSchema)

  private var pandasWriter: BaseStreamingArrowWriter = _

  override protected def writeNextInputToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator:
      Iterator[GroupedInType]): Boolean = {
    if (pandasWriter == null) {
      pandasWriter = new BaseStreamingArrowWriter(root, writer, arrowMaxRecordsPerBatch)
    }

    if (inputIterator.hasNext) {
      val startData = dataOut.size()
      // a new grouping key with data & init state iter
      val next = inputIterator.next()
      val dataIter = next._2
      val initIter = next._3

      while (dataIter.hasNext || initIter.hasNext) {
        val dataRow =
          if (dataIter.hasNext) dataIter.next()
          else InternalRow.empty
        val initRow =
          if (initIter.hasNext) initIter.next()
          else InternalRow.empty
        pandasWriter.writeRow(InternalRow(dataRow, initRow))
      }
      pandasWriter.finalizeCurrentArrowBatch()
      val deltaData = dataOut.size() - startData
      pythonMetrics("pythonDataSent") += deltaData
      true
    } else {
      super[PythonArrowInput].close()
      false
    }
  }
}

/**
 * Base Python runner implementation for TransformWithStateInPandas.
 */
abstract class TransformWithStateInPandasPythonBaseRunner[I](
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    _schema: StructType,
    processorHandle: StatefulProcessorHandleImpl,
    _timeZoneId: String,
    initialWorkerConf: Map[String, String],
    override val pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    groupingKeySchema: StructType,
    hasInitialState: Boolean)
  extends BasePythonRunner[I, ColumnarBatch](funcs.map(_._1), evalType, argOffsets, jobArtifactUUID)
    with PythonArrowInput[I]
    with BasicPythonArrowOutput
    with Logging {
  protected val sqlConf = SQLConf.get
  protected val arrowMaxRecordsPerBatch = sqlConf.arrowMaxRecordsPerBatch

  private var stateServerSocketPort: Int = 0

  override protected val workerConf: Map[String, String] = initialWorkerConf +
    (SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> arrowMaxRecordsPerBatch.toString)

  // Use lazy val to initialize the fields before these are accessed in [[PythonArrowInput]]'s
  // constructor.
  override protected lazy val schema: StructType = _schema
  override protected lazy val timeZoneId: String = _timeZoneId
  override protected val errorOnDuplicatedFieldNames: Boolean = true
  override protected val largeVarTypes: Boolean = sqlConf.arrowUseLargeVarTypes

  override protected def handleMetadataBeforeExec(stream: DataOutputStream): Unit = {
    super.handleMetadataBeforeExec(stream)
    // Also write the port number for state server
    stream.writeInt(stateServerSocketPort)
    PythonRDD.writeUTF(groupingKeySchema.json, stream)
  }

  override def compute(
      inputIterator: Iterator[I],
      partitionIndex: Int,
      context: TaskContext): Iterator[ColumnarBatch] = {
    var stateServerSocket: ServerSocket = null
    var failed = false
    try {
      stateServerSocket = new ServerSocket(/* port = */ 0,
        /* backlog = */ 1)
      stateServerSocketPort = stateServerSocket.getLocalPort
    } catch {
      case e: Throwable =>
        failed = true
        throw e
    } finally {
      if (failed) {
        closeServerSocketChannelSilently(stateServerSocket)
      }
    }

    val executor = ThreadUtils.newDaemonSingleThreadExecutor("stateConnectionListenerThread")
    val executionContext = ExecutionContext.fromExecutor(executor)

    executionContext.execute(
      new TransformWithStateInPandasStateServer(stateServerSocket, processorHandle,
        groupingKeySchema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes,
        sqlConf.arrowTransformWithStateInPandasMaxRecordsPerBatch,
        hasInitialState = hasInitialState))

    context.addTaskCompletionListener[Unit] { _ =>
      logInfo(log"completion listener called")
      executor.shutdownNow()
      closeServerSocketChannelSilently(stateServerSocket)
    }

    super.compute(inputIterator, partitionIndex, context)
  }

  private def closeServerSocketChannelSilently(stateServerSocket: ServerSocket): Unit = {
    try {
      logInfo(log"closing the state server socket")
      stateServerSocket.close()
    } catch {
      case e: Exception =>
        logError(log"failed to close state server socket", e)
    }
  }

  override protected def writeUDF(dataOut: DataOutputStream): Unit = {
    PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets, None)
  }
}

object TransformWithStateInPandasPythonRunner {
  type InType = (InternalRow, Iterator[InternalRow])
  type GroupedInType = (InternalRow, Iterator[InternalRow], Iterator[InternalRow])
}
