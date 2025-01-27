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

import java.io.{DataInputStream, DataOutputStream}
import java.net.ServerSocket

import scala.concurrent.ExecutionContext

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions, PythonFunction, PythonRDD, PythonWorkerUtils, StreamingPythonRunner}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.TransformWithStateInPandasPythonRunner.{GroupedInType, InType}
import org.apache.spark.sql.execution.streaming.{DriverStatefulProcessorHandleImpl, StatefulProcessorHandleImpl}
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
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForEviction: Option[Long])
  extends TransformWithStateInPandasPythonBaseRunner[InType](
    funcs, evalType, argOffsets, _schema, processorHandle, _timeZoneId,
    initialWorkerConf, pythonMetrics, jobArtifactUUID, groupingKeySchema,
    batchTimestampMs, eventTimeWatermarkForEviction)
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
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForEviction: Option[Long])
  extends TransformWithStateInPandasPythonBaseRunner[GroupedInType](
    funcs, evalType, argOffsets, dataSchema, processorHandle, _timeZoneId,
    initialWorkerConf, pythonMetrics, jobArtifactUUID, groupingKeySchema,
    batchTimestampMs, eventTimeWatermarkForEviction)
  with PythonArrowInput[GroupedInType] {

  override protected lazy val schema: StructType = new StructType()
    .add("inputData", dataSchema)
    .add("initState", initStateSchema)

  private var pandasWriter: BaseStreamingArrowWriter = _

  override protected def writeNextInputToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[GroupedInType]): Boolean = {
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
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForEviction: Option[Long])
  extends BasePythonRunner[I, ColumnarBatch](
    funcs.map(_._1), evalType, argOffsets, jobArtifactUUID, pythonMetrics)
  with PythonArrowInput[I]
  with BasicPythonArrowOutput
  with TransformWithStateInPandasPythonRunnerUtils
  with Logging {

  protected val sqlConf = SQLConf.get
  protected val arrowMaxRecordsPerBatch = sqlConf.arrowMaxRecordsPerBatch

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
    initStateServer()

    val executor = ThreadUtils.newDaemonSingleThreadExecutor("stateConnectionListenerThread")
    val executionContext = ExecutionContext.fromExecutor(executor)

    executionContext.execute(
      new TransformWithStateInPandasStateServer(stateServerSocket, processorHandle,
        groupingKeySchema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes,
        sqlConf.arrowTransformWithStateInPandasMaxRecordsPerBatch,
        batchTimestampMs, eventTimeWatermarkForEviction))

    context.addTaskCompletionListener[Unit] { _ =>
      logInfo(log"completion listener called")
      executor.shutdownNow()
      closeServerSocketChannelSilently(stateServerSocket)
    }

    super.compute(inputIterator, partitionIndex, context)
  }

  override protected def writeUDF(dataOut: DataOutputStream): Unit = {
    PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets, None)
  }
}

/**
 * TransformWithStateInPandas driver side Python runner. Similar as executor side runner,
 * will start a new daemon thread on the Python runner to run state server.
 */
class TransformWithStateInPandasPythonPreInitRunner(
    func: PythonFunction,
    workerModule: String,
    timeZoneId: String,
    groupingKeySchema: StructType,
    processorHandleImpl: DriverStatefulProcessorHandleImpl)
  extends StreamingPythonRunner(func, "", "", workerModule)
  with TransformWithStateInPandasPythonRunnerUtils
  with Logging {
  protected val sqlConf = SQLConf.get

  private var dataOut: DataOutputStream = _
  private var dataIn: DataInputStream = _

  private var daemonThread: Thread = _

  override def init(): (DataOutputStream, DataInputStream) = {
    val result = super.init()
    dataOut = result._1
    dataIn = result._2

    // start state server, update socket port
    startStateServer()
    (dataOut, dataIn)
  }

  def process(): Unit = {
    // Also write the port number for state server
    dataOut.writeInt(stateServerSocketPort)
    PythonWorkerUtils.writeUTF(groupingKeySchema.json, dataOut)
    dataOut.flush()

    val resFromPython = dataIn.readInt()
    if (resFromPython != 0) {
      val errMessage = PythonWorkerUtils.readUTF(dataIn)
      throw streamingPythonRunnerInitializationFailure(resFromPython, errMessage)
    }
  }

  override def stop(): Unit = {
    super.stop()
    closeServerSocketChannelSilently(stateServerSocket)
    daemonThread.interrupt()
  }

  private def startStateServer(): Unit = {
    initStateServer()

    daemonThread = new Thread {
      override def run(): Unit = {
        try {
          new TransformWithStateInPandasStateServer(stateServerSocket, processorHandleImpl,
            groupingKeySchema, timeZoneId, errorOnDuplicatedFieldNames = true,
            largeVarTypes = sqlConf.arrowUseLargeVarTypes,
            sqlConf.arrowTransformWithStateInPandasMaxRecordsPerBatch).run()
        } catch {
          case e: Exception =>
            throw new SparkException("TransformWithStateInPandas state server " +
              "daemon thread exited unexpectedly (crashed)", e)
        }
      }
    }
    daemonThread.setDaemon(true)
    daemonThread.setName("stateConnectionListenerThread")
    daemonThread.start()
  }
}

/**
 * TransformWithStateInPandas Python runner utils functions for handling a state server
 * in a new daemon thread.
 */
trait TransformWithStateInPandasPythonRunnerUtils extends Logging {
  protected var stateServerSocketPort: Int = 0
  protected var stateServerSocket: ServerSocket = null
  protected def initStateServer(): Unit = {
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
  }

  protected def closeServerSocketChannelSilently(stateServerSocket: ServerSocket): Unit = {
    try {
      logInfo(log"closing the state server socket")
      stateServerSocket.close()
    } catch {
      case e: Exception =>
        logError(log"failed to close state server socket", e)
    }
  }
}

object TransformWithStateInPandasPythonRunner {
  type InType = (InternalRow, Iterator[InternalRow])
  type GroupedInType = (InternalRow, Iterator[InternalRow], Iterator[InternalRow])
}
