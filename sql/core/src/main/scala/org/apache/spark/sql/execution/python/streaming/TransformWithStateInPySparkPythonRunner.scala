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

package org.apache.spark.sql.execution.python.streaming

import java.io.{DataInputStream, DataOutputStream, File}
import java.net.{InetAddress, InetSocketAddress, StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.channels.ServerSocketChannel
import java.util.UUID

import scala.concurrent.ExecutionContext

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions, PythonFunction, PythonRDD, PythonWorkerUtils, StreamingPythonRunner}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Python.{PYTHON_UNIX_DOMAIN_SOCKET_DIR, PYTHON_UNIX_DOMAIN_SOCKET_ENABLED}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.{BasicPythonArrowOutput, PythonArrowInput, PythonUDFRunner}
import org.apache.spark.sql.execution.python.streaming.TransformWithStateInPySparkPythonRunner.{GroupedInType, InType}
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statefulprocessor.{DriverStatefulProcessorHandleImpl, StatefulProcessorHandleImpl}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils

/**
 * Python runner with no initial state in TransformWithStateInPySpark.
 * Write input data as one single InternalRow in each row in arrow batch.
 */
class TransformWithStateInPySparkPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    _schema: StructType,
    processorHandle: StatefulProcessorHandleImpl,
    _timeZoneId: String,
    initialRunnerConf: Map[String, String],
    override val pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    groupingKeySchema: StructType,
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForEviction: Option[Long])
  extends TransformWithStateInPySparkPythonBaseRunner[InType](
    funcs, evalType, argOffsets, _schema, processorHandle, _timeZoneId,
    initialRunnerConf, pythonMetrics, jobArtifactUUID, groupingKeySchema,
    batchTimestampMs, eventTimeWatermarkForEviction)
  with PythonArrowInput[InType] {

  private var pandasWriter: BaseStreamingArrowWriter = _

  private var currentDataIterator: Iterator[InternalRow] = _

  // Grouping multiple keys into one arrow batch
  override protected def writeNextBatchToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[InType]): Boolean = {
    if (pandasWriter == null) {
      pandasWriter = new BaseStreamingArrowWriter(
        root,
        writer,
        arrowMaxRecordsPerBatch,
        arrowMaxBytesPerBatch
      )
    }

    // If we don't have data left for the current group, move to the next group.
    if (currentDataIterator == null && inputIterator.hasNext) {
      val (_, dataIter) = inputIterator.next()
      currentDataIterator = dataIter
    }

    val startData = dataOut.size()
    val hasInput = if (currentDataIterator != null) {
      var isCurrentBatchFull = false
      // Stop writing when the current arrowBatch is finalized/full. If we have rows left
      while (currentDataIterator.hasNext && !isCurrentBatchFull) {
        val dataRow = currentDataIterator.next()
        isCurrentBatchFull = pandasWriter.writeRow(dataRow)
      }

      if (!currentDataIterator.hasNext) {
        currentDataIterator = null
      }

      true
    } else {
      pandasWriter.finalizeCurrentArrowBatch()
      super[PythonArrowInput].close()
      false
    }
    val deltaData = dataOut.size() - startData
    pythonMetrics("pythonDataSent") += deltaData
    hasInput
  }
}

/**
 * Python runner with initial state in TransformWithStateInPySpark.
 * Write input data as one InternalRow(inputRow, initialState) in each row in arrow batch.
 */
class TransformWithStateInPySparkPythonInitialStateRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    dataSchema: StructType,
    initStateSchema: StructType,
    processorHandle: StatefulProcessorHandleImpl,
    _timeZoneId: String,
    initialRunnerConf: Map[String, String],
    override val pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    groupingKeySchema: StructType,
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForEviction: Option[Long])
  extends TransformWithStateInPySparkPythonBaseRunner[GroupedInType](
    funcs, evalType, argOffsets, dataSchema, processorHandle, _timeZoneId,
    initialRunnerConf, pythonMetrics, jobArtifactUUID, groupingKeySchema,
    batchTimestampMs, eventTimeWatermarkForEviction)
  with PythonArrowInput[GroupedInType] {

  override protected lazy val schema: StructType = new StructType()
    .add("inputData", dataSchema)
    .add("initState", initStateSchema)

  private var pandasWriter: BaseStreamingArrowWriter = _

  private var currentDataIterator: Iterator[InternalRow] = _
  private var isCurrentIterFromInitState: Option[Boolean] = None

  override protected def writeNextBatchToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[GroupedInType]): Boolean = {
    if (pandasWriter == null) {
      pandasWriter = new BaseStreamingArrowWriter(
        root,
        writer,
        arrowMaxRecordsPerBatch,
        arrowMaxBytesPerBatch
      )
    }


    // If we don't have data left for the current group, move to the next group.
    if (currentDataIterator == null && inputIterator.hasNext) {
      val ((_, data), isInitState) = inputIterator.next()
      currentDataIterator = data
      val isPrevIterFromInitState = isCurrentIterFromInitState
      isCurrentIterFromInitState = Some(isInitState)
      if (isPrevIterFromInitState.isDefined &&
        isPrevIterFromInitState.get != isInitState &&
        pandasWriter.getTotalNumRowsForBatch > 0) {
        // So we won't have batches with mixed data and init state.
        pandasWriter.finalizeCurrentArrowBatch()
        return true
      }
    }

    val startData = dataOut.size()

    val hasInput = if (currentDataIterator != null) {
      var isCurrentBatchFull = false
      val isCurrentIterFromInitStateVal = isCurrentIterFromInitState.get
      // Stop writing when the current arrowBatch is finalized/full. If we have rows left
      while (currentDataIterator.hasNext && !isCurrentBatchFull) {
        val dataRow = currentDataIterator.next()
        isCurrentBatchFull = if (isCurrentIterFromInitStateVal) {
          pandasWriter.writeRow(InternalRow(null, dataRow))
        } else {
          pandasWriter.writeRow(InternalRow(dataRow, null))
        }
      }

      if (!currentDataIterator.hasNext) {
        currentDataIterator = null
      }

      true
    } else {
      if (pandasWriter.getTotalNumRowsForBatch > 0) {
        pandasWriter.finalizeCurrentArrowBatch()
      }
      super[PythonArrowInput].close()
      false
    }

    val deltaData = dataOut.size() - startData
    pythonMetrics("pythonDataSent") += deltaData
    hasInput
  }
}

/**
 * Base Python runner implementation for TransformWithStateInPySpark.
 */
abstract class TransformWithStateInPySparkPythonBaseRunner[I](
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    _schema: StructType,
    processorHandle: StatefulProcessorHandleImpl,
    _timeZoneId: String,
    initialRunnerConf: Map[String, String],
    override val pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    groupingKeySchema: StructType,
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForEviction: Option[Long])
  extends BasePythonRunner[I, ColumnarBatch](
    funcs.map(_._1), evalType, argOffsets, jobArtifactUUID, pythonMetrics)
  with PythonArrowInput[I]
  with BasicPythonArrowOutput
  with TransformWithStateInPySparkPythonRunnerUtils
  with Logging {

  protected val sqlConf = SQLConf.get
  protected val arrowMaxRecordsPerBatch = sqlConf.arrowMaxRecordsPerBatch
  protected val arrowMaxBytesPerBatch = sqlConf.arrowMaxBytesPerBatch

  override protected val runnerConf: Map[String, String] = initialRunnerConf +
    (SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> arrowMaxRecordsPerBatch.toString) +
    (SQLConf.ARROW_EXECUTION_MAX_BYTES_PER_BATCH.key -> arrowMaxBytesPerBatch.toString)

  // Use lazy val to initialize the fields before these are accessed in [[PythonArrowInput]]'s
  // constructor.
  override protected lazy val schema: StructType = _schema
  override protected lazy val timeZoneId: String = _timeZoneId
  override protected val errorOnDuplicatedFieldNames: Boolean = true
  override protected val largeVarTypes: Boolean = sqlConf.arrowUseLargeVarTypes

  override protected def handleMetadataBeforeExec(stream: DataOutputStream): Unit = {
    super.handleMetadataBeforeExec(stream)
    // Write the port/path number for state server
    if (isUnixDomainSock) {
      stream.writeInt(-1)
      PythonWorkerUtils.writeUTF(stateServerSocketPath, stream)
    } else {
      stream.writeInt(stateServerSocketPort)
    }
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
      new TransformWithStateInPySparkStateServer(stateServerSocket, processorHandle,
        groupingKeySchema,
        sqlConf.arrowTransformWithStateInPySparkMaxStateRecordsPerBatch,
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
 * TransformWithStateInPySpark driver side Python runner. Similar as executor side runner,
 * will start a new daemon thread on the Python runner to run state server.
 */
class TransformWithStateInPySparkPythonPreInitRunner(
    func: PythonFunction,
    workerModule: String,
    groupingKeySchema: StructType,
    processorHandleImpl: DriverStatefulProcessorHandleImpl)
  extends StreamingPythonRunner(func, "", "", workerModule)
  with TransformWithStateInPySparkPythonRunnerUtils
  with Logging {
  protected val sqlConf = SQLConf.get

  private var dataOut: DataOutputStream = _
  private var dataIn: DataInputStream = _

  private var daemonThread: Thread = _

  override def init(): (DataOutputStream, DataInputStream) = {
    val result = super.init()
    dataOut = result._1
    dataIn = result._2

    // start state server, update socket port/path
    startStateServer()
    (dataOut, dataIn)
  }

  def process(): Unit = {
    // Also write the port/path number for state server
    if (isUnixDomainSock) {
      dataOut.writeInt(-1)
      PythonWorkerUtils.writeUTF(stateServerSocketPath, dataOut)
    } else {
      dataOut.writeInt(stateServerSocketPort)
    }
    PythonWorkerUtils.writeUTF(groupingKeySchema.json, dataOut)
    dataOut.flush()

    val resFromPython = dataIn.readInt()
    if (resFromPython != 0) {
      val errMessage = PythonWorkerUtils.readUTF(dataIn)
      throw new StreamingPythonRunnerInitializationException(resFromPython, errMessage)
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
          new TransformWithStateInPySparkStateServer(stateServerSocket, processorHandleImpl,
            groupingKeySchema,
            sqlConf.arrowTransformWithStateInPySparkMaxStateRecordsPerBatch).run()
        } catch {
          case e: Exception =>
            throw new SparkException("TransformWithStateInPySpark state server " +
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
 * TransformWithStateInPySpark Python runner utils functions for handling a state server
 * in a new daemon thread.
 */
trait TransformWithStateInPySparkPythonRunnerUtils extends Logging {
  protected val isUnixDomainSock: Boolean = SparkEnv.get.conf.get(PYTHON_UNIX_DOMAIN_SOCKET_ENABLED)
  protected var stateServerSocketPort: Int = -1
  protected var stateServerSocketPath: String = null
  protected var stateServerSocket: ServerSocketChannel = null
  protected def initStateServer(): Unit = {
    var failed = false
    try {
      if (isUnixDomainSock) {
        val sockPath = new File(
          SparkEnv.get.conf.get(PYTHON_UNIX_DOMAIN_SOCKET_DIR)
            .getOrElse(System.getProperty("java.io.tmpdir")),
          s".${UUID.randomUUID()}.sock")
        stateServerSocket = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
        stateServerSocket.bind(UnixDomainSocketAddress.of(sockPath.getPath), 1)
        sockPath.deleteOnExit()
        stateServerSocketPath = sockPath.getPath
      } else {
        stateServerSocket = ServerSocketChannel.open()
          .bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 1)
        stateServerSocketPort = stateServerSocket.socket().getLocalPort
      }
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

  protected def closeServerSocketChannelSilently(stateServerSocket: ServerSocketChannel): Unit = {
    try {
      logInfo(log"closing the state server socket")
      stateServerSocket.close()
      if (stateServerSocketPath != null) {
        new File(stateServerSocketPath).delete
      }
    } catch {
      case e: Exception =>
        logError(log"failed to close state server socket", e)
    }
  }
}

object TransformWithStateInPySparkPythonRunner {
  type InType = (InternalRow, Iterator[InternalRow])

  // ((key, rows), isInitState)
  type GroupedInType = ((InternalRow, Iterator[InternalRow]), Boolean)
}
