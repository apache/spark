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

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.nio.channels.Channels

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.ipc.ArrowStreamReader

import org.apache.spark.SparkEnv
import org.apache.spark.api.python.{PythonFunction, PythonWorker, PythonWorkerFactory, PythonWorkerUtils, SpecialLengths}
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.internal.LogKeys.PYTHON_EXEC
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.internal.config.Python.PYTHON_AUTH_SOCKET_TIMEOUT
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

object PythonStreamingSourceRunner {
  // When the python process for python_streaming_source_runner receives one of the
  // integers below, it will invoke the corresponding function of StreamReader instance.
  val INITIAL_OFFSET_FUNC_ID = 884
  val LATEST_OFFSET_FUNC_ID = 885
  val PARTITIONS_FUNC_ID = 886
  val COMMIT_FUNC_ID = 887
  // Status code for JVM to decide how to receive prefetched record batches
  // for simple stream reader.
  val PREFETCHED_RECORDS_NOT_FOUND = 0
  val NON_EMPTY_PYARROW_RECORD_BATCHES = 1
  val EMPTY_PYARROW_RECORD_BATCHES = 2
}

/**
 * This class is a proxy to invoke methods in Python DataSourceStreamReader from JVM.
 * A runner spawns a python worker process. In the main function, set up communication
 * between JVM and python process through socket and create a DataSourceStreamReader instance.
 * In an infinite loop, the python worker process poll information(function name and parameters)
 * from the socket, invoke the corresponding method of StreamReader and send return value to JVM.
 */
class PythonStreamingSourceRunner(
    func: PythonFunction,
    outputSchema: StructType) extends Logging  {
  val workerModule = "pyspark.sql.streaming.python_streaming_source_runner"

  private val conf = SparkEnv.get.conf
  private val bufferSize: Int = conf.get(BUFFER_SIZE)
  private val authSocketTimeout = conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)

  private val envVars: java.util.Map[String, String] = func.envVars
  private val pythonExec: String = func.pythonExec
  private var pythonWorker: Option[PythonWorker] = None
  private var pythonWorkerFactory: Option[PythonWorkerFactory] = None
  private val pythonVer: String = func.pythonVer

  private var dataOut: DataOutputStream = null
  private var dataIn: DataInputStream = null

  import PythonStreamingSourceRunner._

  /**
   * Initializes the Python worker for running the streaming source.
   */
  def init(): Unit = {
    logInfo(log"Initializing Python runner pythonExec: ${MDC(PYTHON_EXEC, pythonExec)}")
    val env = SparkEnv.get

    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir)

    envVars.put("SPARK_AUTH_SOCKET_TIMEOUT", authSocketTimeout.toString)
    envVars.put("SPARK_BUFFER_SIZE", bufferSize.toString)

    val workerFactory =
      new PythonWorkerFactory(pythonExec, workerModule, envVars.asScala.toMap, false)
    val (worker: PythonWorker, _) = workerFactory.createSimpleWorker(blockingMode = true)
    pythonWorker = Some(worker)
    pythonWorkerFactory = Some(workerFactory)

    val stream = new BufferedOutputStream(
      Channels.newOutputStream(pythonWorker.get.channel), bufferSize)
    dataOut = new DataOutputStream(stream)

    PythonWorkerUtils.writePythonVersion(pythonVer, dataOut)

    val pythonIncludes = func.pythonIncludes.asScala.toSet
    PythonWorkerUtils.writeSparkFiles(Some("streaming_job"), pythonIncludes, dataOut)

    // Send the user function to python process
    PythonWorkerUtils.writePythonFunction(func, dataOut)

    // Send output schema
    PythonWorkerUtils.writeUTF(outputSchema.json, dataOut)

    dataOut.writeInt(SQLConf.get.arrowMaxRecordsPerBatch)

    dataOut.flush()

    dataIn = new DataInputStream(
      new BufferedInputStream(Channels.newInputStream(pythonWorker.get.channel), bufferSize))

    val initStatus = dataIn.readInt()
    if (initStatus == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.pythonDataSourceError(
        action = "plan", tpe = "initialize source", msg = msg)
    }
  }

  /**
   * Invokes latestOffset() function of the stream reader and receive the return value.
   */
  def latestOffset(): String = {
    dataOut.writeInt(LATEST_OFFSET_FUNC_ID)
    dataOut.flush()
    val len = dataIn.readInt()
    if (len == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryExecutionErrors.pythonStreamingDataSourceRuntimeError(
        action = "latestOffset", msg)
    }
    PythonWorkerUtils.readUTF(len, dataIn)
  }

  /**
   * Invokes initialOffset() function of the stream reader and receive the return value.
   */
  def initialOffset(): String = {
    dataOut.writeInt(INITIAL_OFFSET_FUNC_ID)
    dataOut.flush()
    val len = dataIn.readInt()
    if (len == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryExecutionErrors.pythonStreamingDataSourceRuntimeError(
        action = "initialOffset", msg)
    }
    PythonWorkerUtils.readUTF(len, dataIn)
  }

  /**
   * Invokes partitions(start, end) function of the stream reader and receive the return value.
   */
  def partitions(start: String, end: String): (Array[Array[Byte]], Option[Iterator[InternalRow]]) =
  {
    dataOut.writeInt(PARTITIONS_FUNC_ID)
    PythonWorkerUtils.writeUTF(start, dataOut)
    PythonWorkerUtils.writeUTF(end, dataOut)
    dataOut.flush()
    // Receive the list of partitions, if any.
    val pickledPartitions = ArrayBuffer.empty[Array[Byte]]
    val numPartitions = dataIn.readInt()
    if (numPartitions == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryExecutionErrors.pythonStreamingDataSourceRuntimeError(
        action = "planPartitions", msg)
    }
    for (_ <- 0 until numPartitions) {
      val pickledPartition: Array[Byte] = PythonWorkerUtils.readBytes(dataIn)
      pickledPartitions.append(pickledPartition)
    }
    val prefetchedRecordsStatus = dataIn.readInt()
    val iter: Option[Iterator[InternalRow]] = prefetchedRecordsStatus match {
      case NON_EMPTY_PYARROW_RECORD_BATCHES => Some(readArrowRecordBatches())
      case PREFETCHED_RECORDS_NOT_FOUND => None
      case EMPTY_PYARROW_RECORD_BATCHES => Some(Iterator.empty)
      case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
        val msg = PythonWorkerUtils.readUTF(dataIn)
        throw QueryExecutionErrors.pythonStreamingDataSourceRuntimeError(
          action = "planPartitions", msg)
      case _ =>
        throw QueryExecutionErrors.pythonStreamingDataSourceRuntimeError(
          action = "planPartitions", s"unknown status code $prefetchedRecordsStatus")
    }
    (pickledPartitions.toArray, iter)
  }

  /**
   * Invokes commit(end) function of the stream reader and receive the return value.
   */
  def commit(end: String): Unit = {
    dataOut.writeInt(COMMIT_FUNC_ID)
    PythonWorkerUtils.writeUTF(end, dataOut)
    dataOut.flush()
    val status = dataIn.readInt()
    if (status == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryExecutionErrors.pythonStreamingDataSourceRuntimeError(
        action = "commitSource", msg)
    }
  }

  /**
   * Stop the python worker process and invoke stop() on stream reader.
   */
  def stop(): Unit = {
    logInfo(log"Stopping streaming runner for module: " +
      log"${MDC(LogKeys.MODULE_NAME, workerModule)}.")
    try {
      pythonWorkerFactory.foreach { factory =>
        pythonWorker.foreach { worker =>
          factory.stopWorker(worker)
          factory.stop()
        }
      }
    } catch {
      case e: Exception =>
        logError("Exception when trying to kill worker", e)
    }
  }

  private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
    s"stream reader for $pythonExec", 0, Long.MaxValue)

  def readArrowRecordBatches(): Iterator[InternalRow] = {
    val status = dataIn.readInt()
    status match {
      case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
        val msg = PythonWorkerUtils.readUTF(dataIn)
        throw QueryExecutionErrors.pythonStreamingDataSourceRuntimeError(
        action = "prefetchArrowBatches", msg)
      case SpecialLengths.START_ARROW_STREAM =>
      case _ =>
        throw QueryExecutionErrors.pythonStreamingDataSourceRuntimeError(
          action = "prefetchArrowBatches", s"unknown status code $status")
    }
    val reader = new ArrowStreamReader(dataIn, allocator)
    val root = reader.getVectorSchemaRoot()
    // When input is empty schema can't be read.
    val schema = ArrowUtils.fromArrowSchema(root.getSchema())
    assert(schema == outputSchema)

    val vectors = root.getFieldVectors().asScala.map { vector =>
      new ArrowColumnVector(vector)
    }.toArray[ColumnVector]
    val rows = ArrayBuffer[InternalRow]()
    while (reader.loadNextBatch()) {
      val batch = new ColumnarBatch(vectors)
      batch.setNumRows(root.getRowCount)
      // Need to copy the row because the ColumnarBatch row iterator use
      // the same underlying Internal row.
      rows.appendAll(batch.rowIterator().asScala.map(_.copy()))
    }
    reader.close(false)
    rows.iterator
  }
}
