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
package org.apache.spark.sql.execution.datasources.v2.python

import java.io.{DataOutputStream, InterruptedIOException, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.{IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}

import org.apache.spark.{JobArtifactSet, SparkEnv, SparkException, TaskContext}
import org.apache.spark.api.python.{PythonException, PythonFunction, PythonWorkerUtils, SpecialLengths}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Python.PYTHON_TASK_KILL_TIMEOUT
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{ExternalLookup, ExternalLookupReader, ExternalLookupReaderFactory, ExternalLookupResultCursor}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.python.{ArrowPythonRunner, PythonExternalLookupProtocol, PythonExternalLookupWorker, WorkerReadTimeoutException}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.{ByteBufferOutputStream, ThreadUtils}

/**
 * The external lookup of a Python data source, planned on the driver by
 * [[PythonScanBuilder.selectLookupPredicates]].
 *
 * @param source the Python data source
 * @param plan the lookup planned in Python
 * @param readSchema the schema of the rows that the lookups return, the schema of the table
 */
class PythonExternalLookup(
    source: UserDefinedPythonDataSource,
    plan: PythonDataSourceLookupPlan,
    readSchema: StructType) extends ExternalLookup {
  // Captured on the driver, as `PythonBatch` does, for the Python workers on the executors. A
  // streaming query plans its micro-batches before `SQLExecution` sets the job artifact state of
  // its session, and the lookup is planned then, unlike the batch of a scan. The artifacts of the
  // active session, the session of the micro-batch jobs, then give the same state, so that
  // `SparkFiles.get` finds the files of a Spark Connect session.
  private val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState
    .orElse(SparkSession.getActiveSession.flatMap(session => Option(session.artifactManager.state)))
    .map(_.uuid)
  private val sessionUUID = {
    SparkSession.getActiveSession.collect {
      case session if session.sessionState.conf.pythonWorkerLoggingEnabled =>
        session.sessionUUID
    }
  }

  override def maxParameterRowsPerRequest(): Int = plan.maxParameterRowsPerRequest

  override def createReaderFactory(): ExternalLookupReaderFactory = {
    val conf = SQLConf.get
    new PythonExternalLookupReaderFactory(
      source.createPythonFunction(plan.reader),
      plan.parameterTypes,
      readSchema,
      conf.arrowMaxRecordsPerBatch,
      ArrowPythonRunner.getPythonRunnerConfMap(conf),
      jobArtifactUUID,
      sessionUUID)
  }
}

/**
 * Creates the readers that look up rows on the executors with the planned Python lookup reader.
 * The session configurations are captured on the driver, because `SQLConf.get` on an executor is
 * not the session's.
 *
 * @param reader the planned lookup reader, pickled, with the Python environment of the source
 * @param parameterTypes the data type of each field of a parameter row
 * @param readSchema the schema of the rows that the lookups return
 * @param maxRecordsPerBatch the maximum number of rows of an Arrow batch
 * @param pythonRunnerConf the session configurations of the Python worker
 * @param jobArtifactUUID the artifacts of the session, for the Python worker
 * @param sessionUUID the session, when Python worker logging is enabled
 */
class PythonExternalLookupReaderFactory(
    val reader: PythonFunction,
    val parameterTypes: Seq[DataType],
    val readSchema: StructType,
    val maxRecordsPerBatch: Int,
    val pythonRunnerConf: Map[String, String],
    val jobArtifactUUID: Option[String],
    val sessionUUID: Option[String]) extends ExternalLookupReaderFactory {
  override def createReader(): ExternalLookupReader = new PythonExternalLookupReader(this)
}

/**
 * Looks up rows with the planned Python lookup reader, for one input partition of a lookup join.
 *
 * The reader starts one Python worker, a [[PythonExternalLookupWorker]], at its first request,
 * and keeps it for all its requests, so Python unpickles the planned lookup reader once and its
 * connections live for the whole partition. Each request is one Arrow batch of the parameter
 * rows; its cursor reads the response batch by batch, as Python writes it.
 *
 * A cursor that Spark closes before the end of its response sends a cancel and reads the rest of
 * the response, which Python then ends early. If the response does not end within
 * `spark.python.task.killTimeout`, the reader destroys the worker, and starts a new one at the
 * next request.
 *
 * Closing the reader on the thread that made its last call, while the task is not interrupted,
 * ends the worker gracefully: Python calls `close()` on the lookup reader, and the worker is
 * released for reuse when `spark.python.worker.reuse` is on. If Python has not finished within
 * `spark.python.task.killTimeout`, the reader destroys the worker, and the close fails. A close
 * that fails while the task fails already, in the task's completion listener, is added to the
 * task's failure as suppressed, so it never replaces that failure. Any other close destroys the
 * worker without waiting, and Python runs no `close()`: a close from another thread, while that
 * thread may still drive the join, does no I/O on the worker. A call that is in progress then
 * fails.
 *
 * The calls that use the worker or the Arrow memory hold `ioLock`, which a close from another
 * thread never waits for: when a call holds it, the close destroys the worker, which ends the
 * call, and the call releases the memory when it ends. Arrow memory is read and freed only while
 * `ioLock` is held: a cursor copies each row out of it in `next()`, so a close from any thread,
 * at any time, never frees the memory of a row that Spark may still read.
 *
 * The reader registers one interrupt listener, which destroys the worker if the task has not
 * completed within `spark.python.task.killTimeout` after it was killed, also when the task thread
 * is not interrupted, and one completion listener, which closes the reader. Both are per reader,
 * not per request.
 */
private class PythonExternalLookupReader(factory: PythonExternalLookupReaderFactory)
  extends ExternalLookupReader with Logging {
  import PythonExternalLookupProtocol._
  import PythonExternalLookupReader._

  private val context = Option(TaskContext.get()).getOrElse {
    throw SparkException.internalError("A Python external lookup reader runs only in a task")
  }
  private val killTimeoutMs = SparkEnv.get.conf.get(PYTHON_TASK_KILL_TIMEOUT)
  private val binaryAsBytes = factory.pythonRunnerConf
    .getOrElse(SQLConf.PYSPARK_BINARY_AS_BYTES.key,
      SQLConf.PYSPARK_BINARY_AS_BYTES.defaultValueString).toBoolean

  private val closed = new AtomicBoolean(false)
  private val ioLock = new ReentrantLock()

  // Guarded by `ioLock`, except that a close from another thread reads `worker` to destroy it.
  @volatile private var worker: PythonExternalLookupWorker = null
  private var ownerThread: Thread = null
  private var activeCursor: PythonExternalLookupResultCursor = null
  private var memoryReleased = false

  // The bytes of the workers that this reader has replaced, which any thread may read.
  @volatile private var bytesSentBefore = 0L
  @volatile private var bytesReceivedBefore = 0L

  private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
    s"Python external lookup reader of task ${context.taskAttemptId()}", 0, Long.MaxValue)
  private val parameterSchema = StructType(factory.parameterTypes.zipWithIndex.map {
    case (dataType, index) => StructField(s"param_$index", dataType)
  })
  private val parameterRoot = VectorSchemaRoot.create(
    ArrowUtils.toArrowSchema(
      parameterSchema, "UTC", errorOnDuplicatedFieldNames = true, largeVarTypes = false),
    allocator)
  private val parameterWriter = ArrowWriter.create(parameterRoot)

  context.addTaskInterruptListener((_: TaskContext, _: String) => scheduleKill())
  context.addTaskCompletionListener[Unit](_ => close())

  override def lookupBatch(uniqueParameters: Array[InternalRow]): ExternalLookupResultCursor =
    withIo {
      context.killTaskIfInterrupted()
      // Spark closes each cursor before its next request; a cursor that another thread closed
      // has not read its response, so it is read here.
      Option(activeCursor).foreach(_.finish())
      activeCursor = null
      if (worker == null || worker.isReleasedOrClosed) startWorker()
      // The parameter rows are copied into the request before this call returns.
      worker.send(request(uniqueParameters))
      val cursor = new PythonExternalLookupResultCursor(this, worker)
      activeCursor = cursor
      cursor
    }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    val current = worker
    val sent = bytesSentBefore + (if (current == null) 0L else current.bytesSent)
    val received = bytesReceivedBefore + (if (current == null) 0L else current.bytesReceived)
    Array[CustomTaskMetric](
      new PythonCustomTaskMetric("pythonDataSent", sent),
      new PythonCustomTaskMetric("pythonDataReceived", received))
  }

  override def close(): Unit = if (closed.compareAndSet(false, true)) {
    if (ioLock.tryLock()) {
      try {
        val current = worker
        if (current != null && !current.isReleasedOrClosed) {
          if ((Thread.currentThread() eq ownerThread) && !context.isInterrupted()) {
            guarded(current) {
              Option(activeCursor).foreach(_.finish())
              if (!current.isReleasedOrClosed) current.closeGracefully(killTimeoutMs)
            }
          } else {
            current.destroy()
          }
        }
      } finally {
        try releaseMemory() finally ioLock.unlock()
      }
    } else {
      // A call is in progress on another thread. Destroying the worker ends it, and the call
      // releases the memory when it ends.
      Option(worker).foreach(_.destroy())
    }
  }

  /** Runs a call that uses the worker or the Arrow memory, as the thread that drives the reader. */
  private[python] def withIo[T](call: => T): T = {
    ioLock.lock()
    try {
      if (closed.get()) {
        throw SparkException.internalError("External lookup reader used after it was closed")
      }
      ownerThread = Thread.currentThread()
      call
    } finally {
      // A close from another thread during the call left the memory to the call.
      try {
        if (closed.get()) releaseMemory()
      } finally {
        ioLock.unlock()
      }
    }
  }

  /** Whether the calling thread made the last call, holding `ioLock`. */
  private[python] def isOwner: Boolean = ioLock.isHeldByCurrentThread &&
    (Thread.currentThread() eq ownerThread)

  private[python] def tryLock(): Boolean = ioLock.tryLock()
  private[python] def unlock(): Unit = ioLock.unlock()
  private[python] def isClosed: Boolean = closed.get()
  private[python] def readerAllocator = allocator
  private[python] def numValueColumns: Int = factory.readSchema.length
  private[python] def cancelTimeoutMs: Long = killTimeoutMs

  /**
   * Runs `call` on the worker, and destroys the worker if it fails: its stream is then at an
   * unknown position. An I/O failure is reported as a crash of the worker, as
   * `PythonPlannerRunner` reports it.
   */
  private[python] def guarded[T](current: PythonExternalLookupWorker)(call: => T): T = {
    try {
      call
    } catch {
      case e: PythonException =>
        current.destroy()
        throw e
      case e @ (_: InterruptedIOException | _: WorkerReadTimeoutException) =>
        current.destroy()
        throw e
      case e: IOException =>
        current.destroy()
        // After a close from another thread, the failure is that close, not a crash.
        if (closed.get()) throw e else throw current.ioError(e)
      case e: Throwable =>
        current.destroy()
        throw e
    }
  }

  private def startWorker(): Unit = {
    val previous = worker
    if (previous != null) {
      bytesSentBefore += previous.bytesSent
      bytesReceivedBefore += previous.bytesReceived
      worker = null
    }
    worker = PythonExternalLookupWorker.start(
      factory.reader, context, factory.pythonRunnerConf, factory.sessionUUID,
      factory.jobArtifactUUID) { dataOut =>
      PythonWorkerUtils.writeUTF(parameterSchema.json, dataOut)
      PythonWorkerUtils.writeUTF(factory.readSchema.json, dataOut)
      dataOut.writeInt(factory.maxRecordsPerBatch)
      dataOut.writeBoolean(binaryAsBytes)
    }
  }

  /** A request of the parameter rows: its tag, and one Arrow stream with one batch of them. */
  private def request(uniqueParameters: Array[InternalRow]): ByteBuffer = {
    val buffer = new ByteBufferOutputStream()
    val dataOut = new DataOutputStream(buffer)
    dataOut.writeInt(LOOKUP_REQUEST)
    try {
      uniqueParameters.foreach(parameterWriter.write)
      parameterWriter.finish()
      val writer = new ArrowStreamWriter(parameterRoot, null, dataOut)
      writer.start()
      writer.writeBatch()
      writer.end()
    } finally {
      parameterWriter.reset()
    }
    dataOut.close()
    buffer.toByteBuffer
  }

  private def scheduleKill(): Unit = {
    killScheduler.schedule(new Runnable {
      override def run(): Unit = if (!context.isCompleted()) {
        Option(worker).filterNot(_.isReleasedOrClosed).foreach { current =>
          logWarning("Destroying the Python lookup worker of a task that was killed and has " +
            s"not completed within $killTimeoutMs ms")
          current.destroy()
        }
      }
    }, killTimeoutMs, TimeUnit.MILLISECONDS)
  }

  /** Releases the Arrow memory, once, holding `ioLock`. */
  private def releaseMemory(): Unit = if (!memoryReleased) {
    memoryReleased = true
    try {
      Option(activeCursor).foreach(_.releaseArrow())
      parameterRoot.close()
    } finally {
      allocator.close()
    }
  }
}

private object PythonExternalLookupReader {
  /** Destroys the workers of killed tasks that do not complete in time. */
  lazy val killScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("python-external-lookup-kill")
}

/**
 * A cursor over the response to one request, read batch by batch.
 *
 * [[next]] copies the ordinal and the values of the next row out of the Arrow batch, holding the
 * reader's `ioLock`, and [[inputOrdinal]] and [[get]] return the copies. The
 * `ExternalLookupReader` contract lets Spark call them, and read the row that [[get]] returned
 * last, while or after another thread closes the cursor and the reader, which frees the Arrow
 * memory; the copies stay readable then. The copy keeps every value as the Arrow row has it,
 * nulls included, so Spark's checks of the results are unchanged.
 */
private class PythonExternalLookupResultCursor(
    reader: PythonExternalLookupReader,
    worker: PythonExternalLookupWorker) extends ExternalLookupResultCursor {
  import PythonExternalLookupProtocol._

  // Guarded by the reader's `ioLock`.
  private var arrowReader: ArrowStreamReader = null
  private var ordinals: IntVector = null
  private var values: ColumnarBatch = null
  private var rowIndex = -1
  private var numRows = 0
  // The copies of the current row, which do not use Arrow memory.
  private var currentOrdinal = -1
  private var currentRow: InternalRow = null
  // Whether the response was read to its end, or will not be, because the worker is gone.
  private var finished = false
  @volatile private var closed = false

  override def next(): Boolean = reader.withIo {
    if (closed) throw SparkException.internalError("External lookup cursor used after it closed")
    reader.guarded(worker) {
      rowIndex += 1
      while (rowIndex >= numRows && !finished) loadNextBatch()
      if (!finished) {
        currentOrdinal = ordinals.get(rowIndex)
        currentRow = values.getRow(rowIndex).copy()
      }
      !finished
    }
  }

  override def inputOrdinal(): Int = currentOrdinal

  override def get(): InternalRow = currentRow

  /**
   * On the thread that drives the reader, reads the rest of the response, which it cancels.
   * Another thread does no I/O on the worker: it marks the cursor closed, and the reader's next
   * call, or its close, reads the rest or destroys the worker.
   */
  override def close(): Unit = if (!closed) {
    closed = true
    if (reader.tryLock()) {
      try {
        if (reader.isClosed) {
          releaseArrow()
        } else if (reader.isOwner) {
          finish()
        }
      } finally {
        reader.unlock()
      }
    }
  }

  /**
   * Reads the rest of the response, after a cancel, holding the reader's `ioLock`. If the
   * response does not end within the kill timeout, as when the Python reader does not return
   * from a blocking call, the worker is destroyed.
   */
  private[python] def finish(): Unit = {
    if (!finished && !worker.isReleasedOrClosed) {
      reader.guarded(worker) {
        worker.setReadTimeout(reader.cancelTimeoutMs)
        try {
          if (arrowReader == null) startResponse()
          worker.sendCancel()
          while (!finished) loadNextBatch()
        } catch {
          case _: WorkerReadTimeoutException =>
            worker.destroy()
        } finally {
          worker.setReadTimeout(-1)
        }
      }
    }
    finished = true
    releaseArrow()
  }

  /** Releases the Arrow memory of the response, holding the reader's `ioLock`. */
  private[python] def releaseArrow(): Unit = if (arrowReader != null) {
    val current = arrowReader
    arrowReader = null
    current.close(false)
  }

  /** Reads the start of the response, once Python has read the request, and its schema. */
  private def startResponse(): Unit = {
    worker.readReplyInt() match {
      case SpecialLengths.START_ARROW_STREAM =>
      case other => throw worker.protocolError(s"response start $other")
    }
    arrowReader = new ArrowStreamReader(worker.dataIn, reader.readerAllocator)
    val root = arrowReader.getVectorSchemaRoot
    val vectors = root.getFieldVectors.asScala
    if (vectors.length != reader.numValueColumns + 1 || !vectors.head.isInstanceOf[IntVector]) {
      throw worker.protocolError(s"response schema ${root.getSchema}")
    }
    ordinals = vectors.head.asInstanceOf[IntVector]
    values = new ColumnarBatch(vectors.tail.map(new ArrowColumnVector(_)).toArray[ColumnVector])
  }

  /** Loads the next batch of the response, or reads its end. */
  private def loadNextBatch(): Unit = {
    if (arrowReader == null) startResponse()
    if (arrowReader.loadNextBatch()) {
      numRows = arrowReader.getVectorSchemaRoot.getRowCount
      values.setNumRows(numRows)
      rowIndex = 0
    } else {
      releaseArrow()
      endResponse()
    }
  }

  /** Reads the end of the response and its status. */
  private def endResponse(): Unit = {
    worker.readReplyInt() match {
      case LOOKUP_RESPONSE_END =>
      case other => throw worker.protocolError(s"response end $other")
    }
    worker.dataIn.readInt() match {
      case STATUS_COMPLETE | STATUS_CANCELLED =>
      case other => throw worker.protocolError(s"response status $other")
    }
    finished = true
  }
}
