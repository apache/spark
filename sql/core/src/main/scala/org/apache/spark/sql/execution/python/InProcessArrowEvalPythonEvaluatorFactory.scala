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

import java.io.File
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import com.google.common.util.concurrent.Uninterruptibles
import org.apache.arrow.c.{ArrowArray, ArrowSchema, BaseStruct}
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.memory.MemoryConsumer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, PythonUDF, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * Evaluates scalar Python UDFs using Arrow CDI in the executor process. Only UDF arguments
 * are converted to Arrow. Original rows are buffered in a spillable queue and joined with
 * the results, unless all of them are UDF arguments that read back from Arrow unchanged.
 * Each batch owns its Arrow buffers so Python can safely retain input arrays.
 *
 * The evaluator owns its queue, so that cleanup at task completion is coordinated with a
 * consumer on another thread, such as a pipelined Python writer or a TRANSFORM feed thread.
 */
class InProcessArrowEvalPythonEvaluatorFactory(
    childOutput: Seq[Attribute],
    udfs: Seq[PythonUDF],
    output: Seq[Attribute],
    batchSize: Int,
    maxBytes: Long,
    timeZoneId: String,
    largeVarTypes: Boolean,
    hideTraceback: Boolean,
    simplifiedTraceback: Boolean,
    tracebackWithLocals: Boolean,
    fullValidation: Boolean,
    metrics: Map[String, SQLMetric])
  extends EvalPythonEvaluatorFactory(childOutput, udfs, output) {

  private[python] def runtimeSession: InProcessPythonRuntime.InterpreterSession =
    InProcessPythonRuntime.currentSession

  /** Unused: `evaluateJoined` always evaluates the UDFs. */
  override protected def evaluate(
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argMetas: Array[Array[ArgumentMetadata]],
      rows: Iterator[InternalRow],
      inputSchema: StructType,
      context: TaskContext): Iterator[InternalRow] =
    throw SparkException.internalError("In-process UDFs are evaluated with their input rows")

  override protected def evaluateJoined(
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argMetas: Array[Array[ArgumentMetadata]],
      rows: Iterator[InternalRow],
      inputs: Seq[Expression],
      inputSchema: StructType,
      context: TaskContext): Option[Iterator[InternalRow]] = {
    import InProcessArrowEvalPythonEvaluatorFactory.{Buffered, ReadBack, readsBack}
    val inputColumns = inputs.length == childOutput.length && inputs.zip(childOutput).forall {
      case (a: Attribute, c) => a.exprId == c.exprId
      case _ => false
    }
    // If all input columns are UDF arguments, they are written to Arrow regardless. Read them
    // back from the exported input vectors instead of buffering every input row, if their
    // values read back from Arrow exactly as written and as fast as an unsafe row copy.
    val joinInput = if (inputColumns && inputSchema.forall(f => readsBack(f.dataType))) {
      ReadBack
    } else if (inputColumns) {
      Buffered(None)
    } else {
      // Each projected row is written to Arrow before the next input row is pulled, so the
      // arguments go into a reused buffer rather than being copied value by value.
      val projection = UnsafeProjection.create(inputs, childOutput)
      projection.initialize(context.partitionId())
      Buffered(Some(projection))
    }
    Some(evaluateBatches(funcs, argMetas, rows, inputSchema, context, joinInput))
  }

  private[python] def evaluateBatches(
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argMetas: Array[Array[ArgumentMetadata]],
      rows: Iterator[InternalRow],
      inputSchema: StructType,
      context: TaskContext,
      joinInput: InProcessArrowEvalPythonEvaluatorFactory.JoinInput): Iterator[InternalRow] = {
    import InProcessArrowEvalPythonEvaluatorFactory.{Buffered, ReadBack}
    ArrowUtils.failDuplicatedFieldNames(inputSchema)
    val functions = funcs.map { case (chain, _) =>
      if (chain.funcs.size != 1) {
        throw SparkException.internalError(
          "In-process UDF chains must use separate evaluation nodes")
      }
      chain.funcs.head
    }
    val inputOrdinals = argMetas.map(_.map(_.offset))
    def checkCancellation(): Unit = context.killTaskIfInterrupted()

    val expectedFields = udfs.map { udf =>
      ArrowUtils.toArrowField("result", udf.dataType, true, timeZoneId, largeVarTypes)
    }
    val processingTime = new InProcessArrowEvalPythonEvaluatorFactory.NanosecondTimer(
      metrics("pythonProcessingTime"))
    val initTime = new InProcessArrowEvalPythonEvaluatorFactory.NanosecondTimer(
      metrics("pythonInitTime"))
    val arrowSchema = ArrowUtils.toArrowSchema(inputSchema, timeZoneId, largeVarTypes)
    // Capture before consuming input: an old task must never join a later context's session.
    val runtime = runtimeSession
    // Rows are copied out of the queue and Arrow vectors before they are returned, so they
    // remain valid after task completion releases those, on whichever thread consumes them.
    val resultProj = UnsafeProjection.create(output, output)
    // Spill files go into a directory of the queue's own, created with the first disk queue, so
    // that task completion can delete them when it cannot close the queue.
    @volatile var spillDir: File = null
    // Guarded by the queue's monitor.
    var queueAbandoned = false
    val (queue, projection) = joinInput match {
      case Buffered(projection) =>
        val localDir = new File(Utils.getLocalDir(SparkEnv.get.conf))
        val serializerManager = SparkEnv.get.serializerManager
        // Only the consumer holding the iterator's lock adds and removes rows.
        val queue = new HybridRowQueue(context.taskMemoryManager(), localDir,
            childOutput.length, serializerManager, lockFree = true) {
          override protected def createDiskQueue(): RowQueue = synchronized {
            if (spillDir == null) {
              spillDir = Files.createTempDirectory(localDir.toPath, "inprocess-udf-").toFile
            }
            DiskRowQueue(Files.createTempFile(spillDir.toPath, "buffer", "").toFile,
              childOutput.length, serializerManager)
          }

          // Once task completion leaves the queue to the executor, it must not spill for other
          // consumers into a directory that nothing deletes.
          override def spill(size: Long, trigger: MemoryConsumer): Long = synchronized {
            if (queueAbandoned) 0L else super.spill(size, trigger)
          }

          // Queues of a task are distinct memory consumers, whatever their case-class fields.
          override def equals(other: Any): Boolean = this eq other.asInstanceOf[AnyRef]
          override def hashCode(): Int = System.identityHashCode(this)
          override def canEqual(other: Any): Boolean = false
        }
        (queue, projection.orNull)
      case ReadBack => (null, null)
    }
    val joined = new JoinedRow
    val handles = functions.map(_ => UUID.randomUUID().toString)
    var registered = false
    var writer: ArrowWriter = null
    val results = ArrayBuffer.empty[ArrowColumnVector]
    var startedAt = 0L

    def closeBatch(): Unit = {
      val resources = ArrayBuffer.empty[AutoCloseable]
      resources ++= results
      results.clear()
      if (writer != null) {
        resources += writer.root
        writer = null
      }
      AutoCloseables.close(resources.asJava)
    }

    val resources = new InProcessArrowEvalPythonEvaluatorFactory.IteratorResources(
      // Closing the queue deletes the spill files it tracks; deleteQuietly also removes any
      // other, without starting a process or throwing, also on an interrupted thread.
      releaseTaskMemory = () => if (queue != null) {
        Utils.tryWithSafeFinally(queue.close())(Utils.deleteQuietly(spillDir))
      },
      abandonTaskMemory = () => if (queue != null) {
        queue.synchronized {
          queueAbandoned = true
          Utils.deleteQuietly(spillDir)
        }
      },
      releaseOthers = () => {
        if (startedAt != 0L) {
          metrics("pythonTotalTime") += (System.nanoTime() - startedAt) / 1000000
        }
        Utils.tryWithSafeFinally {
          closeBatch()
        } {
          if (registered) runtime.release(handles)
        }
      })

    context.addTaskCompletionListener[Unit](_ => resources.close())

    new Iterator[InternalRow] {
      private var batchIter: Iterator[InternalRow] = Iterator.empty

      private def endOfInput: Nothing =
        throw new NoSuchElementException("End of in-process UDF input")

      // Releases the resources on failure without replacing its exception.
      private def fail(t: Throwable): Nothing =
        Utils.tryWithSafeFinally { throw t } { resources.close() }

      // Called with the lock held.
      private def hasNextLocked: Boolean = {
        if (startedAt == 0L) startedAt = System.nanoTime()
        checkCancellation()
        val available = batchIter.hasNext || {
          resources.startReadingInput()
          try rows.hasNext finally resources.endReadingInput()
        }
        if (!available) resources.close()
        available
      }

      // Each call takes the lock without allocating a closure per row. Within a batch, only
      // the consumer advances `batchIter`, whose `hasNext` compares row indexes.
      override def hasNext: Boolean = {
        if (batchIter.hasNext && !resources.isClosed) return true
        if (!resources.enter()) return false
        val available = try {
          try hasNextLocked catch { case t: Throwable => fail(t) }
        } finally {
          resources.exit()
        }
        // Task completion may have closed the iterator while the input was being read.
        available && !resources.isClosed
      }

      override def next(): InternalRow = {
        if (!resources.enter()) endOfInput
        try {
          try {
            if (!hasNextLocked) endOfInput
            if (!batchIter.hasNext) nextBatch()
            val result = batchIter.next()
            resultProj(if (queue != null) joined(queue.remove(), result) else result)
          } catch {
            case t: Throwable => fail(t)
          }
        } finally {
          resources.exit()
        }
      }

      // Runs Python without the lock unless task completion already happened, and ends the
      // input instead of returning the result if it happens meanwhile.
      private def python[T](body: => T): T = {
        if (resources.isClosed) endOfInput
        val result = resources.withoutLock(body)
        if (resources.isClosed) endOfInput
        result
      }

      /**
       * Writes the next input row to the batch, returning false at the end of input or once
       * task completion happened. If it happens while the row is read, the row is dropped.
       */
      private def pullRow(): Boolean = {
        resources.startReadingInput()
        val row = try {
          if (rows.hasNext && !resources.isClosed) rows.next() else null
        } finally {
          resources.endReadingInput()
        }
        if (row == null) return false
        // Checked after reading ends, so that task memory is not left to the executor now.
        if (resources.isClosed) endOfInput
        if (queue != null) queue.add(row.asInstanceOf[UnsafeRow])
        writer.write(if (projection != null) projection(row) else row)
        true
      }

      // Called with the lock held.
      private def nextBatch(): Unit = {
        closeBatch()
        val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)
        writer = try {
          ArrowWriter.create(root)
        } catch {
          case t: Throwable => Utils.tryWithSafeFinally { throw t } { root.close() }
        }
        // Task completion stops the fill within a row, and Python never sees a partial batch.
        var count = 0
        while (!resources.isClosed && (batchSize <= 0 || count < batchSize) &&
            (count == 0 || writer.sizeInBytes() < maxBytes) && {
              checkCancellation()
              pullRow()
            }) {
          count += 1
        }
        if (resources.isClosed) endOfInput
        if (!registered) {
          // Mark before registering so failure after any registration still cleans up.
          registered = true
          functions.indices.foreach { i =>
            val func = functions(i)
            initTime.add(python(runtime.register(handles(i), func.command.toArray,
              expectedFields(i), func.pythonVer, hideTraceback, simplifiedTraceback,
              tracebackWithLocals, fullValidation)))
          }
        }
        writer.finish()
        metrics("pythonDataSent") += writer.sizeInBytes()

        handles.indices.foreach { udfIndex =>
          val handle = handles(udfIndex)
          val ordinals = inputOrdinals(udfIndex)
          checkCancellation()
          // Register each acquired resource immediately, including partially exported
          // inputs and results of earlier UDFs if a later UDF throws.
          val structs = ArrayBuffer.empty[AutoCloseable]
          def track[S <: BaseStruct](struct: S): S = {
            val closer: AutoCloseable = () => InProcessArrowBridge.closeStruct(struct)
            structs += closer
            struct
          }
          def array(): ArrowArray = track(ArrowArray.allocateNew(ArrowUtils.rootAllocator))
          def schema(): ArrowSchema = track(ArrowSchema.allocateNew(ArrowUtils.rootAllocator))
          Utils.tryWithSafeFinally {
            val inArrays = ordinals.map(_ => array())
            val inSchemas = ordinals.map(_ => schema())
            val outArray = array()
            val outSchema = schema()
            ordinals.indices.foreach { i =>
              InProcessArrowBridge.exportColumn(
                writer.root.getVector(ordinals(i)), inArrays(i), inSchemas(i))
            }
            processingTime.add(python(runtime.invoke(
              handle,
              inArrays.map(_.memoryAddress()).toArray,
              inSchemas.map(_.memoryAddress()).toArray,
              outArray.memoryAddress(), outSchema.memoryAddress(),
              count, argMetas(udfIndex).map(_.name.getOrElse("")))))
            results += InProcessArrowBridge.cdiToColumn(
              outArray, outSchema, Some(expectedFields(udfIndex)))
            metrics("pythonDataReceived") += results.last.getValueVector.getBufferSize
          } {
            AutoCloseables.close(structs.asJava)
          }
        }

        metrics("pythonNumRowsReceived") += count
        // Input vectors are closed with the writer's root, not with the results.
        val inputs = if (joinInput == ReadBack) {
          writer.root.getFieldVectors.asScala.map(new ArrowColumnVector(_))
        } else {
          Nil
        }
        val columns = (inputs ++ results).toArray[ColumnVector]
        batchIter = new ColumnarBatch(columns, count).rowIterator().asScala
      }
    }
  }
}

private[python] object InProcessArrowEvalPythonEvaluatorFactory {
  /** How the evaluator joins input rows with their results. */
  sealed trait JoinInput
  /** Read the input columns back from the exported Arrow input vectors. */
  case object ReadBack extends JoinInput
  /** Buffer the input rows, writing their arguments, projected if needed, to Arrow. */
  case class Buffered(projection: Option[UnsafeProjection]) extends JoinInput

  /**
   * Whether `ArrowColumnVector` returns exactly the values `ArrowWriter` wrote for this type,
   * and an unsafe projection copies them about as fast as an unsafe row. Types with derived
   * Arrow representations, such as intervals, nanosecond timestamps, TIME, Variant, geospatial
   * types and UDTs, keep the original rows instead. So do arrays and maps, which a projection
   * copies element by element out of Arrow, but with a single copy out of an unsafe row, and
   * decimals, which Arrow reads back through a `BigDecimal` per value.
   */
  def readsBack(dataType: DataType): Boolean = dataType match {
    case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType |
        FloatType | DoubleType | BinaryType | DateType | TimestampType | TimestampNTZType => true
    case _: StringType => true
    case StructType(fields) => fields.forall(f => readsBack(f.dataType))
    case _ => false
  }

  /**
   * Coordinates cleanup at task completion with the consumer of the evaluator's iterator. The
   * consumer can run on another thread, e.g. a pipelined Python writer or a TRANSFORM feed
   * thread, and the completion listener cannot tell, since a lazily computing parent (such as
   * `coalesce`) can create the iterator on that thread too.
   *
   * The consumer holds the lock while it reads input, the row queue or Arrow vectors, and
   * releases it only while this evaluator's Python runs. The listener (`close`) first requests
   * closing, which the consumer checks after each input row, so the listener waits for at most
   * one row before it releases task memory (the row queue), ahead of the executor. It releases
   * the other resources (Arrow vectors and Python handles) too, unless Python is running; then
   * the consumer releases them when Python returns.
   *
   * Reading one row can take long: the input can be another in-process evaluator, whose next
   * row may need a batch of Python, or an upstream operator that only a later listener
   * unblocks. So while the consumer reads input, the listener waits for the lock only
   * briefly. Then it leaves the task memory to the executor, deleting what lives outside it,
   * and the consumer releases the other resources once its row returns, without touching the
   * task memory again. Otherwise the consumer may use the task memory, e.g. the queue, and
   * the listener waits for the lock until it is done.
   */
  class IteratorResources(
      releaseTaskMemory: () => Unit,
      abandonTaskMemory: () => Unit,
      releaseOthers: () => Unit,
      lockWaitMillis: Long = 1000L) {
    private val lock = new ReentrantLock()
    @volatile private var closeRequested = false
    // Task memory is released by whichever of the consumer and the listener gets here first,
    // or abandoned to the executor if the listener gives up on the lock.
    private val taskMemory = new AtomicInteger(TaskMemoryHeld)
    // Guarded by the lock.
    private var inPython = false
    private var othersReleased = false

    // Set while the consumer reads input; see `startReadingInput`.
    @volatile private var readingInput = false

    def isClosed: Boolean = closeRequested

    /**
     * Marks that the consumer reads input, which may wait for a later listener, so that the
     * listener may leave the task memory to the executor meanwhile. Otherwise the consumer may
     * use the task memory whenever it holds the lock, e.g. to add a row to the queue, read
     * one, or copy it, so the listener waits for the lock however long that takes.
     */
    def startReadingInput(): Unit = readingInput = true

    /**
     * Ends reading input. The consumer must check `isClosed` afterwards, before it uses the
     * task memory: the flag is cleared before that check, while `close` sets its own before it
     * reads the flag, so either the consumer sees the close or the listener waits for it.
     */
    def endReadingInput(): Unit = readingInput = false

    /** Locks for a consumer call; returns false, without the lock, once closed. */
    def enter(): Boolean = {
      lock.lock()
      if (!closeRequested) {
        true
      } else {
        try releaseAll() finally lock.unlock()
        false
      }
    }

    /** Ends a consumer call, releasing anything that task completion left to the consumer. */
    def exit(): Unit = {
      try {
        if (closeRequested) releaseAll()
      } finally {
        lock.unlock()
      }
    }

    /** Runs Python without the lock. Afterwards, the consumer must check `isClosed`. */
    def withoutLock[T](body: => T): T = {
      inPython = true
      lock.unlock()
      try {
        body
      } finally {
        lock.lock()
        inPython = false
      }
    }

    def close(): Unit = {
      closeRequested = true
      if (lock.isHeldByCurrentThread) {
        releaseAll()
      } else if (Uninterruptibles.tryLockUninterruptibly(
          lock, lockWaitMillis, TimeUnit.MILLISECONDS)) {
        try releaseAll() finally lock.unlock()
      } else if (readingInput &&
          taskMemory.compareAndSet(TaskMemoryHeld, TaskMemoryAbandoned)) {
        // The executor frees the task memory, but not what lives outside it, e.g. spill files.
        abandonTaskMemory()
      } else {
        // The consumer may use the task memory, or is releasing it. Either way, it releases
        // everything before it unlocks, ahead of the executor.
        lock.lock()
        lock.unlock()
      }
    }

    // Called with the lock held.
    private def releaseAll(): Unit = {
      Utils.tryWithSafeFinally {
        if (taskMemory.compareAndSet(TaskMemoryHeld, TaskMemoryReleased)) releaseTaskMemory()
      } {
        if (!othersReleased && !inPython) {
          othersReleased = true
          releaseOthers()
        }
      }
    }
  }

  private val TaskMemoryHeld = 0
  private val TaskMemoryReleased = 1
  private val TaskMemoryAbandoned = 2

  /** Carry sub-millisecond time between batches instead of dropping it on every invocation. */
  class NanosecondTimer(metric: SQLMetric) {
    private var remainder = 0L

    def add(nanos: Long): Unit = {
      val elapsed = remainder + nanos
      metric += elapsed / 1000000L
      remainder = elapsed % 1000000L
    }
  }
}
