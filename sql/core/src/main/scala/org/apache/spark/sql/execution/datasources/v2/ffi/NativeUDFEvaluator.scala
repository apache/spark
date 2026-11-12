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
package org.apache.spark.sql.execution.datasources.v2.ffi

import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.vector.{FieldVector, ValueVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import org.apache.spark.{SparkEnv, SparkThrowable, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.HybridRowQueue
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Utils

/**
 * Evaluates calls of native functions in the tasks of an [[EvalNativeUDFsExec]]. A task writes the
 * arguments to Arrow batches of `maxRecordsPerBatch` rows, or fewer once the arguments and the rows
 * that the batch keeps hold `maxBytesPerBatch` bytes, and calls eval for each call and batch. A
 * batch keeps its rows in a [[HybridRowQueue]], in the memory of the task, or reads them back from
 * the arguments when every column of the child is an argument.
 *
 * @param packages the packages of the calls, as the executors find them
 */
private[ffi] class NativeUDFEvaluator(
    childOutput: Seq[Attribute],
    udfs: Seq[NativeUDF],
    packages: Seq[NativeDataSourcePackage],
    batchSize: Int,
    maxBytes: Long,
    timeZoneId: String,
    metrics: Map[String, SQLMetric]) extends Serializable {

  private val (inputs, argumentIndexes) = {
    val inputs = ArrayBuffer.empty[Expression]
    val indexes = udfs.map(_.children.map { argument =>
      val i = inputs.indexWhere(_.semanticEquals(argument))
      if (i >= 0) i else { inputs += argument; inputs.length - 1 }
    })
    (inputs.toSeq, indexes)
  }

  // As create gets them: the types of the manifest, with the nullability of the arguments.
  private val argumentSchemas = udfs.map { udf =>
    StructType(udf.children.zip(udf.function.arguments).zipWithIndex.map {
      case ((argument, declared), i) =>
        StructField(s"arg$i", NativeUDFEvaluator.withNamesOf(declared, argument.dataType),
          argument.nullable)
    })
  }
  private val resultTypes = udfs.map(_.dataType)

  // The index in the inputs of each column of the child, if all are inputs that read back exactly.
  private val readBack: Option[Seq[Int]] = {
    val indexes = childOutput.map { column =>
      inputs.indexWhere {
        case input: Attribute => input.exprId == column.exprId
        case _ => false
      }
    }
    val readable = childOutput.forall(column => NativeUDFEvaluator.readsBack(column.dataType))
    if (readable && indexes.forall(_ >= 0)) Some(indexes) else None
  }

  def evaluate(
      context: TaskContext,
      partitionIndex: Int,
      rows: Iterator[InternalRow]): Iterator[InternalRow] = {
    val batches = new NativeUDFBatches(context, partitionIndex, rows)
    context.addTaskCompletionListener[Unit](_ => batches.end())
    batches
  }

  /**
   * The rows of a task, with the results of the calls. The consumer of the rows and the end-of-task
   * listener may run on different threads: the consumer marks itself busy while it uses the
   * resources, and releases them itself if the task ended meanwhile, so that no call on a state
   * overlaps its close and the listener never waits for a consumer that waits for its input. Only
   * the rows that a batch keeps are released by the listener also while the consumer is busy: see
   * [[KeptRows]].
   */
  private class NativeUDFBatches(
      context: TaskContext,
      partitionIndex: Int,
      rows: Iterator[InternalRow]) extends Iterator[InternalRow] {

    private val projection = UnsafeProjection.create(inputs, childOutput)
    projection.initialize(partitionIndex)
    private val resultProjection = {
      val resultOutput = udfs.zipWithIndex.map { case (udf, i) =>
        AttributeReference(s"nativeUDF$i", udf.dataType)()
      }
      UnsafeProjection.create(childOutput ++ resultOutput, childOutput ++ resultOutput)
    }
    private val joined = new JoinedRow
    private val inputSchema = ArrowUtils.toArrowSchema(
      StructType(inputs.zipWithIndex.map { case (input, i) =>
        StructField(s"_$i", input.dataType, input.nullable)
      }),
      timeZoneId, errorOnDuplicatedFieldNames = true, largeVarTypes = false)
    private val resultFields = resultTypes.map { dataType =>
      ArrowUtils.toArrowField("result", dataType, nullable = true, timeZoneId,
        largeVarTypes = false)
    }

    private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
      s"native functions of task ${context.taskAttemptId()}", 0, Long.MaxValue)
    private val calls = udfs.indices.map(new Call(_))
    private var writer: ArrowWriter = _
    // Whether the library moved an input of the batch, whose buffers it may keep until the
    // state is closed: the next batch is then written to new vectors.
    private var inputMoved = false
    private val keptRows = if (readBack.isEmpty) new KeptRows else null
    private lazy val toUnsafe = UnsafeProjection.create(childOutput, childOutput)
    private val results = new Array[FieldVector](udfs.length)
    private var batch: ColumnarBatch = _
    private var batchRows = 0
    private var position = 0
    private var exhausted = false

    // Guarded by this.
    private var busy = false
    @volatile private var ended = false
    private var released = false

    /** Ends the batches, as the listener of the end of the task does. */
    def end(): Unit = {
      val release = synchronized {
        val idle = !busy
        ended = true
        idle
      }
      Utils.tryWithSafeFinally {
        if (keptRows != null) keptRows.release()
      } {
        if (release) releaseAll()
      }
    }

    private def consume[T](onEnded: => T)(f: => T): T = {
      val run = synchronized {
        busy = !ended
        busy
      }
      if (!run) {
        return onEnded
      }
      try f catch {
        case e: Throwable =>
          synchronized { ended = true }
          throw e
      } finally {
        val release = synchronized {
          busy = false
          ended
        }
        if (release) releaseAll()
      }
    }

    override def hasNext: Boolean = {
      (position < batchRows && !ended) || (!exhausted && consume(false) {
        nextBatch()
        if (batchRows == 0) {
          exhausted = true
          synchronized { ended = true }
        }
        batchRows > 0
      })
    }

    override def next(): InternalRow = {
      if (!hasNext) {
        throw new NoSuchElementException("End of the rows of the native functions.")
      }
      consume[InternalRow](throw new NoSuchElementException("The task ended.")) {
        val result = batch.getRow(position)
        val row = if (keptRows == null) resultProjection(result) else keptRows.joinNext(result)
        if (row == null) {
          throw new NoSuchElementException("The task ended.")
        }
        position += 1
        metrics("numOutputRows") += 1
        row
      }
    }

    private def nextBatch(): Unit = {
      releaseResults()
      context.killTaskIfInterrupted()
      if (writer == null || inputMoved) {
        if (writer != null) {
          writer.root.close()
        }
        writer = ArrowWriter.create(VectorSchemaRoot.create(inputSchema, allocator))
        inputMoved = false
      } else {
        writer.reset()
      }
      batchRows = 0
      position = 0
      var keptBytes = 0L
      var count = 0
      while ((batchSize <= 0 || count < batchSize) &&
          (count == 0 || writer.sizeInBytes() + keptBytes < maxBytes) && rows.hasNext) {
        val row = rows.next()
        if (keptRows != null) {
          val unsafe = row match {
            case unsafe: UnsafeRow => unsafe
            case other => toUnsafe(other)
          }
          // The task ended, and its listener released the kept rows: the batch ends empty.
          if (!keptRows.add(unsafe)) {
            return
          }
          keptBytes += unsafe.getSizeInBytes
        }
        writer.write(projection(row))
        count += 1
      }
      if (count > 0) {
        writer.finish()
        calls.foreach(_.eval(count))
        metrics("numBatches") += 1
        // The vectors of the inputs that are read back stay with the writer.
        val inputColumns = readBack.toSeq.flatten.map { i =>
          new ArrowColumnVector(writer.root.getVector(i)): ColumnVector
        }
        batch = new ColumnarBatch(
          (inputColumns ++ results.map(new ArrowColumnVector(_): ColumnVector)).toArray, count)
        batchRows = count
      }
    }

    private def releaseResults(): Unit = {
      batch = null
      results.indices.foreach { i =>
        if (results(i) != null) {
          val result = results(i)
          results(i) = null
          result.close()
        }
      }
    }

    private def releaseAll(): Unit = {
      val release = synchronized {
        val first = !released
        released = true
        first
      }
      if (release) {
        Utils.tryWithSafeFinally {
          Utils.tryWithSafeFinally {
            Utils.tryWithSafeFinally(releaseResults()) {
              calls.foreach(call => Utils.tryLogNonFatalError(call.close()))
            }
          } {
            Utils.tryWithSafeFinally {
              if (writer != null) {
                writer.root.close()
              }
            } {
              Utils.tryLogNonFatalError(allocator.close())
            }
          }
        } {
          if (keptRows != null) keptRows.release()
        }
      }
    }

    /**
     * Without read-back, the rows of the batch, in the memory of the task, or on disk once the task
     * needs the memory for another consumer, or has no more. The listener releases them also while
     * the consumer is busy, since the executor frees the memory of the task after the listeners:
     * the consumer uses them only under their lock, which it never holds while it reads its input
     * or calls the library, and not once they are released.
     */
    private class KeptRows {
      // Only the consumer adds and removes rows, under the lock.
      private val queue = HybridRowQueue(context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)), childOutput.length,
        SparkEnv.get.serializerManager, lockFree = true)
      // Guarded by this.
      private var closed = false

      /** Keeps a row of the batch, and returns false instead once the rows are released. */
      def add(row: UnsafeRow): Boolean = synchronized {
        if (!closed) {
          queue.add(row)
        }
        !closed
      }

      /** The next row with its results, or null once the rows are released. */
      def joinNext(results: InternalRow): InternalRow = synchronized {
        if (closed) null else resultProjection(joined(queue.remove(), results))
      }

      def release(): Unit = synchronized {
        if (!closed) {
          closed = true
          queue.close()
        }
      }
    }

    /** A call of a native function in the task, with its state once it is created. */
    private class Call(index: Int) {
      private val udf = udfs(index)
      private val pkg = packages(index)
      private var library: NativeUdfLibrary = _
      private var task: NativeLibrary.Task = _
      private var state = 0L

      // Reports a failure of the library, or of its results, as a NATIVE_UDF_ERROR.
      private def native[T](f: => T): T = {
        try f catch {
          case e: Throwable with SparkThrowable => throw e
          case e @ (NonFatal(_) | _: LinkageError) =>
            throw QueryExecutionErrors.nativeUdfError(
              udf.name, udf.function.name, pkg.fileName, NativeCalls.message(e), e)
        }
      }

      private def create(): Unit = {
        library = NativeLibraries.getFunctionLibrary(pkg)
        task = new NativeLibrary.Task(context.stageId(), context.stageAttemptNumber(),
          context.partitionId(), context.attemptNumber(), context.taskAttemptId(),
          () => context.isInterrupted())
        try {
          state = native {
            NativeArrow.exportSchema(argumentSchemas(index), timeZoneId) { arguments =>
              val result = ArrowSchema.allocateNew(allocator)
              try {
                Data.exportField(allocator, resultFields(index), null, result)
                library.create(udf.function.name, NativeFunctionSpec.kindCode(udf.function.kind),
                  arguments, result.memoryAddress(), timeZoneId, task)
              } finally {
                // Releases the schema unless the library moved it to take ownership.
                result.release()
                result.close()
              }
            }
          }
        } catch {
          case e: Throwable =>
            task.close()
            task = null
            throw e
        }
      }

      /** Evaluates the batch of `rows` rows of the writer, and keeps its result. */
      def eval(rows: Int): Unit = {
        if (state == 0) {
          create()
        }
        context.killTaskIfInterrupted()
        val vectors = argumentIndexes(index).map(i => writer.root.getVector(i))
        val view = new VectorSchemaRoot(vectors.map(_.getField).asJava, vectors.asJava, rows)
        val input = ArrowArray.allocateNew(allocator)
        val out = ArrowArray.allocateNew(allocator)
        try {
          // The header promises zeros, which allocateNew does not write.
          out.save(new ArrowArray.Snapshot())
          Data.exportVectorSchemaRoot(allocator, view, null, input)
          val start = System.nanoTime()
          try {
            native(library.eval(state, input.memoryAddress(), out.memoryAddress()))
          } finally {
            metrics("evalTime") += System.nanoTime() - start
          }
          inputMoved |= input.snapshot().release == 0
          native(NativeUDFEvaluator.checkResult(out.snapshot(), resultFields(index), rows))
          val result = resultFields(index).createVector(allocator)
          try {
            native {
              // Leaves out open, for the finally block.
              Data.importIntoVector(allocator, out, result, null, false)
              NativeUDFEvaluator.checkNulls(result, resultTypes(index))
            }
          } catch {
            case e: Throwable =>
              result.close()
              throw e
          }
          results(index) = result
        } finally {
          // Releases the input unless the library moved it, and the result, also of an eval that
          // failed, unless Spark moved it.
          input.release()
          input.close()
          out.release()
          out.close()
        }
      }

      def close(): Unit = {
        if (state != 0) {
          val created = state
          state = 0
          Utils.tryWithSafeFinally(library.close(created))(task.close())
        }
      }
    }
  }
}

object NativeUDFEvaluator {
  /**
   * Whether ArrowColumnVector reads a value of this type back exactly as ArrowWriter wrote it, and
   * about as fast as an unsafe row, as for the in-process Python evaluator.
   */
  private[ffi] def readsBack(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
         BinaryType | DateType | TimestampType | TimestampNTZType => true
    case _: StringType => true
    case StructType(fields) => fields.forall(field => readsBack(field.dataType))
    case _ => false
  }

  /**
   * The type `declared` with the names of its struct fields and the nullability of `actual`, a
   * type that `declared` accepts, as the analyzer casts the arguments of a call.
   */
  private[ffi] def withNamesOf(declared: DataType, actual: DataType): DataType = {
    (declared, actual) match {
      case (ArrayType(declaredElement, _), ArrayType(element, containsNull)) =>
        ArrayType(withNamesOf(declaredElement, element), containsNull)
      case (MapType(declaredKey, declaredValue, _), MapType(key, value, valueContainsNull)) =>
        MapType(withNamesOf(declaredKey, key), withNamesOf(declaredValue, value),
          valueContainsNull)
      case (StructType(declaredFields), StructType(fields))
          if declaredFields.length == fields.length =>
        StructType(declaredFields.zip(fields).map { case (declaredField, field) =>
          field.copy(
            name = declaredField.name,
            dataType = withNamesOf(declaredField.dataType, field.dataType))
        })
      case _ => declared
    }
  }

  /**
   * Checks the array that eval returned, at every level, before Spark imports it, since Arrow Java
   * trusts the layout: one value for each row, offsets of 0, which Arrow Java ignores, the children
   * of the type with as many values as their parents refer to, and a validity buffer for a null
   * count. The buffers cannot be checked, since the C data interface does not carry their sizes.
   */
  private[ffi] def checkResult(result: ArrowArray.Snapshot, field: Field, rows: Long): Unit = {
    def fail(message: String): Nothing = throw new IllegalStateException(message)
    if (result.release == 0) {
      fail("eval succeeded without setting its result.")
    }
    if (result.length != rows) {
      fail(s"eval returned ${result.length} values for a batch of $rows rows.")
    }
    def checkArray(array: ArrowArray.Snapshot, field: Field, owner: String): Unit = {
      if (array.offset != 0) {
        fail(s"$owner has an offset of ${array.offset}, but Spark only reads arrays with an " +
          "offset of 0. Copy sliced arrays before returning them.")
      }
      if (array.dictionary != 0) {
        fail(s"$owner is dictionary-encoded, which Spark does not read.")
      }
      if (array.n_buffers > 0 && array.buffers == 0) {
        fail(s"$owner has ${array.n_buffers} buffers, but a NULL pointer to them.")
      }
      val validity = if (array.n_buffers >= 1) Platform.getLong(null, array.buffers) else 0L
      if (array.null_count < -1) {
        fail(s"$owner has an invalid null_count of ${array.null_count}.")
      }
      if (array.null_count != 0 && array.length > 0 && validity == 0) {
        fail(s"$owner has a null_count of ${array.null_count} but no validity buffer, which " +
          "the C data interface only allows with a null_count of 0.")
      }
      val fields = field.getChildren.asScala.toSeq
      if (array.n_children != fields.length || (fields.nonEmpty && array.children == 0)) {
        fail(s"$owner has ${array.n_children} child arrays, but its type has ${fields.length}.")
      }
      val referenced = field.getType match {
        case _: ArrowType.Struct => array.length
        case _: ArrowType.List | _: ArrowType.Map =>
          val offsets = if (array.length > 0 && array.n_buffers >= 2) {
            Platform.getLong(null, array.buffers + 8L)
          } else {
            0L
          }
          if (offsets == 0) 0L else Platform.getInt(null, offsets + 4L * array.length).toLong
        case _ => 0L
      }
      fields.zipWithIndex.foreach { case (childField, i) =>
        val address = Platform.getLong(null, array.children + 8L * i)
        if (address == 0) {
          fail(s"$owner has a NULL child array.")
        }
        val child = ArrowArray.wrap(address).snapshot()
        if (child.length < referenced) {
          fail(s"$owner has a child array with a length of ${child.length}, but it refers to " +
            s"$referenced values of it.")
        }
        checkArray(child, childField, "A child array of the result")
      }
    }
    checkArray(result, field, "The result")
  }

  /**
   * Checks that a result has no null where its type does not allow one: a field of a struct
   * declared NOT NULL, or a key of a map, except where its parent is null.
   */
  private[ffi] def checkNulls(vector: ValueVector, resultType: DataType): Unit = {
    def check(
        vector: ValueVector,
        dataType: DataType,
        nullable: Boolean,
        what: => String,
        referenced: Int => Boolean): Unit = {
      if (!nullable && vector.getNullCount > 0 &&
          (0 until vector.getValueCount).exists(i => vector.isNull(i) && referenced(i))) {
        throw new IllegalStateException(s"eval returned a null $what, which the type of the " +
          s"result does not allow: ${resultType.sql}.")
      }
      val valid = (i: Int) => !vector.isNull(i) && referenced(i)
      def elements(list: ListVector): Int => Boolean = {
        lazy val bits = {
          val bits = new java.util.BitSet(list.getDataVector.getValueCount)
          (0 until list.getValueCount).foreach { i =>
            if (valid(i)) bits.set(list.getElementStartIndex(i), list.getElementEndIndex(i))
          }
          bits
        }
        i => bits.get(i)
      }
      (dataType, vector) match {
        case (struct: StructType, structVector: StructVector) =>
          struct.fields.zipWithIndex.foreach { case (field, i) =>
            check(structVector.getChildByOrdinal(i), field.dataType, field.nullable,
              s"value in the field ${field.name}", valid)
          }
        case (map: MapType, mapVector: MapVector) =>
          val entries = mapVector.getDataVector.asInstanceOf[StructVector]
          val inMaps = elements(mapVector)
          check(entries.getChildByOrdinal(0), map.keyType, nullable = false, "key of a map",
            inMaps)
          check(entries.getChildByOrdinal(1), map.valueType, map.valueContainsNull,
            "value of a map", inMaps)
        case (array: ArrayType, list: ListVector) =>
          check(list.getDataVector, array.elementType, array.containsNull, "element of an array",
            elements(list))
        case _ =>
      }
    }
    val constrained = resultType.existsRecursively {
      case _: MapType => true
      case ArrayType(_, false) => true
      case struct: StructType => struct.fields.exists(!_.nullable)
      case _ => false
    }
    if (constrained) {
      check(vector, resultType, nullable = true, "value", _ => true)
    }
  }
}
