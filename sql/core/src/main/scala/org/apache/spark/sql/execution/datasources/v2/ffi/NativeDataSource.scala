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

import java.lang.ref.Cleaner
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.arrow.c.{ArrowArray, ArrowArrayStream, ArrowSchema, Data}
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.{SparkException, SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.v2.columnar.{ColumnarDataSource, ColumnarReader, ColumnarStreamReader, ColumnarStreamWriter, ColumnarWriter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.{ArrowUtils, CaseInsensitiveStringMap}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * A [[ColumnarDataSource]] implemented by a native library, through
 * `org.apache.spark.sql.datasource.NativeBridge`.
 *
 * @param location where the library is
 * @param name the name of the data source, as listed in the manifest of its package or in lower
 *             case for an installed library
 */
class NativeDataSource(
    location: NativeLibraryLocation,
    name: String,
    options: CaseInsensitiveStringMap)
  extends ColumnarDataSource {
  import NativeCalls._

  override def schema(): StructType = {
    val library = NativeLibraries.get(location)
    withDataSource(library, "infer the schema of") { dataSource =>
      call(name, "infer the schema of") {
        optional(NativeArrow.importSchema(address => library.schema(dataSource, address)))
      }.getOrElse(throw QueryCompilationErrors.dataSchemaNotSpecifiedError(name))
    }
  }

  override def reader(schema: StructType): ColumnarReader = {
    val library = NativeLibraries.get(location)
    val handle = create(library, "plan a scan of", "DATA_SOURCE_BATCH_SCAN_NOT_SUPPORTED") {
      dataSource =>
        NativeArrow.exportSchema(schema)(address => library.createReader(dataSource, address))
    }
    new NativeDataSourceReader(
      location, name, schema, new NativeHandle(handle, library.closeReader))
  }

  override def streamReader(schema: StructType): ColumnarStreamReader = {
    val library = NativeLibraries.get(location)
    val handle = create(library, "plan a scan of", "DATA_SOURCE_MICRO_BATCH_SCAN_NOT_SUPPORTED") {
      dataSource =>
        NativeArrow.exportSchema(schema)(address => library.createStreamReader(dataSource, address))
    }
    new NativeDataSourceStreamReader(
      location, name, schema, new NativeHandle(handle, library.closeStreamReader))
  }

  override def writer(schema: StructType, overwrite: Boolean): ColumnarWriter = {
    val library = NativeLibraries.get(location)
    val handle = new NativeHandle(
      create(library, "plan a write to", "DATA_SOURCE_BATCH_WRITE_NOT_SUPPORTED") { dataSource =>
        NativeArrow.exportSchema(schema) { address =>
          library.createWriter(dataSource, address, overwrite)
        }
      },
      library.closeWriter)
    new NativeDataSourceWriter(location, name, handle, serializeWriter(library, handle))
  }

  override def streamWriter(schema: StructType, overwrite: Boolean): ColumnarStreamWriter = {
    val library = NativeLibraries.get(location)
    val handle = new NativeHandle(
      create(library, "plan a write to", "DATA_SOURCE_STREAMING_WRITE_NOT_SUPPORTED") {
        dataSource =>
          NativeArrow.exportSchema(schema) { address =>
            library.createStreamWriter(dataSource, address, overwrite)
          }
      },
      library.closeWriter)
    new NativeDataSourceStreamWriter(location, name, handle, serializeWriter(library, handle))
  }

  /** Runs `f` with a new native data source, which is closed afterwards. */
  private def withDataSource[T](library: NativeLibrary, action: String)(f: Long => T): T = {
    // The keys of a case-insensitive map are in lower case.
    val entries = options.entrySet().asScala.toArray
    val dataSource = call(name, "create") {
      library.createDataSource(name, entries.map(_.getKey), entries.map(_.getValue))
    }
    Utils.tryWithSafeFinally(f(dataSource)) {
      call(name, action)(library.closeDataSource(dataSource))
    }
  }

  /** Creates a reader or a writer with `f`, which fails if the library does not implement it. */
  private def create(library: NativeLibrary, action: String, unsupportedError: String)(
      f: Long => Long): Long = {
    withDataSource(library, action) { dataSource =>
      call(name, action)(optional(f(dataSource))).getOrElse {
        throw new SparkUnsupportedOperationException(unsupportedError, Map("description" -> name))
      }
    }
  }

  private def serializeWriter(library: NativeLibrary, handle: NativeHandle): Array[Byte] = {
    try {
      bytesOrEmpty(call(name, "plan a write to")(optional(library.serializeWriter(handle.get))))
    } catch {
      case e: Throwable =>
        handle.close()
        throw e
    }
  }
}

/** Reads a native data source for a batch scan. */
class NativeDataSourceReader(
    location: NativeLibraryLocation,
    name: String,
    schema: StructType,
    @transient private val handle: NativeHandle)
  extends ColumnarReader {
  import NativeCalls._

  // Set on the driver when the partitions are planned, and serialized to the executors.
  private var readerState: Array[Byte] = Array.emptyByteArray
  private var readSchema: StructType = schema
  @transient private var plannedPartitions: Array[InputPartition] = _

  @transient private lazy val library = NativeLibraries.get(location)

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    // Only the predicates that can be expressed in JSON are passed to the library.
    val encoded = predicates.zipWithIndex.flatMap { case (predicate, i) =>
      NativePredicates.toJson(predicate).map(_ -> i)
    }
    val pushed = if (encoded.isEmpty) {
      Set.empty[Int]
    } else {
      call(name, "plan a scan of") {
        optional(library.pushPredicates(handle.get, encoded.map(_._1)))
      } match {
        case Some(flags) if flags != null && flags.length == encoded.length =>
          encoded.map(_._2).zip(flags).collect { case (i, true) => i }.toSet
        case Some(flags) =>
          throw error(name, "plan a scan of", s"pushPredicates returned " +
            s"${Option(flags).map(_.length).getOrElse(0)} values for ${encoded.length} predicates.")
        case None => Set.empty[Int]
      }
    }
    predicates.zipWithIndex.collect { case (predicate, i) if !pushed.contains(i) => predicate }
  }

  override def pushLimit(limit: Int): Boolean = {
    call(name, "plan a scan of")(optional(library.pushLimit(handle.get, limit))).getOrElse(false)
  }

  override def pruneColumns(requiredSchema: StructType): Boolean = {
    val pruned = call(name, "plan a scan of") {
      optional(library.pruneColumns(handle.get, requiredSchema.fieldNames))
    }.getOrElse(false)
    if (pruned) {
      readSchema = requiredSchema
    }
    pruned
  }

  override def partitions(): Array[InputPartition] = synchronized {
    if (plannedPartitions == null) {
      try {
        val partitions = call(name, "plan a scan of")(library.partitions(handle.get))
        readerState = bytesOrEmpty(
          call(name, "plan a scan of")(optional(library.serializeReader(handle.get))))
        plannedPartitions = NativeInputPartition.from(partitions)
      } finally {
        // The executors only need the serialized state.
        handle.close()
      }
    }
    plannedPartitions
  }

  override def read(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    new NativePartitionReader(
      location, name, readerState, partition.asInstanceOf[NativeInputPartition].bytes, readSchema)
  }
}

/** Reads a native data source for a micro-batch streaming scan. */
class NativeDataSourceStreamReader(
    location: NativeLibraryLocation,
    name: String,
    schema: StructType,
    @transient private val handle: NativeHandle)
  extends ColumnarStreamReader {
  import NativeCalls._

  // Set on the driver whenever the partitions of a micro-batch are planned, and serialized to
  // the executors with the partitions of that micro-batch.
  private var readerState: Array[Byte] = Array.emptyByteArray

  @transient private lazy val library = NativeLibraries.get(location)

  override def initialOffset(): String = {
    call(name, "plan a scan of")(library.initialOffset(handle.get))
  }

  override def latestOffset(): String = {
    call(name, "plan a scan of")(library.latestOffset(handle.get))
  }

  override def partitions(start: String, end: String): Array[InputPartition] = {
    val partitions = call(name, "plan a scan of") {
      library.streamPartitions(handle.get, start, end)
    }
    readerState = bytesOrEmpty(
      call(name, "plan a scan of")(optional(library.serializeStreamReader(handle.get))))
    NativeInputPartition.from(partitions)
  }

  override def read(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    new NativePartitionReader(
      location, name, readerState, partition.asInstanceOf[NativeInputPartition].bytes, schema)
  }

  override def commit(end: String): Unit = {
    call(name, "commit an offset of")(optional(library.commitOffset(handle.get, end)))
  }

  override def stop(): Unit = handle.close()
}

/** Writes to a native data source for a batch write. */
class NativeDataSourceWriter(
    location: NativeLibraryLocation,
    name: String,
    @transient private val handle: NativeHandle,
    writerState: Array[Byte])
  extends ColumnarWriter {
  import NativeCalls._

  @transient private lazy val library = NativeLibraries.get(location)

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch] = {
    new NativeDataWriter(location, name, writerState, partitionId, taskId, epochId = -1L)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    call(name, "commit a write to") {
      library.commit(handle.get, -1L, NativeWriterCommitMessage.toBytes(messages))
    }
    handle.close()
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    Utils.tryWithSafeFinally {
      call(name, "abort a write to") {
        library.abort(handle.get, -1L, NativeWriterCommitMessage.toBytes(messages))
      }
    }(handle.close())
  }
}

/** Writes to a native data source for a streaming write. */
class NativeDataSourceStreamWriter(
    location: NativeLibraryLocation,
    name: String,
    @transient private val handle: NativeHandle,
    writerState: Array[Byte])
  extends ColumnarStreamWriter {
  import NativeCalls._

  @transient private lazy val library = NativeLibraries.get(location)

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[ColumnarBatch] = {
    new NativeDataWriter(location, name, writerState, partitionId, taskId, epochId)
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    call(name, "commit a write to") {
      library.commit(handle.get, epochId, NativeWriterCommitMessage.toBytes(messages))
    }
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    call(name, "abort a write to") {
      library.abort(handle.get, epochId, NativeWriterCommitMessage.toBytes(messages))
    }
  }
}

/** A partition planned by a native library. */
case class NativeInputPartition(index: Int, bytes: Array[Byte]) extends InputPartition

object NativeInputPartition {
  def from(partitions: Array[Array[Byte]]): Array[InputPartition] = {
    Option(partitions).getOrElse(Array.empty).zipWithIndex.map { case (bytes, i) =>
      NativeInputPartition(i, Option(bytes).getOrElse(Array.emptyByteArray))
    }
  }
}

/** The message returned by the data writer of a task of a native library. */
case class NativeWriterCommitMessage(bytes: Array[Byte]) extends WriterCommitMessage

object NativeWriterCommitMessage {
  def toBytes(messages: Array[WriterCommitMessage]): Array[Array[Byte]] = messages.map {
    case message: NativeWriterCommitMessage => message.bytes
    case null => null
    case other =>
      throw SparkException.internalError(s"Unexpected commit message: ${other.getClass.getName}")
  }
}

/**
 * Reads a partition of a native data source: the Arrow stream exported by the library is
 * imported into Arrow vectors that back the returned batch.
 */
class NativePartitionReader(
    location: NativeLibraryLocation,
    name: String,
    readerState: Array[Byte],
    partition: Array[Byte],
    schema: StructType)
  extends PartitionReader[ColumnarBatch] {
  import NativeCalls._

  private val allocator =
    ArrowUtils.rootAllocator.newChildAllocator(s"native data source $name", 0, Long.MaxValue)
  private var reader: ArrowReader = _
  private var root: VectorSchemaRoot = _
  private var batch: ColumnarBatch = _

  try {
    reader = openStream()
    root = call(name, "read")(reader.getVectorSchemaRoot)
    checkSchema()
    batch = new ColumnarBatch(
      root.getFieldVectors.asScala.map(new ArrowColumnVector(_): ColumnVector).toArray)
  } catch {
    case e: Throwable =>
      close()
      throw e
  }

  private def openStream(): ArrowReader = {
    val library = NativeLibraries.get(location)
    val stream = ArrowArrayStream.allocateNew(allocator)
    try {
      call(name, "read")(optional(library.read(readerState, partition, stream.memoryAddress())))
        .getOrElse {
          throw new SparkUnsupportedOperationException(
            "DATA_SOURCE_BATCH_SCAN_NOT_SUPPORTED", Map("description" -> name))
        }
      Data.importArrayStream(allocator, stream)
    } catch {
      case e: Throwable =>
        stream.release()
        throw e
    } finally {
      stream.close()
    }
  }

  private def checkSchema(): Unit = {
    def isDictionaryEncoded(field: Field): Boolean = {
      field.getDictionary != null || field.getChildren.asScala.exists(isDictionaryEncoded)
    }
    root.getSchema.getFields.asScala.find(isDictionaryEncoded).foreach { field =>
      throw error(name, "read", s"The column ${field.getName} is dictionary-encoded, which " +
        "is not supported.")
    }
    val actual = ArrowUtils.fromArrowSchema(root.getSchema)
    val matches = actual.length == schema.length && actual.zip(schema).forall {
      case (actualField, field) =>
        actualField.name == field.name &&
          DataType.equalsIgnoreNullability(actualField.dataType, field.dataType)
    }
    if (!matches) {
      throw error(name, "read", s"The data has the schema ${actual.toDDL}, but the expected " +
        s"schema is ${schema.toDDL}.")
    }
  }

  override def next(): Boolean = {
    val hasNext = call(name, "read")(reader.loadNextBatch())
    if (hasNext) {
      batch.setNumRows(root.getRowCount)
    }
    hasNext
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = {
    Utils.tryWithSafeFinally {
      if (reader != null) {
        reader.close()
      }
    }(Utils.tryLogNonFatalError(allocator.close()))
  }
}

/** Writes the batches of a task to the data writer of a native library. */
class NativeDataWriter(
    location: NativeLibraryLocation,
    name: String,
    writerState: Array[Byte],
    partitionId: Int,
    taskId: Long,
    epochId: Long)
  extends DataWriter[ColumnarBatch] {
  import NativeCalls._

  private val library = NativeLibraries.get(location)
  // Zero once the data writer was committed or aborted, which releases it.
  private var handle: Long = call(name, "write to") {
    library.createDataWriter(writerState, partitionId, taskId, epochId)
  }

  override def write(batch: ColumnarBatch): Unit = {
    if (handle == 0) {
      throw SparkException.internalError("The data writer was already committed or aborted.")
    }
    val vectors = (0 until batch.numCols()).map { i =>
      batch.column(i) match {
        case vector: ArrowColumnVector => vector.getValueVector.asInstanceOf[FieldVector]
        case vector => throw SparkException.internalError(
          s"Expected an Arrow column vector, but got ${vector.getClass.getName}.")
      }
    }
    val root = new VectorSchemaRoot(
      vectors.map(_.getField).asJava, vectors.asJava, batch.numRows())
    val array = ArrowArray.allocateNew(ArrowUtils.rootAllocator)
    val arrowSchema = ArrowSchema.allocateNew(ArrowUtils.rootAllocator)
    try {
      Data.exportVectorSchemaRoot(ArrowUtils.rootAllocator, root, null, array, arrowSchema)
      call(name, "write to") {
        library.write(handle, array.memoryAddress(), arrowSchema.memoryAddress())
      }
    } finally {
      // Releases the batch unless the library moved it to take ownership.
      array.release()
      array.close()
      arrowSchema.release()
      arrowSchema.close()
    }
  }

  override def commit(): WriterCommitMessage = {
    val dataWriter = handle
    handle = 0
    NativeWriterCommitMessage(
      bytesOrEmpty(call(name, "write to")(Some(library.commitDataWriter(dataWriter)))))
  }

  override def abort(): Unit = {
    if (handle != 0) {
      val dataWriter = handle
      handle = 0
      call(name, "write to")(library.abortDataWriter(dataWriter))
    }
  }

  override def close(): Unit = abort()
}

/**
 * A handle to a native object. It is released with `close()`, or once it becomes unreachable if
 * it was never closed.
 */
final class NativeHandle(handle: Long, release: Long => Unit) {
  private val state = new NativeHandle.State(handle, release)
  private val cleanable = NativeHandle.cleaner.register(this, state)

  def get: Long = {
    val value = state.handle.get()
    if (value == 0) {
      throw SparkException.internalError("The native handle was already released.")
    }
    value
  }

  def close(): Unit = cleanable.clean()
}

object NativeHandle extends Logging {
  private val cleaner = Cleaner.create()

  // Must not refer to the handle, which would then never become unreachable.
  private class State(value: Long, release: Long => Unit) extends Runnable {
    val handle = new AtomicLong(value)

    override def run(): Unit = {
      val value = handle.getAndSet(0)
      if (value != 0) {
        try {
          release(value)
        } catch {
          case e @ (NonFatal(_) | _: LinkageError) =>
            logWarning("Failed to release a native data source handle.", e)
        }
      }
    }
  }
}

/** Helpers to call native data source libraries. */
object NativeCalls {
  /** Calls a native library, and rethrows the errors it reports as NATIVE_DATA_SOURCE_ERROR. */
  def call[T](name: String, action: String)(f: => T): T = {
    try {
      f
    } catch {
      case e: Throwable with SparkThrowable => throw e
      case e: UnsatisfiedLinkError =>
        throw QueryExecutionErrors.nativeDataSourceError(
          action, name, s"The library does not implement ${e.getMessage}.", e)
      case NonFatal(e) =>
        throw QueryExecutionErrors.nativeDataSourceError(action, name, message(e), e)
    }
  }

  // How Arrow reports the error message of an Arrow C stream.
  private val StreamError = """(?s)\[errno \d+\] CDataJniException\{errno=\d+, message=(.*)\}""".r

  private def message(e: Throwable): String = Option(e.getMessage).getOrElse(e.toString) match {
    case StreamError(message) => message
    case message => message
  }

  /** Calls a function that the library may not implement. Returns None if it does not. */
  def optional[T](f: => T): Option[T] = {
    try {
      Some(f)
    } catch {
      case _: UnsatisfiedLinkError => None
    }
  }

  /** Returns a NATIVE_DATA_SOURCE_ERROR for an error detected by Spark. */
  def error(name: String, action: String, msg: String): Throwable = {
    QueryExecutionErrors.nativeDataSourceError(action, name, msg, null)
  }

  def bytesOrEmpty(bytes: Option[Array[Byte]]): Array[Byte] = {
    bytes.flatMap(Option(_)).getOrElse(Array.emptyByteArray)
  }
}

/** Exchanges schemas with native libraries through the Arrow C data interface. */
object NativeArrow {
  /** Exports the schema as an `ArrowSchema`, and calls `f` with its address. */
  def exportSchema[T](schema: StructType)(f: Long => T): T = {
    val arrowSchema = ArrowUtils.toArrowSchema(
      schema, SQLConf.get.sessionLocalTimeZone, errorOnDuplicatedFieldNames = true,
      largeVarTypes = false)
    val struct = ArrowSchema.allocateNew(ArrowUtils.rootAllocator)
    try {
      Data.exportSchema(ArrowUtils.rootAllocator, arrowSchema, null, struct)
      f(struct.memoryAddress())
    } finally {
      // Releases the schema unless the library moved it to take ownership.
      struct.release()
      struct.close()
    }
  }

  /** Calls `f` with the address of an `ArrowSchema` to initialize, and imports it. */
  def importSchema(f: Long => Unit): StructType = {
    val struct = ArrowSchema.allocateNew(ArrowUtils.rootAllocator)
    var imported = false
    try {
      f(struct.memoryAddress())
      imported = true
      // Releases and closes the struct.
      ArrowUtils.fromArrowSchema(Data.importSchema(ArrowUtils.rootAllocator, struct, null))
    } finally {
      if (!imported) {
        struct.release()
        struct.close()
      }
    }
  }
}
