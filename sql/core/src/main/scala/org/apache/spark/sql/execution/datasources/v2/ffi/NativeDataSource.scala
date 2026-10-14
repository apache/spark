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

import java.lang.ref.{Cleaner, WeakReference}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import org.apache.arrow.c.{ArrowArray, ArrowArrayStream, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.vector.{FieldVector, ValueVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import org.apache.spark.{SparkException, SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.v2.columnar.{ColumnarDataSource, ColumnarReader, ColumnarStreamReader, ColumnarStreamWriter, ColumnarWriter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.{ArrowUtils, CaseInsensitiveStringMap}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Utils

/**
 * A [[ColumnarDataSource]] implemented by a native library, through the C interface in
 * `org/apache/spark/sql/datasource/spark_datasource.h`.
 *
 * @param location where the library is on the driver. The readers and the writers distribute it
 *                 to the executors of the active session when they are created.
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
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    val handle = create(library, "plan a scan of", "DATA_SOURCE_BATCH_SCAN_NOT_SUPPORTED") {
      dataSource =>
        NativeArrow.exportSchema(schema)(address => library.createReader(dataSource, address))
    }
    new NativeDataSourceReader(
      executorLocation, name, schema, new NativeHandle(handle, library.closeReader))
  }

  override def streamReader(schema: StructType): ColumnarStreamReader = {
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    val handle = create(library, "plan a scan of", "DATA_SOURCE_MICRO_BATCH_SCAN_NOT_SUPPORTED") {
      dataSource =>
        NativeArrow.exportSchema(schema)(address => library.createStreamReader(dataSource, address))
    }
    new NativeDataSourceStreamReader(
      executorLocation, name, schema, new NativeHandle(handle, library.closeStreamReader))
  }

  override def writer(schema: StructType, overwrite: Boolean): ColumnarWriter = {
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    val handle = new NativeHandle(
      create(library, "plan a write to", "DATA_SOURCE_BATCH_WRITE_NOT_SUPPORTED") { dataSource =>
        NativeArrow.exportSchema(schema) { address =>
          library.createWriter(dataSource, address, overwrite)
        }
      },
      library.closeWriter)
    new NativeDataSourceWriter(executorLocation, name, handle, serializeWriter(library, handle))
  }

  override def streamWriter(
      schema: StructType,
      overwrite: Boolean,
      queryId: String): ColumnarStreamWriter = {
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    val handle = new NativeHandle(
      create(library, "plan a write to", "DATA_SOURCE_STREAMING_WRITE_NOT_SUPPORTED") {
        dataSource =>
          NativeArrow.exportSchema(schema) { address =>
            library.createStreamWriter(dataSource, address, overwrite, queryId)
          }
      },
      library.closeWriter)
    val writer = new NativeDataSourceStreamWriter(
      executorLocation, name, handle, serializeWriter(library, handle))
    NativeStreamWriters.register(queryId, handle)
    writer
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
  // Why planning the partitions failed. The handle is released after the first try, but the
  // scan can be planned again, for example by the lazy values of the physical plan that call it.
  @transient private var planningError: Throwable = _

  @transient private lazy val library = NativeLibraries.get(location)

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    // Only the predicates that can be expressed in JSON are passed to the library.
    val encoded = predicates.zipWithIndex.flatMap { case (predicate, i) =>
      NativePredicates.toJson(predicate, schema).map(_ -> i)
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
      if (planningError != null) {
        throw planningError
      }
      try {
        val partitions = call(name, "plan a scan of")(library.partitions(handle.get))
        readerState = bytesOrEmpty(
          call(name, "plan a scan of")(optional(library.serializeReader(handle.get))))
        plannedPartitions = NativeInputPartition.from(partitions)
      } catch {
        case e: Throwable =>
          planningError = e
          throw e
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
    checkOffset("spark_ds_initial_offset",
      call(name, "plan a scan of")(library.initialOffset(handle.get)))
  }

  override def latestOffset(): String = {
    checkOffset("spark_ds_latest_offset",
      call(name, "plan a scan of")(library.latestOffset(handle.get)))
  }

  /**
   * Checks an offset returned by the given function of the library: it must be JSON on a single
   * line, because the offset log of the checkpoint stores each offset as a line.
   */
  private def checkOffset(function: String, offset: String): String = {
    if (offset.exists(c => c == '\n' || c == '\r')) {
      throw error(name, "plan a scan of", s"$function returned an offset with a line break, " +
        "but an offset must be JSON on a single line.")
    }
    val valid = try {
      val json = NativeDataSourceStreamReader.mapper.readTree(offset)
      json != null && !json.isMissingNode
    } catch {
      case _: JsonProcessingException => false
    }
    if (!valid) {
      throw error(name, "plan a scan of", s"$function returned an offset that is not valid " +
        s"JSON: $offset")
    }
    offset
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

object NativeDataSourceStreamReader {
  // Parses the offsets of the libraries to check them, and rejects anything after the JSON value.
  private val mapper =
    JsonMapper.builder().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build()
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
    // Not closed if the commit fails: Spark then aborts the write, which closes it.
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

/**
 * Writes to a native data source for a streaming write. Its handle is closed when the streaming
 * query creates its next stream writer, see [[NativeStreamWriters]].
 */
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
 * Reads a partition of a native data source: each array of the Arrow stream exported by the
 * library is checked, and imported into the Arrow vectors that back the returned batch.
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
  // Read here rather than with `Data.importArrayStream`, to check each array before importing it.
  private val stream = ArrowArrayStream.allocateNew(allocator)
  private val dictionaries = new CDataDictionaryProvider()
  private var root: VectorSchemaRoot = _
  private var batch: ColumnarBatch = _

  try {
    openStream()
    root = VectorSchemaRoot.create(importSchema(), allocator)
    checkSchema()
    batch = new ColumnarBatch(root.getFieldVectors.asScala.map(columnVector).toArray)
  } catch {
    case e: Throwable =>
      close()
      throw e
  }

  private def openStream(): Unit = {
    val library = NativeLibraries.get(location)
    call(name, "read")(optional(library.read(readerState, partition, stream.memoryAddress())))
      .getOrElse {
        throw new SparkUnsupportedOperationException(
          "DATA_SOURCE_BATCH_SCAN_NOT_SUPPORTED", Map("description" -> name))
      }
  }

  private def importSchema(): Schema = {
    // Arrow calls the callbacks of the stream without checking them, and calling NULL would crash
    // the JVM.
    val snapshot = stream.snapshot()
    if (snapshot.release == 0 || snapshot.get_schema == 0 || snapshot.get_next == 0 ||
        snapshot.get_last_error == 0) {
      throw error(name, "read", "The library did not initialize the stream, or a callback of " +
        "the stream is NULL.")
    }
    val arrowSchema = ArrowSchema.allocateNew(allocator)
    try {
      call(name, "read")(stream.getSchema(arrowSchema))
      val imported = call(name, "read")(Data.importSchema(allocator, arrowSchema, dictionaries))
      new Schema(
        imported.getFields.asScala.map(withMapEntryNames).asJava, imported.getCustomMetadata)
    } finally {
      arrowSchema.close()
    }
  }

  /**
   * Names the fields of the entries of the maps `key` and `value`, which ArrowColumnVector looks
   * up by name. Arrow does not fix these names: arrow-rs names them `keys` and `values`, for
   * example. The arrays are imported by position, so the names do not change how they are read.
   */
  private def withMapEntryNames(field: Field): Field = {
    val children = field.getChildren.asScala.map(withMapEntryNames).toSeq
    val renamed = (field.getType, children) match {
      case (_: ArrowType.Map, Seq(entries)) if entries.getChildren.size == 2 =>
        val Seq(key, value) = entries.getChildren.asScala.toSeq
        Seq(new Field(entries.getName, entries.getFieldType, Seq(
          new Field(MapVector.KEY_NAME, key.getFieldType, key.getChildren),
          new Field(MapVector.VALUE_NAME, value.getFieldType, value.getChildren)).asJava))
      case _ => children
    }
    new Field(field.getName, field.getFieldType, renamed.asJava)
  }

  private def checkSchema(): Unit = {
    def isDictionaryEncoded(field: Field): Boolean = {
      field.getDictionary != null || field.getChildren.asScala.exists(isDictionaryEncoded)
    }
    root.getSchema.getFields.asScala.find(isDictionaryEncoded).foreach { field =>
      throw error(name, "read", s"The column ${field.getName} is dictionary-encoded, which " +
        "is not supported.")
    }
    // The Arrow types that convert to Spark types, but that Spark does not read: ArrowColumnVector
    // cannot read decimals of other widths than 128 bits, and the lengths of the elements of a
    // list view cannot be checked without a pass over all its lists (see checkArrays).
    def unsupportedType(field: Field): Option[ArrowType] = field.getType match {
      case decimal: ArrowType.Decimal if decimal.getBitWidth != 128 => Some(decimal)
      case listView: ArrowType.ListView => Some(listView)
      case _ => field.getChildren.asScala.view.flatMap(unsupportedType).headOption
    }
    root.getSchema.getFields.asScala.foreach { field =>
      unsupportedType(field).foreach { arrowType =>
        throw error(name, "read", s"The column ${field.getName} contains the Arrow type " +
          s"$arrowType, which is not supported.")
      }
    }
    val actual = StructType(root.getSchema.getFields.asScala.map { field =>
      val dataType = try {
        ArrowUtils.fromArrowField(field)
      } catch {
        case e: SparkUnsupportedOperationException if e.getCondition == "UNSUPPORTED_ARROWTYPE" =>
          throw error(name, "read", s"The column ${field.getName} contains the Arrow type " +
            s"${e.getMessageParameters.get("typeName")}, which is not supported.")
      }
      StructField(field.getName, dataType, field.isNullable)
    }.toSeq)
    // The schema to read as the library got it: Arrow does not carry everything, such as the
    // collations of strings, the fields of intervals and user-defined types, which still apply to
    // the data that Spark reads.
    val expected = ArrowUtils.fromArrowSchema(ArrowUtils.toArrowSchema(
      schema, "UTC", errorOnDuplicatedFieldNames = true, largeVarTypes = false))
    val matches = actual.length == expected.length && actual.zip(expected).forall {
      case (actualField, field) =>
        actualField.name == field.name &&
          DataType.equalsIgnoreNullability(actualField.dataType, field.dataType)
    }
    if (!matches) {
      throw error(name, "read", s"The data has the schema ${actual.toDDL}, but the expected " +
        s"schema is ${expected.toDDL}.")
    }
  }

  private def columnVector(vector: FieldVector): ColumnVector = {
    try {
      new ReusedArrowColumnVector(vector)
    } catch {
      case NonFatal(e) =>
        throw QueryExecutionErrors.nativeDataSourceError("read", name,
          s"Spark cannot read the column ${vector.getName} of the Arrow type " +
            s"${vector.getField.getType}: ${message(e)}", e)
    }
  }

  override def next(): Boolean = {
    val array = ArrowArray.allocateNew(allocator)
    try {
      call(name, "read")(stream.getNext(array))
      val snapshot = array.snapshot()
      if (snapshot.release == 0) {
        // The end of the stream.
        false
      } else {
        try {
          checkArrays(snapshot)
        } catch {
          case e: Throwable =>
            array.release()
            throw e
        }
        call(name, "read")(Data.importIntoVectorSchemaRoot(allocator, array, root, dictionaries))
        batch.setNumRows(root.getRowCount)
        true
      }
    } finally {
      array.close()
    }
  }

  /**
   * Checks the array of a batch, and the arrays of its columns at every level, before importing
   * them. Arrow Java trusts the arrays, and would read them wrongly otherwise:
   *  - An array must have an offset of 0. Arrow Java ignores the offsets of the arrays it imports,
   *    so it would read a sliced array from the start of its buffers.
   *  - An array must have at least as many values as its parent refers to: a column as many as
   *    the batch has rows, a field as many as its struct has values, and the elements of a list or
   *    a map as many as its last offset says. Arrow Java would extend a shorter array with nulls.
   *    Only the lengths that the library exported show this: the import sets the length of each
   *    array to what its parent refers to.
   * The buffers are not checked, since the C data interface does not carry their sizes: Arrow
   * Java takes them from the lengths, so the library must export buffers that hold every value.
   */
  private def checkArrays(batch: ArrowArray.Snapshot): Unit = {
    def children(array: ArrowArray.Snapshot): Seq[ArrowArray.Snapshot] = {
      (0 until array.n_children.toInt).map { i =>
        ArrowArray.wrap(Platform.getLong(null, array.children + 8L * i)).snapshot()
      }
    }
    // The last offset of a list or a map, in its second buffer. Arrow Java rejects the array if
    // the buffer is missing.
    def lastOffset(array: ArrowArray.Snapshot): Long = {
      val offsets = if (array.length > 0 && array.n_buffers >= 2) {
        Platform.getLong(null, array.buffers + 8L)
      } else {
        0L
      }
      if (offsets == 0) 0 else Platform.getInt(null, offsets + 4L * array.length)
    }
    // The number of values that an array refers to in each of its children.
    def referenced(array: ArrowArray.Snapshot, arrowType: ArrowType): Long = arrowType match {
      case _: ArrowType.Struct => array.length
      case _: ArrowType.List | _: ArrowType.Map => lastOffset(array)
      // The other types that Spark reads have no children. List views are rejected before.
      case _ => 0
    }
    def checkColumn(array: ArrowArray.Snapshot, field: Field, column: String): Unit = {
      if (array.offset != 0) {
        throw error(name, "read", s"The column $column has an offset of ${array.offset}, but " +
          "Spark only reads arrays with an offset of 0. Copy sliced arrays before exporting them.")
      }
      val length = referenced(array, field.getType)
      children(array).zip(field.getChildren.asScala).foreach { case (child, childField) =>
        if (child.length < length) {
          throw error(name, "read", s"The column $column contains an array with a length of " +
            s"${child.length}, but its parent refers to $length values.")
        }
        checkColumn(child, childField, column)
      }
    }
    if (batch.offset != 0) {
      throw error(name, "read", s"The batch has an offset of ${batch.offset}, but Spark only " +
        "reads arrays with an offset of 0. Copy sliced arrays before exporting them.")
    }
    children(batch).zip(root.getSchema.getFields.asScala).foreach { case (column, field) =>
      if (column.length < batch.length) {
        throw error(name, "read", s"The column ${field.getName} has a length of " +
          s"${column.length}, but the batch has a length of ${batch.length}.")
      }
      checkColumn(column, field, field.getName)
    }
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = {
    Utils.tryWithSafeFinally {
      if (root != null) {
        root.close()
      }
      dictionaries.close()
      // Releases the stream of the library, unless it was not exported.
      stream.release()
      stream.close()
    }(Utils.tryLogNonFatalError(allocator.close()))
  }
}

/**
 * The Arrow column vector of a column of a [[NativePartitionReader]], which imports all its batches
 * into the same Arrow vectors. It is not freeable between batches: Spark must not close it when it
 * moves to the next batch, as `ColumnarToRowExec` does with the column vectors that are freeable,
 * because closing a struct column also drops its child column vectors. The reader closes the
 * Arrow vectors at the end.
 */
private class ReusedArrowColumnVector(vector: ValueVector) extends ArrowColumnVector(vector) {
  override def closeIfFreeable(): Unit = {}
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
  // Zero once the data writer was committed or aborted, which releases it. It stays set if the
  // library fails to commit, so that Spark aborts the data writer.
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
    if (handle == 0) {
      throw SparkException.internalError("The data writer was already committed or aborted.")
    }
    // The library releases the data writer once it commits, even if its message is then rejected,
    // so Spark must not abort it afterwards.
    val message = call(name, "write to")(library.commitDataWriter(handle, () => handle = 0))
    NativeWriterCommitMessage(bytesOrEmpty(Some(message)))
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
          case NonFatal(e) =>
            logWarning("Failed to release a native data source handle.", e)
        }
      }
    }
  }
}

/**
 * The stream writers of the streaming queries, by query ID. The Data Source V2 streaming write
 * has no hook for when the query stops, but a micro-batch streaming query creates a new stream
 * writer for each micro-batch, after the previous micro-batch committed or aborted its write, and
 * a restarted query creates a new one. So the previous stream writer of a query is closed when
 * the query creates a new one, and the last one when it becomes unreachable.
 */
object NativeStreamWriters {
  // Weak, so that the last stream writer of a query can still become unreachable.
  private val writers = new ConcurrentHashMap[String, WeakReference[NativeHandle]]()

  /** Registers the new stream writer of the query, and closes its previous one. */
  def register(queryId: String, handle: NativeHandle): Unit = {
    // Forgets the queries whose last stream writer was released.
    writers.values().removeIf(_.get() == null)
    Option(writers.put(queryId, new WeakReference(handle))).flatMap(r => Option(r.get()))
      .foreach(_.close())
  }
}

/** Helpers to call native data source libraries. */
object NativeCalls {
  /**
   * Calls a native library, and rethrows the errors it reports as NATIVE_DATA_SOURCE_ERROR, as
   * well as the link errors, such as when Arrow cannot load its own JNI library.
   */
  def call[T](name: String, action: String)(f: => T): T = {
    try {
      f
    } catch {
      case e: Throwable with SparkThrowable => throw e
      case e: NativeLibrary.MissingFunctionException =>
        throw QueryExecutionErrors.nativeDataSourceError(
          action, name, s"The library does not implement ${e.getMessage}.", e)
      case e @ (NonFatal(_) | _: LinkageError) =>
        throw QueryExecutionErrors.nativeDataSourceError(action, name, message(e), e)
    }
  }

  // How Arrow reports the error message of an Arrow C stream.
  private val StreamError = """(?s)\[errno \d+\] CDataJniException\{errno=\d+, message=(.*)\}""".r

  private[ffi] def message(e: Throwable): String = {
    Option(e.getMessage).getOrElse(e.toString) match {
      case StreamError(message) => message
      case message => message
    }
  }

  /** Calls a function that the library may not implement. Returns None if it does not. */
  def optional[T](f: => T): Option[T] = {
    try {
      Some(f)
    } catch {
      case _: NativeLibrary.MissingFunctionException => None
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
