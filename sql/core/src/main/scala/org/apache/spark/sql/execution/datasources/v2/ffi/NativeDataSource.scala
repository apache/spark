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
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import org.apache.arrow.c.{ArrowArray, ArrowArrayStream, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.vector.{FieldVector, ValueVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import org.apache.spark.{SparkException, SparkThrowable, SparkUnsupportedOperationException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ, STREAMING_WRITE, TRUNCATE}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.v2.columnar.{ColumnarBatchPlanner, ColumnarDataSource, ColumnarOnlyPartitionReaderFactory, ColumnarPlannedBatch, ColumnarStreamReader, ColumnarStreamWriter, ColumnarWriter}
import org.apache.spark.sql.execution.vectorized.ReusedArrowColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
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
      val schema = call(name, "infer the schema of") {
        optional(NativeArrow.importSchema(address => library.schema(dataSource.handle, address)))
      }.getOrElse(throw QueryCompilationErrors.dataSchemaNotSpecifiedError(name))
      NativeArrow.checkSupported(schema, name, "infer the schema of")
      schema
    }
  }

  /**
   * Returns the batch writes that the data source declares, and always the reads and the
   * streaming writes: Data Source V2 handles a table without those as a Data Source V1 source,
   * which then fails with errors that do not say why. A scan or a streaming write that the data
   * source does not declare fails when Spark plans it instead, on the driver. The capability
   * TRUNCATE is the one of batch writes, which Spark checks while it analyzes a query.
   */
  override def capabilities(): util.Set[TableCapability] = {
    val library = NativeLibraries.get(location)
    val declared = withDataSource(library, "check the capabilities of")(_.capabilities)
    val capabilities = util.EnumSet.of(BATCH_READ, MICRO_BATCH_READ, STREAMING_WRITE)
    if ((declared & NativeLibrary.BATCH_WRITE) != 0) {
      capabilities.add(BATCH_WRITE)
    }
    if ((declared & NativeLibrary.BATCH_TRUNCATE) != 0) {
      capabilities.add(TRUNCATE)
    }
    capabilities
  }

  // The readers and the writers export the schemas with the time zone of the session on the
  // driver, also on the executors, where they pass the same schemas to the library again. A data
  // source is created for each scan and each write, so it is read once for each.
  private val timeZoneId = SQLConf.get.sessionLocalTimeZone

  override def batchPlanner(schema: StructType): ColumnarBatchPlanner = {
    NativeArrow.checkSupported(schema, name, "plan a scan of")
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    val handle = create(library, "plan a scan of", NativeLibrary.BATCH_READ,
        "DATA_SOURCE_BATCH_SCAN_NOT_SUPPORTED", NativeDataSource.BatchReadFunctions) {
      dataSource =>
        NativeArrow.exportSchema(schema, timeZoneId) { address =>
          library.createReader(dataSource.handle, address)
        }
    }
    new NativeBatchPlanner(library, executorLocation, name, schema, timeZoneId,
      new NativeHandle(handle, library.closeReader))
  }

  override def streamReader(
      schema: StructType,
      checkpointLocation: String): ColumnarStreamReader = {
    NativeArrow.checkSupported(schema, name, "plan a scan of")
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    val handle = create(library, "plan a scan of", NativeLibrary.MICRO_BATCH_READ,
        "DATA_SOURCE_MICRO_BATCH_SCAN_NOT_SUPPORTED", NativeDataSource.MicroBatchReadFunctions) {
      dataSource =>
        NativeArrow.exportSchema(schema, timeZoneId) { address =>
          library.createStreamReader(dataSource.handle, address, checkpointLocation)
        }
    }
    new NativeDataSourceStreamReader(executorLocation, name, schema, timeZoneId,
      new NativeHandle(handle, library.closeStreamReader))
  }

  override def writer(schema: StructType, overwrite: Boolean): ColumnarWriter = {
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    val created = createWriter(library, schema, timeZoneId, overwrite, queryId = null)
    new NativeDataSourceWriter(executorLocation, name, schema, timeZoneId,
      new NativeHandle(created.handle, library.closeWriter), bytesOrEmpty(Option(created.state)))
  }

  override def streamWriter(
      schema: StructType,
      overwrite: Boolean,
      queryId: String): ColumnarStreamWriter = {
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    val created = createWriter(library, schema, timeZoneId, overwrite, queryId)
    val handle = new NativeHandle(created.handle, library.closeWriter)
    val writer = new NativeDataSourceStreamWriter(
      executorLocation, name, schema, timeZoneId, handle, bytesOrEmpty(Option(created.state)))
    NativeStreamWriters.register(queryId, handle)
    writer
  }

  /** Runs `f` with a new native data source, which is closed afterwards. */
  private def withDataSource[T](library: NativeLibrary, action: String)(
      f: NativeLibrary.DataSourceHandle => T): T = {
    // Checked first, so that a data source is never created without a way to release it.
    requireFunctions(library, action, Seq(NativeLibrary.Function.CLOSE))
    // The keys of a case-insensitive map are in lower case.
    val entries = options.entrySet().asScala.toArray
    entries.find(e => e.getKey.contains('\u0000') || e.getValue.contains('\u0000')).foreach { e =>
      throw error(name, "create", s"The option ${NativeArrow.printable(e.getKey)} contains the " +
        "character U+0000, which Spark cannot pass to a library.")
    }
    val dataSource = call(name, "create") {
      library.createDataSource(name, entries.map(_.getKey), entries.map(_.getValue))
    }
    Utils.tryWithSafeFinally(f(dataSource)) {
      call(name, action)(library.closeDataSource(dataSource.handle))
    }
  }

  /**
   * Creates a reader or a writer with `f`, which fails if the data source does not declare the
   * capability, or if the library does not implement the functions that it requires.
   */
  private def create[T](
      library: NativeLibrary,
      action: String,
      capability: Long,
      unsupportedError: String,
      functions: Seq[NativeLibrary.Function])(
      f: NativeLibrary.DataSourceHandle => T): T = {
    withDataSource(library, action) { dataSource =>
      if ((dataSource.capabilities & capability) == 0) {
        throw new SparkUnsupportedOperationException(unsupportedError, Map("description" -> name))
      }
      requireFunctions(library, action, functions)
      call(name, action)(f(dataSource))
    }
  }

  /**
   * Creates a writer for a batch write, or for a streaming write with the ID of its query, with
   * the state that its data writers need.
   */
  private def createWriter(
      library: NativeLibrary,
      schema: StructType,
      timeZoneId: String,
      overwrite: Boolean,
      queryId: String): NativeLibrary.WriterHandle = {
    val action = "plan a write to"
    NativeArrow.checkSupported(schema, name, action)
    val (capability, truncate, unsupportedError, overwriteMode) = if (queryId == null) {
      (NativeLibrary.BATCH_WRITE, NativeLibrary.BATCH_TRUNCATE,
        "DATA_SOURCE_BATCH_WRITE_NOT_SUPPORTED", "the save mode overwrite")
    } else {
      (NativeLibrary.STREAMING_WRITE, NativeLibrary.STREAMING_TRUNCATE,
        "DATA_SOURCE_STREAMING_WRITE_NOT_SUPPORTED", "the output mode complete")
    }
    create(library, action, capability, unsupportedError, NativeDataSource.WriteFunctions) {
      dataSource =>
        if (overwrite && (dataSource.capabilities & truncate) == 0) {
          throw error(name, action, s"The data source does not support $overwriteMode.")
        }
        val mode = if (overwrite) NativeLibrary.WRITE_TRUNCATE else NativeLibrary.WRITE_APPEND
        NativeArrow.exportSchema(schema, timeZoneId) { address =>
          library.createWriter(dataSource.handle, address, mode, queryId)
        }
    }
  }

  private def requireFunctions(
      library: NativeLibrary,
      action: String,
      functions: Seq[NativeLibrary.Function]): Unit = {
    functions.find(!library.hasFunction(_)).foreach { function =>
      throw error(name, action, s"The library does not implement ${function.fieldName}.")
    }
  }
}

object NativeDataSource {
  import NativeLibrary.Function._

  // The functions that the operations of a data source require, besides create and close. The
  // others are optional. They are checked on the driver, before the operation starts.
  private val BatchReadFunctions = Seq(CREATE_READER, PARTITIONS, CLOSE_READER, READ)
  private val MicroBatchReadFunctions = Seq(CREATE_STREAM_READER, INITIAL_OFFSET, LATEST_OFFSET,
    STREAM_PARTITIONS, CLOSE_STREAM_READER, READ)
  private val WriteFunctions = Seq(CREATE_WRITER, COMMIT, ABORT, CLOSE_WRITER, CREATE_DATA_WRITER,
    WRITE, COMMIT_DATA_WRITER, ABORT_DATA_WRITER, CLOSE_DATA_WRITER)
}

/**
 * Plans a batch scan of a native data source, on the driver, with a reader of the library. The
 * reader is released once the scan is planned, or when the planner becomes unreachable if the
 * scan is never planned: the executors read the partitions with the state that it returned.
 *
 * @param executorLocation where the executors find the library
 * @param timeZoneId the time zone of the timestamps of the schema that the reader got
 */
class NativeBatchPlanner(
    library: NativeLibrary,
    executorLocation: NativeLibraryLocation,
    name: String,
    schema: StructType,
    timeZoneId: String,
    handle: NativeHandle)
  extends ColumnarBatchPlanner {
  import NativeCalls._

  private var readSchema: StructType = schema

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

  override def plan(): ColumnarPlannedBatch = {
    try {
      val planned = call(name, "plan a scan of")(library.partitions(handle.get))
      ColumnarPlannedBatch(
        NativeInputPartition.from(planned.partitions),
        new NativePartitionReaderFactory(executorLocation, name,
          bytesOrEmpty(Option(planned.state)), readSchema, timeZoneId))
    } finally {
      // The executors only need the state that the reader returned.
      handle.close()
    }
  }
}

/** Reads the partitions of a planned batch scan of a native data source, on the executors. */
class NativePartitionReaderFactory(
    location: NativeLibraryLocation,
    name: String,
    readerState: Array[Byte],
    readSchema: StructType,
    timeZoneId: String)
  extends ColumnarOnlyPartitionReaderFactory {
  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    new NativePartitionReader(location, name, NativeLibrary.READ_BATCH, readerState,
      partition.asInstanceOf[NativeInputPartition].bytes, readSchema, timeZoneId)
  }
}

/**
 * Reads a native data source for a micro-batch streaming scan.
 *
 * @param timeZoneId the time zone of the timestamps of the schema that the stream reader got
 */
class NativeDataSourceStreamReader(
    location: NativeLibraryLocation,
    name: String,
    schema: StructType,
    timeZoneId: String,
    @transient private val handle: NativeHandle)
  extends ColumnarStreamReader {
  import NativeCalls._

  // Set on the driver whenever the partitions of a micro-batch are planned, and serialized to
  // the executors with the partitions of that micro-batch.
  private var readerState: Array[Byte] = Array.emptyByteArray

  @transient private lazy val library = NativeLibraries.get(location)

  override def initialOffset(): String = {
    checkOffset("initial_offset", call(name, "plan a scan of")(library.initialOffset(handle.get)))
  }

  override def latestOffset(start: String): String = {
    checkOffset("latest_offset",
      call(name, "plan a scan of")(library.latestOffset(handle.get, start)))
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
    val planned = call(name, "plan a scan of") {
      library.streamPartitions(handle.get, start, end)
    }
    readerState = bytesOrEmpty(Option(planned.state))
    NativeInputPartition.from(planned.partitions)
  }

  override def read(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    new NativePartitionReader(location, name, NativeLibrary.READ_MICRO_BATCH, readerState,
      partition.asInstanceOf[NativeInputPartition].bytes, schema, timeZoneId)
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

/**
 * Writes to a native data source for a batch write.
 *
 * @param schema the schema of the data to write
 * @param timeZoneId the time zone of the timestamps of the schema that the writer got
 */
class NativeDataSourceWriter(
    location: NativeLibraryLocation,
    name: String,
    schema: StructType,
    timeZoneId: String,
    @transient private val handle: NativeHandle,
    writerState: Array[Byte])
  extends ColumnarWriter {
  import NativeCalls._

  @transient private lazy val library = NativeLibraries.get(location)

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch] = {
    new NativeDataWriter(
      location, name, schema, timeZoneId, writerState, partitionId, taskId, epochId = -1L)
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
    schema: StructType,
    timeZoneId: String,
    @transient private val handle: NativeHandle,
    writerState: Array[Byte])
  extends ColumnarStreamWriter {
  import NativeCalls._

  @transient private lazy val library = NativeLibraries.get(location)

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[ColumnarBatch] = {
    new NativeDataWriter(
      location, name, schema, timeZoneId, writerState, partitionId, taskId, epochId)
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
 *
 * @param kind the kind of the scan, `NativeLibrary.READ_BATCH` or `READ_MICRO_BATCH`
 * @param schema the schema to read
 * @param timeZoneId the time zone of the timestamps of the schema to read, as the reader of the
 *                   library got it on the driver
 */
class NativePartitionReader(
    location: NativeLibraryLocation,
    name: String,
    kind: Int,
    readerState: Array[Byte],
    partition: Array[Byte],
    schema: StructType,
    timeZoneId: String)
  extends PartitionReader[ColumnarBatch] {
  import NativeCalls._

  private val allocator =
    ArrowUtils.rootAllocator.newChildAllocator(s"native data source $name", 0, Long.MaxValue)
  // Read here rather than with `Data.importArrayStream`, to check each array before importing it.
  private val stream = ArrowArrayStream.allocateNew(allocator)
  private val dictionaries = new CDataDictionaryProvider()
  // The task that reads the partition, which the library may use until the stream is released.
  private var task: NativeLibrary.Task = _
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
    task = NativeTask.create()
    NativeArrow.exportSchema(schema, timeZoneId) { schemaAddress =>
      call(name, "read") {
        library.read(task, kind, schemaAddress, readerState, partition, stream.memoryAddress())
      }
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

  /**
   * Checks that the data agrees with the nullability of the schema to read: a column, a field,
   * an element or a map value that it declares not nullable has no null, except where its parent
   * is null. Spark relies on the nullability, for example to evaluate `isnull` of such a column to
   * false without reading it.
   */
  private def checkNulls(): Unit = {
    root.getFieldVectors.asScala.zip(schema.fields).foreach { case (vector, field) =>
      checkNulls(vector, field.dataType, field.nullable, field.name, _ => true)
    }
  }

  /** `referenced` tells whether a value belongs to a value of the parent that is not null. */
  private def checkNulls(
      vector: ValueVector,
      dataType: DataType,
      nullable: Boolean,
      column: String,
      referenced: Int => Boolean): Unit = {
    if (!nullable && vector.getNullCount > 0 &&
        (0 until vector.getValueCount).exists(i => vector.isNull(i) && referenced(i))) {
      throw error(name, "read", s"The column $column has a null, but the schema to read " +
        "declares it not nullable.")
    }
    val valid = (i: Int) => !vector.isNull(i) && referenced(i)
    (dataType, vector) match {
      case (struct: StructType, structVector: StructVector) =>
        struct.fields.zipWithIndex.foreach { case (field, i) =>
          checkNulls(structVector.getChildByOrdinal(i), field.dataType, field.nullable,
            s"$column.${field.name}", valid)
        }
      case (map: MapType, mapVector: MapVector) =>
        val entries = mapVector.getDataVector.asInstanceOf[StructVector]
        val inMaps = elements(mapVector, valid)
        // Spark does not allow null keys.
        checkNulls(entries.getChildByOrdinal(0), map.keyType, nullable = false, s"$column.key",
          inMaps)
        checkNulls(entries.getChildByOrdinal(1), map.valueType, map.valueContainsNull,
          s"$column.value", inMaps)
      case (array: ArrayType, list: ListVector) =>
        checkNulls(list.getDataVector, array.elementType, array.containsNull, s"$column.element",
          elements(list, valid))
      case _ =>
    }
  }

  /** Whether each element of a list belongs to a list that `valid` accepts. */
  private def elements(list: ListVector, valid: Int => Boolean): Int => Boolean = {
    lazy val referenced = {
      val bits = new java.util.BitSet(list.getDataVector.getValueCount)
      (0 until list.getValueCount).foreach { i =>
        if (valid(i)) bits.set(list.getElementStartIndex(i), list.getElementEndIndex(i))
      }
      bits
    }
    i => referenced.get(i)
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

  /**
   * Wraps a vector of the root, into which every batch is imported. It is not freeable between
   * batches: Spark must not close it when it moves to the next batch, as `ColumnarToRowExec` does
   * with the column vectors that are freeable, because closing a struct column also drops its
   * child column vectors. The reader closes the Arrow vectors at the end.
   */
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
        checkNulls()
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
   *  - An array without a validity buffer has a null_count of 0, as the C data interface requires:
   *    Arrow Java fails to import it otherwise.
   *  - The batch itself has no null rows. Arrow Java ignores the validity of the struct of the
   *    batch, so it would read a null row as a row of the values of its columns. As in Arrow C++,
   *    which also rejects them, the nulls are counted when the library did not count them.
   * The buffers are not checked, since the C data interface does not carry their sizes: Arrow
   * Java takes them from the lengths, so the library must export buffers that hold every value.
   */
  private def checkArrays(batch: ArrowArray.Snapshot): Unit = {
    // The arrays of the children, one for each field of the type: Arrow Java only checks their
    // number when an array has children, and imports the missing columns of a batch as nulls.
    def children(
        array: ArrowArray.Snapshot,
        fields: Int,
        owner: String,
        expected: String): Seq[ArrowArray.Snapshot] = {
      if (array.n_children != fields || (fields > 0 && array.children == 0)) {
        throw error(name, "read", s"$owner has ${array.n_children} child arrays, but $expected.")
      }
      (0 until fields).map { i =>
        val child = Platform.getLong(null, array.children + 8L * i)
        if (child == 0) {
          throw error(name, "read", s"$owner has a NULL child array.")
        }
        ArrowArray.wrap(child).snapshot()
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
    // The validity buffer of an array, its first buffer, or 0 if it has none.
    def validity(array: ArrowArray.Snapshot): Long = {
      if (array.n_buffers >= 1 && array.buffers != 0) Platform.getLong(null, array.buffers) else 0L
    }
    // The C data interface only allows an array of values without a validity buffer if its
    // null_count is 0. Arrow Java takes another null_count as a mix of nulls and values, and then
    // fails to load the missing buffer.
    def checkValidity(array: ArrowArray.Snapshot, owner: String): Unit = {
      if (array.null_count != 0 && array.length > 0 && validity(array) == 0) {
        throw error(name, "read", s"$owner has a null_count of ${array.null_count} but no " +
          "validity buffer, which the C data interface only allows with a null_count of 0.")
      }
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
      checkValidity(array, s"The column $column")
      val length = referenced(array, field.getType)
      val fields = field.getChildren.size
      children(array, fields, s"An array of the column $column", s"its type has $fields")
          .zip(field.getChildren.asScala).foreach { case (child, childField) =>
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
    checkValidity(batch, "The batch")
    val nullRows = batch.null_count match {
      // The library did not count the nulls, so they are counted from the validity buffer, which
      // holds a bit for each row since the offset is 0. A batch without one has no rows here.
      case -1 =>
        val bitmap = validity(batch)
        var nulls = 0L
        var i = 0L
        while (bitmap != 0 && i < batch.length) {
          val bits = math.min(8L, batch.length - i).toInt
          val valid = Platform.getByte(null, bitmap + i / 8) & ((1 << bits) - 1)
          nulls += bits - Integer.bitCount(valid)
          i += 8
        }
        nulls
      case n if n < -1 =>
        throw error(name, "read", s"The batch has an invalid null_count of $n.")
      case n => n
    }
    if (nullRows != 0) {
      throw error(name, "read", s"The struct of the batch marks $nullRows of its rows as null, " +
        "but Spark only reads batches without null rows: mark the values of the columns as null " +
        "instead.")
    }
    val columns = root.getSchema.getFields.size
    children(batch, columns, "The batch", s"the schema has $columns columns")
        .zip(root.getSchema.getFields.asScala).foreach { case (column, field) =>
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
      Utils.tryWithSafeFinally {
        if (root != null) {
          root.close()
        }
        dictionaries.close()
      } {
        // Releases the stream of the library, unless it was not exported, even if closing the
        // vectors failed: the library may use the task until the stream is released.
        stream.release()
        stream.close()
      }
    } {
      if (task != null) {
        task.close()
      }
      Utils.tryLogNonFatalError(allocator.close())
    }
  }
}

/**
 * Writes the batches of a task to the data writer of a native library. The data writer gets the
 * schema of the data once, when it is created, and then only the arrays of the batches.
 *
 * @param schema the schema of the data to write
 * @param timeZoneId the time zone of the timestamps of the schema, as the writer got it
 */
class NativeDataWriter(
    location: NativeLibraryLocation,
    name: String,
    schema: StructType,
    timeZoneId: String,
    writerState: Array[Byte],
    partitionId: Int,
    taskId: Long,
    epochId: Long)
  extends DataWriter[ColumnarBatch] {
  import NativeCalls._

  private val library = NativeLibraries.get(location)
  // The task, which the library may use until the data writer is closed.
  private val task = NativeTask.create(Some(partitionId), Some(taskId))
  private val handle: Long = try {
    NativeArrow.exportSchema(schema, timeZoneId) { schemaAddress =>
      call(name, "write to") {
        library.createDataWriter(task, schemaAddress, writerState, epochId)
      }
    }
  } catch {
    case e: Throwable =>
      task.close()
      throw e
  }
  // Whether the data writer was committed or aborted. It stays false if the library fails to
  // commit, so that Spark aborts the data writer, but becomes true as soon as the library commits,
  // even if Spark then fails the commit, for example because of an invalid message.
  private var finished = false
  private var closed = false

  override def write(batch: ColumnarBatch): Unit = {
    if (finished || closed) {
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
    try {
      // Only the array: the data writer got the schema when it was created.
      Data.exportVectorSchemaRoot(ArrowUtils.rootAllocator, root, null, array)
      call(name, "write to")(library.write(handle, array.memoryAddress()))
    } finally {
      // Releases the batch unless the library moved it to take ownership.
      array.release()
      array.close()
    }
  }

  override def commit(): WriterCommitMessage = {
    if (finished || closed) {
      throw SparkException.internalError("The data writer was already committed or aborted.")
    }
    val message = call(name, "write to")(library.commitDataWriter(handle, () => finished = true))
    NativeWriterCommitMessage(bytesOrEmpty(Some(message)))
  }

  override def abort(): Unit = {
    if (!finished && !closed) {
      finished = true
      call(name, "write to")(library.abortDataWriter(handle))
    }
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      Utils.tryWithSafeFinally {
        call(name, "write to")(library.closeDataWriter(handle))
      }(task.close())
    }
  }
}

/** Creates the `SparkDsTaskContext` of the running task. */
private[ffi] object NativeTask {
  /**
   * @param partitionId the partition that the task reads or writes, by default the partition of
   *                    the task
   * @param taskAttemptId the ID of the task attempt, by default the one of the task
   */
  def create(
      partitionId: Option[Int] = None,
      taskAttemptId: Option[Long] = None): NativeLibrary.Task = {
    val context = Option(TaskContext.get())
    new NativeLibrary.Task(
      context.map(_.stageId()).getOrElse(-1),
      context.map(_.stageAttemptNumber()).getOrElse(-1),
      partitionId.orElse(context.map(_.partitionId())).getOrElse(-1),
      context.map(_.attemptNumber()).getOrElse(0),
      taskAttemptId.orElse(context.map(_.taskAttemptId())).getOrElse(-1L),
      () => context.exists(_.isInterrupted()))
  }
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
  /**
   * Checks that the schema only has the types of version 1 of the interface, which
   * `spark_datasource.h` lists with their Arrow types, and no names with the character U+0000,
   * which a NUL-terminated string cannot hold.
   */
  def checkSupported(schema: StructType, name: String, action: String): Unit = {
    def checkType(dataType: DataType, column: String): Unit = dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
           _: DecimalType | _: StringType | BinaryType | DateType | TimestampType |
           TimestampNTZType =>
      case array: ArrayType => checkType(array.elementType, s"$column.element")
      case map: MapType =>
        checkType(map.keyType, s"$column.key")
        checkType(map.valueType, s"$column.value")
      case struct: StructType => checkFields(struct, s"$column.")
      case other =>
        throw NativeCalls.error(name, action, s"The column $column has the type ${other.sql}, " +
          "which native data sources do not support.")
    }
    def checkFields(struct: StructType, prefix: String): Unit = struct.fields.foreach { field =>
      if (field.name.contains('\u0000')) {
        throw NativeCalls.error(name, action, s"The name of the column " +
          s"${printable(prefix + field.name)} contains the character U+0000, which Spark cannot " +
          "pass to a library.")
      }
      checkType(field.dataType, prefix + field.name)
    }
    checkFields(schema, "")
  }

  /** Shows the character U+0000 in a message. */
  def printable(value: String): String = value.replace("\u0000", "\\u0000")

  /**
   * Exports the schema as an `ArrowSchema`, and calls `f` with its address.
   *
   * @param timeZoneId the time zone of the timestamps, the time zone of the session on the driver
   */
  def exportSchema[T](schema: StructType, timeZoneId: String)(f: Long => T): T = {
    val arrowSchema = ArrowUtils.toArrowSchema(
      schema, timeZoneId, errorOnDuplicatedFieldNames = true, largeVarTypes = false)
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
