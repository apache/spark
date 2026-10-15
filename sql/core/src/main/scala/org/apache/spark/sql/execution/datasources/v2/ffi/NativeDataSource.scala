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
import com.google.protobuf.ByteString
import org.apache.arrow.c.{ArrowArray, ArrowArrayStream, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.vector.{FieldVector, ValueVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import org.apache.spark.{SPARK_VERSION, SparkException, SparkThrowable, SparkUnsupportedOperationException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.v2.columnar.{ArrowRowReader, ArrowStreamDataWriter, ColumnarDataSource, ColumnarReader, ColumnarStreamReader, ColumnarStreamWriter, ColumnarWriter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{ArrowUtils, CaseInsensitiveStringMap}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.Utils

/**
 * A [[ColumnarDataSource]] implemented by a native library, through the C interface in
 * `org/apache/spark/sql/datasource/spark_datasource.h` and the messages of
 * `org/apache/spark/sql/datasource/spark_datasource.proto`.
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

  // The data source, as the requests of spark_ds_open carry it. The keys of a case-insensitive
  // map are in lower case.
  private def dataSource: proto.DataSource = {
    val builder = proto.DataSource.newBuilder().setName(name)
    options.entrySet().asScala.foreach(e => builder.putOptions(e.getKey, e.getValue))
    builder.build()
  }

  override def schema(): StructType = {
    val library = NativeLibraries.get(location)
    withDataSource(library, "infer the schema of") { (handle, _) =>
      val reply = call(name, "infer the schema of") {
        library.call(handle, NativeMessages.InferSchema)
      }
      if (!reply.getInferSchema.hasSchema) {
        throw QueryCompilationErrors.dataSchemaNotSpecifiedError(name)
      }
      call(name, "infer the schema of")(NativeSchemas.fromProto(reply.getInferSchema.getSchema))
    }
  }

  override def reader(schema: StructType): ColumnarReader = {
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    val handle = open(library, "plan a scan of", proto.Capability.CAPABILITY_BATCH_READ,
        "DATA_SOURCE_BATCH_SCAN_NOT_SUPPORTED") {
      _.setBatchReader(proto.BatchReader.newBuilder()
        .setDataSource(dataSource)
        .setSchema(NativeSchemas.toProto(schema)))
    }
    new NativeDataSourceReader(
      executorLocation, name, schema, new NativeHandle(handle, library.close))
  }

  override def streamReader(schema: StructType): ColumnarStreamReader = {
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    val handle = open(library, "plan a scan of", proto.Capability.CAPABILITY_MICRO_BATCH_READ,
        "DATA_SOURCE_MICRO_BATCH_SCAN_NOT_SUPPORTED") {
      _.setStreamReader(proto.StreamReader.newBuilder()
        .setDataSource(dataSource)
        .setSchema(NativeSchemas.toProto(schema)))
    }
    new NativeDataSourceStreamReader(
      executorLocation, name, schema, new NativeHandle(handle, library.close))
  }

  override def writer(schema: StructType, overwrite: Boolean): ColumnarWriter = {
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    val handle =
      new NativeHandle(openWriter(library, schema, overwrite, queryId = null), library.close)
    new NativeDataSourceWriter(executorLocation, name, handle, serializeWriter(library, handle))
  }

  override def streamWriter(
      schema: StructType,
      overwrite: Boolean,
      queryId: String): ColumnarStreamWriter = {
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    val handle = new NativeHandle(openWriter(library, schema, overwrite, queryId), library.close)
    val writer = new NativeDataSourceStreamWriter(
      executorLocation, name, handle, serializeWriter(library, handle))
    NativeStreamWriters.register(queryId, handle)
    writer
  }

  /**
   * Runs `f` with a new native data source and its capabilities, and closes the data source
   * afterwards.
   */
  private def withDataSource[T](library: NativeLibrary, action: String)(
      f: (Long, Set[proto.Capability]) => T): T = {
    val opened = call(name, "create") {
      library.open(NativeMessages.open().setDataSource(dataSource).build())
    }
    Utils.tryWithSafeFinally {
      f(opened.handle, opened.reply.getCapabilitiesList.asScala.toSet)
    } {
      call(name, action)(library.close(opened.handle))
    }
  }

  /** Returns the capabilities of the data source, and fails if it does not declare `capability`. */
  private def capabilities(
      library: NativeLibrary,
      action: String,
      capability: proto.Capability,
      unsupportedError: String): Set[proto.Capability] = {
    val capabilities = withDataSource(library, action)((_, capabilities) => capabilities)
    if (!capabilities.contains(capability)) {
      throw new SparkUnsupportedOperationException(unsupportedError, Map("description" -> name))
    }
    capabilities
  }

  /** Opens a reader with the request that `f` builds, if the data source declares `capability`. */
  private def open(
      library: NativeLibrary,
      action: String,
      capability: proto.Capability,
      unsupportedError: String)(
      f: proto.OpenRequest.Builder => proto.OpenRequest.Builder): Long = {
    capabilities(library, action, capability, unsupportedError)
    call(name, action)(library.open(f(NativeMessages.open()).build()).handle)
  }

  /** Opens a writer for a batch write, or for a streaming write with the ID of its query. */
  private def openWriter(
      library: NativeLibrary,
      schema: StructType,
      overwrite: Boolean,
      queryId: String): Long = {
    val action = "plan a write to"
    val (capability, unsupportedError, overwriteMode) = if (queryId == null) {
      (proto.Capability.CAPABILITY_BATCH_WRITE, "DATA_SOURCE_BATCH_WRITE_NOT_SUPPORTED",
        "the save mode overwrite")
    } else {
      (proto.Capability.CAPABILITY_STREAMING_WRITE, "DATA_SOURCE_STREAMING_WRITE_NOT_SUPPORTED",
        "the output mode complete")
    }
    val declared = capabilities(library, action, capability, unsupportedError)
    if (overwrite && !declared.contains(proto.Capability.CAPABILITY_TRUNCATE)) {
      throw error(name, action, s"The data source does not support $overwriteMode.")
    }
    val writer = proto.Writer.newBuilder()
      .setDataSource(dataSource)
      .setSchema(NativeSchemas.toProto(schema))
      .setMode(
        if (overwrite) proto.WriteMode.WRITE_MODE_TRUNCATE else proto.WriteMode.WRITE_MODE_APPEND)
    if (queryId != null) {
      writer.setQueryId(queryId)
    }
    call(name, action)(library.open(NativeMessages.open().setWriter(writer).build()).handle)
  }

  private def serializeWriter(library: NativeLibrary, handle: NativeHandle): Array[Byte] = {
    try {
      call(name, "plan a write to") {
        library.call(handle.get, NativeMessages.Serialize).getSerialize.getState.toByteArray
      }
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
    val used = if (encoded.isEmpty) {
      Set.empty[Int]
    } else {
      val request = proto.CallRequest.newBuilder().setPushPredicates(
        proto.PushPredicates.newBuilder().addAllPredicatesJson(encoded.map(_._1).toSeq.asJava))
      val uses = call(name, "plan a scan of") {
        library.call(handle.get, request.build()).getPushPredicates.getUsesValueList.asScala
      }
      if (uses.length > encoded.length) {
        throw error(name, "plan a scan of", s"PushPredicates returned ${uses.length} answers " +
          s"for ${encoded.length} predicates.")
      }
      // How the reader uses each predicate: whether it uses it completely or only to skip data
      // does not matter, since Spark evaluates all the predicates again after reading. The
      // missing answers are PREDICATE_USE_UNUSED, and Spark takes the values that it does not
      // know as PREDICATE_USE_PARTIAL.
      encoded.map(_._2).zip(uses).collect {
        case (i, use) if use != proto.PredicateUse.PREDICATE_USE_UNUSED_VALUE => i
      }.toSet
    }
    predicates.zipWithIndex.collect { case (predicate, i) if used.contains(i) => predicate }
  }

  override def pushLimit(limit: Int): Boolean = {
    val request = proto.CallRequest.newBuilder()
      .setPushLimit(proto.PushLimit.newBuilder().setLimit(limit))
    call(name, "plan a scan of") {
      library.call(handle.get, request.build()).getPushLimit.getApplied
    }
  }

  override def pruneColumns(requiredSchema: StructType): Boolean = {
    val request = proto.CallRequest.newBuilder().setPruneColumns(
      proto.PruneColumns.newBuilder().addAllColumns(requiredSchema.fieldNames.toSeq.asJava))
    val pruned = call(name, "plan a scan of") {
      library.call(handle.get, request.build()).getPruneColumns.getPruned
    }
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
        val partitions = call(name, "plan a scan of") {
          library.call(handle.get, NativeMessages.PlanPartitions).getPlanPartitions
            .getPartitionsList.asScala.map(_.toByteArray).toArray
        }
        readerState = call(name, "plan a scan of") {
          library.call(handle.get, NativeMessages.Serialize).getSerialize.getState.toByteArray
        }
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
    checkOffset("InitialOffset", call(name, "plan a scan of") {
      library.call(handle.get, NativeMessages.InitialOffset).getInitialOffset.getOffsetJson
    })
  }

  override def latestOffset(): String = {
    checkOffset("LatestOffset", call(name, "plan a scan of") {
      library.call(handle.get, NativeMessages.LatestOffset).getLatestOffset.getOffsetJson
    })
  }

  /**
   * Checks an offset returned by the given request: it must be JSON on a single line, because the
   * offset log of the checkpoint stores each offset as a line.
   */
  private def checkOffset(request: String, offset: String): String = {
    if (offset.isEmpty) {
      throw error(name, "plan a scan of", s"$request returned no offset.")
    }
    if (offset.exists(c => c == '\n' || c == '\r')) {
      throw error(name, "plan a scan of", s"$request returned an offset with a line break, " +
        "but an offset must be JSON on a single line.")
    }
    val valid = try {
      val json = NativeDataSourceStreamReader.mapper.readTree(offset)
      json != null && !json.isMissingNode
    } catch {
      case _: JsonProcessingException => false
    }
    if (!valid) {
      throw error(name, "plan a scan of", s"$request returned an offset that is not valid " +
        s"JSON: $offset")
    }
    offset
  }

  override def partitions(start: String, end: String): Array[InputPartition] = {
    val request = proto.CallRequest.newBuilder().setPlanStreamPartitions(
      proto.PlanStreamPartitions.newBuilder().setStartJson(start).setEndJson(end))
    val partitions = call(name, "plan a scan of") {
      library.call(handle.get, request.build()).getPlanStreamPartitions.getPartitionsList.asScala
        .map(_.toByteArray).toArray
    }
    readerState = call(name, "plan a scan of") {
      library.call(handle.get, NativeMessages.Serialize).getSerialize.getState.toByteArray
    }
    NativeInputPartition.from(partitions)
  }

  override def read(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    new NativePartitionReader(
      location, name, readerState, partition.asInstanceOf[NativeInputPartition].bytes, schema)
  }

  override def commit(end: String): Unit = {
    val request = proto.CallRequest.newBuilder()
      .setCommitOffset(proto.CommitOffset.newBuilder().setEndJson(end))
    call(name, "commit an offset of")(library.call(handle.get, request.build()))
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
      library.call(handle.get, NativeMessages.commit(-1L, messages))
    }
    // Not closed if the commit fails: Spark then aborts the write, which closes it.
    handle.close()
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    Utils.tryWithSafeFinally {
      call(name, "abort a write to") {
        library.call(handle.get, NativeMessages.abort(-1L, messages))
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
      library.call(handle.get, NativeMessages.commit(epochId, messages))
    }
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    call(name, "abort a write to") {
      library.call(handle.get, NativeMessages.abort(epochId, messages))
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
  /** The messages of the tasks, as the requests of a commit or an abort carry them. */
  def toTaskMessages(messages: Array[WriterCommitMessage]): Seq[proto.TaskMessage] = {
    messages.toSeq.map {
      case message: NativeWriterCommitMessage =>
        proto.TaskMessage.newBuilder()
          .setCommitted(true)
          .setMessage(ByteString.copyFrom(message.bytes))
          .build()
      case null => proto.TaskMessage.getDefaultInstance
      case other =>
        throw SparkException.internalError(s"Unexpected commit message: ${other.getClass.getName}")
    }
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
  private var library: NativeLibrary = _
  // The partition reader of the library, which is closed after its stream is released, and the
  // registration of the task for SparkDsHost.is_interrupted, until then.
  private var partitionReader: Long = 0L
  private var task: AutoCloseable = _
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
    library = NativeLibraries.get(location)
    val (taskContext, registration) = NativeTask.create()
    task = registration
    val request = proto.PartitionReader.newBuilder()
      .setTask(taskContext)
      .setReaderState(ByteString.copyFrom(readerState))
      .setPartition(ByteString.copyFrom(partition))
    partitionReader = call(name, "read") {
      library.open(NativeMessages.open().setPartitionReader(request).build()).handle
    }
    call(name, "read") {
      library.stream(partitionReader, NativeMessages.Read, 0L, stream.memoryAddress())
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
    } {
      Utils.tryWithSafeFinally {
        if (partitionReader != 0L) {
          call(name, "read")(library.close(partitionReader))
        }
      } {
        if (task != null) {
          task.close()
        }
        Utils.tryLogNonFatalError(allocator.close())
      }
    }
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

/** Writes the data of a task to the data writer of a native library. */
class NativeDataWriter(
    location: NativeLibraryLocation,
    name: String,
    writerState: Array[Byte],
    partitionId: Int,
    taskId: Long,
    epochId: Long)
  extends ArrowStreamDataWriter {
  import NativeCalls._

  private val library = NativeLibraries.get(location)
  // The task, registered for SparkDsHost.is_interrupted until the data writer is closed.
  private val (taskContext, task) = NativeTask.create(Some(partitionId), Some(taskId))
  private val handle: Long = try {
    val request = proto.DataWriter.newBuilder()
      .setTask(taskContext)
      .setWriterState(ByteString.copyFrom(writerState))
      .setEpochId(epochId)
    call(name, "write to") {
      library.open(NativeMessages.open().setDataWriter(request).build()).handle
    }
  } catch {
    case e: Throwable =>
      task.close()
      throw e
  }
  // Whether the data writer was committed or aborted. It stays false if the library fails to
  // commit, so that Spark aborts the data writer, but becomes true as soon as the library commits,
  // even if Spark then fails the commit, for example because of an invalid reply.
  private var finished = false
  private var closed = false

  private def checkOpen(): Unit = {
    if (finished || closed) {
      throw SparkException.internalError("The data writer was already committed or aborted.")
    }
  }

  override def write(batch: ColumnarBatch): Unit = {
    checkOpen()
    val vectors = (0 until batch.numCols()).map { i =>
      batch.column(i) match {
        case vector: ArrowColumnVector => vector.getValueVector.asInstanceOf[FieldVector]
        case vector => throw SparkException.internalError(
          s"Expected an Arrow column vector, but got ${vector.getClass.getName}.")
      }
    }
    writeStream(new SingleBatchArrowReader(
      new VectorSchemaRoot(vectors.map(_.getField).asJava, vectors.asJava, batch.numRows())))
  }

  override def writeStream(reader: ArrowReader): Unit = {
    checkOpen()
    val stream = ArrowArrayStream.allocateNew(ArrowUtils.rootAllocator)
    try {
      Data.exportArrayStream(ArrowUtils.rootAllocator, reader, stream)
      try {
        call(name, "write to") {
          library.stream(handle, NativeMessages.Write, stream.memoryAddress(), 0L)
        }
      } catch {
        // The library reads the batches through the stream, which only reports the message of
        // an error of the rows, such as a failure of an upstream operator: that error is thrown.
        case e: Throwable => throw ArrowRowReader.failure(reader).getOrElse(e)
      }
    } finally {
      // Releases the stream, and so the reader, unless the library moved it to take ownership.
      stream.release()
      stream.close()
    }
  }

  override def commit(): WriterCommitMessage = {
    checkOpen()
    val reply = call(name, "write to") {
      library.call(handle, NativeMessages.CommitDataWriter, () => finished = true)
    }
    NativeWriterCommitMessage(reply.getCommitDataWriter.getMessage.toByteArray)
  }

  override def abort(): Unit = {
    if (!finished && !closed) {
      finished = true
      call(name, "write to")(library.call(handle, NativeMessages.AbortDataWriter))
    }
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      Utils.tryWithSafeFinally {
        call(name, "write to")(library.close(handle))
      }(task.close())
    }
  }
}

/**
 * Exports one batch of a [[VectorSchemaRoot]] as an Arrow reader. The vectors of the batch are
 * transferred to the reader, which releases them when it is closed.
 */
private class SingleBatchArrowReader(batch: VectorSchemaRoot)
  extends ArrowReader(ArrowUtils.rootAllocator) {
  private var loaded = false

  override def loadNextBatch(): Boolean = {
    if (loaded) {
      false
    } else {
      loaded = true
      val root = getVectorSchemaRoot
      batch.getFieldVectors.asScala.zip(root.getFieldVectors.asScala).foreach {
        case (from, to) => from.makeTransferPair(to).transfer()
      }
      root.setRowCount(batch.getRowCount)
      true
    }
  }

  override def bytesRead(): Long = 0L

  override protected def closeReadSource(): Unit = {}

  override protected def readSchema(): Schema = batch.getSchema
}

/** Creates the `TaskContext` message of the running task. */
private[ffi] object NativeTask {
  /**
   * Returns the message, and the registration of the task for `SparkDsHost.is_interrupted`, which
   * the caller closes once the library no longer uses the object of the task.
   *
   * @param partitionId the partition that the task reads or writes, by default the partition of
   *                    the task
   * @param taskAttemptId the ID of the task attempt, by default the one of the task
   */
  def create(
      partitionId: Option[Int] = None,
      taskAttemptId: Option[Long] = None): (proto.TaskContext, AutoCloseable) = {
    val context = Option(TaskContext.get())
    val attemptId = taskAttemptId.orElse(context.map(_.taskAttemptId())).getOrElse(-1L)
    val message = proto.TaskContext.newBuilder()
      .setStageId(context.map(_.stageId()).getOrElse(-1))
      .setStageAttemptNumber(context.map(_.stageAttemptNumber()).getOrElse(-1))
      .setPartitionId(partitionId.orElse(context.map(_.partitionId())).getOrElse(-1))
      .setAttemptNumber(context.map(_.attemptNumber()).getOrElse(0))
      .setTaskAttemptId(attemptId)
      .build()
    (message, NativeLibrary.registerTask(attemptId, () => context.exists(_.isInterrupted())))
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

  /** Returns a NATIVE_DATA_SOURCE_ERROR for an error detected by Spark. */
  def error(name: String, action: String, msg: String): Throwable = {
    QueryExecutionErrors.nativeDataSourceError(action, name, msg, null)
  }
}

/** The messages of `spark_datasource.proto` that Spark sends. */
private[ffi] object NativeMessages {
  /** An `OpenRequest`, with the versions of Spark and of the messages. */
  def open(): proto.OpenRequest.Builder = proto.OpenRequest.newBuilder()
    .setSparkVersion(SPARK_VERSION)
    .setProtocolVersion(NativeLibrary.PROTOCOL_VERSION)

  val InferSchema: proto.CallRequest = proto.CallRequest.newBuilder()
    .setInferSchema(proto.InferSchema.getDefaultInstance).build()
  val PlanPartitions: proto.CallRequest = proto.CallRequest.newBuilder()
    .setPlanPartitions(proto.PlanPartitions.getDefaultInstance).build()
  val Serialize: proto.CallRequest = proto.CallRequest.newBuilder()
    .setSerialize(proto.Serialize.getDefaultInstance).build()
  val InitialOffset: proto.CallRequest = proto.CallRequest.newBuilder()
    .setInitialOffset(proto.InitialOffset.getDefaultInstance).build()
  val LatestOffset: proto.CallRequest = proto.CallRequest.newBuilder()
    .setLatestOffset(proto.LatestOffset.getDefaultInstance).build()
  val CommitDataWriter: proto.CallRequest = proto.CallRequest.newBuilder()
    .setCommitDataWriter(proto.CommitDataWriter.getDefaultInstance).build()
  val AbortDataWriter: proto.CallRequest = proto.CallRequest.newBuilder()
    .setAbortDataWriter(proto.AbortDataWriter.getDefaultInstance).build()
  val Read: proto.StreamRequest =
    proto.StreamRequest.newBuilder().setRead(proto.Read.getDefaultInstance).build()
  val Write: proto.StreamRequest =
    proto.StreamRequest.newBuilder().setWrite(proto.Write.getDefaultInstance).build()

  def commit(epochId: Long, messages: Array[WriterCommitMessage]): proto.CallRequest = {
    proto.CallRequest.newBuilder().setCommit(proto.Commit.newBuilder()
      .setEpochId(epochId)
      .addAllMessages(NativeWriterCommitMessage.toTaskMessages(messages).asJava)).build()
  }

  def abort(epochId: Long, messages: Array[WriterCommitMessage]): proto.CallRequest = {
    proto.CallRequest.newBuilder().setAbort(proto.Abort.newBuilder()
      .setEpochId(epochId)
      .addAllMessages(NativeWriterCommitMessage.toTaskMessages(messages).asJava)).build()
  }
}

/** Converts schemas to and from the Spark SQL types of `spark_datasource.proto`. */
private[ffi] object NativeSchemas {
  import proto.{DataType => P}

  def toProto(schema: StructType): proto.StructType = {
    val builder = proto.StructType.newBuilder()
    schema.fields.foreach { field =>
      builder.addFields(proto.StructField.newBuilder()
        .setName(field.name)
        .setDataType(toProto(field.dataType))
        .setNullable(field.nullable))
    }
    builder.build()
  }

  def toProto(dataType: DataType): proto.DataType = {
    val builder = proto.DataType.newBuilder()
    dataType match {
      case NullType => builder.setNull(P.Null.getDefaultInstance)
      case BooleanType => builder.setBoolean(P.Boolean.getDefaultInstance)
      case ByteType => builder.setByte(P.Byte.getDefaultInstance)
      case ShortType => builder.setShort(P.Short.getDefaultInstance)
      case IntegerType => builder.setInteger(P.Integer.getDefaultInstance)
      case LongType => builder.setLong(P.Long.getDefaultInstance)
      case FloatType => builder.setFloat(P.Float.getDefaultInstance)
      case DoubleType => builder.setDouble(P.Double.getDefaultInstance)
      case d: DecimalType =>
        builder.setDecimal(P.Decimal.newBuilder().setPrecision(d.precision).setScale(d.scale))
      case s: StringType =>
        val collation = if (s.isUTF8BinaryCollation) {
          ""
        } else {
          CollationFactory.fetchCollation(s.collationId).collationName
        }
        builder.setString(P.String.newBuilder().setCollation(collation))
      case BinaryType => builder.setBinary(P.Binary.getDefaultInstance)
      case DateType => builder.setDate(P.Date.getDefaultInstance)
      case TimestampType => builder.setTimestamp(P.Timestamp.getDefaultInstance)
      case TimestampNTZType => builder.setTimestampNtz(P.TimestampNtz.getDefaultInstance)
      case y: YearMonthIntervalType =>
        builder.setYearMonthInterval(
          P.YearMonthInterval.newBuilder().setStartField(y.startField).setEndField(y.endField))
      case d: DayTimeIntervalType =>
        builder.setDayTimeInterval(
          P.DayTimeInterval.newBuilder().setStartField(d.startField).setEndField(d.endField))
      case CalendarIntervalType =>
        builder.setCalendarInterval(P.CalendarInterval.getDefaultInstance)
      case a: ArrayType =>
        builder.setArray(P.Array.newBuilder()
          .setElementType(toProto(a.elementType))
          .setContainsNull(a.containsNull))
      case m: MapType =>
        builder.setMap(P.Map.newBuilder()
          .setKeyType(toProto(m.keyType))
          .setValueType(toProto(m.valueType))
          .setValueContainsNull(m.valueContainsNull))
      case s: StructType => builder.setStruct(toProto(s))
      case udt: UserDefinedType[_] => return toProto(udt.sqlType)
      case other =>
        throw new IllegalArgumentException(
          s"Native data sources do not support the type ${other.catalogString}.")
    }
    builder.build()
  }

  def fromProto(schema: proto.StructType): StructType = {
    StructType(schema.getFieldsList.asScala.map { field =>
      StructField(field.getName, fromProto(field.getDataType), field.getNullable)
    }.toSeq)
  }

  def fromProto(dataType: proto.DataType): DataType = dataType.getKindCase match {
    case P.KindCase.NULL => NullType
    case P.KindCase.BOOLEAN => BooleanType
    case P.KindCase.BYTE => ByteType
    case P.KindCase.SHORT => ShortType
    case P.KindCase.INTEGER => IntegerType
    case P.KindCase.LONG => LongType
    case P.KindCase.FLOAT => FloatType
    case P.KindCase.DOUBLE => DoubleType
    case P.KindCase.DECIMAL =>
      DecimalType(dataType.getDecimal.getPrecision, dataType.getDecimal.getScale)
    case P.KindCase.STRING =>
      val collation = dataType.getString.getCollation
      if (collation.isEmpty) StringType else StringType(collation)
    case P.KindCase.BINARY => BinaryType
    case P.KindCase.DATE => DateType
    case P.KindCase.TIMESTAMP => TimestampType
    case P.KindCase.TIMESTAMP_NTZ => TimestampNTZType
    case P.KindCase.YEAR_MONTH_INTERVAL =>
      val interval = dataType.getYearMonthInterval
      YearMonthIntervalType(interval.getStartField.toByte, interval.getEndField.toByte)
    case P.KindCase.DAY_TIME_INTERVAL =>
      val interval = dataType.getDayTimeInterval
      DayTimeIntervalType(interval.getStartField.toByte, interval.getEndField.toByte)
    case P.KindCase.CALENDAR_INTERVAL => CalendarIntervalType
    case P.KindCase.ARRAY =>
      ArrayType(fromProto(dataType.getArray.getElementType), dataType.getArray.getContainsNull)
    case P.KindCase.MAP =>
      val map = dataType.getMap
      MapType(fromProto(map.getKeyType), fromProto(map.getValueType), map.getValueContainsNull)
    case P.KindCase.STRUCT => fromProto(dataType.getStruct)
    case P.KindCase.KIND_NOT_SET =>
      throw new IllegalStateException("The library returned a data type without a kind.")
  }
}
