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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.function.BooleanSupplier
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.{classTag, ClassTag}
import scala.util.control.NonFatal

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.google.common.collect.MapMaker
import org.apache.arrow.c.{ArrowArray, ArrowArrayStream, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.vector.{FieldVector, ValueVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import org.apache.spark.{SparkContext, SparkException, SparkThrowable, SparkUnsupportedOperationException, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ, STREAMING_WRITE, TRUNCATE}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, SupportsArrowStreamRead}
import org.apache.spark.sql.connector.read.streaming.{ReadAllAvailable, ReadLimit, ReadMaxRows}
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.datasources.v2.columnar.{ColumnarBatchPlanner, ColumnarDataSource, ColumnarOnlyPartitionReaderFactory, ColumnarPlannedBatch, ColumnarStreamReader, ColumnarStreamWriter, ColumnarWriter, ColumnDomain}
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

  // A library built against a header without push_domains has a table that ends before it: Spark
  // then neither derives the domains of a scan nor creates its reader to push them down.
  override def supportsDomainPushdown(): Boolean =
    NativeLibraries.get(location).hasFunction(NativeLibrary.Function.PUSH_DOMAINS)

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
    val (handle, admissionControl) = create(library, "plan a scan of",
        NativeLibrary.MICRO_BATCH_READ, "DATA_SOURCE_MICRO_BATCH_SCAN_NOT_SUPPORTED",
        NativeDataSource.MicroBatchReadFunctions) { dataSource =>
      val admissionControl = (dataSource.capabilities & NativeLibrary.ADMISSION_CONTROL) != 0
      if (admissionControl) {
        requireFunctions(library, "plan a scan of", Seq(NativeLibrary.Function.DEFAULT_READ_LIMIT))
      }
      val handle = NativeArrow.exportSchema(schema, timeZoneId) { address =>
        library.createStreamReader(dataSource.handle, address, checkpointLocation)
      }
      (handle, admissionControl)
    }
    new NativeDataSourceStreamReader(executorLocation, name, schema, timeZoneId,
      new NativeHandle(handle, library.closeStreamReader), admissionControl)
  }

  override def writer(schema: StructType, overwrite: Boolean): ColumnarWriter = {
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    val created = createWriter(library, schema, overwrite, queryId = null)
    new NativeDataSourceWriter(executorLocation, name, schema, timeZoneId,
      new NativeHandle(created.handle, library.closeWriter), bytesOrEmpty(Option(created.state)))
  }

  override def streamWriter(
      schema: StructType,
      overwrite: Boolean,
      queryId: String): ColumnarStreamWriter = {
    val executorLocation = NativeDataSourceRegistry.distribute(location)
    val library = NativeLibraries.get(location)
    // The previous stream writer of the query is closed before create_writer, which may wait for
    // it to release its destination.
    NativeStreamWriters.closePrevious(queryId)
    val created = createWriter(library, schema, overwrite, queryId)
    val handle = new NativeHandle(created.handle, library.closeWriter)
    NativeStreamWriters.register(queryId, handle)
    new NativeDataSourceStreamWriter(
      executorLocation, name, schema, timeZoneId, handle, bytesOrEmpty(Option(created.state)))
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

  // Only called when the library has push_domains, see NativeDataSource.supportsDomainPushdown.
  override def pushDomains(domains: Seq[ColumnDomain]): Seq[Boolean] = {
    call(name, "plan a scan of") {
      NativeArrow.exportDomains(domains, timeZoneId) { exported =>
        library.pushDomains(handle.get, exported.toArray).toSeq
      }
    }
  }

  override def pushLimit(limit: Int): Boolean = {
    call(name, "plan a scan of")(optional(library.pushLimit(handle.get, limit))).getOrElse(false)
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    // read gets the columns to read as its schema, and returns exactly them, so a library that
    // does not implement prune_columns, which only lets it plan to read them, reads them too.
    call(name, "plan a scan of") {
      optional(library.pruneColumns(handle.get, requiredSchema.fieldNames))
    }
    readSchema = requiredSchema
  }

  override def plan(): ColumnarPlannedBatch = {
    try {
      val planned = call(name, "plan a scan of")(library.partitions(handle.get))
      ColumnarPlannedBatch(
        NativeInputPartition.from(planned.partitions).toArray[InputPartition],
        new NativePartitionReaderFactory(executorLocation, name,
          bytesOrEmpty(Option(planned.state)), readSchema, timeZoneId))
    } finally {
      // The executors only need the state that the reader returned.
      handle.close()
    }
  }
}

/**
 * Reads the partitions of a planned batch scan of a native data source, on the executors. An
 * engine can also get the Arrow C stream of a partition from it, see [[SupportsArrowStreamRead]].
 */
class NativePartitionReaderFactory(
    location: NativeLibraryLocation,
    name: String,
    readerState: Array[Byte],
    readSchema: StructType,
    timeZoneId: String)
  extends ColumnarOnlyPartitionReaderFactory with SupportsArrowStreamRead {
  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    new NativePartitionReader(location, name, NativeLibrary.READ_BATCH, readerState,
      partition.asInstanceOf[NativeInputPartition], readSchema, timeZoneId)
  }

  override def openArrowStream(partition: InputPartition, streamAddress: Long): Unit = {
    NativePartitionReader.openEngineStream[NativeInputPartition](location, name,
      NativeLibrary.READ_BATCH, partition, readSchema, timeZoneId, streamAddress) { partition =>
      (readerState, partition)
    }
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
    handle: NativeHandle,
    admissionControl: Boolean)
  extends ColumnarStreamReader {
  import NativeCalls._

  // The ends of the planned ranges that are not committed yet, in the order in which they were
  // planned, with the broadcast of the state of their micro-batch, if it was broadcast. Spark plans
  // each range once (see ColumnarMicroBatchStream), so the scans of a micro-batch get the
  // partitions of a single stream_partitions call, with the state that it returned.
  private val uncommitted = mutable.LinkedHashMap.empty[String, Option[Broadcast[Array[Byte]]]]
  // The context that broadcasts the states, taken on the driver when the reader is created, so
  // that a range can also be planned on a thread without an active session.
  private val sparkContext =
    SparkContext.getActive.getOrElse(SparkSession.active.sparkContext)

  private lazy val library = NativeLibraries.get(location)

  // The calls of the reader on the driver are synchronized, since calls on a handle must not run
  // at the same time: Spark may plan the scans of a micro-batch on several threads, such as the
  // side of a broadcast join, and stop the stream from another thread than the query.

  override def initialOffset(): String = synchronized {
    checkOffset("initial_offset", call(name, "plan a scan of")(library.initialOffset(handle.get)))
  }

  /** The limit that the library returns, with the capability SPARK_DS_ADMISSION_CONTROL. */
  override def defaultReadLimit(): ReadLimit = synchronized {
    if (!admissionControl) {
      ReadLimit.allAvailable()
    } else {
      val limit = call(name, "plan a scan of")(library.defaultReadLimit(handle.get))
      val (kind, maxRows) = (limit.kind, limit.maxRows)
      kind match {
        case NativeLibrary.READ_ALL_AVAILABLE => ReadLimit.allAvailable()
        case NativeLibrary.READ_MAX_ROWS if maxRows >= 1 => ReadLimit.maxRows(maxRows)
        case NativeLibrary.READ_MAX_ROWS =>
          throw error(name, "plan a scan of", s"default_read_limit returned a maximum of " +
            s"$maxRows rows, but a micro-batch reads at least 1 row.")
        case _ =>
          throw error(name, "plan a scan of", s"default_read_limit returned the unknown kind " +
            s"of read limit $kind.")
      }
    }
  }

  override def latestOffset(
      start: String,
      limit: ReadLimit,
      runEnd: Option[String]): String = synchronized {
    val info = limit match {
      case _: ReadAllAvailable =>
        new NativeLibrary.ReadLimitInfo(NativeLibrary.READ_ALL_AVAILABLE, 0L)
      case maxRows: ReadMaxRows =>
        new NativeLibrary.ReadLimitInfo(NativeLibrary.READ_MAX_ROWS, maxRows.maxRows())
      case other => throw SparkException.internalError(
        s"A native data source does not support the read limit $other.")
    }
    checkOffset("latest_offset", call(name, "plan a scan of") {
      library.latestOffset(handle.get, start, info, runEnd.orNull)
    })
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

  override def partitions(start: String, end: String): Array[InputPartition] = synchronized {
    val planned = call(name, "plan a scan of")(library.streamPartitions(handle.get, start, end))
    val partitions = NativeInputPartition.from(planned.partitions)
    val bytes = bytesOrEmpty(Option(planned.state))
    // A small state goes with each task, while the executors fetch a larger one once for the
    // micro-batch, as a broadcast. Spark only plans the next range once the micro-batch of the
    // previous one finished, so the copies of the previous broadcast on the executors are removed
    // then. The broadcast itself is destroyed once its micro-batch is committed, whose partitions
    // are not read anymore then, also by an engine.
    uncommitted.values.lastOption.flatten.foreach(_.unpersist(blocking = false))
    val state = if (bytes.length > NativeDataSourceStreamReader.maxInlineStateBytes &&
        partitions.nonEmpty) {
      Right(sparkContext.broadcast(bytes))
    } else {
      Left(bytes)
    }
    uncommitted.remove(end).flatten.foreach(_.destroy(blocking = false))
    uncommitted.put(end, state.toOption)
    partitions.map[InputPartition](NativeMicroBatchPartition(_, state))
  }

  override def readerFactory(): PartitionReaderFactory = {
    new NativeMicroBatchPartitionReaderFactory(location, name, schema, timeZoneId)
  }

  override def commit(end: String): Unit = synchronized {
    // The micro-batches up to the committed one are committed too, also those that Spark did not
    // commit one by one, as with async progress tracking, and their broadcasts are destroyed.
    if (uncommitted.contains(end)) {
      val committed = uncommitted.keys.toSeq.takeWhile(_ != end) :+ end
      committed.foreach(uncommitted.remove(_).flatten.foreach(_.destroy(blocking = false)))
    }
    call(name, "commit an offset of")(optional(library.commitOffset(handle.get, end)))
  }

  // The broadcasts of the states of the planned micro-batches that are not committed yet, for
  // tests.
  private[ffi] def uncommittedStates: Seq[Broadcast[Array[Byte]]] =
    synchronized(uncommitted.values.flatten.toSeq)

  override def stop(): Unit = synchronized {
    Utils.tryWithSafeFinally {
      uncommitted.values.flatten.foreach(_.destroy(blocking = false))
      uncommitted.clear()
    }(handle.close())
  }
}

object NativeDataSourceStreamReader {
  // Parses the offsets of the libraries to check them, and rejects anything after the JSON value.
  private val mapper =
    JsonMapper.builder().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build()

  // The size of the largest state of a micro-batch that goes with each of its tasks, rather than
  // as a broadcast, which costs more than it saves for a state of a few offsets, written to the
  // block manager of the driver, fetched by each executor, and removed from them for each
  // micro-batch.
  private[ffi] val maxInlineStateBytes: Int = 64 * 1024
}

/**
 * Reads the partitions of a micro-batch of a native data source, on the executors, each with the
 * state of its micro-batch. An engine can also get the Arrow C stream of a partition from it, see
 * [[SupportsArrowStreamRead]].
 *
 * @param timeZoneId the time zone of the timestamps of the schema that the stream reader got
 */
class NativeMicroBatchPartitionReaderFactory(
    location: NativeLibraryLocation,
    name: String,
    schema: StructType,
    timeZoneId: String)
  extends ColumnarOnlyPartitionReaderFactory with SupportsArrowStreamRead {
  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val microBatchPartition = partition.asInstanceOf[NativeMicroBatchPartition]
    new NativePartitionReader(location, name, NativeLibrary.READ_MICRO_BATCH,
      microBatchPartition.readerState, microBatchPartition.partition, schema, timeZoneId)
  }

  override def openArrowStream(partition: InputPartition, streamAddress: Long): Unit = {
    NativePartitionReader.openEngineStream[NativeMicroBatchPartition](location, name,
      NativeLibrary.READ_MICRO_BATCH, partition, schema, timeZoneId, streamAddress) { partition =>
      (partition.readerState, partition.partition)
    }
  }
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
    commitWrite(library, name, handle, -1L, messages)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    abortWrite(library, name, handle, -1L, messages)
  }
}

/**
 * Writes to a native data source for a streaming write, one micro-batch. As for a batch write, its
 * handle is closed once the micro-batch is committed or aborted. A stream writer that is neither
 * committed nor aborted is closed before the streaming query creates its next stream writer, or
 * once the handle becomes unreachable, see [[NativeStreamWriters]].
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
    commitWrite(library, name, handle, epochId, messages)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    abortWrite(library, name, handle, epochId, messages)
  }
}

/** A partition planned by a native library. */
case class NativeInputPartition(index: Int, bytes: Array[Byte]) extends InputPartition

object NativeInputPartition {
  def from(partitions: Array[Array[Byte]]): Array[NativeInputPartition] = {
    Option(partitions).getOrElse(Array.empty).zipWithIndex.map { case (bytes, i) =>
      NativeInputPartition(i, Option(bytes).getOrElse(Array.emptyByteArray))
    }
  }
}

/**
 * A partition of a micro-batch, with the state that the stream reader returned with the
 * partitions of the micro-batch, if any. The state changes with each micro-batch, so it goes with
 * the partitions rather than with the reader factory, which an engine may keep while Spark plans
 * the next micro-batch.
 *
 * @param state the bytes of the state, empty if the stream reader returned none, or for a state
 *              larger than 64 KiB, their broadcast, so that each executor fetches it once for the
 *              micro-batch rather than with each task
 */
case class NativeMicroBatchPartition(
    partition: NativeInputPartition,
    state: Either[Array[Byte], Broadcast[Array[Byte]]])
  extends InputPartition {
  /** The state to read the partition with, empty if the stream reader returned none. */
  def readerState: Array[Byte] = state.fold(identity, _.value)
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
 * A reader can be closed while another thread is in [[next]]: some operators read their input on
 * a thread of their own that the task does not wait for, such as the stdin writer thread of
 * `PipedRDD`, while `DataSourceRDD` closes the reader once the task completes. The library must
 * not get the release of the stream during get_next, and the task context must stay valid until
 * then, so [[close]] then leaves the release to [[next]], once get_next returns, rather than wait
 * for it and keep the task from completing. That [[next]] then throws the error of its read, or
 * fails as a [[next]] called once the reader is closed does, without reading from the library: so
 * such a thread does not take the close for the end of the data, whether or not it was in
 * get_next then. Script transformation, for example, then destroys its process.
 *
 * Such a thread may also still read the batch that [[next]] returned last when another thread
 * closes the reader: script transformation gets its first batch on the task thread, and reads its
 * rows on a thread of its own. The column vectors read the buffers of the batch at their
 * addresses, so [[close]] frees these buffers at once only if [[next]] was called again since it
 * returned the batch, as at the end of the stream, after which consumers no longer read the
 * batch. Otherwise, it releases the stream, the dictionaries and the task at once, since the
 * arrays of a stream may outlive it, and their release does not use the task (see
 * `SparkDsTaskContext` in the header). A Cleaner then frees the buffers of the batch and the
 * allocator once the reader is unreachable, which each of its column vectors keeps it from being,
 * or once a thread calls [[next]] again, which then fails: that thread no longer reads the batch.
 * So a reader closed before the end of its stream, as with a LIMIT, and not called again holds the
 * buffers of one batch until a garbage collection, which these buffers do not trigger: the JVM
 * does not count them. Under `DataSourceRDD`, only a garbage collection frees them: its
 * `PartitionIterator` drops the reader once it closes it, and never calls [[next]] on it again,
 * even for a thread that then asks the iterator for the next batch. That is the only effect of
 * `DataSourceRDD` here: however the iterator then answers that thread, the thread does not get
 * the batch, and a [[next]] called on the reader would free the buffers at once and fail.
 *
 * @param kind the kind of the scan, `NativeLibrary.READ_BATCH` or `READ_MICRO_BATCH`
 * @param partition the partition, whose index in the scan the library gets as `partition_id`
 * @param schema the schema to read
 * @param timeZoneId the time zone of the timestamps of the schema to read, as the reader of the
 *                   library got it on the driver
 */
class NativePartitionReader(
    location: NativeLibraryLocation,
    name: String,
    kind: Int,
    readerState: Array[Byte],
    partition: NativeInputPartition,
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
  // The column vectors of the batch, which read the buffers of each batch that `readNext` imports.
  private var columns: Array[NativeColumnVector] = _
  private var batch: ColumnarBatch = _
  @GuardedBy("this")
  private var closed = false
  // Whether a thread is in next. While it is, only that thread uses the stream and the vectors,
  // and it releases them if the reader was closed meanwhile. Otherwise, they are only released
  // under the lock, except for the buffers of a batch that may still be read when the reader is
  // closed, which a Cleaner frees once no thread can use them (see close).
  @GuardedBy("this")
  private var reading = false
  // Whether next returned the batch and was not called again since, so that a consumer may still
  // read the batch.
  @GuardedBy("this")
  private var batchReturned = false
  // Frees the buffers of the batch that a consumer may still read once the reader is closed: the
  // Cleaner does when the reader is unreachable, and next does when it is called again.
  @GuardedBy("this")
  private var batchCleanable: Cleaner.Cleanable = _

  // Synchronized so that a close on another thread sees the task and the vectors.
  synchronized {
    try {
      NativePartitionReader.openStream(location, name, kind, readerState, partition, schema,
        timeZoneId, stream.memoryAddress())(task = _)
      root = VectorSchemaRoot.create(importSchema(), allocator)
      checkSchema()
      columns = root.getFieldVectors.asScala.zip(schema.fields).map { case (vector, field) =>
        columnVector(vector, field.nullable)
      }.toArray
      batch = new ColumnarBatch(columns.toArray[ColumnVector])
    } catch {
      case e: Throwable =>
        close()
        throw e
    }
  }

  private def importSchema(): Schema = {
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
   * false without reading it. `nullCounts` are the null_counts that the library exported for the
   * columns.
   */
  private def checkNulls(nullCounts: Array[Long]): Unit = {
    root.getFieldVectors.asScala.lazyZip(schema.fields).lazyZip(nullCounts).foreach {
      case (vector, field, nullCount) =>
        checkNulls(vector, field.dataType, field.nullable, field.name, _ => true, nullCount)
    }
  }

  /**
   * `referenced` tells whether a value belongs to a value of the parent that is not null.
   * `nullCount` is the null_count that the library exported for the vector, if known, and -1
   * otherwise.
   */
  private def checkNulls(
      vector: ValueVector,
      dataType: DataType,
      nullable: Boolean,
      column: String,
      referenced: Int => Boolean,
      nullCount: Long = -1): Unit = {
    if (!nullable && NativeColumnVector.hasNulls(vector, nullCount) &&
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
   * Wraps a vector of the root, into which every batch is imported, in a `NativeColumnVector`,
   * which reads the buffers of the batch that `readNext` imported last, and keeps the reader
   * reachable, so that a Cleaner frees these buffers only once the column vector is unreachable
   * too (see the class doc). Like the other `ReusedArrowColumnVector`s, it is not freeable
   * between batches: Spark must not close it when it moves to the next batch, as
   * `ColumnarToRowExec` does with the column vectors that are freeable, because closing a struct
   * column also drops its child column vectors. The reader closes the Arrow vectors at the end.
   * `nullable` is the nullability of the column in the schema to read, which `checkNulls` enforces
   * on each batch after the column vector is refreshed: the batch has no rows until the check
   * passes.
   */
  private def columnVector(vector: FieldVector, nullable: Boolean): NativeColumnVector = {
    try {
      NativeColumnVector.create(vector, nullable, this)
    } catch {
      case NonFatal(e) =>
        throw QueryExecutionErrors.nativeDataSourceError("read", name,
          s"Spark cannot read the column ${vector.getName} of the Arrow type " +
            s"${vector.getField.getType}: ${message(e)}", e)
    }
  }

  override def next(): Boolean = {
    startReading()
    val hasBatch = Utils.tryWithSafeFinally(readNext())(stopReading())
    synchronized {
      // A reader that was closed while it read has released the batch that it read, and fails as
      // it does for a next called once it is closed (see the class doc).
      if (closed) {
        throw closedError()
      }
      batchReturned = hasBatch
      batchReturned
    }
  }

  /**
   * Fails once the reader is closed. In either case, the caller no longer reads the batch that
   * next returned last, if any, so a closed reader frees its buffers at once. This assumes that no
   * other thread still reads that batch, as an open reader does when it imports the next batch,
   * which releases the buffers of the previous one: a consumer that hands a batch to another
   * thread must not call next until that thread is done with the batch.
   */
  private def startReading(): Unit = synchronized {
    if (closed) {
      if (batchCleanable != null) {
        // The Cleaner action does not refer to the batch, which then has no rows, so that it is
        // not read at the freed addresses.
        batch.setNumRows(0)
        batchCleanable.clean()
      }
      throw closedError()
    }
    reading = true
    batchReturned = false
  }

  // Not an internal error: a thread that the task does not wait for gets it as the task completes.
  private def closedError(): Throwable = error(name, "read", "The partition reader is closed.")

  private def stopReading(): Unit = synchronized {
    reading = false
    if (closed) {
      // The reader was closed by another thread, which no longer gets a failure from here. No
      // consumer reads the batch: next does not return the one that it read, and it was called
      // since it returned the previous one.
      Utils.tryLogNonFatalError(closeResources(freeBatch = true))
    }
  }

  private def readNext(): Boolean = {
    val array = ArrowArray.allocateNew(allocator)
    try {
      call(name, "read")(stream.getNext(array))
      val snapshot = array.snapshot()
      if (snapshot.release == 0) {
        // The end of the stream.
        false
      } else {
        val nullCounts = try {
          checkArrays(snapshot)
        } catch {
          case e: Throwable =>
            array.release()
            throw e
        }
        // The import releases the buffers of the previous batch: the batch has no rows until the
        // new one passes the check, and the column vectors read its buffers before the check.
        batch.setNumRows(0)
        call(name, "read")(Data.importIntoVectorSchemaRoot(allocator, array, root, dictionaries))
        columns.zip(nullCounts).foreach { case (column, nullCount) => column.refresh(nullCount) }
        checkNulls(nullCounts)
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
   *  - An array that has buffers has a pointer to them, as the C data interface requires: Spark
   *    reads the offsets of a list or a map through it, to check the lengths of its elements.
   *  - The batch itself has no null rows. Arrow Java ignores the validity of the struct of the
   *    batch, so it would read a null row as a row of the values of its columns. As in Arrow C++,
   *    which also rejects them, the nulls are counted when the library did not count them.
   * The buffers are not checked, since the C data interface does not carry their sizes: Arrow
   * Java takes them from the lengths, so the library must export buffers that hold every value.
   * Returns the null_count that the library exported for each column.
   */
  private def checkArrays(batch: ArrowArray.Snapshot): Array[Long] = {
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
    // The C data interface requires the pointer to the buffers of an array that has buffers, even
    // if a buffer itself is NULL. Arrow Java reads the buffers through it, as `lastOffset` does.
    def checkBuffers(array: ArrowArray.Snapshot, owner: => String): Unit = {
      if (array.n_buffers > 0 && array.buffers == 0) {
        throw error(name, "read", s"$owner has ${array.n_buffers} buffers, but a NULL pointer " +
          "to them.")
      }
    }
    // The last offset of a list or a map, in its second buffer, of an array whose buffers were
    // checked. Arrow Java rejects the array if the buffer itself is missing.
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
    def checkValidity(array: ArrowArray.Snapshot, owner: => String): Unit = {
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
      checkBuffers(array, s"The column $column")
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
    checkBuffers(batch, "The batch")
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
    val numColumns = root.getSchema.getFields.size
    val arrays = children(batch, numColumns, "The batch", s"the schema has $numColumns columns")
    arrays.zip(root.getSchema.getFields.asScala).foreach { case (column, field) =>
      if (column.length < batch.length) {
        throw error(name, "read", s"The column ${field.getName} has a length of " +
          s"${column.length}, but the batch has a length of ${batch.length}.")
      }
      checkColumn(column, field, field.getName)
    }
    arrays.map(_.null_count).toArray
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      // A thread in next releases the reader once get_next returns, see stopReading. Otherwise, a
      // consumer may still read the batch that next returned, unless next was called since.
      if (!reading) {
        closeResources(freeBatch = !batchReturned)
      }
    }
  }

  /**
   * Releases the stream, the dictionaries and the task, and frees the buffers of the batch and then
   * the allocator: at once with `freeBatch`, and otherwise once the reader is unreachable or next
   * is called again, as a consumer may still read the batch (see the class doc).
   */
  private def closeResources(freeBatch: Boolean): Unit = {
    Utils.tryWithSafeFinally {
      Utils.tryWithSafeFinally {
        if (freeBatch && root != null) {
          // The batch has no rows, so that it is not read at the freed addresses.
          if (batch != null) {
            batch.setNumRows(0)
          }
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
      if (freeBatch) {
        Utils.tryLogNonFatalError(allocator.close())
      } else {
        // The action must not refer to the reader, which would then never become unreachable.
        val (batchRoot, batchAllocator) = (root, allocator)
        batchCleanable = NativeHandle.cleaner.register(this, () =>
          Utils.tryLogNonFatalError(
            Utils.tryWithSafeFinally(batchRoot.close())(batchAllocator.close())))
      }
    }
  }
}

object NativePartitionReader {
  import NativeCalls._

  /**
   * Initializes the stream at `streamAddress` with the data of a partition for an engine, see
   * [[SupportsArrowStreamRead]]: the context of its task is closed once the stream is released.
   * It first checks the arguments of `openArrowStream`, before it looks at the partition, which
   * must be a `P`, the class of the partitions of the scan of the reader factory. It then reads
   * the partition with the state and the partition of the library that `stateAndPartition`
   * returns for it.
   */
  private[ffi] def openEngineStream[P <: InputPartition: ClassTag](
      location: NativeLibraryLocation,
      name: String,
      kind: Int,
      partition: InputPartition,
      schema: StructType,
      timeZoneId: String,
      streamAddress: Long)(
      stateAndPartition: P => (Array[Byte], NativeInputPartition)): Unit = {
    val context = TaskContext.get()
    if (context != null) {
      // A task that was killed gets the error of its kill, as Spark reports it.
      context.killTaskIfInterrupted()
    }
    // A task that failed is over too, also while its listeners run.
    if (context == null || context.isFailed() || context.isCompleted()) {
      throw new IllegalStateException("openArrowStream must be called in the running task that " +
        "reads the partition, on a thread that has its TaskContext.")
    }
    if (streamAddress == 0 || ArrowArrayStream.wrap(streamAddress).snapshot().release != 0) {
      throw new IllegalArgumentException(
        "openArrowStream needs the address of a stream whose release callback is NULL.")
    }
    val (readerState, nativePartition) = partition match {
      case expected: P => stateAndPartition(expected)
      case _ =>
        throw new IllegalArgumentException("openArrowStream needs a partition of the scan of " +
          s"the reader factory, a ${classTag[P].runtimeClass.getSimpleName}, but got " +
          s"${Option(partition).map(_.getClass.getName).orNull}.")
    }
    var task: NativeLibrary.Task = null
    try {
      openStream(location, name, kind, readerState, nativePartition, schema, timeZoneId,
        streamAddress)(task = _)
      // As a failure of read, a failure to create the callbacks of the streams of engines, such as
      // a link error, is an error of the data source.
      call(name, "read")(NativeLibrary.wrapEngineStream(streamAddress, task, name))
    } catch {
      case e: Throwable =>
        releaseOnFailure(e, streamAddress, Option(task))
        throw e
    }
  }

  /**
   * Initializes the Arrow C stream at `streamAddress`, whose release callback is NULL, with the
   * data of a partition, by calling read of the library in the running task. It gives the context
   * of the task to `setTask` before read, since the library may use it until the stream is
   * released, also if this fails: the caller then releases the stream, and closes the context
   * after it, as the reader of Spark closes both.
   *
   * @param partition the partition, whose index in the scan the library gets as `partition_id`,
   *                  rather than the partition of the task: a task may read several partitions of
   *                  a scan, or of several scans, as after a coalesce or a union
   */
  private def openStream(
      location: NativeLibraryLocation,
      name: String,
      kind: Int,
      readerState: Array[Byte],
      partition: NativeInputPartition,
      schema: StructType,
      timeZoneId: String,
      streamAddress: Long)(
      setTask: NativeLibrary.Task => Unit): Unit = {
    val library = NativeLibraries.get(location)
    val stream = ArrowArrayStream.wrap(streamAddress)
    val task = NativeTask.create(Some(partition.index), read = true)
    setTask(task)
    NativeArrow.exportSchema(schema, timeZoneId) { schemaAddress =>
      call(name, "read") {
        library.read(task, kind, schemaAddress, readerState, partition.bytes, streamAddress)
      }
    }
    // Arrow calls the callbacks of the stream without checking them, and calling NULL would crash
    // the JVM.
    val snapshot = stream.snapshot()
    if (snapshot.release == 0 || snapshot.get_schema == 0 || snapshot.get_next == 0 ||
        snapshot.get_last_error == 0) {
      throw error(name, "read", "The library did not initialize the stream, or a callback of the " +
        "stream is NULL.")
    }
  }

  /**
   * After `failure`, releases the stream of an engine at `streamAddress`, if the library
   * initialized it, before the task that it may use, if any, and marks it released in case its
   * release callback did not. If the stream cannot be released, the task is not closed either,
   * since the library may still use it: both then leak, rather than letting the release of the
   * stream use a freed task. A failure to clean up is added to `failure`.
   */
  private def releaseOnFailure(
      failure: Throwable,
      streamAddress: Long,
      task: Option[NativeLibrary.Task]): Unit = {
    val released = try {
      val stream = ArrowArrayStream.wrap(streamAddress)
      stream.release()
      stream.markReleased()
      true
    } catch {
      case e: Throwable =>
        failure.addSuppressed(e)
        false
    }
    if (released) {
      task.foreach { task =>
        try task.close() catch {
          case e: Throwable => failure.addSuppressed(e)
        }
      }
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
  // Whether a write or a commit failed, after which the library only gets abort and close, as the
  // header says.
  private var failed = false

  // Fails a write or a commit that the header rules out, and records a failure of `f`.
  private def writeOrCommit[T](f: => T): T = {
    if (finished || closed) {
      throw SparkException.internalError("The data writer was already committed or aborted.")
    }
    if (failed) {
      throw SparkException.internalError("A write or a commit of the data writer failed.")
    }
    try f catch {
      case e: Throwable =>
        failed = true
        throw e
    }
  }

  override def write(batch: ColumnarBatch): Unit = writeOrCommit {
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

  override def commit(): WriterCommitMessage = writeOrCommit {
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
  // The flag of each running task that reads, set once the task failed or completed by a single
  // pair of listeners for all its reads, which then also remove it: a task that reads many
  // partitions, as after a coalesce, registers them once. The contexts of the reads keep their
  // flag, so they still see it once it is removed. A task is the key rather than its attempt ID,
  // which is 0 for each context of `TaskContext.empty()`. The keys are weak, compared by identity,
  // so that a context that never ends, such as one that a test sets on its thread, does not stay.
  private val endedTasks = new MapMaker().weakKeys().makeMap[TaskContext, AtomicBoolean]()

  /** Returns the flag that is set once the given task failed or completed. */
  private def endedFlag(taskContext: TaskContext): AtomicBoolean = {
    Option(endedTasks.get(taskContext)).getOrElse {
      val ended = new AtomicBoolean(false)
      Option(endedTasks.putIfAbsent(taskContext, ended)).getOrElse {
        // A listener runs right away if the task already failed or completed.
        def end(): Unit = {
          ended.set(true)
          endedTasks.remove(taskContext, ended)
        }
        taskContext.addTaskFailureListener((_, _) => end())
        taskContext.addTaskCompletionListener[Unit](_ => end())
        ended
      }
    }
  }

  // The number of tasks whose reads have a flag, for tests.
  private[ffi] def tasksWithEndedFlags: Int = endedTasks.size()

  /**
   * @param partitionId the `partition_id` of the context: the index of the partition in the scan
   *                    for a read, and by default the partition of the task
   * @param taskAttemptId the ID of the task attempt, by default the one of the task
   * @param read whether the context is the one of a read, which the library may use after the
   *             task ended: an engine may read the stream of a partition then, and a thread that
   *             the task does not wait for may be in get_next once it completes, as with
   *             `PipedRDD`. `is_interrupted` then also reports that the task failed or completed,
   *             and the context does not keep the task alive. A data writer only runs within its
   *             task, whose kill `is_interrupted` reports. Outside of a task, it reports nothing.
   */
  def create(
      partitionId: Option[Int] = None,
      taskAttemptId: Option[Long] = None,
      read: Boolean = false): NativeLibrary.Task = {
    val context = Option(TaskContext.get())
    val interrupted: BooleanSupplier = context match {
      case Some(taskContext) if read =>
        // Set once the task failed or completed, by listeners, so that a check only reads
        // volatile fields, unlike `isFailed()` and `isCompleted()`, which lock the task.
        val ended = endedFlag(taskContext)
        val task = new WeakReference(taskContext)
        () => ended.get() || Option(task.get()).forall(_.isInterrupted())
      case Some(taskContext) =>
        () => taskContext.isInterrupted()
      case None =>
        // Outside of a task, as in tests, nothing interrupts the library.
        () => false
    }
    new NativeLibrary.Task(
      context.map(_.stageId()).getOrElse(-1),
      context.map(_.stageAttemptNumber()).getOrElse(-1),
      partitionId.orElse(context.map(_.partitionId())).getOrElse(-1),
      context.map(_.attemptNumber()).getOrElse(0),
      taskAttemptId.orElse(context.map(_.taskAttemptId())).getOrElse(-1L),
      interrupted)
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
  // Also frees the buffers of the batches that may still be read when their readers are closed,
  // see `NativePartitionReader`.
  private[ffi] val cleaner = Cleaner.create()

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
 * The stream writers of the streaming queries, by query ID, to close those that are neither
 * committed nor aborted: a stream writer closes its handle once its micro-batch is committed or
 * aborted. The Data Source V2 streaming write has no hook for when the query stops, but a
 * micro-batch streaming query creates a new stream writer for each micro-batch, and a restarted
 * query creates a new one. So a previous stream writer of a query that is still open is closed
 * before the query creates a new one, and the last one when it becomes unreachable.
 */
object NativeStreamWriters {
  // Weak, so that the last stream writer of a query can still become unreachable.
  private val writers = new ConcurrentHashMap[String, WeakReference[NativeHandle]]()

  /**
   * Closes the previous stream writer of the query, before the query creates a new one. This has
   * no effect if that one is already closed.
   */
  def closePrevious(queryId: String): Unit = {
    // Forgets the queries whose last stream writer became unreachable.
    writers.values().removeIf(_.get() == null)
    Option(writers.remove(queryId)).flatMap(r => Option(r.get())).foreach(_.close())
  }

  /** Registers the new stream writer of the query. */
  def register(queryId: String, handle: NativeHandle): Unit = {
    writers.put(queryId, new WeakReference(handle))
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

  /**
   * Commits the write of a writer, for the epoch of a micro-batch or -1 for a batch write, and
   * closes the writer. It is not closed if the commit fails: Spark then aborts the write, which
   * closes it. Spark does not abort a committed write, see ColumnarBatchWrite and
   * ColumnarStreamingWrite.
   */
  def commitWrite(
      library: NativeLibrary,
      name: String,
      handle: NativeHandle,
      epochId: Long,
      messages: Array[WriterCommitMessage]): Unit = {
    call(name, "commit a write to") {
      library.commit(handle.get, epochId, NativeWriterCommitMessage.toBytes(messages))
    }
    handle.close()
  }

  /**
   * Aborts the write of a writer, for the epoch of a micro-batch or -1 for a batch write, and
   * closes the writer, also if the abort fails.
   */
  def abortWrite(
      library: NativeLibrary,
      name: String,
      handle: NativeHandle,
      epochId: Long,
      messages: Array[WriterCommitMessage]): Unit = {
    Utils.tryWithSafeFinally {
      call(name, "abort a write to") {
        library.abort(handle.get, epochId, NativeWriterCommitMessage.toBytes(messages))
      }
    }(handle.close())
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

  /**
   * Exports the domains, with their ranges as struct arrays whose fields are low and high, of the
   * Arrow type of the column in the schema that the reader got from create_reader, and
   * low_inclusive and high_inclusive, and calls `f` with them.
   *
   * @param timeZoneId the time zone of the timestamps of the schema that the reader got
   */
  def exportDomains[T](domains: Seq[ColumnDomain], timeZoneId: String)(
      f: Seq[NativeLibrary.Domain] => T): T = {
    // Frees what was allocated, in the reverse order, also when a later allocation fails.
    val cleanups = ArrayBuffer.empty[() => Unit]
    try {
      val structs = domains.map { domain =>
        val (ranges, rangesSchema) = domain.ranges match {
          case None => (0L, 0L)
          case Some(values) =>
            val schema = StructType(Seq(
              StructField("low", domain.dataType),
              StructField("high", domain.dataType),
              StructField("low_inclusive", BooleanType, nullable = false),
              StructField("high_inclusive", BooleanType, nullable = false)))
            val root = VectorSchemaRoot.create(
              ArrowUtils.toArrowSchema(
                schema, timeZoneId, errorOnDuplicatedFieldNames = true, largeVarTypes = false),
              ArrowUtils.rootAllocator)
            cleanups += (() => root.close())
            // Releasing the ranges does nothing if the library moved them to take ownership.
            val array = ArrowArray.allocateNew(ArrowUtils.rootAllocator)
            cleanups += { () => array.release(); array.close() }
            val arrowSchema = ArrowSchema.allocateNew(ArrowUtils.rootAllocator)
            cleanups += { () => arrowSchema.release(); arrowSchema.close() }
            val writer = ArrowWriter.create(root)
            values.foreach { range =>
              writer.write(InternalRow(range.low.orNull, range.high.orNull, range.lowInclusive,
                range.highInclusive))
            }
            writer.finish()
            Data.exportVectorSchemaRoot(ArrowUtils.rootAllocator, root, null, array, arrowSchema)
            (array.memoryAddress(), arrowSchema.memoryAddress())
        }
        new NativeLibrary.Domain(
          domain.path.toArray, domain.nullAllowed, domain.nanAllowed, ranges, rangesSchema)
      }
      f(structs)
    } finally {
      cleanups.reverseIterator.foreach(cleanup => cleanup())
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
