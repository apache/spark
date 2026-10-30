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
package org.apache.spark.sql.execution.datasources.v2.columnar

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.util
import java.util.Locale

import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit, SupportsTriggerAvailableNow}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.streaming.checkpointing.HDFSMetadataLog
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/**
 * The Data Source V2 [[Table]] of a [[ColumnarDataSource]]. It creates a new data source for each
 * scan and each write, with their options on top of the options of the table. Its capabilities
 * are those that the data source reports with the options of the table, while a scan or a write
 * may have other options: the data source checks them again with those.
 *
 * @param tableOptions the options of the table, given to `TableProvider.getTable`. They are the
 *                     options of `CREATE TABLE ... USING ... OPTIONS (...)` for a table in a
 *                     catalog, while the scans and the writes of the table only get the options
 *                     of their query.
 */
class ColumnarTable(
    shortName: String,
    tableSchema: StructType,
    tableOptions: util.Map[String, String],
    createDataSource: CaseInsensitiveStringMap => ColumnarDataSource)
  extends Table with SupportsRead with SupportsWrite {

  override def name(): String = shortName

  override def schema(): StructType = tableSchema

  private lazy val tableCapabilities =
    createDataSource(withTableOptions(CaseInsensitiveStringMap.empty())).capabilities()

  override def capabilities(): util.Set[TableCapability] = tableCapabilities

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ColumnarScanBuilder(
      shortName, tableSchema, () => createDataSource(withTableOptions(options)))
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new ColumnarWriteBuilder(
      shortName, info, () => createDataSource(withTableOptions(info.options())))
  }

  private def withTableOptions(options: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    val merged = new util.HashMap[String, String]()
    tableOptions.forEach((key, value) => merged.put(key.toLowerCase(Locale.ROOT), value))
    // The keys of a case-insensitive map are in lower case.
    options.forEach((key, value) => merged.put(key, value))
    new CaseInsensitiveStringMap(merged)
  }
}

class ColumnarScanBuilder(
    shortName: String,
    schema: StructType,
    createDataSource: () => ColumnarDataSource)
  extends ScanBuilder
  with SupportsPushDownLimit
  with SupportsPushDownRequiredColumns {

  private lazy val dataSource = createDataSource()

  // Created on first use, because a streaming scan neither pushes anything down nor reads in
  // batch, and its data source may not support batch scans at all.
  private var planner: ColumnarBatchPlanner = _

  private var pushedLimit: Option[Int] = None
  // The limit passed to the reader, and whether the reader applies it.
  private var limitResult: Option[(Int, Boolean)] = None
  // The top-level columns that the query needs, if fewer than those of the schema.
  private var requiredSchema: Option[StructType] = None

  private def batchPlanner: ColumnarBatchPlanner = {
    if (planner == null) {
      planner = dataSource.batchPlanner(schema)
    }
    planner
  }

  override def pushLimit(limit: Int): Boolean = limitResult match {
    // Spark can push a limit more than once, for example the limit of LIMIT ... OFFSET, but the
    // reader is only asked once. A limit at least as large as the first one is already applied.
    case Some((firstLimit, isPushed)) =>
      isPushed && limit >= firstLimit
    case None =>
      val isPushed = batchPlanner.pushLimit(limit)
      limitResult = Some(limit -> isPushed)
      if (isPushed) {
        pushedLimit = Some(limit)
      }
      isPushed
  }

  // Spark always applies the limit again: a data source is free to return more rows.
  override def isPartiallyPushed(): Boolean = true

  override def pruneColumns(requiredSchema: StructType): Unit = {
    // Only top-level columns are pruned by the data source. Spark prunes the nested fields of the
    // columns it reads. The required columns are only recorded here: a batch scan asks its planner
    // to prune the others, and a streaming scan, which has no planner, passes the required
    // columns to its stream reader.
    val required =
      StructType(requiredSchema.fieldNames.flatMap(name => schema.find(_.name == name)))
    if (required.length < schema.length) {
      this.requiredSchema = Some(required)
    }
  }

  // The scan takes over the planner, so it is built once.
  private lazy val scan = new ColumnarScan(
    shortName, schema, requiredSchema, dataSource, Option(planner), pushedLimit)

  override def build(): Scan = scan
}

/**
 * @param requiredSchema the top-level columns that the query needs, if fewer than those of the
 *                       full schema. A batch scan asks its planner to prune the other columns,
 *                       while a streaming scan reads only these columns: they are the schema that
 *                       its stream reader gets.
 * @param pushedDownPlanner the planner that the operations were pushed down to, if any. Otherwise,
 *                          the scan creates one when its batch is planned or pruned.
 */
class ColumnarScan(
    shortName: String,
    fullSchema: StructType,
    requiredSchema: Option[StructType],
    dataSource: ColumnarDataSource,
    pushedDownPlanner: Option[ColumnarBatchPlanner],
    pushedLimit: Option[Int])
  extends Scan with SupportsMetadata {

  private lazy val planner = pushedDownPlanner.getOrElse(dataSource.batchPlanner(fullSchema))

  // The columns that the batch reads: the required ones if the planner prunes the others.
  private lazy val batchSchema = requiredSchema.filter(planner.pruneColumns).getOrElse(fullSchema)

  // The columns that the stream reads.
  private def streamSchema: StructType = requiredSchema.getOrElse(fullSchema)

  // Spark may call `toBatch` more than once for the same scan; the partitions are planned once,
  // after the columns are pruned.
  private lazy val batch = {
    batchSchema
    new ColumnarBatchScan(planner)
  }

  // Whether the scan is the one of a micro-batch stream, which Spark asks for its read schema
  // after it creates the stream.
  @volatile private var streaming = false

  override def readSchema(): StructType = if (streaming) streamSchema else batchSchema

  override def description(): String = shortName

  override def columnarSupportMode(): Scan.ColumnarSupportMode =
    Scan.ColumnarSupportMode.SUPPORTED

  override def toBatch: Batch = batch

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    streaming = true
    new ColumnarMicroBatchStream(
      dataSource.streamReader(streamSchema, checkpointLocation), checkpointLocation)
  }

  override def getMetaData(): Map[String, String] = {
    Map("ReadSchema" -> readSchema().simpleString) ++
      pushedLimit.map(limit => "PushedLimit" -> s"LIMIT $limit")
  }
}

/**
 * The batch of a scan, which plans it once, when Spark first asks for its partitions or its reader
 * factory. A failure is planned once too: the physical plan can ask again, for example from a lazy
 * value, and gets the same error.
 */
class ColumnarBatchScan(planner: ColumnarBatchPlanner) extends Batch {
  private lazy val planned: Try[ColumnarPlannedBatch] = Try(planner.plan())

  override def planInputPartitions(): Array[InputPartition] = planned.get.partitions

  override def createReaderFactory(): PartitionReaderFactory = planned.get.readerFactory
}

case class ColumnarStreamOffset(json: String) extends Offset

/**
 * The micro-batch stream of a [[ColumnarDataSource]]. The reader gets the offset that each
 * micro-batch starts after, the end of the last one, for every trigger, and the limit on the data
 * of a micro-batch, its default read limit, all the available data unless it limits it. With
 * `Trigger.AvailableNow`, Spark runs micro-batches up to the first latest offset without a limit
 * that Spark asks for, rather than wrapping the stream in one that asks for the latest offset from
 * the initial offset: in one micro-batch, or within the limit in several.
 *
 * @param checkpointLocation the location of the checkpoint of the source, where the stream keeps
 *                           the initial offset, under `ColumnarMicroBatchStream.InitialOffsetLog`
 */
class ColumnarMicroBatchStream(reader: ColumnarStreamReader, checkpointLocation: String)
  extends MicroBatchStream with SupportsTriggerAvailableNow {

  // The latest offset without a limit that the reader returned last, which the progress of the
  // query reports.
  @volatile private var lastLatestOffset: Offset = _
  // The limit on the data of each micro-batch, asked once.
  private lazy val readLimit = reader.defaultReadLimit()
  // Whether the trigger is Trigger.AvailableNow.
  @volatile private var availableNow = false
  // The end of the run with Trigger.AvailableNow: the first latest offset without a limit asked.
  @volatile private var availableNowEnd: Offset = _
  // Spark asks for the initial offset for the start of the first latest offset, and again to plan
  // the first micro-batch, which both get the same offset. The reader is only asked when the
  // checkpoint does not have it yet: a query that restarts before its first micro-batch is
  // committed plans it again from the same start, also when the initial offset of the reader
  // depends on when it is asked, such as the latest data.
  private lazy val initial = {
    val log = new ColumnarInitialOffsetLog(SparkSession.active,
      new Path(checkpointLocation, ColumnarMicroBatchStream.InitialOffsetLog).toString)
    ColumnarStreamOffset(log.get(0).getOrElse {
      val offset = reader.initialOffset()
      log.add(0, offset)
      offset
    })
  }

  override def initialOffset(): Offset = initial

  override def latestOffset(): Offset = {
    throw SparkException.internalError(
      "latestOffset(Offset, ReadLimit) should be called instead of latestOffset().")
  }

  override def getDefaultReadLimit: ReadLimit = readLimit

  override def latestOffset(start: Offset, limit: ReadLimit): Offset = {
    def ask(limit: ReadLimit, runEnd: Option[Offset]): Offset =
      ColumnarStreamOffset(reader.latestOffset(start.json(), limit, runEnd.map(_.json())))
    // The latest offset without a limit, which the progress reports: with Trigger.AvailableNow,
    // the end of the run, asked once, and otherwise asked for each micro-batch.
    val latest = if (!availableNow) {
      ask(ReadLimit.allAvailable(), None)
    } else {
      if (availableNowEnd == null) availableNowEnd = ask(ReadLimit.allAvailable(), None)
      availableNowEnd
    }
    lastLatestOffset = latest
    if (limit == ReadLimit.allAvailable()) latest else ask(limit, Option(availableNowEnd))
  }

  override def prepareForTriggerAvailableNow(): Unit = {
    availableNow = true
  }

  override def reportLatestOffset(): Offset = lastLatestOffset

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    reader.partitions(start.json(), end.json())
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new ColumnarStreamPartitionReaderFactory(reader)
  }

  override def deserializeOffset(json: String): Offset = ColumnarStreamOffset(json)

  override def commit(end: Offset): Unit = reader.commit(end.json())

  override def stop(): Unit = reader.stop()
}

object ColumnarMicroBatchStream {
  /**
   * The directory, in the checkpoint of the source, of the log that keeps the initial offset. The
   * names that start with `_spark` are reserved for Spark there, while the reader may keep what it
   * needs under the other names.
   */
  val InitialOffsetLog = "_spark_initial_offset"
}

/** Keeps the initial offset of a [[ColumnarMicroBatchStream]], as the JSON of the reader. */
class ColumnarInitialOffsetLog(session: SparkSession, path: String)
  extends HDFSMetadataLog[String](session, path) {

  private val Version = 1

  override protected def serialize(offset: String, out: OutputStream): Unit = {
    out.write(s"v$Version\n$offset".getBytes(UTF_8))
  }

  override protected def deserialize(in: InputStream): String = {
    val content = Utils.toString(in)
    val newLine = content.indexOf('\n')
    if (newLine < 0) {
      throw new IllegalStateException(
        "Log file was malformed: failed to detect the log file version line.")
    }
    validateVersion(content.substring(0, newLine), Version)
    content.substring(newLine + 1)
  }
}

/**
 * A [[ColumnarDataSource]] only produces columnar batches, so Spark never asks for rows: the scan
 * reports [[Scan.ColumnarSupportMode.SUPPORTED]].
 */
abstract class ColumnarOnlyPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    throw SparkException.internalError("Columnar data sources only support columnar reads.")
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = true
}

class ColumnarStreamPartitionReaderFactory(reader: ColumnarStreamReader)
  extends ColumnarOnlyPartitionReaderFactory {
  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    reader.read(partition)
  }
}
