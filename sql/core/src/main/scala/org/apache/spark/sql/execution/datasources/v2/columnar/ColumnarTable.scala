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

import java.util
import java.util.Locale

import scala.util.Try

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit, SupportsTriggerAvailableNow}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

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
  private var readSchema: StructType = schema
  // Whether the columns were pruned.
  private var pruned = false

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
    // columns it reads.
    val required =
      StructType(requiredSchema.fieldNames.flatMap(name => schema.find(_.name == name)))
    if (!pruned) {
      // The reader is asked at most once, as the header promises, though Spark prunes the columns
      // of a scan builder once anyway.
      pruned = true
      if (required.length < schema.length && batchPlanner.pruneColumns(required)) {
        readSchema = required
      }
    } else if (!required.fieldNames.forall(readSchema.fieldNames.contains)) {
      throw SparkException.internalError(
        "The columns of a scan were pruned again, with columns that it does not read.")
    }
  }

  // The scan takes over the planner, so it is built once.
  private lazy val scan = new ColumnarScan(
    shortName, schema, readSchema, dataSource, Option(planner), pushedLimit)

  override def build(): Scan = scan
}

/**
 * @param pushedDownPlanner the planner that the operations were pushed down to, if any. Otherwise,
 *                          the scan creates one when its batch is planned.
 */
class ColumnarScan(
    shortName: String,
    fullSchema: StructType,
    prunedSchema: StructType,
    dataSource: ColumnarDataSource,
    pushedDownPlanner: Option[ColumnarBatchPlanner],
    pushedLimit: Option[Int])
  extends Scan with SupportsMetadata {

  // Spark may call `toBatch` more than once for the same scan; the partitions are planned once.
  private lazy val batch = new ColumnarBatchScan(
    pushedDownPlanner.getOrElse(dataSource.batchPlanner(fullSchema)))

  override def readSchema(): StructType = prunedSchema

  override def description(): String = shortName

  override def columnarSupportMode(): Scan.ColumnarSupportMode =
    Scan.ColumnarSupportMode.SUPPORTED

  override def toBatch: Batch = batch

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new ColumnarMicroBatchStream(dataSource.streamReader(fullSchema, checkpointLocation))
  }

  override def getMetaData(): Map[String, String] = {
    Map("ReadSchema" -> prunedSchema.simpleString) ++
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
 * The micro-batch stream of a [[ColumnarDataSource]]. It supports admission control only so that
 * the data source gets the offset that each micro-batch starts after: its default read limit reads
 * all the available data, which Spark then always asks for.
 */
/**
 * The micro-batch stream of a [[ColumnarDataSource]]. The reader gets the start of each
 * micro-batch, the end of the last one, for every trigger: with `Trigger.AvailableNow`, Spark runs
 * micro-batches up to the latest offset that the first micro-batch got, rather than wrapping the
 * stream in one that asks for the latest offset from the initial offset.
 */
class ColumnarMicroBatchStream(reader: ColumnarStreamReader)
  extends MicroBatchStream with SupportsTriggerAvailableNow {

  // The offset that the reader returned last, which the progress of the query reports.
  @volatile private var lastLatestOffset: Offset = _
  // With Trigger.AvailableNow, whether the latest offset of the run is the first one asked.
  @volatile private var availableNow = false
  @volatile private var availableNowEnd: Offset = _

  override def initialOffset(): Offset = ColumnarStreamOffset(reader.initialOffset())

  override def latestOffset(): Offset = {
    throw SparkException.internalError(
      "latestOffset(Offset, ReadLimit) should be called instead of latestOffset().")
  }

  override def latestOffset(start: Offset, limit: ReadLimit): Offset = {
    val latest = if (availableNow && availableNowEnd != null) {
      availableNowEnd
    } else {
      ColumnarStreamOffset(reader.latestOffset(start.json()))
    }
    if (availableNow) {
      availableNowEnd = latest
    }
    lastLatestOffset = latest
    latest
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
