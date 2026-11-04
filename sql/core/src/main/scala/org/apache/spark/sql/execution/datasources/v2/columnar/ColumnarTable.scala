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
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
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

  // The streams of the scans of the table return the columns of their read schemas, which they
  // project from the batches of their stream readers, see ColumnarScan.
  private lazy val tableCapabilities = {
    val capabilities = new util.HashSet[TableCapability](
      createDataSource(withTableOptions(CaseInsensitiveStringMap.empty())).capabilities())
    capabilities.add(TableCapability.STREAMING_COLUMN_PRUNING)
    util.Collections.unmodifiableSet(capabilities)
  }

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
    // columns it reads. The required columns are only recorded here, see ColumnarScan.
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
 * A scan of a [[ColumnarDataSource]]. It reads the top-level columns that the query needs, the
 * columns of its read schema: a batch scan asks its planner to prune the other columns when it
 * plans its partitions, and drops them from the batches of the reader if the planner does not,
 * while a streaming scan drops them from the batches of its stream reader, which reads all the
 * columns of the full schema.
 *
 * @param requiredSchema the top-level columns that the query needs, if fewer than those of the
 *                       full schema
 * @param pushedDownPlanner the planner that the operations were pushed down to, if any. Otherwise,
 *                          the scan creates one when its batch is planned.
 */
class ColumnarScan(
    shortName: String,
    fullSchema: StructType,
    requiredSchema: Option[StructType],
    dataSource: ColumnarDataSource,
    pushedDownPlanner: Option[ColumnarBatchPlanner],
    pushedLimit: Option[Int])
  extends Scan with SupportsMetadata {

  // The ordinals in the full schema of the columns that the scan reads, if it reads fewer.
  private def requiredOrdinals: Option[Array[Int]] =
    requiredSchema.map(_.fieldNames.map(fullSchema.fieldIndex))

  // Spark may call `toBatch` more than once for the same scan; the partitions are planned once,
  // after the planner prunes the columns, if it does. A failure is kept too, so that the planner
  // is asked to prune the columns at most once.
  private lazy val batch: Try[ColumnarBatchScan] = Try {
    val planner = pushedDownPlanner.getOrElse(dataSource.batchPlanner(fullSchema))
    val pruned = requiredSchema.forall(planner.pruneColumns)
    new ColumnarBatchScan(planner, if (pruned) None else requiredOrdinals)
  }

  // Whether the scan created a micro-batch stream, whose reader reads all the columns.
  @volatile private var streamCreated = false

  override def readSchema(): StructType = requiredSchema.getOrElse(fullSchema)

  override def description(): String = shortName

  override def columnarSupportMode(): Scan.ColumnarSupportMode =
    Scan.ColumnarSupportMode.SUPPORTED

  override def toBatch: Batch = batch.get

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    streamCreated = true
    new ColumnarMicroBatchStream(dataSource.streamReader(fullSchema), requiredOrdinals)
  }

  override def getMetaData(): Map[String, String] = {
    // The columns that the data source reads, when the scan drops some of them after the read: the
    // reader of a stream reads all of them, and so does a batch whose planner does not prune them.
    val dropsColumns = requiredSchema.isDefined &&
      (streamCreated || batch.toOption.exists(_.projection.isDefined))
    Map("ReadSchema" -> readSchema().simpleString) ++
      (if (dropsColumns) Map("SourceReadSchema" -> fullSchema.simpleString) else Map.empty) ++
      pushedLimit.map(limit => "PushedLimit" -> s"LIMIT $limit")
  }
}

/**
 * The batch of a scan, which plans it once, when Spark first asks for its partitions or its reader
 * factory. A failure is planned once too: the physical plan can ask again, for example from a lazy
 * value, and gets the same error.
 *
 * @param projection the ordinals of the columns that the scan reads, if the partitions return
 *                   more columns
 */
class ColumnarBatchScan(
    planner: ColumnarBatchPlanner,
    val projection: Option[Array[Int]] = None)
  extends Batch {
  private lazy val planned: Try[ColumnarPlannedBatch] = Try(planner.plan())

  override def planInputPartitions(): Array[InputPartition] = planned.get.partitions

  override def createReaderFactory(): PartitionReaderFactory = {
    ColumnarProjection(planned.get.readerFactory, projection)
  }
}

case class ColumnarStreamOffset(json: String) extends Offset

/**
 * @param projection the ordinals of the columns that the scan reads, if the reader reads more
 *                   columns
 */
class ColumnarMicroBatchStream(reader: ColumnarStreamReader, projection: Option[Array[Int]])
  extends MicroBatchStream {

  override def initialOffset(): Offset = ColumnarStreamOffset(reader.initialOffset())

  override def latestOffset(): Offset = ColumnarStreamOffset(reader.latestOffset())

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    reader.partitions(start.json(), end.json())
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    ColumnarProjection(new ColumnarStreamPartitionReaderFactory(reader), projection)
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

object ColumnarProjection {
  /**
   * Returns `factory`, or a factory whose readers only return the columns at `projection` of the
   * batches of the readers of `factory`, if any.
   */
  def apply(
      factory: PartitionReaderFactory,
      projection: Option[Array[Int]]): PartitionReaderFactory = projection match {
    case Some(ordinals) => new ColumnarProjectionReaderFactory(factory, ordinals)
    case None => factory
  }
}

/**
 * Reads only the columns at `ordinals` of the batches of the readers of `factory`, for a scan
 * whose partitions return more columns than the scan reads.
 */
class ColumnarProjectionReaderFactory(factory: PartitionReaderFactory, ordinals: Array[Int])
  extends ColumnarOnlyPartitionReaderFactory {
  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    new ColumnarProjectionReader(factory.createColumnarReader(partition), ordinals)
  }
}

private class ColumnarProjectionReader(
    reader: PartitionReader[ColumnarBatch],
    ordinals: Array[Int])
  extends PartitionReader[ColumnarBatch] {

  // The projection of the current batch of the reader, which `get` returns until `next`.
  private var batch: ColumnarBatch = _

  override def next(): Boolean = {
    val hasNext = reader.next()
    batch = if (hasNext) new ProjectedColumnarBatch(reader.get(), ordinals) else null
    hasNext
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = reader.close()

  override def currentMetricsValues(): Array[CustomTaskMetric] = reader.currentMetricsValues()
}

/**
 * The columns at `ordinals` of `batch`. Releasing it releases all the columns of `batch`, which a
 * consumer that releases each batch would not release otherwise.
 */
private class ProjectedColumnarBatch(batch: ColumnarBatch, ordinals: Array[Int])
  extends ColumnarBatch(ordinals.map(batch.column), batch.numRows()) {

  override def close(): Unit = batch.close()

  override def closeIfFreeable(): Unit = batch.closeIfFreeable()
}
