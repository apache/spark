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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ, STREAMING_WRITE, TRUNCATE}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The Data Source V2 [[Table]] of a [[ColumnarDataSource]]. It creates a new data source for each
 * scan and each write, with their options on top of the options of the table.
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

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ, STREAMING_WRITE, TRUNCATE)

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
  with SupportsPushDownV2Filters
  with SupportsPushDownLimit
  with SupportsPushDownRequiredColumns {

  private lazy val dataSource = createDataSource()

  // Created on first use, because a streaming scan neither pushes anything down nor reads in
  // batch, and its data source may not support batch scans at all.
  private var reader: ColumnarReader = _

  private var pushed: Array[Predicate] = Array.empty
  private var pushedLimit: Option[Int] = None
  private var readSchema: StructType = schema

  private def batchReader: ColumnarReader = {
    if (reader == null) {
      reader = dataSource.reader(schema)
    }
    reader
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val remaining = batchReader.pushPredicates(predicates)
    pushed = predicates.filterNot(remaining.contains)
    remaining
  }

  override def pushedPredicates(): Array[Predicate] = pushed

  override def pushLimit(limit: Int): Boolean = {
    val isPushed = batchReader.pushLimit(limit)
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
    if (required.length < schema.length && batchReader.pruneColumns(required)) {
      readSchema = required
    }
  }

  override def build(): Scan = {
    new ColumnarScan(
      shortName, schema, readSchema, () => batchReader, () => dataSource, pushed, pushedLimit)
  }
}

class ColumnarScan(
    shortName: String,
    fullSchema: StructType,
    prunedSchema: StructType,
    batchReader: () => ColumnarReader,
    dataSource: () => ColumnarDataSource,
    pushedPredicates: Array[Predicate],
    pushedLimit: Option[Int])
  extends Scan with SupportsMetadata {

  // Spark may call `toBatch` more than once for the same scan; the partitions are planned once.
  private lazy val batch = new ColumnarBatchScan(batchReader())

  override def readSchema(): StructType = prunedSchema

  override def description(): String = shortName

  override def columnarSupportMode(): Scan.ColumnarSupportMode =
    Scan.ColumnarSupportMode.SUPPORTED

  override def toBatch: Batch = batch

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new ColumnarMicroBatchStream(dataSource().streamReader(fullSchema))
  }

  override def getMetaData(): Map[String, String] = {
    Map(
      "PushedPredicates" -> pushedPredicates.mkString("[", ", ", "]"),
      "ReadSchema" -> prunedSchema.simpleString
    ) ++ pushedLimit.map(limit => "PushedLimit" -> s"LIMIT $limit")
  }
}

class ColumnarBatchScan(reader: ColumnarReader) extends Batch {
  private lazy val partitions: Array[InputPartition] = reader.partitions()

  override def planInputPartitions(): Array[InputPartition] = partitions

  override def createReaderFactory(): PartitionReaderFactory = {
    // The reader is serialized to the executors, so it has to be planned first.
    partitions
    new ColumnarPartitionReaderFactory(reader)
  }
}

case class ColumnarStreamOffset(json: String) extends Offset

class ColumnarMicroBatchStream(reader: ColumnarStreamReader) extends MicroBatchStream {

  override def initialOffset(): Offset = ColumnarStreamOffset(reader.initialOffset())

  override def latestOffset(): Offset = ColumnarStreamOffset(reader.latestOffset())

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

class ColumnarPartitionReaderFactory(reader: ColumnarReader)
  extends ColumnarOnlyPartitionReaderFactory {
  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    reader.read(partition)
  }
}

class ColumnarStreamPartitionReaderFactory(reader: ColumnarStreamReader)
  extends ColumnarOnlyPartitionReaderFactory {
  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    reader.read(partition)
  }
}
