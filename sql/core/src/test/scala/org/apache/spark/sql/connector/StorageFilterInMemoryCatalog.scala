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

package org.apache.spark.sql.connector

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, JoinedRow}
import org.apache.spark.sql.connector.catalog.{BufferedRows, Column, InMemoryBaseTable, InMemoryCatalog, InMemoryCatalystRuntimeFilterTable}
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference, SortOrder, Transform}
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, StorageFilterRequest, SupportsReportOrdering, SupportsStorageFilters}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.connector.SchemaAlignmentConfig
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, LongType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * An in-memory catalog whose scans implement [[SupportsStorageFilters]] over the columns they read,
 * or only those the table property `storage-filter-columns` names, comma-separated. Their
 * readers apply the filter, and record what they read in `reads` and `plainReads`. With the table
 * property `columnar-reads` set to `true`, they return each split in one batch. Their tables
 * take runtime filters as [[InMemoryCatalystRuntimeFilterTable]] does, on the source columns of
 * every partition transform by default, as Iceberg's do. Their scans report the ordering of the
 * table.
 */
class StorageFilterInMemoryCatalog extends InMemoryCatalog {
  // scalastyle:off argcount
  override protected def newInMemoryTable(
      name: String,
      columns: Array[Column],
      partitioning: Array[Transform],
      properties: java.util.Map[String, String],
      constraints: Array[Constraint],
      distribution: Distribution,
      ordering: Array[SortOrder],
      requiredNumPartitions: Option[Int],
      advisoryPartitionSize: Option[Long],
      distributionStrictlyRequired: Boolean,
      numRowsPerSplit: Int,
      id: String,
      schemaAlignmentConfig: SchemaAlignmentConfig): InMemoryBaseTable = {
    // scalastyle:on argcount
    val tableName = name
    val tableOrdering = ordering
    val declared = Option(properties.get(StorageFilterInMemoryCatalog.DeclaredKey))
      .map(_.split(",").map(_.trim).toSet)
    val columnar = properties.get(StorageFilterInMemoryCatalog.ColumnarKey) == "true"
    new InMemoryCatalystRuntimeFilterTable(name, columns, partitioning, properties, constraints,
        distribution, ordering, requiredNumPartitions, advisoryPartitionSize,
        distributionStrictlyRequired, numRowsPerSplit) {
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
        new InMemoryCatalystRuntimeFilterScanBuilder(schema(), options) {
          override def build: Scan = {
            val built = super.build.asInstanceOf[InMemoryCatalystRuntimeFilterBatchScan]
            val scanSchema = built.readSchema
            new InMemoryCatalystRuntimeFilterBatchScan(
                built._data, scanSchema, built.tableSchema, built.options)
                with SupportsStorageFilters with SupportsReportOrdering {
              override def outputOrdering(): Array[SortOrder] = tableOrdering
              override def storageFilterAttributes(): Array[NamedReference] =
                scanSchema.fieldNames.filter(n => declared.forall(_.contains(n)))
                  .map(FieldReference.column(_): NamedReference)
              override def createReaderFactory(): PartitionReaderFactory =
                StorageFilterApplyingReaderFactory(
                  tableName, super.createReaderFactory(), scanSchema, columnar)
            }
          }
        }
    }
  }
}

object StorageFilterInMemoryCatalog {
  val DeclaredKey = "storage-filter-columns"
  val ColumnarKey = "columnar-reads"

  /** For each reader that got a storage filter: its table, its split's key, rows read and kept. */
  val reads = new ConcurrentLinkedQueue[(String, String, Int, Int)]()

  /** For each reader that got no storage filter: its table and its split's key. */
  val plainReads = new ConcurrentLinkedQueue[(String, String)]()

  /** The values each reader asks a filter of one column about, before it reads. */
  @volatile var probes: Seq[Any] = Nil

  /**
   * For each reader that got a filter of one column: its split's key and the probes its filter
   * keeps.
   */
  val probed = new ConcurrentLinkedQueue[(String, Set[Any])]()
}

/**
 * Reads as `factory` does, dropping the rows a storage filter rejects. Asked for the checked
 * column, it marks every row it returns, since it returns only those the filter kept. With
 * `columnar`, it returns the rows of each split in one batch.
 */
case class StorageFilterApplyingReaderFactory(
    table: String,
    factory: PartitionReaderFactory,
    readSchema: StructType,
    columnar: Boolean = false) extends PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = columnar

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
    new OneBatchReader(createReader(partition), readSchema)

  override def createColumnarReader(
      partition: InputPartition,
      request: StorageFilterRequest): PartitionReader[ColumnarBatch] = {
    val schema =
      if (request.appendsCheckedColumn()) readSchema.add("checked", BooleanType) else readSchema
    new OneBatchReader(createReader(partition, request), schema)
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    StorageFilterInMemoryCatalog.plainReads.add(
      table -> partition.asInstanceOf[BufferedRows].keyString())
    factory.createReader(partition)
  }

  override def createReader(
      partition: InputPartition,
      request: StorageFilterRequest): PartitionReader[InternalRow] = {
    val filter = request.filter()
    val positions = filter.references().map(r => readSchema.fieldIndex(r.fieldNames().head))
    val types = positions.map(readSchema(_).dataType)
    val checked = new GenericInternalRow(Array[Any](true))
    val key = partition.asInstanceOf[BufferedRows].keyString()
    if (positions.length == 1) {
      StorageFilterInMemoryCatalog.probed.add(key ->
        StorageFilterInMemoryCatalog.probes.filter(p => filter.eval(new GenericInternalRow(
          Array[Any](p)))).toSet)
    }
    val reader = factory.createReader(partition)
    new PartitionReader[InternalRow] {
      private var current: InternalRow = _
      private var numRead = 0
      private var numKept = 0

      override def next(): Boolean = {
        var found = false
        while (!found && reader.next()) {
          val row = reader.get()
          numRead += 1
          val values = new GenericInternalRow(positions.indices.map { i =>
            row.get(positions(i), types(i))
          }.toArray)
          if (filter.eval(values)) {
            numKept += 1
            current = if (request.appendsCheckedColumn()) new JoinedRow(row, checked) else row
            found = true
          }
        }
        found
      }

      override def get(): InternalRow = current

      override def currentMetricsValues(): Array[CustomTaskMetric] = reader.currentMetricsValues()

      override def close(): Unit = {
        StorageFilterInMemoryCatalog.reads.add((table, key, numRead, numKept))
        reader.close()
      }
    }
  }
}

/** Returns the rows of `rows`, which have the columns of `schema`, in one batch. */
class OneBatchReader(rows: PartitionReader[InternalRow], schema: StructType)
  extends PartitionReader[ColumnarBatch] {

  private var batch: ColumnarBatch = _

  override def next(): Boolean = batch == null && {
    val buffered = ArrayBuffer.empty[InternalRow]
    while (rows.next()) buffered += rows.get().copy()
    val vectors = OnHeapColumnVector.allocateColumns(math.max(buffered.size, 1), schema)
    buffered.zipWithIndex.foreach { case (row, i) =>
      schema.fields.indices.foreach { j =>
        if (row.isNullAt(j)) {
          vectors(j).putNull(i)
        } else {
          schema(j).dataType match {
            case LongType | TimestampType => vectors(j).putLong(i, row.getLong(j))
            case IntegerType | DateType => vectors(j).putInt(i, row.getInt(j))
            case BooleanType => vectors(j).putBoolean(i, row.getBoolean(j))
          }
        }
      }
    }
    batch = new ColumnarBatch(vectors.toArray[ColumnVector], buffered.size)
    buffered.nonEmpty
  }

  override def get(): ColumnarBatch = batch

  override def currentMetricsValues(): Array[CustomTaskMetric] = rows.currentMetricsValues()

  override def close(): Unit = {
    rows.close()
    if (batch != null) batch.close()
  }
}
