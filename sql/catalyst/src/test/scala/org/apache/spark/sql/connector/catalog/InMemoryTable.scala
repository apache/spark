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

package org.apache.spark.sql.connector.catalog

import java.time.{Instant, ZoneId}
import java.time.temporal.ChronoUnit
import java.util
import java.util.OptionalLong

import scala.collection.mutable

import org.scalatest.Assertions._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, JoinedRow}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, DateTimeUtils}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/**
 * A simple in-memory table. Rows are stored as a buffered group produced by each output task.
 */
class InMemoryTable(
    val name: String,
    val schema: StructType,
    override val partitioning: Array[Transform],
    override val properties: util.Map[String, String],
    val distribution: Distribution = Distributions.unspecified(),
    val ordering: Array[SortOrder] = Array.empty,
    val numPartitions: Option[Int] = None)
  extends Table with SupportsRead with SupportsWrite with SupportsDelete
      with SupportsMetadataColumns {

  private object PartitionKeyColumn extends MetadataColumn {
    override def name: String = "_partition"
    override def dataType: DataType = StringType
    override def comment: String = "Partition key used to store the row"
  }

  private object IndexColumn extends MetadataColumn {
    override def name: String = "index"
    override def dataType: DataType = IntegerType
    override def comment: String = "Metadata column used to conflict with a data column"
  }

  // purposely exposes a metadata column that conflicts with a data column in some tests
  override val metadataColumns: Array[MetadataColumn] = Array(IndexColumn, PartitionKeyColumn)
  private val metadataColumnNames = metadataColumns.map(_.name).toSet -- schema.map(_.name)

  private val allowUnsupportedTransforms =
    properties.getOrDefault("allow-unsupported-transforms", "false").toBoolean

  partitioning.foreach {
    case _: IdentityTransform =>
    case _: YearsTransform =>
    case _: MonthsTransform =>
    case _: DaysTransform =>
    case _: HoursTransform =>
    case _: BucketTransform =>
    case t if !allowUnsupportedTransforms =>
      throw new IllegalArgumentException(s"Transform $t is not a supported transform")
  }

  // The key `Seq[Any]` is the partition values.
  val dataMap: mutable.Map[Seq[Any], BufferedRows] = mutable.Map.empty

  def data: Array[BufferedRows] = dataMap.values.toArray

  def rows: Seq[InternalRow] = dataMap.values.flatMap(_.rows).toSeq

  private val partCols: Array[Array[String]] = partitioning.flatMap(_.references).map { ref =>
    schema.findNestedField(ref.fieldNames(), includeCollections = false) match {
      case Some(_) => ref.fieldNames()
      case None => throw new IllegalArgumentException(s"${ref.describe()} does not exist.")
    }
  }

  private val UTC = ZoneId.of("UTC")
  private val EPOCH_LOCAL_DATE = Instant.EPOCH.atZone(UTC).toLocalDate

  private def getKey(row: InternalRow): Seq[Any] = {
    def extractor(
        fieldNames: Array[String],
        schema: StructType,
        row: InternalRow): (Any, DataType) = {
      val index = schema.fieldIndex(fieldNames(0))
      val value = row.toSeq(schema).apply(index)
      if (fieldNames.length > 1) {
        (value, schema(index).dataType) match {
          case (row: InternalRow, nestedSchema: StructType) =>
            extractor(fieldNames.drop(1), nestedSchema, row)
          case (_, dataType) =>
            throw new IllegalArgumentException(s"Unsupported type, ${dataType.simpleString}")
        }
      } else {
        (value, schema(index).dataType)
      }
    }

    val cleanedSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema)
    partitioning.map {
      case IdentityTransform(ref) =>
        extractor(ref.fieldNames, cleanedSchema, row)._1
      case YearsTransform(ref) =>
        extractor(ref.fieldNames, cleanedSchema, row) match {
          case (days: Int, DateType) =>
            ChronoUnit.YEARS.between(EPOCH_LOCAL_DATE, DateTimeUtils.daysToLocalDate(days))
          case (micros: Long, TimestampType) =>
            val localDate = DateTimeUtils.microsToInstant(micros).atZone(UTC).toLocalDate
            ChronoUnit.YEARS.between(EPOCH_LOCAL_DATE, localDate)
          case (v, t) =>
            throw new IllegalArgumentException(s"Match: unsupported argument(s) type - ($v, $t)")
        }
      case MonthsTransform(ref) =>
        extractor(ref.fieldNames, cleanedSchema, row) match {
          case (days: Int, DateType) =>
            ChronoUnit.MONTHS.between(EPOCH_LOCAL_DATE, DateTimeUtils.daysToLocalDate(days))
          case (micros: Long, TimestampType) =>
            val localDate = DateTimeUtils.microsToInstant(micros).atZone(UTC).toLocalDate
            ChronoUnit.MONTHS.between(EPOCH_LOCAL_DATE, localDate)
          case (v, t) =>
            throw new IllegalArgumentException(s"Match: unsupported argument(s) type - ($v, $t)")
        }
      case DaysTransform(ref) =>
        extractor(ref.fieldNames, cleanedSchema, row) match {
          case (days, DateType) =>
            days
          case (micros: Long, TimestampType) =>
            ChronoUnit.DAYS.between(Instant.EPOCH, DateTimeUtils.microsToInstant(micros))
          case (v, t) =>
            throw new IllegalArgumentException(s"Match: unsupported argument(s) type - ($v, $t)")
        }
      case HoursTransform(ref) =>
        extractor(ref.fieldNames, cleanedSchema, row) match {
          case (micros: Long, TimestampType) =>
            ChronoUnit.HOURS.between(Instant.EPOCH, DateTimeUtils.microsToInstant(micros))
          case (v, t) =>
            throw new IllegalArgumentException(s"Match: unsupported argument(s) type - ($v, $t)")
        }
      case BucketTransform(numBuckets, ref) =>
        val (value, dataType) = extractor(ref.fieldNames, cleanedSchema, row)
        val valueHashCode = if (value == null) 0 else value.hashCode
        ((valueHashCode + 31 * dataType.hashCode()) & Integer.MAX_VALUE) % numBuckets
    }
  }

  protected def addPartitionKey(key: Seq[Any]): Unit = {}

  protected def renamePartitionKey(
      partitionSchema: StructType,
      from: Seq[Any],
      to: Seq[Any]): Boolean = {
    val rows = dataMap.remove(from).getOrElse(new BufferedRows(from))
    val newRows = new BufferedRows(to)
    rows.rows.foreach { r =>
      val newRow = new GenericInternalRow(r.numFields)
      for (i <- 0 until r.numFields) newRow.update(i, r.get(i, schema(i).dataType))
      for (i <- 0 until partitionSchema.length) {
        val j = schema.fieldIndex(partitionSchema(i).name)
        newRow.update(j, to(i))
      }
      newRows.withRow(newRow)
    }
    dataMap.put(to, newRows).foreach { _ =>
      throw new IllegalStateException(
        s"The ${to.mkString("[", ", ", "]")} partition exists already")
    }
    true
  }

  protected def removePartitionKey(key: Seq[Any]): Unit = dataMap.synchronized {
    dataMap.remove(key)
  }

  protected def createPartitionKey(key: Seq[Any]): Unit = dataMap.synchronized {
    if (!dataMap.contains(key)) {
      val emptyRows = new BufferedRows(key)
      val rows = if (key.length == schema.length) {
        emptyRows.withRow(InternalRow.fromSeq(key))
      } else emptyRows
      dataMap.put(key, rows)
    }
  }

  protected def clearPartition(key: Seq[Any]): Unit = dataMap.synchronized {
    assert(dataMap.contains(key))
    dataMap(key).clear()
  }

  def withData(data: Array[BufferedRows]): InMemoryTable = dataMap.synchronized {
    data.foreach(_.rows.foreach { row =>
      val key = getKey(row)
      dataMap += dataMap.get(key)
        .map(key -> _.withRow(row))
        .getOrElse(key -> new BufferedRows(key).withRow(row))
      addPartitionKey(key)
    })
    this
  }

  override def capabilities: util.Set[TableCapability] = util.EnumSet.of(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.STREAMING_WRITE,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.OVERWRITE_DYNAMIC,
    TableCapability.TRUNCATE)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryScanBuilder(schema)
  }

  class InMemoryScanBuilder(tableSchema: StructType) extends ScanBuilder
      with SupportsPushDownRequiredColumns {
    private var schema: StructType = tableSchema

    override def build: Scan =
      new InMemoryBatchScan(data.map(_.asInstanceOf[InputPartition]), schema, tableSchema)

    override def pruneColumns(requiredSchema: StructType): Unit = {
      val schemaNames = metadataColumnNames ++ tableSchema.map(_.name)
      schema = StructType(requiredSchema.filter(f => schemaNames.contains(f.name)))
    }
  }

  case class InMemoryStats(sizeInBytes: OptionalLong, numRows: OptionalLong) extends Statistics

  case class InMemoryBatchScan(
      var data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType)
    extends Scan with Batch with SupportsRuntimeFiltering with SupportsReportStatistics {

    override def toBatch: Batch = this

    override def estimateStatistics(): Statistics = {
      if (data.isEmpty) {
        return InMemoryStats(OptionalLong.of(0L), OptionalLong.of(0L))
      }

      val inputPartitions = data.map(_.asInstanceOf[BufferedRows])
      val numRows = inputPartitions.map(_.rows.size).sum
      // we assume an average object header is 12 bytes
      val objectHeaderSizeInBytes = 12L
      val rowSizeInBytes = objectHeaderSizeInBytes + schema.defaultSize
      val sizeInBytes = numRows * rowSizeInBytes
      InMemoryStats(OptionalLong.of(sizeInBytes), OptionalLong.of(numRows))
    }

    override def planInputPartitions(): Array[InputPartition] = data.toArray

    override def createReaderFactory(): PartitionReaderFactory = {
      val metadataColumns = readSchema.map(_.name).filter(metadataColumnNames.contains)
      val nonMetadataColumns = readSchema.filterNot(f => metadataColumns.contains(f.name))
      new BufferedRowsReaderFactory(metadataColumns, nonMetadataColumns, tableSchema)
    }

    override def filterAttributes(): Array[NamedReference] = {
      val scanFields = readSchema.fields.map(_.name).toSet
      partitioning.flatMap(_.references)
        .filter(ref => scanFields.contains(ref.fieldNames.mkString(".")))
    }

    override def filter(filters: Array[Filter]): Unit = {
      if (partitioning.length == 1) {
        filters.foreach {
          case In(attrName, values) if attrName == partitioning.head.name =>
            val matchingKeys = values.map(_.toString).toSet
            data = data.filter(partition => {
              val key = partition.asInstanceOf[BufferedRows].keyString
              matchingKeys.contains(key)
            })

          case _ => // skip
        }
      }
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    InMemoryTable.maybeSimulateFailedTableWrite(new CaseInsensitiveStringMap(properties))
    InMemoryTable.maybeSimulateFailedTableWrite(info.options)

    new WriteBuilder with SupportsTruncate with SupportsOverwrite with SupportsDynamicOverwrite {
      private var writer: BatchWrite = Append
      private var streamingWriter: StreamingWrite = StreamingAppend

      override def truncate(): WriteBuilder = {
        assert(writer == Append)
        writer = TruncateAndAppend
        streamingWriter = StreamingTruncateAndAppend
        this
      }

      override def overwrite(filters: Array[Filter]): WriteBuilder = {
        assert(writer == Append)
        writer = new Overwrite(filters)
        streamingWriter = new StreamingNotSupportedOperation(s"overwrite ($filters)")
        this
      }

      override def overwriteDynamicPartitions(): WriteBuilder = {
        assert(writer == Append)
        writer = DynamicOverwrite
        streamingWriter = new StreamingNotSupportedOperation("overwriteDynamicPartitions")
        this
      }

      override def build(): Write = new Write with RequiresDistributionAndOrdering {
        override def requiredDistribution: Distribution = distribution

        override def requiredOrdering: Array[SortOrder] = ordering

        override def requiredNumPartitions(): Int = {
          numPartitions.getOrElse(0)
        }

        override def toBatch: BatchWrite = writer

        override def toStreaming: StreamingWrite = streamingWriter match {
          case exc: StreamingNotSupportedOperation => exc.throwsException()
          case s => s
        }

        override def supportedCustomMetrics(): Array[CustomMetric] = {
          Array(new InMemorySimpleCustomMetric)
        }
      }
    }
  }

  private abstract class TestBatchWrite extends BatchWrite {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
      BufferedRowsWriterFactory
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {}
  }

  private object Append extends TestBatchWrite {
    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      withData(messages.map(_.asInstanceOf[BufferedRows]))
    }
  }

  private object DynamicOverwrite extends TestBatchWrite {
    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      dataMap --= newData.flatMap(_.rows.map(getKey))
      withData(newData)
    }
  }

  private class Overwrite(filters: Array[Filter]) extends TestBatchWrite {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val deleteKeys = InMemoryTable.filtersToKeys(
        dataMap.keys, partCols.map(_.toSeq.quoted), filters)
      dataMap --= deleteKeys
      withData(messages.map(_.asInstanceOf[BufferedRows]))
    }
  }

  private object TruncateAndAppend extends TestBatchWrite {
    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      dataMap.clear
      withData(messages.map(_.asInstanceOf[BufferedRows]))
    }
  }

  private abstract class TestStreamingWrite extends StreamingWrite {
    def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
      BufferedRowsWriterFactory
    }

    def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
  }

  private class StreamingNotSupportedOperation(operation: String) extends TestStreamingWrite {
    override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory =
      throwsException()

    override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit =
      throwsException()

    override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit =
      throwsException()

    def throwsException[T](): T = throw new IllegalStateException("The operation " +
      s"${operation} isn't supported for streaming query.")
  }

  private object StreamingAppend extends TestStreamingWrite {
    override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
      dataMap.synchronized {
        withData(messages.map(_.asInstanceOf[BufferedRows]))
      }
    }
  }

  private object StreamingTruncateAndAppend extends TestStreamingWrite {
    override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
      dataMap.synchronized {
        dataMap.clear
        withData(messages.map(_.asInstanceOf[BufferedRows]))
      }
    }
  }

  override def canDeleteWhere(filters: Array[Filter]): Boolean = {
    InMemoryTable.supportsFilters(filters)
  }

  override def deleteWhere(filters: Array[Filter]): Unit = dataMap.synchronized {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    dataMap --= InMemoryTable.filtersToKeys(dataMap.keys, partCols.map(_.toSeq.quoted), filters)
  }
}

object InMemoryTable {
  val SIMULATE_FAILED_WRITE_OPTION = "spark.sql.test.simulateFailedWrite"

  def filtersToKeys(
      keys: Iterable[Seq[Any]],
      partitionNames: Seq[String],
      filters: Array[Filter]): Iterable[Seq[Any]] = {
    keys.filter { partValues =>
      filters.flatMap(splitAnd).forall {
        case EqualTo(attr, value) =>
          value == extractValue(attr, partitionNames, partValues)
        case EqualNullSafe(attr, value) =>
          val attrVal = extractValue(attr, partitionNames, partValues)
          if (attrVal == null && value === null) {
            true
          } else if (attrVal == null || value === null) {
            false
          } else {
            value == attrVal
          }
        case IsNull(attr) =>
          null == extractValue(attr, partitionNames, partValues)
        case IsNotNull(attr) =>
          null != extractValue(attr, partitionNames, partValues)
        case AlwaysTrue() => true
        case f =>
          throw new IllegalArgumentException(s"Unsupported filter type: $f")
      }
    }
  }

  def supportsFilters(filters: Array[Filter]): Boolean = {
    filters.flatMap(splitAnd).forall {
      case _: EqualTo => true
      case _: EqualNullSafe => true
      case _: IsNull => true
      case _: IsNotNull => true
      case _: AlwaysTrue => true
      case _ => false
    }
  }

  private def extractValue(
      attr: String,
      partFieldNames: Seq[String],
      partValues: Seq[Any]): Any = {
    partFieldNames.zipWithIndex.find(_._1 == attr) match {
      case Some((_, partIndex)) =>
        partValues(partIndex)
      case _ =>
        throw new IllegalArgumentException(s"Unknown filter attribute: $attr")
    }
  }

  private def splitAnd(filter: Filter): Seq[Filter] = {
    filter match {
      case And(left, right) => splitAnd(left) ++ splitAnd(right)
      case _ => filter :: Nil
    }
  }

  def maybeSimulateFailedTableWrite(tableOptions: CaseInsensitiveStringMap): Unit = {
    if (tableOptions.getBoolean(SIMULATE_FAILED_WRITE_OPTION, false)) {
      throw new IllegalStateException("Manual write to table failure.")
    }
  }
}

class BufferedRows(val key: Seq[Any] = Seq.empty) extends WriterCommitMessage
    with InputPartition with HasPartitionKey with Serializable {
  val rows = new mutable.ArrayBuffer[InternalRow]()

  def withRow(row: InternalRow): BufferedRows = {
    rows.append(row)
    this
  }

  def keyString(): String = key.toArray.mkString("/")

  override def partitionKey(): InternalRow = {
    InternalRow.fromSeq(key)
  }

  def clear(): Unit = rows.clear()
}

private class BufferedRowsReaderFactory(
    metadataColumnNames: Seq[String],
    nonMetaDataColumns: Seq[StructField],
    tableSchema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new BufferedRowsReader(partition.asInstanceOf[BufferedRows], metadataColumnNames,
      nonMetaDataColumns, tableSchema)
  }
}

private class BufferedRowsReader(
    partition: BufferedRows,
    metadataColumnNames: Seq[String],
    nonMetadataColumns: Seq[StructField],
    tableSchema: StructType) extends PartitionReader[InternalRow] {
  private def addMetadata(row: InternalRow): InternalRow = {
    val metadataRow = new GenericInternalRow(metadataColumnNames.map {
      case "index" => index
      case "_partition" => UTF8String.fromString(partition.keyString)
    }.toArray)
    new JoinedRow(row, metadataRow)
  }

  private var index: Int = -1

  override def next(): Boolean = {
    index += 1
    index < partition.rows.length
  }

  override def get(): InternalRow = {
    val originalRow = partition.rows(index)
    val values = new Array[Any](nonMetadataColumns.length)
    nonMetadataColumns.zipWithIndex.foreach { case (col, idx) =>
      values(idx) = extractFieldValue(col, tableSchema, originalRow)
    }
    addMetadata(new GenericInternalRow(values))
  }

  override def close(): Unit = {}

  private def extractFieldValue(
      field: StructField,
      schema: StructType,
      row: InternalRow): Any = {
    val index = schema.fieldIndex(field.name)
    field.dataType match {
      case StructType(fields) =>
        if (row.isNullAt(index)) {
          return null
        }
        val childRow = row.toSeq(schema)(index).asInstanceOf[InternalRow]
        val childSchema = schema(index).dataType.asInstanceOf[StructType]
        val resultValue = new Array[Any](fields.length)
        fields.zipWithIndex.foreach { case (childField, idx) =>
          val childValue = extractFieldValue(childField, childSchema, childRow)
          resultValue(idx) = childValue
        }
        new GenericInternalRow(resultValue)
      case dt =>
        row.get(index, dt)
    }
  }
}

private object BufferedRowsWriterFactory extends DataWriterFactory with StreamingDataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new BufferWriter
  }

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    new BufferWriter
  }
}

private class BufferWriter extends DataWriter[InternalRow] {
  private val buffer = new BufferedRows

  override def write(row: InternalRow): Unit = buffer.rows.append(row.copy())

  override def commit(): WriterCommitMessage = buffer

  override def abort(): Unit = {}

  override def close(): Unit = {}

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    val metric = new CustomTaskMetric {
      override def name(): String = "in_memory_buffer_rows"

      override def value(): Long = buffer.rows.size
    }
    Array(metric)
  }
}

class InMemorySimpleCustomMetric extends CustomMetric {
  override def name(): String = "in_memory_buffer_rows"
  override def description(): String = "number of rows in buffer"
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    s"in-memory rows: ${taskMetrics.sum}"
  }
}
