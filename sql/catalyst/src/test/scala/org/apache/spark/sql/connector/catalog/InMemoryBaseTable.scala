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

import com.google.common.base.Objects

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, JoinedRow, MetadataStructFieldWithLogicalName}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, DateTimeUtils}
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.colstats.{ColumnStatistics, Histogram, HistogramBin}
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.internal.connector.SupportsStreamingUpdateAsAppend
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

/**
 * A simple in-memory table. Rows are stored as a buffered group produced by each output task.
 */
abstract class InMemoryBaseTable(
    val name: String,
    val schema: StructType,
    override val partitioning: Array[Transform],
    override val properties: util.Map[String, String],
    val distribution: Distribution = Distributions.unspecified(),
    val ordering: Array[SortOrder] = Array.empty,
    val numPartitions: Option[Int] = None,
    val advisoryPartitionSize: Option[Long] = None,
    val isDistributionStrictlyRequired: Boolean = true,
    val numRowsPerSplit: Int = Int.MaxValue)
  extends Table with SupportsRead with SupportsWrite with SupportsMetadataColumns {

  protected object PartitionKeyColumn extends MetadataColumn {
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
  private val metadataColumnNames = metadataColumns.map(_.name).toSet

  // Metadata column renaming is supported -- see [[InMemoryScanBuilder.pruneColumns]] and
  // [[BatchScanBaseClass.createReaderFactory]] for implementation details.
  override val canRenameConflictingMetadataColumns: Boolean = true

  private val allowUnsupportedTransforms =
    properties.getOrDefault("allow-unsupported-transforms", "false").toBoolean

  partitioning.foreach {
    case _: IdentityTransform =>
    case _: YearsTransform =>
    case _: MonthsTransform =>
    case _: DaysTransform =>
    case _: HoursTransform =>
    case _: BucketTransform =>
    case _: SortedBucketTransform =>
    case _: ClusterByTransform =>
    case NamedTransform("truncate", Seq(_: NamedReference, _: Literal[_])) =>
    case t if !allowUnsupportedTransforms =>
      throw new IllegalArgumentException(s"Transform $t is not a supported transform")
  }

  // The key `Seq[Any]` is the partition values, value is a set of splits, each with a set of rows.
  val dataMap: mutable.Map[Seq[Any], Seq[BufferedRows]] = mutable.Map.empty

  def data: Array[BufferedRows] = dataMap.values.flatten.toArray

  def rows: Seq[InternalRow] = dataMap.values.flatten.flatMap(_.rows).toSeq

  val partCols: Array[Array[String]] = partitioning.flatMap(_.references).map { ref =>
    schema.findNestedField(ref.fieldNames().toImmutableArraySeq, includeCollections = false) match {
      case Some(_) => ref.fieldNames()
      case None => throw new IllegalArgumentException(s"${ref.describe()} does not exist.")
    }
  }

  private val UTC = ZoneId.of("UTC")
  private val EPOCH_LOCAL_DATE = Instant.EPOCH.atZone(UTC).toLocalDate

  protected def getKey(row: InternalRow): Seq[Any] = {
    getKey(row, schema)
  }

  protected def getKey(row: InternalRow, rowSchema: StructType): Seq[Any] = {
    @scala.annotation.tailrec
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

    val cleanedSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(rowSchema)
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
      case BucketTransform(numBuckets, cols, _) =>
        val valueTypePairs = cols.map(col => extractor(col.fieldNames, cleanedSchema, row))
        var valueHashCode = 0
        valueTypePairs.foreach( pair =>
          if ( pair._1 != null) valueHashCode += pair._1.hashCode()
        )
        var dataTypeHashCode = 0
        valueTypePairs.foreach(dataTypeHashCode += _._2.hashCode())
        ((valueHashCode + 31 * dataTypeHashCode) & Integer.MAX_VALUE) % numBuckets
      case NamedTransform("truncate", Seq(ref: NamedReference, length: Literal[_])) =>
        extractor(ref.fieldNames, cleanedSchema, row) match {
          case (str: UTF8String, StringType) =>
            str.substring(0, length.value.asInstanceOf[Int])
          case (v, t) =>
            throw new IllegalArgumentException(s"Match: unsupported argument(s) type - ($v, $t)")
        }
      case ClusterByTransform(columnNames) =>
        columnNames.map { colName =>
          extractor(colName.fieldNames, cleanedSchema, row)._1
        }
    }.toImmutableArraySeq
  }

  protected def addPartitionKey(key: Seq[Any]): Unit = {}

  protected def renamePartitionKey(
      partitionSchema: StructType,
      from: Seq[Any],
      to: Seq[Any]): Boolean = {
    val splits = dataMap.remove(from).getOrElse(Seq(new BufferedRows(from)))
    val newSplits = splits.map { rows =>
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
      newRows
    }
    dataMap.put(to, newSplits).foreach { _ =>
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
      dataMap.put(key, Seq(rows))
    }
  }

  protected def clearPartition(key: Seq[Any]): Unit = dataMap.synchronized {
    assert(dataMap.contains(key))
    dataMap.update(key, Seq(new BufferedRows(key)))
  }

  def withDeletes(data: Array[BufferedRows]): InMemoryBaseTable = {
    data.foreach { p =>
      dataMap ++= dataMap.map { case (key, currentSplits) =>
        val newSplits = currentSplits.map { currentRows =>
          val newRows = new BufferedRows(currentRows.key)
          newRows.rows ++= currentRows.rows.filter(r => !p.deletes.contains(r.getInt(0)))
          newRows
        }
        key -> newSplits
      }
    }
    this
  }

  def withData(data: Array[BufferedRows]): InMemoryBaseTable = {
    withData(data, schema)
  }

  def withData(
      data: Array[BufferedRows],
      writeSchema: StructType): InMemoryBaseTable = dataMap.synchronized {
    data.foreach(_.rows.foreach { row =>
      val key = getKey(row, writeSchema)
      dataMap += dataMap.get(key)
          .map { splits =>
            val newSplits = if (splits.last.rows.size >= numRowsPerSplit) {
              splits :+ new BufferedRows(key)
            } else {
              splits
            }
            newSplits.last.withRow(row)
            key -> newSplits
          }
          .getOrElse(key -> Seq(new BufferedRows(key).withRow(row)))
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

  private def canEvaluate(filter: Filter): Boolean = {
    if (partitioning.length == 1 && partitioning.head.references.length == 1) {
      filter match {
        case In(attrName, _) if attrName == partitioning.head.references.head.toString => true
        case _ => false
      }
    } else {
      false
    }
  }

  class InMemoryScanBuilder(tableSchema: StructType) extends ScanBuilder
      with SupportsPushDownRequiredColumns with SupportsPushDownFilters {
    private var schema: StructType = tableSchema
    private var postScanFilters: Array[Filter] = Array.empty
    private var evaluableFilters: Array[Filter] = Array.empty
    private var _pushedFilters: Array[Filter] = Array.empty

    override def build: Scan = {
      val scan = InMemoryBatchScan(
        data.map(_.asInstanceOf[InputPartition]).toImmutableArraySeq, schema, tableSchema)
      if (evaluableFilters.nonEmpty) {
        scan.filter(evaluableFilters)
      }
      scan
    }

    override def pruneColumns(requiredSchema: StructType): Unit = {
      // The required schema could contain conflict-renamed metadata columns, so we need to match
      // them by their logical (original) names, not their current names.
      val schemaNames = tableSchema.map(_.name).toSet
      val prunedFields = requiredSchema.filter {
        case MetadataStructFieldWithLogicalName(f, name) => metadataColumnNames.contains(name)
        case f => schemaNames.contains(f.name)
      }
      schema = StructType(prunedFields)
    }

    override def pushFilters(filters: Array[Filter]): Array[Filter] = {
      val (evaluableFilters, postScanFilters) = filters.partition(canEvaluate)
      this.evaluableFilters = evaluableFilters
      this.postScanFilters = postScanFilters
      this._pushedFilters = filters
      postScanFilters
    }

    override def pushedFilters(): Array[Filter] = this._pushedFilters
  }

  case class InMemoryStats(
      sizeInBytes: OptionalLong,
      numRows: OptionalLong,
      override val columnStats: util.Map[NamedReference, ColumnStatistics])
    extends Statistics

  case class InMemoryColumnStats(
      override val distinctCount: OptionalLong,
      override val nullCount: OptionalLong) extends ColumnStatistics

  case class InMemoryHistogramBin(lo: Double, hi: Double, ndv: Long) extends HistogramBin

  case class InMemoryHistogram(height: Double, bins: Array[HistogramBin]) extends Histogram

  abstract class BatchScanBaseClass(
      var data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType)
    extends Scan with Batch with SupportsReportStatistics with SupportsReportPartitioning {

    override def toBatch: Batch = this

    override def estimateStatistics(): Statistics = {
      if (data.isEmpty) {
        return InMemoryStats(OptionalLong.of(0L), OptionalLong.of(0L), new util.HashMap())
      }

      val inputPartitions = data.map(_.asInstanceOf[BufferedRows])
      val numRows = inputPartitions.map(_.rows.size).sum
      // we assume an average object header is 12 bytes
      val objectHeaderSizeInBytes = 12L
      val rowSizeInBytes = objectHeaderSizeInBytes + schema.defaultSize
      val sizeInBytes = numRows * rowSizeInBytes

      val numOfCols = tableSchema.fields.length
      val dataTypes = tableSchema.fields.map(_.dataType)
      val colValueSets = new Array[util.HashSet[Object]](numOfCols)
      val numOfNulls = new Array[Long](numOfCols)
      for (i <- 0 until numOfCols) {
        colValueSets(i) = new util.HashSet[Object]
      }

      inputPartitions.foreach(inputPartition =>
        inputPartition.rows.foreach(row =>
          for (i <- 0 until numOfCols) {
            colValueSets(i).add(row.get(i, dataTypes(i)))
            if (row.isNullAt(i)) {
              numOfNulls(i) += 1
            }
          }
        )
      )

      val map = new util.HashMap[NamedReference, ColumnStatistics]()
      val colNames = tableSchema.fields.map(_.name)
      var i = 0
      for (col <- colNames) {
        val fieldReference = FieldReference.column(col)
        val colStats = InMemoryColumnStats(
          OptionalLong.of(colValueSets(i).size()),
          OptionalLong.of(numOfNulls(i)))
        map.put(fieldReference, colStats)
        i = i + 1
      }

      InMemoryStats(OptionalLong.of(sizeInBytes), OptionalLong.of(numRows), map)
    }

    override def outputPartitioning(): Partitioning = {
      if (InMemoryBaseTable.this.partitioning.nonEmpty) {
        new KeyGroupedPartitioning(
          InMemoryBaseTable.this.partitioning.map(_.asInstanceOf[Expression]),
          data.size)
      } else {
        new UnknownPartitioning(data.size)
      }
    }

    override def planInputPartitions(): Array[InputPartition] = data.toArray

    override def createReaderFactory(): PartitionReaderFactory = {
      val metadataColumns = new mutable.ArrayBuffer[String]()
      val nonMetadataColumns = readSchema.filter {
        case MetadataStructFieldWithLogicalName(_, name) =>
          metadataColumns += name
          false
        case _ => true
      }
      new BufferedRowsReaderFactory(metadataColumns.toSeq, nonMetadataColumns, tableSchema)
    }
  }

  case class InMemoryBatchScan(
      var _data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType)
    extends BatchScanBaseClass(_data, readSchema, tableSchema) with SupportsRuntimeFiltering {

    override def filterAttributes(): Array[NamedReference] = {
      val scanFields = readSchema.fields.map(_.name).toSet
      partitioning.flatMap(_.references)
        .filter(ref => scanFields.contains(ref.fieldNames.mkString(".")))
    }

    override def filter(filters: Array[Filter]): Unit = {
      if (partitioning.length == 1 && partitioning.head.references().length == 1) {
        val ref = partitioning.head.references().head
        filters.foreach {
          case In(attrName, values) if attrName == ref.toString =>
            val matchingKeys = values.map { value =>
              if (value != null) value.toString else null
            }.toSet
            this.data = this.data.filter(partition => {
              val rows = partition.asInstanceOf[BufferedRows]
              rows.key match {
                // null partitions are represented as Seq(null)
                case Seq(null) => matchingKeys.contains(null)
                case _ => matchingKeys.contains(rows.keyString())
              }
            })

          case _ => // skip
        }
      }
    }
  }

  abstract class InMemoryWriterBuilder() extends SupportsTruncate with SupportsDynamicOverwrite
    with SupportsStreamingUpdateAsAppend {

    protected var writer: BatchWrite = Append
    protected var streamingWriter: StreamingWrite = StreamingAppend

    override def overwriteDynamicPartitions(): WriteBuilder = {
      if (writer != Append) {
        throw new IllegalArgumentException(s"Unsupported writer type: $writer")
      }
      writer = DynamicOverwrite
      streamingWriter = new StreamingNotSupportedOperation("overwriteDynamicPartitions")
      this
    }

    override def build(): Write = new Write with RequiresDistributionAndOrdering {
      override def requiredDistribution: Distribution = distribution

      override def distributionStrictlyRequired: Boolean = isDistributionStrictlyRequired

      override def requiredOrdering: Array[SortOrder] = ordering

      override def requiredNumPartitions(): Int = {
        numPartitions.getOrElse(0)
      }

      override def advisoryPartitionSizeInBytes(): Long = {
        advisoryPartitionSize.getOrElse(0)
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

  protected abstract class TestBatchWrite extends BatchWrite {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
      BufferedRowsWriterFactory
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {}
  }

  protected object Append extends TestBatchWrite {
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

  protected object TruncateAndAppend extends TestBatchWrite {
    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      dataMap.clear()
      withData(messages.map(_.asInstanceOf[BufferedRows]))
    }
  }

  protected abstract class TestStreamingWrite extends StreamingWrite {
    def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
      BufferedRowsWriterFactory
    }

    def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
  }

  protected class StreamingNotSupportedOperation(operation: String) extends TestStreamingWrite {
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

  protected object StreamingTruncateAndAppend extends TestStreamingWrite {
    override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
      dataMap.synchronized {
        dataMap.clear()
        withData(messages.map(_.asInstanceOf[BufferedRows]))
      }
    }
  }
}

object InMemoryBaseTable {
  val SIMULATE_FAILED_WRITE_OPTION = "spark.sql.test.simulateFailedWrite"

  def extractValue(
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

  def maybeSimulateFailedTableWrite(tableOptions: CaseInsensitiveStringMap): Unit = {
    if (tableOptions.getBoolean(SIMULATE_FAILED_WRITE_OPTION, false)) {
      throw new IllegalStateException("Manual write to table failure.")
    }
  }
}

class BufferedRows(val key: Seq[Any] = Seq.empty) extends WriterCommitMessage
    with InputPartition with HasPartitionKey with HasPartitionStatistics with Serializable {
  val rows = new mutable.ArrayBuffer[InternalRow]()
  val deletes = new mutable.ArrayBuffer[Int]()

  def withRow(row: InternalRow): BufferedRows = {
    rows.append(row)
    this
  }

  def keyString(): String = key.toArray.mkString("/")

  override def partitionKey(): InternalRow = PartitionInternalRow(key.toArray)
  override def sizeInBytes(): OptionalLong = OptionalLong.of(100L)
  override def numRows(): OptionalLong = OptionalLong.of(rows.size)
  override def filesCount(): OptionalLong = OptionalLong.of(100L)

  def clear(): Unit = rows.clear()
}

/**
 * Theoretically, [[InternalRow]] returned by [[HasPartitionKey#partitionKey()]]
 * does not need to implement equal and hashcode methods.
 * But [[GenericInternalRow]] implements equals and hashcode methods already. Here we override it
 * to simulate that it has not been implemented to verify codes correctness.
 */
case class PartitionInternalRow(keys: Array[Any])
  extends GenericInternalRow(keys) {
  override def equals(other: Any): Boolean = {
    if (!other.isInstanceOf[PartitionInternalRow]) {
      return false
    }
    // Just compare by reference, not by value
    this.keys == other.asInstanceOf[PartitionInternalRow].keys
  }
  override def hashCode: Int = {
    Objects.hashCode(keys)
  }
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
      case "_partition" => UTF8String.fromString(partition.keyString())
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
  protected val buffer = new BufferedRows

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
