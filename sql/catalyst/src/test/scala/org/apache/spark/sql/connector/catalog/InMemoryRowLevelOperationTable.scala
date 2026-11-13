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

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, LogicalExpressions, NamedReference, SortDirection, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, DeltaBatchWrite, DeltaWrite, DeltaWriteBuilder, DeltaWriter, DeltaWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, RequiresDistributionAndOrdering, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, SupportsColumnUpdates, SupportsDelta, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.internal.connector.SchemaAlignmentConfig
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

/**
 * Test helper trait mixed into the in-memory row-level operations so tests can verify that
 * per-statement SQL options reach the operation via [[RowLevelOperationInfo#options]].
 */
trait RowLevelOperationWithOptions {
  def options: CaseInsensitiveStringMap
}

class InMemoryRowLevelOperationTable private (
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: util.Map[String, String],
    constraints: Array[Constraint],
    tableId: String,
    schemaAlignmentConfig: SchemaAlignmentConfig)
  extends InMemoryTable(
    name,
    columns,
    partitioning,
    properties,
    constraints,
    id = tableId,
    schemaAlignmentConfig = schemaAlignmentConfig)
  with SupportsRowLevelOperations {

  def this(
      name: String,
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String],
      constraints: Array[Constraint] = Array.empty,
      tableId: String = java.util.UUID.randomUUID().toString,
      schemaAlignmentConfig: SchemaAlignmentConfig = SchemaAlignmentConfig.DEFAULT) = {
    this(
      name = name,
      columns = CatalogV2Util.structTypeToV2Columns(schema),
      partitioning = partitioning,
      properties = properties,
      constraints = constraints,
      tableId = tableId,
      schemaAlignmentConfig = schemaAlignmentConfig)
  }

  private final val PARTITION_COLUMN_REF = FieldReference(PartitionKeyColumn.name)
  private final val INDEX_COLUMN_REF = FieldReference(IndexColumn.name)
  private final val SUPPORTS_DELTAS = "supports-deltas"
  private final val SPLIT_UPDATES = "split-updates"
  private final val NO_METADATA = "no-metadata"
  private final val USE_CATALYST_RUNTIME_FILTERING = "use-catalyst-runtime-filtering"
  private final val COLUMN_UPDATE = "column-update"
  private final val COLUMN_UPDATE_REQ_ATTRS = "column-update-req-attrs"
  private final val COLUMN_UPDATE_COW = "column-update-cow"
  private final val COLUMN_UPDATE_COW_REQ_ATTRS = "column-update-cow-req-attrs"
  // makes the copy-on-write column-update write use the plain writer, which overrides neither
  // writeColumnUpdate method
  private final val COLUMN_UPDATE_COW_NO_WRITE_COLUMN_UPDATE =
    "column-update-cow-no-write-column-update"
  // makes the copy-on-write column-update writer override only writeColumnUpdate(record)
  private final val COLUMN_UPDATE_COW_RECORD_ONLY_WRITER = "column-update-cow-record-only-writer"
  // makes the copy-on-write column-update scan report every identity partition column from
  // filterAttributes(), even one it doesn't read
  private final val COLUMN_UPDATE_COW_UNREAD_FILTER_ATTRS = "column-update-cow-unread-filter-attrs"
  private final val COLUMN_UPDATE_SPLIT_ROW_ID = "column-update-split-row-id"
  // comma-separated columns that the delta column-update write clusters and orders by
  private final val COLUMN_UPDATE_CLUSTER_BY = "column-update-cluster-by"
  // comma-separated columns that the delta column-update write orders by instead of the
  // clustering columns
  private final val COLUMN_UPDATE_ORDER_BY = "column-update-order-by"
  // wraps each column-update clustering and ordering column in two nested transforms, f(g(col))
  private final val COLUMN_UPDATE_CLUSTER_BY_NESTED_TRANSFORM =
    "column-update-cluster-by-nested-transform"
  private final val noMetadata = properties.getOrDefault(NO_METADATA, "false") == "true"
  private final val useCatalystRuntimeFiltering =
    properties.getOrDefault(USE_CATALYST_RUNTIME_FILTERING, "false") == "true"

  // used in row-level operation tests to verify replaced partitions
  var replacedPartitions: Seq[Seq[Any]] = Seq.empty
  // used in row-level operation tests to verify reported write schema
  var lastWriteInfo: LogicalWriteInfo = _
  // used in column-update tests to verify that Spark passed the correct updated column list
  // to the connector via RowLevelOperationInfo.updatedColumns()
  var lastUpdatedColumns: Array[NamedReference] = _
  // used in row-level operation tests to verify passed records
  // (operation, id, metadata, row)
  var lastWriteLog: Seq[InternalRow] = Seq.empty
  // used in delete-with-options tests to verify options reach the connector on the deleteWhere path
  var lastDeleteOptions: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()

  override def deleteWhere(
      predicates: Array[Predicate], options: CaseInsensitiveStringMap): Unit = {
    lastDeleteOptions = options
    deleteWhere(predicates)
  }

  override def copy(): Table = {
    val copied = InMemoryRowLevelOperationTable.withColumns(
      name = name,
      columns = columns(),
      partitioning = partitioning,
      properties = properties,
      constraints = constraints,
      tableId = id,
      schemaAlignmentConfig = schemaAlignmentConfig)
    dataMap.synchronized {
      dataMap.foreach { case (key, splits) =>
        val copiedSplits = splits.map { bufferedRows =>
          val copiedBufferedRows = new BufferedRows(bufferedRows.key, bufferedRows.schema)
          copiedBufferedRows.rows ++= bufferedRows.rows.map(_.copy())
          copiedBufferedRows
        }
        copied.dataMap.put(key, copiedSplits)
      }
    }
    copied.commits ++= commits.map(_.copy())
    copied.copyTableStateFrom(this)
    copied.replacedPartitions = replacedPartitions
    copied.lastWriteInfo = lastWriteInfo
    copied.lastWriteLog = lastWriteLog
    copied.lastUpdatedColumns = lastUpdatedColumns
    copied
  }

  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder = {
    lastUpdatedColumns = info.updatedColumns()
    val splitUpdates = properties.getOrDefault(SPLIT_UPDATES, "false").toBoolean
    // a column-update fixture declares `pk`, `dep` and the updated columns, or a fixed list
    def declaredBy(updatedKey: String, fixedKey: String): Option[Seq[NamedReference]] = {
      if (properties.getOrDefault(updatedKey, "false") == "true") {
        Some(declaredColumns(info.updatedColumns().toSeq))
      } else {
        Option(properties.get(fixedKey)).map(requiredColumns(_).map(FieldReference(_)).toSeq)
      }
    }
    // the copy-on-write commit finds the original row by `pk`, so an UPDATE that assigns a
    // declared `pk` falls back to full-width rows
    lazy val pkIdx = findFieldIndex(schema, "pk")
    def isPk(name: String): Boolean = pkIdx.exists(findFieldIndex(schema, name).contains)
    (declaredBy(COLUMN_UPDATE, COLUMN_UPDATE_REQ_ATTRS),
        declaredBy(COLUMN_UPDATE_COW, COLUMN_UPDATE_COW_REQ_ATTRS)) match {
      case (Some(declared), _) if splitUpdates =>
        () => new DeltaBasedColumnUpdateSplitOperation(info.command, declared, info.options)
      case (Some(declared), _) =>
        () => new DeltaBasedColumnUpdateOperation(info.command, declared, info.options)
      case (_, Some(declared)) if info.updatedColumns().exists(c => isPk(c.describe())) &&
          declared.exists(c => isPk(c.describe())) =>
        () => PartitionBasedOperation(info.command, info.options)
      case (_, Some(declared)) =>
        () => new PartitionBasedColumnUpdateOperation(info.command, declared, info.options)
      case _ if properties.containsKey(COLUMN_UPDATE_SPLIT_ROW_ID) =>
        val rowIdCol = properties.get(COLUMN_UPDATE_SPLIT_ROW_ID)
        () => new DeltaBasedColumnUpdateSplitOperationWithRowId(
          info.command, rowIdCol, info.updatedColumns().toSeq, info.options)
      case _ if properties.getOrDefault(SUPPORTS_DELTAS, "false") == "true" =>
        () => DeltaBasedOperation(info.command, info.options)
      case _ =>
        () => PartitionBasedOperation(info.command, info.options)
    }
  }

  case class PartitionBasedOperation(command: Command, options: CaseInsensitiveStringMap)
    extends RowLevelOperation with RowLevelOperationWithOptions {
    var configuredScan: BatchScanBaseClass = _

    override def requiredMetadataAttributes(): Array[NamedReference] = {
      if (noMetadata) {
        Array.empty
      } else {
        Array(PARTITION_COLUMN_REF, INDEX_COLUMN_REF)
      }
    }

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      newRowLevelScanBuilder(options) { scan =>
        configuredScan = scan
      }
    }

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      lastWriteInfo = info
      new WriteBuilder {
        override def build(): Write = if (noMetadata) {
          new Write {
            override def toBatch: BatchWrite = PartitionBasedReplaceData(configuredScan)
            override def description: String = "InMemoryWrite"
          }
        } else {
          new Write with RequiresDistributionAndOrdering {
            override def requiredDistribution: Distribution = {
              Distributions.clustered(Array(PARTITION_COLUMN_REF))
            }

            override def requiredOrdering: Array[SortOrder] = {
              Array[SortOrder](
                LogicalExpressions.sort(
                  PARTITION_COLUMN_REF,
                  SortDirection.ASCENDING,
                  SortDirection.ASCENDING.defaultNullOrdering()))
            }

            override def toBatch: BatchWrite = PartitionBasedReplaceData(configuredScan)

            override def description: String = "InMemoryWrite"
          }
        }
      }
    }

    override def description(): String = "InMemoryPartitionReplaceOperation"
  }

  // A group-based write replaces every partition its scan handed to Spark after runtime
  // filtering. Drops those partitions and returns them.
  private def replaceReadPartitions(scan: BatchScanBaseClass): Seq[BufferedRows] = {
    val read = scan.readPartitions.map(_.asInstanceOf[BufferedRows])
    val readPartitionKeys = read.flatMap(_.rows).map(r => getKey(r, schema)).distinct
    dataMap --= readPartitionKeys
    replacedPartitions = readPartitionKeys
    read
  }

  private case class PartitionBasedReplaceData(scan: BatchScanBaseClass)
    extends TestBatchWrite {

    override protected def doCommit(
        messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      replaceReadPartitions(scan)
      withData(newData, schema)
      lastWriteLog = newData.flatMap(buffer => buffer.log).toImmutableArraySeq
    }
  }

  case class DeltaBasedOperation(command: Command, options: CaseInsensitiveStringMap)
    extends RowLevelOperation with SupportsDelta with RowLevelOperationWithOptions {
    private final val PK_COLUMN_REF = FieldReference("pk")

    override def requiredMetadataAttributes(): Array[NamedReference] = {
      if (noMetadata) {
        Array.empty
      } else {
        Array(PARTITION_COLUMN_REF, INDEX_COLUMN_REF)
      }
    }

    override def rowId(): Array[NamedReference] = Array(PK_COLUMN_REF)

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      newRowLevelScanBuilder(options)(_ => ())
    }

    override def newWriteBuilder(info: LogicalWriteInfo): DeltaWriteBuilder = {
      lastWriteInfo = info
      new DeltaWriteBuilder {
        override def build(): DeltaWrite = if (noMetadata) {
          new DeltaWrite {
            override def toBatch: DeltaBatchWrite = TestDeltaBatchWrite
          }
        } else {
          new DeltaWrite with RequiresDistributionAndOrdering {

            override def requiredDistribution(): Distribution = {
              Distributions.clustered(Array(PARTITION_COLUMN_REF))
            }

            override def requiredOrdering(): Array[SortOrder] = {
              Array[SortOrder](
                LogicalExpressions.sort(
                  PARTITION_COLUMN_REF,
                  SortDirection.ASCENDING,
                  SortDirection.ASCENDING.defaultNullOrdering())
              )
            }

            override def toBatch: DeltaBatchWrite = TestDeltaBatchWrite
          }
        }
      }
    }

    override def representUpdateAsDeleteAndInsert(): Boolean = {
      properties.getOrDefault(SPLIT_UPDATES, "false").toBoolean
    }
  }

  private object TestDeltaBatchWrite extends TestBatchWrite with DeltaBatchWrite {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DeltaWriterFactory = {
      new DeltaBufferedRowsWriterFactory(CatalogV2Util.v2ColumnsToStructType(columns()))
    }

    override protected def doCommit(messages: Array[WriterCommitMessage]): Unit = {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      withDeletes(newData)
      withData(newData, columns())
      lastWriteLog = newData.flatMap(buffer => buffer.log).toIndexedSeq
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {}
  }

  // The rows of `buf` in the table's current schema, matched to columns by name like a scan does.
  private def currentRows(buf: BufferedRows): Seq[InternalRow] = {
    val reader = new BufferedRowsReader(buf, Seq.empty, schema.fields.toSeq, schema)
    try {
      Iterator.continually(reader).takeWhile(_.next()).map(_.get().copy()).toList
    } finally {
      reader.close()
    }
  }

  // Spark resolves a row-ID reference such as `pk` against a column such as `PK`, so look a name
  // up exactly first and then case-insensitively, in the same order.
  private def findFieldIndex(s: StructType, name: String): Option[Int] =
    s.getFieldIndex(name).orElse(s.getFieldIndexCaseInsensitive(name))

  private def fieldIndexOf(s: StructType, name: String): Int =
    findFieldIndex(s, name).getOrElse(s.fieldIndex(name))

  // Finds the one current row with the given row-ID values, matching a data column by value and
  // `_partition` by the partition key.
  private def currentRowByRowId(values: Seq[(String, Any)]): InternalRow = {
    val matches = dataMap.values.iterator.flatten.flatMap { buf =>
      currentRows(buf).filter { row =>
        values.forall {
          case (name, value) if name.equalsIgnoreCase(PartitionKeyColumn.name) =>
            buf.keyString() == value.toString
          case (name, value) =>
            val i = fieldIndexOf(schema, name)
            row.get(i, schema.fields(i).dataType) == value
        }
      }
    }.toSeq
    matches match {
      case Seq(row) => row
      case _ => throw new IllegalStateException(
        s"Expected one row with row ID $values, found ${matches.size}")
    }
  }

  // Rebuilds a full table row for a narrow column-update row: the base row's values overlaid with
  // the narrow row's columns. The base row is in the table's current schema.
  private def overlayNarrowRow(
      baseRow: Option[InternalRow],
      narrowRow: InternalRow,
      narrowSchema: StructType): InternalRow = {
    val fullRow = new GenericInternalRow(schema.length)
    baseRow.foreach { base =>
      for (i <- schema.fields.indices) {
        val field = schema.fields(i)
        fullRow.update(i, base.get(i, field.dataType))
      }
    }
    schema.fields.zipWithIndex.foreach { case (field, i) =>
      narrowSchema.getFieldIndex(field.name).foreach { j =>
        fullRow.update(i, narrowRow.get(j, field.dataType))
      }
    }
    fullRow
  }

  private def requiredColumns(prop: String): Array[String] =
    prop.split(",").map(_.trim).filter(_.nonEmpty)

  // The base columns `pk` and `dep`, where the table has them, followed by the updated columns.
  private def declaredColumns(updatedCols: Seq[NamedReference]): Seq[NamedReference] = {
    val base = Seq("pk", "dep").flatMap(findFieldIndex(schema, _))
      .map(i => FieldReference(schema.fields(i).name))
    val baseNames = base.map(_.describe()).toSet
    base ++ updatedCols.filterNot(r => baseNames.contains(r.describe()))
  }

  // A delta-based operation that supports column-level updates: Spark sends only the declared
  // columns in the row projection instead of the full row schema.
  class DeltaBasedColumnUpdateOperation(
      command: Command,
      declared: Seq[NamedReference],
      options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty())
      extends DeltaBasedOperation(command, options)
        with SupportsColumnUpdates {
    override def representUpdateAsDeleteAndInsert(): Boolean = false
    override def requiredDataAttributes(): Array[NamedReference] = declared.toArray

    // The case-class equality of DeltaBasedOperation would make this operation equal to a wide
    // one, or to another column-update operation, with the same command and options.
    override def equals(other: Any): Boolean = other match {
      case ref: AnyRef => this eq ref
      case _ => false
    }
    override def hashCode(): Int = System.identityHashCode(this)
    override def canEqual(other: Any): Boolean = false

    private def writeRequirementRefs(key: String): Option[Seq[Expression]] =
      Option(properties.get(key)).map(requiredColumns(_).map(FieldReference(_)).toSeq)

    private def withNestedTransforms(refs: Seq[Expression]): Seq[Expression] = {
      if (properties.getOrDefault(COLUMN_UPDATE_CLUSTER_BY_NESTED_TRANSFORM, "false") == "true") {
        refs.map(ref => LogicalExpressions.apply("f", LogicalExpressions.apply("g", ref)))
      } else {
        refs
      }
    }

    private val clusterExprs: Seq[Expression] = withNestedTransforms(
      writeRequirementRefs(COLUMN_UPDATE_CLUSTER_BY).getOrElse(Seq(PARTITION_COLUMN_REF)))

    private val orderExprs: Seq[Expression] =
      writeRequirementRefs(COLUMN_UPDATE_ORDER_BY).map(withNestedTransforms).getOrElse(clusterExprs)

    override def newWriteBuilder(info: LogicalWriteInfo): DeltaWriteBuilder = {
      lastWriteInfo = info
      val writeSchema = info.columnUpdateSchema().orElse(info.schema())
      val batchWrite = new TestBatchWrite with DeltaBatchWrite {
        override def createBatchWriterFactory(
            physicalInfo: PhysicalWriteInfo): DeltaWriterFactory = {
          newWriterFactory(info, writeSchema)
        }

        override protected def doCommit(messages: Array[WriterCommitMessage]): Unit =
          dataMap.synchronized {
            val newData = messages.map(_.asInstanceOf[BufferedRows])
            commitColumnUpdate(info, writeSchema, newData)
          }

        override def abort(messages: Array[WriterCommitMessage]): Unit = {}
      }
      new DeltaWriteBuilder {
        override def build(): DeltaWrite =
          if (noMetadata && !properties.containsKey(COLUMN_UPDATE_CLUSTER_BY)) {
            new DeltaWrite {
              override def toBatch: DeltaBatchWrite = batchWrite
            }
          } else {
            new DeltaWrite with RequiresDistributionAndOrdering {

              override def requiredDistribution(): Distribution = {
                Distributions.clustered(clusterExprs.toArray)
              }

              override def requiredOrdering(): Array[SortOrder] = {
                orderExprs.map { expr =>
                  LogicalExpressions.sort(
                    expr,
                    SortDirection.ASCENDING,
                    SortDirection.ASCENDING.defaultNullOrdering()): SortOrder
                }.toArray
              }

              override def toBatch: DeltaBatchWrite = batchWrite
            }
          }
      }
    }

    protected def newWriterFactory(
        info: LogicalWriteInfo,
        writeSchema: StructType): DeltaWriterFactory = {
      new DeltaBufferedRowsWriterFactory(writeSchema)
    }

    // For column-update writes, updated rows contain only the declared columns
    // (columnUpdateSchema from LogicalWriteInfo). We expand each row to the full
    // table schema by overlaying them on the base row found by pk.
    protected def commitColumnUpdate(
        info: LogicalWriteInfo,
        writeSchema: StructType,
        newData: Array[BufferedRows]): Unit = {
      val updateOpName = UTF8String.fromString(Update.toString)
      val insertOpName = UTF8String.fromString(Insert.toString)

      val mergedData = newData.map { buf =>
        val merged = new BufferedRows(buf.key, schema)
        buf.log.foreach { logRow =>
          val opName = logRow.getUTF8String(0)
          if (opName == updateOpName) {
            val pk = logRow.getInt(1)
            val narrowRow = logRow.get(3, writeSchema).asInstanceOf[InternalRow]
            val baseRow = Some(currentRowByRowId(Seq("pk" -> pk)))
            val fullRow = overlayNarrowRow(baseRow, narrowRow, writeSchema)
            merged.rows.append(fullRow)
          } else if (opName == insertOpName) {
            // INSERT rows arrive with the full table schema via writer.insert()
            val insertRow = logRow.get(3, schema).asInstanceOf[InternalRow]
            merged.rows.append(insertRow.copy())
          }
        }
        merged
      }

      withDeletes(newData)
      withData(mergedData)
      lastWriteLog = newData.flatMap(buffer => buffer.log).toIndexedSeq
    }
  }

  class DeltaBasedColumnUpdateSplitOperation(
      command: Command,
      declared: Seq[NamedReference],
      options: CaseInsensitiveStringMap)
      extends DeltaBasedColumnUpdateOperation(command, declared, options) {
    override def representUpdateAsDeleteAndInsert(): Boolean = true

    override protected def newWriterFactory(
        info: LogicalWriteInfo,
        writeSchema: StructType): DeltaWriterFactory = {
      val pkRowId = rowId().map(_.describe()).toSeq == Seq("pk") &&
        info.rowIdSchema().get().fields.map(_.dataType).toSeq == Seq(IntegerType)
      new DeltaBufferedRowsWriterFactory(writeSchema, pkRowId)
    }

    // For delete+reinsert with narrow writes, the REINSERT row has only the connector-declared
    // columns (requiredDataAttributes order). The original row is located by the row ID, taking a
    // data row-ID column from the narrow row and a metadata row-ID column from the preserved
    // metadata, and the full row is rebuilt by overlaying the narrow row onto it. A DELETE logged
    // without a `pk` row ID is resolved the same way from its metadata, so such a row ID must
    // consist of metadata columns.
    override protected def commitColumnUpdate(
        info: LogicalWriteInfo,
        writeSchema: StructType,
        newData: Array[BufferedRows]): Unit = {
      val metadataSchema = info.metadataSchema().orElse(new StructType())
      val coversTable = schema.fieldNames.forall(writeSchema.getFieldIndex(_).isDefined)
      val deleteOpName = UTF8String.fromString(Delete.toString)
      val reinsertOpName = UTF8String.fromString(Reinsert.toString)
      val insertOpName = UTF8String.fromString(Insert.toString)

      def rowIdValues(narrowRow: Option[InternalRow], metadata: InternalRow): Seq[(String, Any)] = {
        rowId().toSeq.map(_.describe()).map { name =>
          (findFieldIndex(writeSchema, name), narrowRow) match {
            case (Some(i), Some(row)) => name -> row.get(i, writeSchema(i).dataType)
            case _ =>
              val i = findFieldIndex(metadataSchema, name).filter(_ => metadata != null)
                .getOrElse(throw new IllegalStateException(
                  s"The split fixture can only locate a row by pk or by metadata row-ID " +
                    s"columns, but row-ID column $name is in neither"))
              name -> metadata.get(i, metadataSchema(i).dataType)
          }
        }
      }

      def metadataOf(logRow: InternalRow): InternalRow =
        if (logRow.isNullAt(2)) null else logRow.getStruct(2, metadataSchema.length)

      val expandedData = newData.map { buf =>
        val expanded = new BufferedRows(buf.key, schema)
        buf.log.foreach { logRow =>
          val opName = logRow.getUTF8String(0)
          if (opName == deleteOpName && logRow.isNullAt(1)) {
            val values = rowIdValues(None, metadataOf(logRow))
            buf.deletes += currentRowByRowId(values).getInt(fieldIndexOf(schema, "pk"))
          } else if (opName == reinsertOpName) {
            val narrowRow = logRow.get(3, writeSchema).asInstanceOf[InternalRow]
            val baseRow = if (coversTable) {
              None
            } else {
              Some(currentRowByRowId(rowIdValues(Some(narrowRow), metadataOf(logRow))))
            }
            val fullRow = overlayNarrowRow(baseRow, narrowRow, writeSchema)
            expanded.rows.append(fullRow)
          } else if (opName == insertOpName) {
            // INSERT rows arrive with the full table schema via writer.insert()
            val insertRow = logRow.get(3, schema).asInstanceOf[InternalRow]
            expanded.rows.append(insertRow.copy())
          }
        }
        expanded
      }

      withDeletes(newData)
      withData(expandedData)
      lastWriteLog = newData.flatMap(buffer => buffer.log).toIndexedSeq
    }
  }

  // Test-only: a split-update fixture whose row ID is the given column, which the dispatcher does
  // not declare: it declares only the updated columns.
  class DeltaBasedColumnUpdateSplitOperationWithRowId(
      command: Command,
      rowIdCol: String,
      declared: Seq[NamedReference],
      options: CaseInsensitiveStringMap)
      extends DeltaBasedColumnUpdateSplitOperation(command, declared, options) {
    override def rowId(): Array[NamedReference] = Array(FieldReference(rowIdCol))
  }

  class PartitionBasedColumnUpdateOperation(
      command: Command,
      declared: Seq[NamedReference],
      override val options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty())
      extends RowLevelOperation with SupportsColumnUpdates with RowLevelOperationWithOptions {
    var configuredScan: BatchScanBaseClass = _

    override def command(): Command = command

    override def requiredDataAttributes(): Array[NamedReference] = declared.toArray

    override def requiredMetadataAttributes(): Array[NamedReference] = {
      if (noMetadata) {
        Array.empty
      } else {
        Array(PARTITION_COLUMN_REF, INDEX_COLUMN_REF)
      }
    }

    private def clusterColumnRef: NamedReference =
      if (noMetadata) FieldReference("dep") else PARTITION_COLUMN_REF

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      val onBuild: BatchScanBaseClass => Unit = { scan =>
        configuredScan = scan
      }
      if (properties.getOrDefault(COLUMN_UPDATE_COW_UNREAD_FILTER_ATTRS, "false") == "true") {
        new InMemoryScanBuilder(schema, options) {
          override protected def createScan(
              partitions: Seq[InputPartition],
              readSchema: StructType,
              tableSchema: StructType,
              options: CaseInsensitiveStringMap): BatchScanBaseClass = {
            if (useCatalystRuntimeFiltering) {
              new InMemoryCatalystRowLevelBatchScan(partitions, readSchema, tableSchema, options) {
                override def filterAttributes(): Array[NamedReference] =
                  identityPartitionReferences
              }
            } else {
              new InMemoryBatchScan(partitions, readSchema, tableSchema, options) {
                override def filterAttributes(): Array[NamedReference] =
                  identityPartitionReferences
              }
            }
          }

          override def build(): Scan = {
            val scan = super.build().asInstanceOf[BatchScanBaseClass]
            onBuild(scan)
            scan
          }
        }
      } else {
        newRowLevelScanBuilder(options)(onBuild)
      }
    }

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      lastWriteInfo = info
      new WriteBuilder {
        override def build(): Write = new Write with RequiresDistributionAndOrdering {
          override def requiredDistribution: Distribution =
            Distributions.clustered(Array(clusterColumnRef))

          override def requiredOrdering: Array[SortOrder] = Array[SortOrder](
            LogicalExpressions.sort(
              clusterColumnRef,
              SortDirection.ASCENDING,
              SortDirection.ASCENDING.defaultNullOrdering()))

          override def toBatch: BatchWrite = PartitionBasedNarrowReplaceData(
            configuredScan, info.columnUpdateSchema().orElse(info.schema()))

          override def description: String = "InMemoryNarrowCoWWrite"
        }
      }
    }

    override def description(): String = "InMemoryPartitionColumnUpdateOperation"
  }

  // Narrow UPDATE and COPY rows arrive through writeColumnUpdate and INSERT rows through write,
  // in one buffer; the operation tag of each log entry, in the same order as the rows, tells
  // them apart.
  private case class PartitionBasedNarrowReplaceData(
      scan: BatchScanBaseClass,
      writeSchema: StructType) extends TestBatchWrite {

    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
      if (properties.getOrDefault(COLUMN_UPDATE_COW_RECORD_ONLY_WRITER, "false") == "true") {
        new RecordOnlyColumnUpdateWriterFactory(CatalogV2Util.v2ColumnsToStructType(columns()))
      } else if (
          properties.getOrDefault(COLUMN_UPDATE_COW_NO_WRITE_COLUMN_UPDATE, "false") == "true") {
        super.createBatchWriterFactory(info)
      } else {
        new ColumnUpdateBufferedRowsWriterFactory(CatalogV2Util.v2ColumnsToStructType(columns()))
      }
    }

    override protected def doCommit(
        messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      val readRows = replaceReadPartitions(scan).flatMap(currentRows)

      val writeColumnUpdateOpName = UTF8String.fromString(WriteColumnUpdate.toString)

      val expandedData = newData.map { buf =>
        val expanded = new BufferedRows(buf.key, schema)
        buf.log.zip(buf.rows).foreach { case (logEntry, row) =>
          val opName = logEntry.getUTF8String(0)
          if (opName == writeColumnUpdateOpName) {
            val pkIdxInWrite = findFieldIndex(writeSchema, "pk").getOrElse(
              throw new IllegalStateException(
                "A narrow copy-on-write update must declare pk to locate the original row"))
            val pk = row.getInt(pkIdxInWrite)
            val pkIdx = fieldIndexOf(schema, "pk")
            val origRow = readRows.filter(r => r.getInt(pkIdx) == pk) match {
              case Seq(r) => r
              case matches => throw new IllegalStateException(
                s"Expected one original row with pk = $pk for a narrow update, " +
                  s"found ${matches.size}")
            }
            val fullRow = overlayNarrowRow(Some(origRow), row, writeSchema)
            expanded.rows.append(fullRow)
          } else {
            expanded.rows.append(row)
          }
        }
        expanded
      }

      withData(expandedData, schema)
      lastWriteLog = newData.flatMap(buffer => buffer.log).toImmutableArraySeq
    }
  }

  /**
   * Builds a scan for row-level operations. When `use-catalyst-runtime-filtering` is set, the
   * scan mixes in [[CatalystRuntimeFilteringScan]] so group filtering goes through the Catalyst
   * path.
   */
  protected def newRowLevelScanBuilder(
      options: CaseInsensitiveStringMap)(
      onBuild: BatchScanBaseClass => Unit): ScanBuilder = {
    new InMemoryScanBuilder(schema, options) {
      override protected def createScan(
          partitions: Seq[InputPartition],
          readSchema: StructType,
          tableSchema: StructType,
          options: CaseInsensitiveStringMap): BatchScanBaseClass = {
        if (useCatalystRuntimeFiltering) {
          InMemoryCatalystRowLevelBatchScan(partitions, readSchema, tableSchema, options)
        } else {
          super.createScan(partitions, readSchema, tableSchema, options)
        }
      }

      override def build: Scan = {
        val scan = super.build().asInstanceOf[BatchScanBaseClass]
        onBuild(scan)
        scan
      }
    }
  }

  /**
   * Row-level batch scan that receives runtime filters as Catalyst expressions. Pruning comes
   * from [[CatalystRuntimeFilteringScan]], so group filtering actually drops partitions (needed
   * for `replacedPartitions` assertions).
   */
  case class InMemoryCatalystRowLevelBatchScan(
      var _data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType,
      options: CaseInsensitiveStringMap)
    extends BatchScanBaseClass(_data, readSchema, tableSchema)
    with CatalystRuntimeFilteringScan {

    override def filterAttributes(): Array[NamedReference] = {
      identityPartitionAttributes
    }
  }
}

private class DeltaBufferedRowsWriterFactory(
    schema: StructType,
    pkRowId: Boolean = true) extends DeltaWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DeltaWriter[InternalRow] = {
    new DeltaBufferWriter(schema, pkRowId)
  }
}

// Logs the row ID of a DELETE as an integer when `pkRowId` is set, i.e. when the row ID is `pk`,
// and as null otherwise; the commit then resolves such a DELETE from its metadata.
private class DeltaBufferWriter(schema: StructType, pkRowId: Boolean)
  extends BufferWriter(schema) with DeltaWriter[InternalRow] {

  private final val DELETE = UTF8String.fromString(Delete.toString)
  private final val UPDATE = UTF8String.fromString(Update.toString)
  private final val REINSERT = UTF8String.fromString(Reinsert.toString)
  private final val INSERT = UTF8String.fromString(Insert.toString)

  override def delete(meta: InternalRow, id: InternalRow): Unit = {
    val pk = if (pkRowId) Some(id.getInt(0)) else None
    pk.foreach(buffer.deletes += _)
    val metaCopy = if (meta != null) meta.copy() else null
    val logEntry = new GenericInternalRow(Array[Any](DELETE, pk.getOrElse(null), metaCopy, null))
    buffer.log += logEntry
  }

  override def update(meta: InternalRow, id: InternalRow, row: InternalRow): Unit = {
    val pk = id.getInt(0)
    buffer.deletes += pk
    buffer.rows.append(row.copy())
    val metaCopy = if (meta != null) meta.copy() else null
    val logEntry = new GenericInternalRow(Array[Any](UPDATE, pk, metaCopy, row.copy()))
    buffer.log += logEntry
  }

  override def reinsert(meta: InternalRow, row: InternalRow): Unit = {
    buffer.rows.append(row.copy())
    val metaCopy = if (meta != null) meta.copy() else null
    val logEntry = new GenericInternalRow(Array[Any](REINSERT, null, metaCopy, row.copy()))
    buffer.log += logEntry
  }

  override def insert(row: InternalRow): Unit = {
    buffer.rows.append(row.copy())
    val logEntry = new GenericInternalRow(Array[Any](INSERT, null, null, row.copy()))
    buffer.log += logEntry
  }

  override def write(row: InternalRow): Unit = {
    throw new UnsupportedOperationException()
  }

  override def commit(): WriterCommitMessage = buffer
}

// Test-only: a writer that accepts writeColumnUpdate and tags those rows in the write log. Only the
// column-update writers use it, so a plain writer rejects rows sent to writeColumnUpdate.
private class ColumnUpdateBufferWriter(schema: StructType) extends BufferWriter(schema) {
  private final val WRITE_COLUMN_UPDATE = UTF8String.fromString(WriteColumnUpdate.toString)

  override def writeColumnUpdate(metadata: InternalRow, row: InternalRow): Unit = {
    buffer.rows.append(row.copy())
    val logEntry = new GenericInternalRow(
      Array[Any](WRITE_COLUMN_UPDATE, null, metadata.copy(), row.copy()))
    buffer.log.append(logEntry)
  }

  override def writeColumnUpdate(row: InternalRow): Unit = {
    buffer.rows.append(row.copy())
    val logEntry = new GenericInternalRow(Array[Any](WRITE_COLUMN_UPDATE, null, null, row.copy()))
    buffer.log.append(logEntry)
  }
}

private class ColumnUpdateBufferedRowsWriterFactory(schema: StructType)
  extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new ColumnUpdateBufferWriter(schema)
  }
}

// Test-only: a writer that overrides writeColumnUpdate(record) but not
// writeColumnUpdate(metadata, record), so rows with metadata reach it through the default.
private class RecordOnlyColumnUpdateWriterFactory(schema: StructType) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new RecordOnlyColumnUpdateWriter(schema)
  }
}

private class RecordOnlyColumnUpdateWriter(schema: StructType) extends DataWriter[InternalRow] {
  private val delegate = new ColumnUpdateBufferWriter(schema)
  override def write(record: InternalRow): Unit = delegate.write(record)
  override def writeColumnUpdate(record: InternalRow): Unit = delegate.writeColumnUpdate(record)
  override def commit(): WriterCommitMessage = delegate.commit()
  override def abort(): Unit = delegate.abort()
  override def close(): Unit = delegate.close()
}

object InMemoryRowLevelOperationTable {
  def withColumns(
      name: String,
      columns: Array[Column],
      partitioning: Array[Transform],
      properties: util.Map[String, String],
      constraints: Array[Constraint] = Array.empty,
      tableId: String = java.util.UUID.randomUUID().toString,
      schemaAlignmentConfig: SchemaAlignmentConfig = SchemaAlignmentConfig.DEFAULT)
    : InMemoryRowLevelOperationTable = {
    new InMemoryRowLevelOperationTable(
      name, columns, partitioning, properties, constraints, tableId, schemaAlignmentConfig)
  }
}

/**
 * An in-memory table that implements only [[TruncatableTable]] (NOT [[SupportsDeleteV2]]).
 * Used to exercise the [[TruncateTableExec]] planning path for DELETE statements with no WHERE.
 */
class InMemoryTruncatableOnlyTable(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: util.Map[String, String],
    constraints: Array[Constraint])
  extends InMemoryBaseTable(name, columns, partitioning, properties, constraints)
  with TruncatableTable {

  var lastTruncateOptions: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty()

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    InMemoryBaseTable.maybeSimulateFailedTableWrite(new CaseInsensitiveStringMap(properties))
    InMemoryBaseTable.maybeSimulateFailedTableWrite(info.options)
    new InMemoryWriterBuilder(info) {
      override def truncate(): WriteBuilder = {
        writer = new TruncateAndAppend(this.info)
        streamingWriter = new StreamingTruncateAndAppend(this.info)
        this
      }
    }
  }

  override def truncateTable(): Boolean = {
    lastTruncateOptions = CaseInsensitiveStringMap.empty()
    dataMap.synchronized { dataMap.clear() }
    increaseVersion()
    true
  }

  override def truncateTable(options: CaseInsensitiveStringMap): Boolean = {
    lastTruncateOptions = options
    dataMap.synchronized { dataMap.clear() }
    increaseVersion()
    true
  }
}
