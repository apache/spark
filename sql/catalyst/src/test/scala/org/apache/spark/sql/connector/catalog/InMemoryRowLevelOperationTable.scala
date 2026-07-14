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
import org.apache.spark.sql.connector.expressions.{FieldReference, LogicalExpressions, NamedReference, SortDirection, SortOrder, Transform}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DeltaBatchWrite, DeltaWrite, DeltaWriteBuilder, DeltaWriter, DeltaWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, RequiresDistributionAndOrdering, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, SupportsColumnUpdates, SupportsDelta, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.types.StructType
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
    tableId: String)
  extends InMemoryTable(
    name,
    columns,
    partitioning,
    properties,
    constraints,
    id = tableId)
  with SupportsRowLevelOperations {

  def this(
      name: String,
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String],
      constraints: Array[Constraint] = Array.empty,
      tableId: String = java.util.UUID.randomUUID().toString) = {
    this(
      name = name,
      columns = CatalogV2Util.structTypeToV2Columns(schema),
      partitioning = partitioning,
      properties = properties,
      constraints = constraints,
      tableId = tableId)
  }

  private final val PARTITION_COLUMN_REF = FieldReference(PartitionKeyColumn.name)
  private final val INDEX_COLUMN_REF = FieldReference(IndexColumn.name)
  private final val SUPPORTS_DELTAS = "supports-deltas"
  private final val SPLIT_UPDATES = "split-updates"
  private final val NO_METADATA = "no-metadata"
  private final val noMetadata = properties.getOrDefault(NO_METADATA, "false") == "true"
  private final val COLUMN_UPDATE = "column-update"
  private final val COLUMN_UPDATE_REQ_ATTRS = "column-update-req-attrs"
  private final val COLUMN_UPDATE_COW = "column-update-cow"
  private final val COLUMN_UPDATE_FROM_INFO = "column-update-from-info"
  private final val COLUMN_UPDATE_SPLIT = "column-update-split"
  private final val COLUMN_UPDATE_EMPTY_REQ_ATTRS = "column-update-empty-req-attrs"

  // used in row-level operation tests to verify replaced partitions
  var replacedPartitions: Seq[Seq[Any]] = Seq.empty
  // used in row-level operation tests to verify reported write schema
  var lastWriteInfo: LogicalWriteInfo = _
  // used in column-update tests to verify that Spark passed the correct updated column list
  // to the connector via RowLevelOperationInfo.updatedColumns()
  var lastUpdatedColumns: Array[NamedReference] = Array.empty
  // used in scan-narrowing tests to verify the schema Spark asked the connector to read.
  // Routed through the companion object (see InMemoryRowLevelOperationTable.lastScanSchema)
  // because Spark's planner and the test harness can hold references to different table
  // instances for the same identifier -- a per-instance field would only be visible to one.
  def lastScanSchema: StructType =
    InMemoryRowLevelOperationTable.lastScanSchemaRef.get()
  // used in row-level operation tests to verify passed records
  // (operation, id, metadata, row)
  var lastWriteLog: Seq[InternalRow] = Seq.empty

  override def copy(): Table = {
    val copied = InMemoryRowLevelOperationTable.withColumns(
      name = name,
      columns = columns(),
      partitioning = partitioning,
      properties = properties,
      constraints = constraints,
      tableId = id)
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
    copied.setVersionAndValidatedVersionFrom(this)
    copied.replacedPartitions = replacedPartitions
    copied.lastWriteInfo = lastWriteInfo
    copied.lastWriteLog = lastWriteLog
    copied
  }

  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder = {
    lastUpdatedColumns = info.updatedColumns()
    if (properties.getOrDefault(COLUMN_UPDATE, "false") == "true") {
      () => new DeltaBasedColumnUpdateOperation(info.command, info.updatedColumns().toSeq)
    } else if (properties.containsKey(COLUMN_UPDATE_REQ_ATTRS)) {
      val reqCols = properties.get(COLUMN_UPDATE_REQ_ATTRS).split(",").map(_.trim)
      () => new DeltaBasedColumnUpdateOperationWithReqAttrs(info.command, reqCols)
    } else if (properties.getOrDefault(COLUMN_UPDATE_EMPTY_REQ_ATTRS, "false") == "true") {
      // Test-only: returns an empty requiredDataAttributes() so we can verify Spark rejects it.
      () => new DeltaBasedColumnUpdateOperationWithReqAttrs(info.command, Array.empty)
    } else if (properties.getOrDefault(COLUMN_UPDATE_FROM_INFO, "false") == "true") {
      () => new DeltaBasedColumnUpdateOperationFromInfo(info.command, info.updatedColumns().toSeq)
    } else if (properties.getOrDefault(COLUMN_UPDATE_COW, "false") == "true") {
      () => new PartitionBasedColumnUpdateOperation(info.command, info.updatedColumns().toSeq)
    } else if (properties.getOrDefault(COLUMN_UPDATE_SPLIT, "false") == "true") {
      () => new DeltaBasedColumnUpdateSplitOperation(info.command, info.updatedColumns().toSeq)
    } else if (properties.getOrDefault(SUPPORTS_DELTAS, "false") == "true") {
      () => DeltaBasedOperation(info.command, info.options)
    } else {
      () => PartitionBasedOperation(info.command, info.options)
    }
  }

  case class PartitionBasedOperation(command: Command, options: CaseInsensitiveStringMap)
    extends RowLevelOperation with RowLevelOperationWithOptions {
    var configuredScan: InMemoryBatchScan = _

    override def requiredMetadataAttributes(): Array[NamedReference] = {
      if (noMetadata) {
        Array.empty
      } else {
        Array(PARTITION_COLUMN_REF, INDEX_COLUMN_REF)
      }
    }

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new InMemoryScanBuilder(schema, options) {
        override def build: Scan = {
          val scan = super.build()
          InMemoryRowLevelOperationTable.recordLastScanSchema(scan.readSchema())
          configuredScan = scan.asInstanceOf[InMemoryBatchScan]
          scan
        }
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

  private case class PartitionBasedReplaceData(scan: InMemoryBatchScan)
    extends TestBatchWrite {

    override protected def doCommit(
        messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      val readRows = scan.data.flatMap(_.asInstanceOf[BufferedRows].rows)
      val readPartitions = readRows.map(r => getKey(r, schema)).distinct
      dataMap --= readPartitions
      replacedPartitions = readPartitions
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
      new InMemoryScanBuilder(schema, options) {
        override def build: Scan = {
          val scan = super.build()
          InMemoryRowLevelOperationTable.recordLastScanSchema(scan.readSchema())
          scan
        }
      }
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

  // A delta-based operation that supports column-level updates: Spark sends only the
  // declared + assigned columns in the row projection instead of the full row schema. The base
  // class composes its required-attrs set as `pk` (the row-lookup key) plus whatever columns
  // Spark reports as being assigned via `RowLevelOperationInfo#updatedColumns()`.
  class DeltaBasedColumnUpdateOperation(
      command: Command,
      updatedCols: Seq[NamedReference] = Nil)
      extends DeltaBasedOperation(command, CaseInsensitiveStringMap.empty())
        with SupportsColumnUpdates {
    override def representUpdateAsDeleteAndInsert(): Boolean = false
    override def requiredDataAttributes(): Array[NamedReference] = {
      val pk: NamedReference = FieldReference("pk")
      val updatedNames = updatedCols.map(_.describe()).toSet
      if (updatedNames.contains("pk")) updatedCols.toArray
      else (pk +: updatedCols).toArray
    }

    override def newWriteBuilder(info: LogicalWriteInfo): DeltaWriteBuilder = {
      lastWriteInfo = info
      // Capture info into a local val so nested writer/commit closures see a stable schema
      // even if a subsequent newWriteBuilder call mutates lastWriteInfo.
      val capturedInfo = info
      val capturedWriteSchema = if (capturedInfo.updateSchema().isPresent) {
        capturedInfo.updateSchema().get()
      } else {
        capturedInfo.schema()
      }
      new DeltaWriteBuilder {
        override def build(): DeltaWrite =
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

            override def toBatch: DeltaBatchWrite =
              new TestBatchWrite with DeltaBatchWrite {
                override def createBatchWriterFactory(
                    info: PhysicalWriteInfo): DeltaWriterFactory = {
                  // Use the narrow update schema for UPDATE/COPY/REINSERT rows when present.
                  new DeltaBufferedRowsWriterFactory(capturedWriteSchema)
                }

                // For column-update writes, rows contain only the assigned columns
                // (narrow schema from LogicalWriteInfo). We expand each row to the full table
                // schema by overlaying write-schema columns on the base row found by pk.
                override protected def doCommit(messages: Array[WriterCommitMessage]): Unit =
                  dataMap.synchronized {
                    val newData = messages.map(_.asInstanceOf[BufferedRows])
                    val writeSchema = capturedWriteSchema
                    val writeFieldIdx = writeSchema.fieldNames.zipWithIndex.toMap

                    val mergedData = newData.map { buf =>
                      val merged = new BufferedRows(buf.key, schema)
                      val updateOpName = UTF8String.fromString(Update.toString)
                      val insertOpName = UTF8String.fromString(Insert.toString)
                      buf.log.foreach { logRow =>
                        val opName = logRow.getUTF8String(0)
                        if (opName == updateOpName) {
                          val pk = logRow.getInt(1)
                          val narrowRow = logRow.get(3, writeSchema).asInstanceOf[InternalRow]
                          val baseRow = dataMap.values.iterator.flatten
                            .flatMap(_.rows)
                            .find(r => r.getInt(schema.fieldIndex("pk")) == pk)
                          val fullRow = new GenericInternalRow(schema.length)
                          baseRow.foreach { base =>
                            for (i <- schema.fields.indices) {
                              fullRow.update(i, base.get(i, schema.fields(i).dataType))
                            }
                          }
                          schema.fields.zipWithIndex.foreach { case (field, i) =>
                            writeFieldIdx.get(field.name).foreach { j =>
                              fullRow.update(i, narrowRow.get(j, field.dataType))
                            }
                          }
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

                override def abort(messages: Array[WriterCommitMessage]): Unit = {}
              }
          }
      }
    }
  }

  class DeltaBasedColumnUpdateOperationWithReqAttrs(command: Command, reqCols: Array[String])
      extends DeltaBasedColumnUpdateOperation(command) {
    override def requiredDataAttributes(): Array[NamedReference] = reqCols.map(FieldReference(_))
  }

  class DeltaBasedColumnUpdateOperationFromInfo(
      command: Command,
      updatedCols: Seq[NamedReference])
      extends DeltaBasedColumnUpdateOperation(command, updatedCols) {
  }

  class DeltaBasedColumnUpdateSplitOperation(
      command: Command,
      updatedCols: Seq[NamedReference] = Nil)
      extends DeltaBasedColumnUpdateOperation(command, updatedCols) {
    override def representUpdateAsDeleteAndInsert(): Boolean = true

    override def newWriteBuilder(info: LogicalWriteInfo): DeltaWriteBuilder = {
      lastWriteInfo = info
      // Capture info into a local val so nested writer/commit closures see a stable schema
      // even if a subsequent newWriteBuilder call mutates lastWriteInfo.
      val capturedInfo = info
      val capturedWriteSchema = if (capturedInfo.updateSchema().isPresent) {
        capturedInfo.updateSchema().get()
      } else {
        capturedInfo.schema()
      }
      new DeltaWriteBuilder {
        override def build(): DeltaWrite =
          new DeltaWrite with RequiresDistributionAndOrdering {
            override def requiredDistribution(): Distribution =
              Distributions.clustered(Array(PARTITION_COLUMN_REF))
            override def requiredOrdering(): Array[SortOrder] = Array[SortOrder](
              LogicalExpressions.sort(
                PARTITION_COLUMN_REF,
                SortDirection.ASCENDING,
                SortDirection.ASCENDING.defaultNullOrdering()))
            override def toBatch: DeltaBatchWrite =
              new TestBatchWrite with DeltaBatchWrite {
                override def createBatchWriterFactory(
                    info: PhysicalWriteInfo): DeltaWriterFactory = {
                  new DeltaBufferedRowsWriterFactory(capturedWriteSchema)
                }

                // For delete+reinsert with narrow writes, the REINSERT row has only the
                // connector-declared columns (requiredDataAttributes order).
                // pk is the first field in the write schema (declared before updatedCols).
                // Reconstruct the full row by overlaying the narrow row onto the original.
                override protected def doCommit(messages: Array[WriterCommitMessage]): Unit =
                  dataMap.synchronized {
                    val newData = messages.map(_.asInstanceOf[BufferedRows])
                    val writeSchema = capturedWriteSchema
                    val writeFieldIdx = writeSchema.fieldNames.zipWithIndex.toMap
                    val reinsertOpName = UTF8String.fromString(Reinsert.toString)
                    val pkIdx = writeFieldIdx("pk")

                    val expandedData = newData.map { buf =>
                      val expanded = new BufferedRows(buf.key, schema)
                      buf.log.foreach { logRow =>
                        val opName = logRow.getUTF8String(0)
                        if (opName == reinsertOpName) {
                          val narrowRow = logRow.get(3, writeSchema).asInstanceOf[InternalRow]
                          val pk = narrowRow.getInt(pkIdx)
                          val baseRow = dataMap.values.iterator.flatten
                            .flatMap(_.rows)
                            .find(r => r.getInt(schema.fieldIndex("pk")) == pk)
                          val fullRow = new GenericInternalRow(schema.length)
                          baseRow.foreach { base =>
                            for (i <- schema.fields.indices) {
                              fullRow.update(i, base.get(i, schema.fields(i).dataType))
                            }
                          }
                          schema.fields.zipWithIndex.foreach { case (field, i) =>
                            writeFieldIdx.get(field.name).foreach { j =>
                              fullRow.update(i, narrowRow.get(j, field.dataType))
                            }
                          }
                          expanded.rows.append(fullRow)
                        }
                      }
                      expanded
                    }

                    withDeletes(newData)
                    withData(expandedData)
                    lastWriteLog = newData.flatMap(buffer => buffer.log).toIndexedSeq
                  }

                override def abort(messages: Array[WriterCommitMessage]): Unit = {}
              }
          }
      }
    }
  }

  class PartitionBasedColumnUpdateOperation(
      command: Command,
      updatedCols: Seq[NamedReference] = Nil)
      extends RowLevelOperation with SupportsColumnUpdates {
    var configuredScan: InMemoryBatchScan = _

    override def command(): Command = command

    override def requiredDataAttributes(): Array[NamedReference] = {
      val base = Seq(FieldReference("pk"), FieldReference("dep"))
      val baseNames = base.map(_.describe()).toSet
      (base ++ updatedCols.filterNot(r => baseNames.contains(r.describe()))).toArray
    }

    override def requiredMetadataAttributes(): Array[NamedReference] =
      Array(PARTITION_COLUMN_REF, INDEX_COLUMN_REF)

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new InMemoryScanBuilder(schema, options) {
        override def build(): Scan = {
          val scan = super.build()
          InMemoryRowLevelOperationTable.recordLastScanSchema(scan.readSchema())
          configuredScan = scan.asInstanceOf[InMemoryBatchScan]
          scan
        }
      }
    }

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      lastWriteInfo = info
      new WriteBuilder {
        override def build(): Write = new Write with RequiresDistributionAndOrdering {
          override def requiredDistribution: Distribution =
            Distributions.clustered(Array(PARTITION_COLUMN_REF))

          override def requiredOrdering: Array[SortOrder] = Array[SortOrder](
            LogicalExpressions.sort(
              PARTITION_COLUMN_REF,
              SortDirection.ASCENDING,
              SortDirection.ASCENDING.defaultNullOrdering()))

          override def toBatch: BatchWrite = {
            val narrowSchema = if (info.updateSchema().isPresent) {
              info.updateSchema().get()
            } else {
              info.schema()
            }
            PartitionBasedNarrowReplaceData(configuredScan, narrowSchema, info.schema())
          }

          override def description: String = "InMemoryNarrowCoWWrite"
        }
      }
    }

    override def description(): String = "InMemoryPartitionColumnUpdateOperation"
  }

  // CoW write handler for narrow column-update writes.
  // Narrow rows (UPDATE/COPY) are sent via writeUpdate, wide rows (INSERT) via write.
  // Both arrive in the same buffer; rows are routed by the operation tag in the log entry,
  // which lets tests assert that Spark dispatched through the correct writer method.
  private case class PartitionBasedNarrowReplaceData(
      scan: InMemoryBatchScan,
      writeSchema: StructType,
      fullSchema: StructType) extends TestBatchWrite {

    override protected def doCommit(
        messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      val readRows = scan.data.flatMap(_.asInstanceOf[BufferedRows].rows)
      val readPartitions = readRows.map(r => getKey(r, schema)).distinct
      dataMap --= readPartitions
      replacedPartitions = readPartitions

      val writeFieldIdx = writeSchema.fieldNames.zipWithIndex.toMap
      val pkIdxInWrite = writeFieldIdx("pk")
      val pkIdxInFull = schema.fieldIndex("pk")
      val writeUpdateOpName = UTF8String.fromString(WriteUpdate.toString)

      val expandedData = newData.map { buf =>
        val expanded = new BufferedRows(buf.key, schema)
        // Walk the log so we can route on the operation tag (Write vs WriteUpdate).
        // buf.rows contains rows in the same order as the log; iterate together.
        buf.log.zip(buf.rows).foreach { case (logEntry, row) =>
          val opName = logEntry.getUTF8String(0)
          if (opName == writeUpdateOpName) {
            // UPDATE/COPY narrow row: look up base row by pk, overlay narrow values
            val pk = row.getInt(pkIdxInWrite)
            val origRow = readRows.find(r => r.getInt(pkIdxInFull) == pk)
            val fullRow = new GenericInternalRow(schema.length)
            origRow.foreach { base =>
              for (i <- schema.fields.indices) {
                fullRow.update(i, base.get(i, schema.fields(i).dataType))
              }
            }
            schema.fields.zipWithIndex.foreach { case (field, i) =>
              writeFieldIdx.get(field.name).foreach { j =>
                fullRow.update(i, row.get(j, field.dataType))
              }
            }
            expanded.rows.append(fullRow)
          } else {
            // INSERT row: full schema, append directly aligned to table schema
            val fullRow = new GenericInternalRow(schema.length)
            schema.fields.zipWithIndex.foreach { case (field, i) =>
              val srcIdx = fullSchema.fieldIndex(field.name)
              fullRow.update(i, row.get(srcIdx, field.dataType))
            }
            expanded.rows.append(fullRow)
          }
        }
        expanded
      }

      withData(expandedData, schema)
      lastWriteLog = newData.flatMap(buffer => buffer.log).toImmutableArraySeq
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
}

private class DeltaBufferedRowsWriterFactory(schema: StructType) extends DeltaWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DeltaWriter[InternalRow] = {
    new DeltaBufferWriter(schema)
  }
}

private class DeltaBufferWriter(schema: StructType) extends BufferWriter(schema)
  with DeltaWriter[InternalRow] {

  private final val DELETE = UTF8String.fromString(Delete.toString)
  private final val UPDATE = UTF8String.fromString(Update.toString)
  private final val REINSERT = UTF8String.fromString(Reinsert.toString)
  private final val INSERT = UTF8String.fromString(Insert.toString)

  override def delete(meta: InternalRow, id: InternalRow): Unit = {
    val pk = id.getInt(0)
    buffer.deletes += pk
    val metaCopy = if (meta != null) meta.copy() else null
    val logEntry = new GenericInternalRow(Array[Any](DELETE, pk, metaCopy, null))
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

object InMemoryRowLevelOperationTable {
  // Global holder for the scan schema of the last row-level operation. See the class-level
  // `lastScanSchema` def for why this is a companion-object field rather than a per-instance one.
  // Tests that assert on scan narrowing should reset this before running the operation under test
  // (see `RowLevelOperationSuiteBase.beforeEach`).
  private[catalog] val lastScanSchemaRef: java.util.concurrent.atomic.AtomicReference[StructType] =
    new java.util.concurrent.atomic.AtomicReference[StructType]()

  /** Called by test-only scan builders on every scan build. Overwrites; tests reset between
   * cases. */
  private[catalog] def recordLastScanSchema(schema: StructType): Unit = {
    lastScanSchemaRef.set(schema)
  }

  /** Called by test setup to clear the last recorded scan schema between test cases. */
  def resetLastScanSchema(): Unit = lastScanSchemaRef.set(null)

  def withColumns(
      name: String,
      columns: Array[Column],
      partitioning: Array[Transform],
      properties: util.Map[String, String],
      constraints: Array[Constraint] = Array.empty,
      tableId: String = java.util.UUID.randomUUID().toString): InMemoryRowLevelOperationTable = {
    new InMemoryRowLevelOperationTable(
      name, columns, partitioning, properties, constraints, tableId)
  }
}
