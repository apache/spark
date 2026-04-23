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

import java.time.Instant
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{FieldReference, LogicalExpressions, NamedReference, SortDirection, SortOrder, Transform}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DeltaBatchWrite, DeltaWrite, DeltaWriteBuilder, DeltaWriter, DeltaWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, RequiresDistributionAndOrdering, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, SupportsDelta, Write, WriteBuilder, WriterCommitMessage, WriteSummary}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

/**
 * Row-level operation plumbing shared by [[InMemoryRowLevelOperationTable]] (which also mixes in
 * `SupportsDelete`) and [[InMemoryRowLevelOperationOnlyTable]] (which does not). Keeping this in
 * a trait avoids duplicating the per-operation builders, scan/write wiring, and delta writer.
 */
trait InMemoryRowLevelOperationsMixin extends SupportsRowLevelOperations {
  self: InMemoryBaseTable =>

  private final val PARTITION_COLUMN_REF = FieldReference(PartitionKeyColumn.name)
  private final val INDEX_COLUMN_REF = FieldReference(IndexColumn.name)
  private final val SUPPORTS_DELTAS = "supports-deltas"
  private final val SPLIT_UPDATES = "split-updates"
  private final val NO_METADATA = "no-metadata"
  private final def noMetadata: Boolean =
    properties.getOrDefault(NO_METADATA, "false") == "true"

  // used in row-level operation tests to verify replaced partitions
  var replacedPartitions: Seq[Seq[Any]] = Seq.empty
  // used in row-level operation tests to verify reported write schema
  var lastWriteInfo: LogicalWriteInfo = _
  // used in row-level operation tests to verify passed records
  // (operation, id, metadata, row)
  var lastWriteLog: Seq[InternalRow] = Seq.empty

  override def newRowLevelOperationBuilder(
      info: RowLevelOperationInfo): RowLevelOperationBuilder = {
    if (properties.getOrDefault(SUPPORTS_DELTAS, "false") == "true") {
      () => DeltaBasedOperation(info.command)
    } else {
      () => PartitionBasedOperation(info.command)
    }
  }

  case class PartitionBasedOperation(command: Command) extends RowLevelOperation {
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

  abstract class RowLevelOperationBatchWrite extends TestBatchWrite {

    override def commit(messages: Array[WriterCommitMessage], metrics: WriteSummary): Unit = {
      commit(messages)
      commits += Commit(Instant.now().toEpochMilli, Some(metrics))
    }
  }

  private case class PartitionBasedReplaceData(scan: InMemoryBatchScan)
    extends RowLevelOperationBatchWrite {

    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      // If the row-level scan was optimized away before
      // GroupBasedRowLevelOperationScanPlanning could attach one (e.g. the rewritten Filter
      // folded to FALSE and PruneFilters replaced the scan with an empty LocalRelation), no
      // partitions were read -- there is nothing to replace.
      val readPartitions: Seq[Seq[Any]] = if (scan == null) {
        Seq.empty
      } else {
        scan.data
          .flatMap(_.asInstanceOf[BufferedRows].rows)
          .map(r => getKey(r, schema))
          .distinct
      }
      dataMap --= readPartitions
      replacedPartitions = readPartitions
      withData(newData, schema)
      lastWriteLog = newData.flatMap(buffer => buffer.log).toImmutableArraySeq
    }
  }

  case class DeltaBasedOperation(command: Command) extends RowLevelOperation with SupportsDelta {
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
      new InMemoryScanBuilder(schema, options)
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

  private object TestDeltaBatchWrite extends RowLevelOperationBatchWrite with DeltaBatchWrite{
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DeltaWriterFactory = {
      new DeltaBufferedRowsWriterFactory(CatalogV2Util.v2ColumnsToStructType(columns()))
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      withDeletes(newData)
      withData(newData, columns())
      lastWriteLog = newData.flatMap(buffer => buffer.log).toIndexedSeq
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {}
  }
}

class InMemoryRowLevelOperationTable(
    name: String,
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String],
    constraints: Array[Constraint] = Array.empty)
  extends InMemoryTable(
    name,
    CatalogV2Util.structTypeToV2Columns(schema),
    partitioning,
    properties,
    constraints)
  with InMemoryRowLevelOperationsMixin

object InMemoryRowLevelOperationTable {
  /**
   * Table property that makes the catalog instantiate the row-level-only variant of the
   * row-level operation table, i.e. a table that mixes in `SupportsRowLevelOperations` but
   * not `SupportsDelete` / `TruncatableTable`. This forces every DELETE onto the row-level
   * rewrite path regardless of predicate, which is not otherwise reachable with the default
   * fixture.
   */
  val SUPPORTS_DELETE_FILTER_PROP: String = "supports-delete-filter"
}

/**
 * Row-level operation table without `SupportsDelete`. Used by tests that need DELETE to take
 * the row-level rewrite path for every predicate (including ones that fold to `TrueLiteral`).
 */
class InMemoryRowLevelOperationOnlyTable(
    name: String,
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String],
    constraints: Array[Constraint] = Array.empty)
  extends InMemoryBaseTable(
    name,
    CatalogV2Util.structTypeToV2Columns(schema),
    partitioning,
    properties,
    constraints)
  with InMemoryRowLevelOperationsMixin {

  // Minimal writer builder that supports append (needed by test fixture's createAndInitTable).
  // Overwrite/truncate are not supported -- this is a pure row-level operation table.
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    InMemoryBaseTable.maybeSimulateFailedTableWrite(new CaseInsensitiveStringMap(properties))
    InMemoryBaseTable.maybeSimulateFailedTableWrite(info.options)
    new InMemoryWriterBuilder(info) {
      override def truncate(): WriteBuilder =
        throw new UnsupportedOperationException(
          "truncate is not supported on InMemoryRowLevelOperationOnlyTable")
    }
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
