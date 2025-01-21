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
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{FieldReference, LogicalExpressions, NamedReference, SortDirection, SortOrder, Transform}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DeltaBatchWrite, DeltaWrite, DeltaWriteBuilder, DeltaWriter, DeltaWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, RequiresDistributionAndOrdering, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, SupportsDelta, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class InMemoryRowLevelOperationTable(
    name: String,
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryTable(name, schema, partitioning, properties) with SupportsRowLevelOperations {

  private final val PARTITION_COLUMN_REF = FieldReference(PartitionKeyColumn.name)
  private final val SUPPORTS_DELTAS = "supports-deltas"
  private final val SPLIT_UPDATES = "split-updates"

  // used in row-level operation tests to verify replaced partitions
  var replacedPartitions: Seq[Seq[Any]] = Seq.empty

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
      Array(PARTITION_COLUMN_REF)
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

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new WriteBuilder {

      override def build(): Write = new Write with RequiresDistributionAndOrdering {
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

        override def toBatch: BatchWrite = PartitionBasedReplaceData(configuredScan)

        override def description(): String = "InMemoryWrite"
      }
    }

    override def description(): String = "InMemoryPartitionReplaceOperation"
  }

  private case class PartitionBasedReplaceData(scan: InMemoryBatchScan) extends TestBatchWrite {

    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      val readRows = scan.data.flatMap(_.asInstanceOf[BufferedRows].rows)
      val readPartitions = readRows.map(r => getKey(r, schema)).distinct
      dataMap --= readPartitions
      replacedPartitions = readPartitions
      withData(newData, schema)
    }
  }

  case class DeltaBasedOperation(command: Command) extends RowLevelOperation with SupportsDelta {
    private final val PK_COLUMN_REF = FieldReference("pk")

    override def requiredMetadataAttributes(): Array[NamedReference] = {
      Array(PARTITION_COLUMN_REF)
    }

    override def rowId(): Array[NamedReference] = Array(PK_COLUMN_REF)

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new InMemoryScanBuilder(schema, options)
    }

    override def newWriteBuilder(info: LogicalWriteInfo): DeltaWriteBuilder =
      new DeltaWriteBuilder {
        override def build(): DeltaWrite = new DeltaWrite with RequiresDistributionAndOrdering {

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

    override def representUpdateAsDeleteAndInsert(): Boolean = {
      properties.getOrDefault(SPLIT_UPDATES, "false").toBoolean
    }
  }

  private object TestDeltaBatchWrite extends DeltaBatchWrite {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DeltaWriterFactory = {
      DeltaBufferedRowsWriterFactory
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      withDeletes(messages.map(_.asInstanceOf[BufferedRows]))
      withData(messages.map(_.asInstanceOf[BufferedRows]))
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {}
  }
}

private object DeltaBufferedRowsWriterFactory extends DeltaWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DeltaWriter[InternalRow] = {
    new DeltaBufferWriter
  }
}

private class DeltaBufferWriter extends BufferWriter with DeltaWriter[InternalRow] {

  override def delete(meta: InternalRow, id: InternalRow): Unit = buffer.deletes += id.getInt(0)

  override def update(meta: InternalRow, id: InternalRow, row: InternalRow): Unit = {
    buffer.deletes += id.getInt(0)
    write(row)
  }

  override def insert(row: InternalRow): Unit = write(row)

  override def write(row: InternalRow): Unit = super[BufferWriter].write(row)

  override def commit(): WriterCommitMessage = buffer
}
