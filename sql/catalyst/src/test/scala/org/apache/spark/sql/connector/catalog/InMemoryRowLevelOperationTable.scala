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

import java.{lang, util}
import java.time.Instant

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{FieldReference, LogicalExpressions, NamedReference, SortDirection, SortOrder, Transform}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DeltaBatchWrite, DeltaWrite, DeltaWriteBuilder, DeltaWriter, DeltaWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, RequiresDistributionAndOrdering, RowLevelOperation, RowLevelOperationBuilder, RowLevelOperationInfo, SupportsDelta, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

class InMemoryRowLevelOperationTable(
    name: String,
    val initialSchema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String],
    constraints: Array[Constraint] = Array.empty)
  extends InMemoryTable(
    name,
    CatalogV2Util.structTypeToV2Columns(initialSchema),
    partitioning,
    properties,
    constraints)
  with SupportsRowLevelOperations {

  private final val PARTITION_COLUMN_REF = FieldReference(PartitionKeyColumn.name)
  private final val INDEX_COLUMN_REF = FieldReference(IndexColumn.name)
  private final val SUPPORTS_DELTAS = "supports-deltas"
  private final val SPLIT_UPDATES = "split-updates"

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

  override def mergeSchema(sourceTableSchema: StructType): StructType = {
    mergeTypes(CatalogV2Util.v2ColumnsToStructType(columns()), sourceTableSchema)
      .asInstanceOf[StructType]
  }

  private def mergeTypes(current: DataType, newType: DataType): DataType = {
    (current, newType) match {
      case (StructType(currentFields), StructType(newFields)) =>
        val newFieldMap = toFieldMap(newFields)

        // Update existing field types
        val updatedCurrentFields = currentFields.map { currentField =>
          newFieldMap.get(currentField.name) match {
            case Some(newField) if newField.dataType != currentField.dataType =>
              val newType = mergeTypes(currentField.dataType, newField.dataType)
              StructField(currentField.name, newType, currentField.nullable,
                currentField.metadata)
            case _ => currentField
          }
        }

        // Identify the newly added fields and append to the end
        val currentFieldMap = toFieldMap(currentFields)
        val remainingNewFields = newFields.filterNot (f => currentFieldMap.contains (f.name) )
        StructType( updatedCurrentFields ++ remainingNewFields )

      case (ArrayType(currentElementType, currentContainsNull), ArrayType(newElementType, _)) =>
        ArrayType(mergeTypes(currentElementType, newElementType), currentContainsNull)

      case (MapType(currentKeyType, currentElementType, currentContainsNull),
      MapType(updateKeyType, updateElementType, _)) =>
        MapType(
          mergeTypes(currentKeyType, updateKeyType),
          mergeTypes(currentElementType, updateElementType),
          currentContainsNull)

      case (_, newType: DataType) =>
        // For now support any type change for testing
        // Data source could implement type widening checks here.
        newType
    }

  }

  override def updateSchema(newSchema: StructType): Unit = {
    tableColumns = CatalogV2Util.structTypeToV2Columns(newSchema)
  }

  def toFieldMap(fields: Array[StructField]): Map[String, StructField] = {
    val fieldMap = fields.map(field => field.name -> field).toMap
    if (SQLConf.get.caseSensitiveAnalysis) {
      fieldMap
    } else {
      CaseInsensitiveMap(fieldMap)
    }
  }

  case class PartitionBasedOperation(command: Command) extends RowLevelOperation {
    var configuredScan: InMemoryBatchScan = _

    override def requiredMetadataAttributes(): Array[NamedReference] = {
      Array(PARTITION_COLUMN_REF, INDEX_COLUMN_REF)
    }

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new InMemoryScanBuilder(schema(), options) {
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
        override def build(): Write = new Write with RequiresDistributionAndOrdering {
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

    override def description(): String = "InMemoryPartitionReplaceOperation"
  }

  abstract class RowLevelOperationBatchWrite extends TestBatchWrite {

    override def commit(messages: Array[WriterCommitMessage],
                                            metrics: util.Map[String, lang.Long]): Unit = {
      metrics.asScala.map {
        case (key, value) => commitProperties += key -> String.valueOf(value)
      }
      commit(messages)
      commits += Commit(Instant.now().toEpochMilli, commitProperties.toMap)
      commitProperties.clear()
    }
  }

  private case class PartitionBasedReplaceData(scan: InMemoryBatchScan)
    extends RowLevelOperationBatchWrite {

    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      val readRows = scan.data.flatMap(_.asInstanceOf[BufferedRows].rows)
      val readPartitions = readRows.map(r => getKey(r, schema)).distinct
      dataMap --= readPartitions
      replacedPartitions = readPartitions
      withData(newData, schema)
      lastWriteLog = newData.flatMap(buffer => buffer.log).toImmutableArraySeq
    }
  }

  case class DeltaBasedOperation(command: Command) extends RowLevelOperation with SupportsDelta {
    private final val PK_COLUMN_REF = FieldReference("pk")

    override def requiredMetadataAttributes(): Array[NamedReference] = {
      Array(PARTITION_COLUMN_REF, INDEX_COLUMN_REF)
    }

    override def rowId(): Array[NamedReference] = Array(PK_COLUMN_REF)

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new InMemoryScanBuilder(schema(), options)
    }

    override def newWriteBuilder(info: LogicalWriteInfo): DeltaWriteBuilder = {
      lastWriteInfo = info
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

          override def toBatch: DeltaBatchWrite = {
            TestDeltaBatchWrite
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
      DeltaBufferedRowsWriterFactory
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      val newData = messages.map(_.asInstanceOf[BufferedRows])
      withDeletes(newData)
      withData(newData)
      lastWriteLog = newData.flatMap(buffer => buffer.log).toIndexedSeq
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

  private final val DELETE = UTF8String.fromString(Delete.toString)
  private final val UPDATE = UTF8String.fromString(Update.toString)
  private final val REINSERT = UTF8String.fromString(Reinsert.toString)
  private final val INSERT = UTF8String.fromString(Insert.toString)

  override def delete(meta: InternalRow, id: InternalRow): Unit = {
    val pk = id.getInt(0)
    buffer.deletes += pk
    val logEntry = new GenericInternalRow(Array[Any](DELETE, pk, meta.copy(), null))
    buffer.log += logEntry
  }

  override def update(meta: InternalRow, id: InternalRow, row: InternalRow): Unit = {
    val pk = id.getInt(0)
    buffer.deletes += pk
    buffer.rows.append(row.copy())
    val logEntry = new GenericInternalRow(Array[Any](UPDATE, pk, meta.copy(), row.copy()))
    buffer.log += logEntry
  }

  override def reinsert(meta: InternalRow, row: InternalRow): Unit = {
    buffer.rows.append(row.copy())
    val logEntry = new GenericInternalRow(Array[Any](REINSERT, null, meta.copy(), row.copy()))
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
