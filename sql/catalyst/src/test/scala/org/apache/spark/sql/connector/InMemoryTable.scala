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

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{IdentityTransform, Transform}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.{And, EqualTo, Filter, IsNotNull}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A simple in-memory table. Rows are stored as a buffered group produced by each output task.
 */
class InMemoryTable(
    val name: String,
    val schema: StructType,
    override val partitioning: Array[Transform],
    override val properties: util.Map[String, String])
  extends Table with SupportsRead with SupportsWrite with SupportsDelete {

  partitioning.foreach { t =>
    if (!t.isInstanceOf[IdentityTransform]) {
      throw new IllegalArgumentException(s"Transform $t must be IdentityTransform")
    }
  }

  // The key `Seq[Any]` is the partition values.
  val dataMap: mutable.Map[Seq[Any], BufferedRows] = mutable.Map.empty

  def data: Array[BufferedRows] = dataMap.values.toArray

  def rows: Seq[InternalRow] = dataMap.values.flatMap(_.rows).toSeq

  private val partFieldNames = partitioning.flatMap(_.references).toSeq.flatMap(_.fieldNames)
  private val partIndexes = partFieldNames.map(schema.fieldIndex)

  private def getKey(row: InternalRow): Seq[Any] = partIndexes.map(row.toSeq(schema)(_))

  def withData(data: Array[BufferedRows]): InMemoryTable = dataMap.synchronized {
    data.foreach(_.rows.foreach { row =>
      val key = getKey(row)
      dataMap += dataMap.get(key)
        .map(key -> _.withRow(row))
        .getOrElse(key -> new BufferedRows().withRow(row))
    })
    this
  }

  override def capabilities: util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.OVERWRITE_DYNAMIC,
    TableCapability.TRUNCATE).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    () => new InMemoryBatchScan(data.map(_.asInstanceOf[InputPartition]))
  }

  class InMemoryBatchScan(data: Array[InputPartition]) extends Scan with Batch {
    override def readSchema(): StructType = schema

    override def toBatch: Batch = this

    override def planInputPartitions(): Array[InputPartition] = data

    override def createReaderFactory(): PartitionReaderFactory = BufferedRowsReaderFactory
  }

  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = {
    InMemoryTable.maybeSimulateFailedTableWrite(options)

    new WriteBuilder with SupportsTruncate with SupportsOverwrite with SupportsDynamicOverwrite {
      private var writer: BatchWrite = Append

      override def truncate(): WriteBuilder = {
        assert(writer == Append)
        writer = TruncateAndAppend
        this
      }

      override def overwrite(filters: Array[Filter]): WriteBuilder = {
        assert(writer == Append)
        writer = new Overwrite(filters)
        this
      }

      override def overwriteDynamicPartitions(): WriteBuilder = {
        assert(writer == Append)
        writer = DynamicOverwrite
        this
      }

      override def buildForBatch(): BatchWrite = writer
    }
  }

  private abstract class TestBatchWrite extends BatchWrite {
    override def createBatchWriterFactory(): DataWriterFactory = {
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
    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val deleteKeys = InMemoryTable.filtersToKeys(dataMap.keys, partFieldNames, filters)
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

  override def deleteWhere(filters: Array[Filter]): Unit = dataMap.synchronized {
    dataMap --= InMemoryTable.filtersToKeys(dataMap.keys, partFieldNames, filters)
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
        case IsNotNull(attr) =>
          null != extractValue(attr, partitionNames, partValues)
        case f =>
          throw new IllegalArgumentException(s"Unsupported filter type: $f")
      }
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

class BufferedRows extends WriterCommitMessage with InputPartition with Serializable {
  val rows = new mutable.ArrayBuffer[InternalRow]()

  def withRow(row: InternalRow): BufferedRows = {
    rows.append(row)
    this
  }
}

private object BufferedRowsReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new BufferedRowsReader(partition.asInstanceOf[BufferedRows])
  }
}

private class BufferedRowsReader(partition: BufferedRows) extends PartitionReader[InternalRow] {
  private var index: Int = -1

  override def next(): Boolean = {
    index += 1
    index < partition.rows.length
  }

  override def get(): InternalRow = partition.rows(index)

  override def close(): Unit = {}
}

private object BufferedRowsWriterFactory extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new BufferWriter
  }
}

private class BufferWriter extends DataWriter[InternalRow] {
  private val buffer = new BufferedRows

  override def write(row: InternalRow): Unit = buffer.rows.append(row.copy())

  override def commit(): WriterCommitMessage = buffer

  override def abort(): Unit = {}
}
