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

package org.apache.spark.sql.sources.v2

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{EqualNullSafe, Literal, Not}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.maintain.{Maintainer, MaintainerBuilder, SupportsDelete}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String


/**
 * A simple in-memory table. Rows are stored as a buffered group produced by each output task.
 */
private[v2] class InMemoryTable(
    val name: String,
    val schema: StructType,
    override val properties: util.Map[String, String])
    extends Table with SupportsRead with SupportsWrite with SupportsMaintenance {

  def this(
      name: String,
      schema: StructType,
      properties: util.Map[String, String],
      data: Array[BufferedRows]) = {
    this(name, schema, properties)
    replaceData(data)
  }

  def rows: Seq[InternalRow] = data.flatMap(_.rows)

  @volatile var data: Array[BufferedRows] = Array.empty

  def replaceData(buffers: Array[BufferedRows]): Unit = synchronized {
    data = buffers
  }

  override def capabilities: util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.TRUNCATE).asJava

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
    new WriteBuilder with SupportsTruncate {
      private var shouldTruncate: Boolean = false

      override def truncate(): WriteBuilder = {
        shouldTruncate = true
        this
      }

      override def buildForBatch(): BatchWrite = {
        if (shouldTruncate) TruncateAndAppend else Append
      }
    }
  }

  override def newMaintainerBuilder(options: CaseInsensitiveStringMap): MaintainerBuilder = {
    () => {
      Delete
    }
  }

  private object Delete extends Maintainer with SupportsDelete {

    override def delete(filters: Array[Filter]): Unit = {
      val filtered = data.map {
        rows =>
          val newRows = filter(rows.rows, filters)
          val newBufferedRows = new BufferedRows()
          newBufferedRows.rows.appendAll(newRows)
          newBufferedRows
      }.filter(_.rows.nonEmpty)
      replaceData(filtered)
    }
  }

  def filter(rows: mutable.ArrayBuffer[InternalRow],
      filters: Array[Filter]): Array[InternalRow] = {
    if (rows.isEmpty) {
      rows.toArray
    }
    val filterStr =
      filters.map {
        filter => filter.sql
      }.toList.mkString("AND")
    val sparkSession = SparkSession.getActiveSession.getOrElse(
      throw new RuntimeException("Could not get active sparkSession.")
    )
    val filterExpr = sparkSession.sessionState.sqlParser.parseExpression(filterStr)
    val antiFilter = Not(EqualNullSafe(filterExpr, Literal(true, BooleanType)))
    val rdd = sparkSession.sparkContext.parallelize(rows)

    sparkSession.internalCreateDataFrame(rdd, schema)
        .filter(Column(antiFilter)).collect().map {
      row =>
        val values = row.toSeq.map {
          case s: String => UTF8String.fromBytes(s.asInstanceOf[String].getBytes("UTF-8"))
          case other => other
        }
      InternalRow.fromSeq(values)
    }
  }

  private object TruncateAndAppend extends BatchWrite {
    override def createBatchWriterFactory(): DataWriterFactory = {
      BufferedRowsWriterFactory
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      replaceData(messages.map(_.asInstanceOf[BufferedRows]))
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {
    }
  }

  private object Append extends BatchWrite {
    override def createBatchWriterFactory(): DataWriterFactory = {
      BufferedRowsWriterFactory
    }

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      replaceData(data ++ messages.map(_.asInstanceOf[BufferedRows]))
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {
    }
  }
}

private class BufferedRows extends WriterCommitMessage with InputPartition with Serializable {
  val rows = new mutable.ArrayBuffer[InternalRow]()
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
