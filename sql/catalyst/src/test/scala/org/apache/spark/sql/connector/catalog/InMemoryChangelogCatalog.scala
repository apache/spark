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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.ChangelogRange.{TimestampRange, UnboundedRange, VersionRange}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An [[InMemoryTableCatalog]] that implements [[TableCatalog.loadChangelog()]].
 *
 * Change rows can be pre-populated via [[addChangeRows()]] before querying.
 */
class InMemoryChangelogCatalog extends InMemoryCatalog {

  // tableName -> list of change rows (each row: Array[Any] matching changelog schema)
  private val changeData: mutable.Map[String, mutable.ArrayBuffer[InternalRow]] =
    mutable.Map.empty

  // Stores the most recent ChangelogInfo passed to loadChangelog(), so tests can verify
  // that the parser/DataFrame API correctly constructed and forwarded it.
  private var _lastChangelogInfo: Option[ChangelogInfo] = None
  def lastChangelogInfo: Option[ChangelogInfo] = _lastChangelogInfo

  override def loadChangelog(
      ident: Identifier,
      changelogInfo: ChangelogInfo): Changelog = {
    _lastChangelogInfo = Some(changelogInfo)
    if (!tableExists(ident)) {
      throw new NoSuchTableException(ident.asMultipartIdentifier)
    }
    val table = loadTable(ident)
    val allRows = changeData.getOrElse(
      ident.toString, mutable.ArrayBuffer.empty)
    val numDataCols = table.columns.length
    // _commit_version is at index numDataCols + 1 (after _change_type)
    val commitVersionIdx = numDataCols + 1
    val filtered = filterByRange(allRows.toSeq, commitVersionIdx, changelogInfo.range())
    new InMemoryChangelog(
      table.name + "_changelog", table.columns, filtered)
  }

  /**
   * Filter rows by the requested [[ChangelogRange]]. For [[VersionRange]], compares
   * the `_commit_version` (Long) against the start/end versions (parsed as Long).
   * For [[UnboundedRange]], returns all rows.
   */
  private def filterByRange(
      rows: Seq[InternalRow],
      commitVersionIdx: Int,
      range: ChangelogRange): Seq[InternalRow] = range match {
    case vr: VersionRange =>
      val startVer = vr.startingVersion().toLong
      val startInc = vr.startingBoundInclusive()
      val endVerOpt = if (vr.endingVersion().isPresent) {
        Some(vr.endingVersion().get().toLong)
      } else None
      val endInc = vr.endingBoundInclusive()
      rows.filter { row =>
        val ver = row.getLong(commitVersionIdx)
        val aboveStart = if (startInc) ver >= startVer else ver > startVer
        val belowEnd = endVerOpt match {
          case Some(endVer) => if (endInc) ver <= endVer else ver < endVer
          case None => true
        }
        aboveStart && belowEnd
      }
    case _: TimestampRange =>
      // Timestamp filtering not implemented in test catalog
      rows
    case _: UnboundedRange =>
      rows
  }

  /**
   * Add change rows for a table. Each row should match the changelog schema:
   * (data columns..., _change_type STRING, _commit_version LONG, _commit_timestamp LONG).
   */
  def addChangeRows(ident: Identifier, rows: Seq[InternalRow]): Unit = {
    val buf = changeData.getOrElseUpdate(
      ident.toString, mutable.ArrayBuffer.empty)
    buf ++= rows
  }

  def clearChangeRows(ident: Identifier): Unit = {
    changeData.remove(ident.toString)
  }
}

/**
 * A test [[Changelog]] that returns pre-populated change rows.
 *
 * Reports `containsCarryoverRows = false` so Spark skips carry-over removal.
 */
class InMemoryChangelog(
    tableName: String,
    dataColumns: Array[Column],
    changeRows: Seq[InternalRow]) extends Changelog {

  private val cdcColumns: Array[Column] = dataColumns ++ Array(
    Column.create("_change_type", StringType),
    Column.create("_commit_version", LongType),
    Column.create("_commit_timestamp", TimestampType))

  override def name(): String = tableName

  override def columns(): Array[Column] = cdcColumns

  override def containsCarryoverRows(): Boolean = false

  override def containsIntermediateChanges(): Boolean = false

  override def representsUpdateAsDeleteAndInsert(): Boolean = false

  override def newScanBuilder(
      options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryChangelogScanBuilder(readSchema, changeRows)
  }

  def readSchema: StructType = {
    CatalogV2Util.v2ColumnsToStructType(cdcColumns)
  }
}

private class InMemoryChangelogScanBuilder(
    schema: StructType,
    rows: Seq[InternalRow]) extends ScanBuilder {
  override def build(): Scan =
    new InMemoryChangelogScan(schema, rows)
}

private class InMemoryChangelogScan(
    schema: StructType,
    rows: Seq[InternalRow]) extends Scan with Batch {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new InMemoryChangelogMicroBatchStream(schema, rows)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    Array(InMemoryChangelogPartition(rows))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new InMemoryChangelogReaderFactory()
  }
}

private case class InMemoryChangelogPartition(
    rows: Seq[InternalRow]) extends InputPartition

private class InMemoryChangelogReaderFactory
    extends PartitionReaderFactory {
  override def createReader(
      partition: InputPartition): PartitionReader[InternalRow] = {
    new InMemoryChangelogReader(
      partition.asInstanceOf[InMemoryChangelogPartition])
  }
}

private class InMemoryChangelogReader(
    partition: InMemoryChangelogPartition)
    extends PartitionReader[InternalRow] {

  private var index = -1
  private val rows = partition.rows

  override def next(): Boolean = {
    index += 1
    index < rows.size
  }

  override def get(): InternalRow = rows(index)

  override def close(): Unit = {}
}

/**
 * A simple offset for [[InMemoryChangelogMicroBatchStream]].
 * The offset value represents the number of rows consumed so far.
 */
private class InMemoryChangelogOffset(val offset: Long) extends Offset {
  override def json(): String = offset.toString
}

/**
 * A [[MicroBatchStream]] that serves pre-populated change rows in a single batch.
 */
private class InMemoryChangelogMicroBatchStream(
    schema: StructType,
    rows: Seq[InternalRow]) extends MicroBatchStream {

  override def initialOffset(): Offset = new InMemoryChangelogOffset(-1)

  override def latestOffset(): Offset = new InMemoryChangelogOffset(rows.size - 1)

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startIdx = start.asInstanceOf[InMemoryChangelogOffset].offset.toInt + 1
    val endIdx = end.asInstanceOf[InMemoryChangelogOffset].offset.toInt + 1
    val slice = rows.slice(startIdx, endIdx)
    Array(InMemoryChangelogPartition(slice))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new InMemoryChangelogReaderFactory()
  }

  override def deserializeOffset(json: String): Offset = {
    new InMemoryChangelogOffset(json.toLong)
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {}
}
