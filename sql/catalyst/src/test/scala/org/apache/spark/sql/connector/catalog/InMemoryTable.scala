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

import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{SortOrder, Transform}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsOverwrite, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * A simple in-memory table. Rows are stored as a buffered group produced by each output task.
 */
class InMemoryTable(
    name: String,
    schema: StructType,
    override val partitioning: Array[Transform],
    override val properties: util.Map[String, String],
    override val constraints: Array[Constraint] = Array.empty,
    distribution: Distribution = Distributions.unspecified(),
    ordering: Array[SortOrder] = Array.empty,
    numPartitions: Option[Int] = None,
    advisoryPartitionSize: Option[Long] = None,
    isDistributionStrictlyRequired: Boolean = true,
    override val numRowsPerSplit: Int = Int.MaxValue)
  extends InMemoryBaseTable(name, schema, partitioning, properties, constraints, distribution,
    ordering, numPartitions, advisoryPartitionSize, isDistributionStrictlyRequired,
    numRowsPerSplit) with SupportsDelete {

  override def canDeleteWhere(filters: Array[Filter]): Boolean = {
    InMemoryTable.supportsFilters(filters)
  }

  override def deleteWhere(filters: Array[Filter]): Unit = dataMap.synchronized {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    dataMap --= InMemoryTable
      .filtersToKeys(dataMap.keys, partCols.map(_.toSeq.quoted).toImmutableArraySeq, filters)
    increaseCurrentVersion()
  }

  override def withData(data: Array[BufferedRows]): InMemoryTable = {
    withData(data, schema)
  }

  override def withData(
      data: Array[BufferedRows],
      writeSchema: StructType): InMemoryTable = {
    dataMap.synchronized {
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

      if (data.exists(_.rows.exists(row => row.numFields == 1 &&
          row.getInt(0) == InMemoryTable.uncommittableValue()))) {
        throw new IllegalArgumentException(s"Test only mock write failure")
      }
      increaseCurrentVersion()
      this
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    InMemoryBaseTable.maybeSimulateFailedTableWrite(new CaseInsensitiveStringMap(properties))
    InMemoryBaseTable.maybeSimulateFailedTableWrite(info.options)

    new InMemoryWriterBuilderWithOverWrite(info)
  }

  class InMemoryWriterBuilderWithOverWrite(override val info: LogicalWriteInfo)
    extends InMemoryWriterBuilder(info) with SupportsOverwrite {

    override def truncate(): WriteBuilder = {
      if (!writer.isInstanceOf[Append]) {
        throw new IllegalArgumentException(s"Unsupported writer type: $writer")
      }
      writer = new TruncateAndAppend(info)
      streamingWriter = new StreamingTruncateAndAppend(info)
      this
    }

    override def overwrite(filters: Array[Filter]): WriteBuilder = {
      if (!writer.isInstanceOf[Append]) {
        throw new IllegalArgumentException(s"Unsupported writer type: $writer")
      }
      writer = new Overwrite(filters)
      streamingWriter = new StreamingNotSupportedOperation(
        s"overwrite (${filters.mkString("filters(", ", ", ")")})")
      this
    }

    override def canOverwrite(filters: Array[Filter]): Boolean = {
      InMemoryTable.supportsFilters(filters)
    }
  }

  private class Overwrite(filters: Array[Filter]) extends TestBatchWrite {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val deleteKeys = InMemoryTable.filtersToKeys(
        dataMap.keys, partCols.map(_.toSeq.quoted).toImmutableArraySeq, filters)
      dataMap --= deleteKeys
      withData(messages.map(_.asInstanceOf[BufferedRows]))
    }
  }
}

object InMemoryTable {

  def filtersToKeys(
      keys: Iterable[Seq[Any]],
      partitionNames: Seq[String],
      filters: Array[Filter]): Iterable[Seq[Any]] = {
    keys.filter { partValues =>
      filters.flatMap(splitAnd).forall {
        case EqualTo(attr, value) =>
          value == InMemoryBaseTable.extractValue(attr, partitionNames, partValues)
        case EqualNullSafe(attr, value) =>
          val attrVal = InMemoryBaseTable.extractValue(attr, partitionNames, partValues)
          if (attrVal == null && value == null) {
            true
          } else if (attrVal == null || value == null) {
            false
          } else {
            value == attrVal
          }
        case IsNull(attr) =>
          null == InMemoryBaseTable.extractValue(attr, partitionNames, partValues)
        case IsNotNull(attr) =>
          null != InMemoryBaseTable.extractValue(attr, partitionNames, partValues)
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

  def uncommittableValue(): Int = Int.MaxValue / 2

  private def splitAnd(filter: Filter): Seq[Filter] = {
    filter match {
      case And(left, right) => splitAnd(left) ++ splitAnd(right)
      case _ => filter :: Nil
    }
  }
}
