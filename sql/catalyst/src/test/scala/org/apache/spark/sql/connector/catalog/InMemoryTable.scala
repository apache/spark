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

import org.scalatest.Assertions.assert

import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{SortOrder, Transform}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsOverwrite, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A simple in-memory table. Rows are stored as a buffered group produced by each output task.
 */
class InMemoryTable(
    name: String,
    schema: StructType,
    override val partitioning: Array[Transform],
    override val properties: util.Map[String, String],
    distribution: Distribution = Distributions.unspecified(),
    ordering: Array[SortOrder] = Array.empty,
    numPartitions: Option[Int] = None,
    isDistributionStrictlyRequired: Boolean = true)
  extends InMemoryBaseTable(name, schema, partitioning, properties, distribution,
    ordering, numPartitions, isDistributionStrictlyRequired) with SupportsDelete {

  override def canDeleteWhere(filters: Array[Filter]): Boolean = {
    InMemoryTable.supportsFilters(filters)
  }

  override def deleteWhere(filters: Array[Filter]): Unit = dataMap.synchronized {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    dataMap --= InMemoryTable.filtersToKeys(dataMap.keys, partCols.map(_.toSeq.quoted), filters)
  }

  override def withData(data: Array[BufferedRows]): InMemoryTable = {
    withData(data, schema)
  }

  override def withData(
      data: Array[BufferedRows],
      writeSchema: StructType): InMemoryTable = dataMap.synchronized {
    data.foreach(_.rows.foreach { row =>
      val key = getKey(row, writeSchema)
      dataMap += dataMap.get(key)
        .map(key -> _.withRow(row))
        .getOrElse(key -> new BufferedRows(key).withRow(row))
      addPartitionKey(key)
    })
    this
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    InMemoryBaseTable.maybeSimulateFailedTableWrite(new CaseInsensitiveStringMap(properties))
    InMemoryBaseTable.maybeSimulateFailedTableWrite(info.options)

    new InMemoryWriterBuilderWithOverWrite()
  }

  private class InMemoryWriterBuilderWithOverWrite() extends InMemoryWriterBuilder
    with SupportsOverwrite {

    override def truncate(): WriteBuilder = {
      assert(writer == Append)
      writer = TruncateAndAppend
      streamingWriter = StreamingTruncateAndAppend
      this
    }

    override def overwrite(filters: Array[Filter]): WriteBuilder = {
      assert(writer == Append)
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
        dataMap.keys, partCols.map(_.toSeq.quoted), filters)
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

  private def splitAnd(filter: Filter): Seq[Filter] = {
    filter match {
      case And(left, right) => splitAnd(left) ++ splitAnd(right)
      case _ => filter :: Nil
    }
  }
}
