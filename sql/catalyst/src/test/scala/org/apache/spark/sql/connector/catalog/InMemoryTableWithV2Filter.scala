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

import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue, NamedReference, Transform}
import org.apache.spark.sql.connector.expressions.filter.{And, Predicate}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsRuntimeV2Filtering}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsOverwriteV2, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

class InMemoryTableWithV2Filter(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryBaseTable(name, columns, partitioning, properties) with SupportsDeleteV2 {

  override def canDeleteWhere(predicates: Array[Predicate]): Boolean = {
    InMemoryTableWithV2Filter.supportsPredicates(predicates)
  }

  override def deleteWhere(filters: Array[Predicate]): Unit = dataMap.synchronized {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    dataMap --= InMemoryTableWithV2Filter
      .filtersToKeys(dataMap.keys, partCols.map(_.toSeq.quoted).toImmutableArraySeq, filters)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryV2FilterScanBuilder(schema, options)
  }

  class InMemoryV2FilterScanBuilder(
     tableSchema: StructType,
     options: CaseInsensitiveStringMap)
    extends InMemoryScanBuilder(tableSchema, options) {
    override def build: Scan = InMemoryV2FilterBatchScan(
      data.map(_.asInstanceOf[InputPartition]).toImmutableArraySeq, schema, tableSchema, options)
  }

  case class InMemoryV2FilterBatchScan(
      var _data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType,
      options: CaseInsensitiveStringMap)
    extends BatchScanBaseClass(_data, readSchema, tableSchema) with SupportsRuntimeV2Filtering {

    override def filterAttributes(): Array[NamedReference] = {
      val scanFields = readSchema.fields.map(_.name).toSet
      partitioning.flatMap(_.references)
        .filter(ref => scanFields.contains(ref.fieldNames.mkString(".")))
    }

    override def filter(filters: Array[Predicate]): Unit = {
      if (partitioning.length == 1 && partitioning.head.references().length == 1) {
        val ref = partitioning.head.references().head
        filters.foreach {
          case p : Predicate if p.name().equals("IN") =>
            if (p.children().length > 1) {
              val filterRef = p.children()(0).asInstanceOf[FieldReference].references.head
              if (filterRef.toString.equals(ref.toString)) {
                val matchingKeys =
                  p.children().drop(1).map(_.asInstanceOf[LiteralValue[_]].value.toString).toSet
                data = data.filter(partition => {
                  val key = partition.asInstanceOf[BufferedRows].keyString()
                  matchingKeys.contains(key)
                })
              }
            }
          case p : Predicate if p.name().equals("=") =>
            if (p.children().length == 2) {
              val filterRef = p.children()(0).asInstanceOf[FieldReference].references.head
              if (filterRef.toString.equals(ref.toString)) {
                val matchingKey = p.children()(1).asInstanceOf[LiteralValue[_]].value
                if (matchingKey != null) {
                  data = data.filter(partition => {
                    val key = partition.asInstanceOf[BufferedRows].keyString()
                    key == matchingKey.toString
                  })
                } else {
                  data = Seq.empty // NULL = anything is always false
                }
              }
            }
          case _ => // Ignore unsupported predicate types
        }
      }
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    InMemoryBaseTable.maybeSimulateFailedTableWrite(new CaseInsensitiveStringMap(properties))
    InMemoryBaseTable.maybeSimulateFailedTableWrite(info.options)

    new InMemoryWriterBuilderWithOverWrite(info)
  }

  class InMemoryWriterBuilderWithOverWrite(override val info: LogicalWriteInfo)
    extends InMemoryWriterBuilder(info) with SupportsOverwriteV2 {

    override def truncate(): WriteBuilder = {
      assert(writer.isInstanceOf[Append])
      writer = new TruncateAndAppend(info)
      streamingWriter = new StreamingTruncateAndAppend(info)
      this
    }

    override def overwrite(predicates: Array[Predicate]): WriteBuilder = {
      assert(writer.isInstanceOf[Append])
      writer = new Overwrite(predicates)
      streamingWriter = new StreamingNotSupportedOperation(
        s"overwrite (${predicates.mkString("filters(", ", ", ")")})")
      this
    }

    override def canOverwrite(predicates: Array[Predicate]): Boolean = {
      InMemoryTableWithV2Filter.supportsPredicates(predicates)
    }
  }

  private class Overwrite(predicates: Array[Predicate]) extends TestBatchWrite {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    override def commit(messages: Array[WriterCommitMessage]): Unit = dataMap.synchronized {
      val deleteKeys = InMemoryTableWithV2Filter.filtersToKeys(
        dataMap.keys, partCols.map(_.toSeq.quoted).toImmutableArraySeq, predicates)
      dataMap --= deleteKeys
      withData(messages.map(_.asInstanceOf[BufferedRows]))
    }
  }
}

object InMemoryTableWithV2Filter {

  def filtersToKeys(
      keys: Iterable[Seq[Any]],
      partitionNames: Seq[String],
      filters: Array[Predicate]): Iterable[Seq[Any]] = {
    keys.filter { partValues =>
      val resolve: String => Any = attr =>
        InMemoryBaseTable.extractValue(attr, partitionNames, partValues)
      filters.flatMap(splitAnd).forall(evalPredicate(_, resolve))
    }
  }

  /**
   * Evaluates a single V2 predicate by resolving column values through the
   * given function. Supports =, <=>, IS_NULL, IS_NOT_NULL, and ALWAYS_TRUE.
   */
  def evalPredicate(
      pred: Predicate,
      resolveValue: String => Any): Boolean = {
    lazy val attr = pred.children()(0).toString
    pred.name() match {
      case "=" =>
        resolveValue(attr) ==
          pred.children()(1).asInstanceOf[LiteralValue[_]].value
      case "<=>" =>
        val attrVal = resolveValue(attr)
        val litVal =
          pred.children()(1).asInstanceOf[LiteralValue[_]].value
        (attrVal == null && litVal == null) ||
          (attrVal != null && litVal != null && attrVal == litVal)
      case "IS_NULL" => resolveValue(attr) == null
      case "IS_NOT_NULL" => resolveValue(attr) != null
      case "ALWAYS_TRUE" => true
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported filter type: $other")
    }
  }

  def supportsPredicates(predicates: Array[Predicate]): Boolean = {
    predicates.flatMap(splitAnd).forall {
      case p: Predicate if p.name().equals("=") => true
      case p: Predicate if p.name().equals("<=>") => true
      case p: Predicate if p.name().equals("IS_NULL") => true
      case p: Predicate if p.name().equals("IS_NOT_NULL") => true
      case p: Predicate if p.name().equals("ALWAYS_TRUE") => true
      case _ => false
    }
  }

  private def splitAnd(filter: Predicate): Seq[Predicate] = {
    filter match {
      case and: And => splitAnd(and.left()) ++ splitAnd(and.right())
      case _ => filter :: Nil
    }
  }
}
