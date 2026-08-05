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

import scala.collection.mutable.ArrayBuffer

import InMemoryCatalystRuntimeFilterTable._

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BindReferences, Expression, Predicate => CatalystPredicate}
import org.apache.spark.sql.connector.expressions.{NamedReference, Transform}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsRuntimeCatalystFiltering
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * In-memory table whose batch scan implements
 * [[SupportsRuntimeCatalystFiltering]], so runtime filters arrive as Catalyst
 * [[Expression]]s rather than connector predicates.
 *
 * Table properties:
 *  - `filter-attributes` (default: all partition cols): comma-separated list of
 *    column names to expose from `filterAttributes`.
 *  - `fully-pushed-filter-attributes` (default: none): comma-separated list of
 *    column names to expose from `fullyPushedFilterAttributes`.
 */
class InMemoryCatalystRuntimeFilterTable(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryTableWithV2Filter(name, columns, partitioning, properties) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryCatalystRuntimeFilterScanBuilder(schema, options)
  }

  class InMemoryCatalystRuntimeFilterScanBuilder(
      tableSchema: StructType,
      options: CaseInsensitiveStringMap)
    extends InMemoryScanBuilder(tableSchema, options) {
    override def build: Scan = InMemoryCatalystRuntimeFilterBatchScan(
      data.map(_.asInstanceOf[InputPartition]).toImmutableArraySeq,
      schema, tableSchema, options)
  }

  /**
   * Scan that receives runtime filters as Catalyst expressions.
   * Records what was pushed and evaluates expressions that reference only
   * partition columns against each partition key, so fully-pushed predicates
   * are enforced when Spark drops the post-scan [[org.apache.spark.sql.execution.FilterExec]].
   */
  case class InMemoryCatalystRuntimeFilterBatchScan(
      var _data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType,
      options: CaseInsensitiveStringMap)
    extends BatchScanBaseClass(_data, readSchema, tableSchema)
    with SupportsRuntimeCatalystFiltering {

    private val _catalystPredicates = ArrayBuffer.empty[Expression]

    private val restrictedFilterAttrs: Option[Set[String]] =
      Option(InMemoryCatalystRuntimeFilterTable.this.properties.get(FilterAttributesKey))
        .map(_.split(",").map(_.trim).toSet)

    override def filterAttributes(): Array[NamedReference] = {
      val scanFields = readSchema.fields.map(_.name).toSet
      partitioning.flatMap(_.references()).filter { ref =>
        val name = ref.fieldNames.mkString(".")
        scanFields.contains(name) &&
          restrictedFilterAttrs.forall(_.contains(name))
      }
    }

    override def fullyPushedFilterAttributes(): Array[NamedReference] = {
      val fullyPushedFilterAttrs = Option(
        InMemoryCatalystRuntimeFilterTable.this.properties.get(FullyPushedFilterAttributesKey))
        .map(_.split(",").map(_.trim).toSet)
        .getOrElse(Set.empty)
      filterAttributes().filter { ref =>
        fullyPushedFilterAttrs.contains(ref.fieldNames.mkString("."))
      }
    }

    override def filter(expressions: Array[Expression]): Unit = {
      _catalystPredicates ++= expressions
      val partAttrs = partitionAttributes
      if (partAttrs.isEmpty) return

      val resolver = SQLConf.get.resolver
      expressions.foreach { expr =>
        val remapped = expr.transform {
          case a: AttributeReference =>
            partAttrs.find(p => resolver(p.name, a.name)).getOrElse(a)
        }
        // Only evaluate expressions whose refs are all partition columns, so we can bind
        // against the partition key InternalRow (same approach as PartitionPredicateImpl).
        if (remapped.references.forall(r => partAttrs.exists(_.exprId == r.exprId))) {
          val bound = BindReferences.bindReference(remapped, partAttrs)
          val pred = CatalystPredicate.createInterpreted(bound)
          data = data.filter { p =>
            try {
              pred.eval(p.asInstanceOf[BufferedRows].partitionKey())
            } catch {
              // Keep the partition on eval failure, matching PartitionPredicateImpl.
              case _: Exception => true
            }
          }
        }
      }
    }

    /** Predicates recorded by [[filter]], for test assertions only. */
    def pushedCatalystPredicates: Seq[Expression] = _catalystPredicates.toSeq

    /** AttributeReferences matching the partition-key InternalRow field order. */
    private def partitionAttributes: Seq[AttributeReference] = {
      partitioning.flatMap(_.references()).flatMap { ref =>
        val name = ref.fieldNames.mkString(".")
        readSchema.find(_.name == name).orElse(tableSchema.find(_.name == name)).map { f =>
          AttributeReference(f.name, f.dataType, f.nullable)()
        }
      }.toSeq
    }
  }
}

object InMemoryCatalystRuntimeFilterTable {
  /**
   * Table property: comma-separated column names to expose from
   * filterAttributes. Default: all partition columns.
   */
  private[catalog] val FilterAttributesKey = "filter-attributes"

  /**
   * Table property: comma-separated column names to expose from
   * fullyPushedFilterAttributes. Default: none.
   */
  private[catalog] val FullyPushedFilterAttributesKey = "fully-pushed-filter-attributes"
}
