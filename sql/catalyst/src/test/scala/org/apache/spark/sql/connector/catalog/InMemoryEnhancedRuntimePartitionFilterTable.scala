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

import InMemoryEnhancedRuntimePartitionFilterTable._

import org.apache.spark.sql.connector.expressions.{NamedReference, Transform}
import org.apache.spark.sql.connector.expressions.filter.{PartitionPredicate, Predicate}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsRuntimeV2Filtering}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * In-memory table whose batch scan implements [[SupportsRuntimeV2Filtering]] with
 * iterative filtering support, so that [[PartitionPredicate]] instances derived from
 * runtime filters are pushed via a second [[SupportsRuntimeV2Filtering#filter]] call.
 *
 * Table properties:
 *  - `accept-v2-predicates` (default `false`): when true, non-PartitionPredicate
 *    V2 predicates are reported via `pushedPredicates()` (i.e. accepted).
 *  - `filter-attributes` (default: all partition cols): comma-separated list of
 *    column names to expose from `filterAttributes()`.
 */
class InMemoryEnhancedRuntimePartitionFilterTable(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryTableWithV2Filter(name, columns, partitioning, properties) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryEnhancedRuntimePartitionFilterScanBuilder(schema, options)
  }

  class InMemoryEnhancedRuntimePartitionFilterScanBuilder(
      tableSchema: StructType,
      options: CaseInsensitiveStringMap)
    extends InMemoryScanBuilder(tableSchema, options) {
    override def build: Scan = InMemoryEnhancedRuntimePartitionFilterBatchScan(
      data.map(_.asInstanceOf[InputPartition]).toImmutableArraySeq,
      schema, tableSchema, options)
  }

  case class InMemoryEnhancedRuntimePartitionFilterBatchScan(
      var _data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType,
      options: CaseInsensitiveStringMap)
    extends BatchScanBaseClass(_data, readSchema, tableSchema)
    with SupportsRuntimeV2Filtering {

    private val _allPushedPredicates = ArrayBuffer.empty[Predicate]

    private val props = InMemoryEnhancedRuntimePartitionFilterTable.this.properties

    private val acceptV2Predicates =
      props.getOrDefault(AcceptV2PredicatesKey, "false").toBoolean

    private val restrictedFilterAttrs: Option[Set[String]] =
      Option(props.get(FilterAttributesKey)).map(_.split(",").map(_.trim).toSet)

    def pushedPartitionPredicates: Seq[PartitionPredicate] =
      _allPushedPredicates.collect { case pp: PartitionPredicate => pp }.toSeq

    override def pushedPredicates(): Array[Predicate] = _allPushedPredicates.toArray

    override def supportsIterativePushdown(): Boolean =
      props.getOrDefault(SupportsIterativePushdownKey, "true").toBoolean

    override def filterAttributes(): Array[NamedReference] = {
      val scanFields = readSchema.fields.map(_.name).toSet
      partitioning.flatMap(_.references()).filter { ref =>
        val name = ref.fieldNames.mkString(".")
        scanFields.contains(name) &&
          restrictedFilterAttrs.forall(_.contains(name))
      }
    }

    override def filter(filters: Array[Predicate]): Unit = filters.foreach {
      case pp: PartitionPredicate =>
        _allPushedPredicates += pp
        data = data.filter(p => pp.eval(p.asInstanceOf[BufferedRows].partitionKey()))
      case other =>
        if (acceptV2Predicates) _allPushedPredicates += other
    }
  }
}

object InMemoryEnhancedRuntimePartitionFilterTable {
  /**
   * Table property: when "true", non-PartitionPredicate V2 predicates
   * pushed via filter() are reported in pushedPredicates() (accepted).
   */
  private[catalog] val AcceptV2PredicatesKey = "accept-v2-predicates"

  /**
   * Table property: comma-separated column names to expose from
   * filterAttributes(). Default: all partition columns.
   */
  private[catalog] val FilterAttributesKey = "filter-attributes"

  /**
   * Table property: when "false", supportsIterativePushdown() returns false.
   * Default: "true".
   */
  private[catalog] val SupportsIterativePushdownKey = "supports-iterative-pushdown"
}
