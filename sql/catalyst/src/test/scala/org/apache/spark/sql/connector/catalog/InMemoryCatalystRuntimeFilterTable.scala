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

import InMemoryCatalystRuntimeFilterTable._

import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference, Transform}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * In-memory table whose batch scan mixes in [[CatalystRuntimeFilteringScan]], so runtime filters
 * arrive as Catalyst expressions rather than connector predicates.
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
    properties: util.Map[String, String],
    numRowsPerSplit: Int = Int.MaxValue)
  extends InMemoryTableWithV2Filter(name, columns, partitioning, properties, numRowsPerSplit) {

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
   * Scan that receives runtime filters as Catalyst expressions. Pruning comes from
   * [[CatalystRuntimeFilteringScan]], so fully-pushed predicates are enforced when Spark drops
   * the post-scan [[org.apache.spark.sql.execution.FilterExec]].
   */
  case class InMemoryCatalystRuntimeFilterBatchScan(
      var _data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType,
      options: CaseInsensitiveStringMap)
    extends BatchScanBaseClass(_data, readSchema, tableSchema)
    with CatalystRuntimeFilteringScan {

    private val restrictedFilterAttrs: Option[Set[String]] =
      Option(InMemoryCatalystRuntimeFilterTable.this.properties.get(FilterAttributesKey))
        .map(_.split(",").map(_.trim).toSet)

    private val fullyPushedFilterAttrs: Set[String] = Option(
      InMemoryCatalystRuntimeFilterTable.this.properties.get(FullyPushedFilterAttributesKey))
      .map(_.split(",").map(_.trim).toSet)
      .getOrElse(Set.empty)

    /**
     * The partition columns, each named by the top level read schema column it lives under, the
     * form both interface methods require. Columns pruned out of the read schema are dropped,
     * since neither method may name one. Examples:
     *   - `PARTITIONED BY (part)` -> `"part"`
     *   - `PARTITIONED BY (s.nested)` -> `"s"`, the struct column holding the partition field
     */
    private def partitionAttrNames: Array[String] = {
      val scanFields = readSchema.fields.map(_.name).toSet
      partitioning.flatMap(_.references()).map(_.fieldNames.head).distinct
        .filter(scanFields.contains)
    }

    override def filterAttributes(): Array[NamedReference] = {
      partitionAttrNames
        .filter(name => restrictedFilterAttrs.forall(_.contains(name)))
        .map(FieldReference.column)
    }

    // Not intersected with `filterAttributes()`, so a table can declare a fully pushed attribute
    // that is not a filter attribute, a combination the interface forbids.
    override def fullyPushedFilterAttributes(): Array[NamedReference] = {
      partitionAttrNames.filter(fullyPushedFilterAttrs.contains).map(FieldReference.column)
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
