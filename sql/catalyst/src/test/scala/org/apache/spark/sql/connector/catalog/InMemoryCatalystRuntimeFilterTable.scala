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

import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{NamedReference, SortOrder, Transform}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder}
import org.apache.spark.sql.internal.SQLConf
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
    constraints: Array[Constraint] = Array.empty,
    distribution: Distribution = Distributions.unspecified(),
    ordering: Array[SortOrder] = Array.empty,
    numPartitions: Option[Int] = None,
    advisoryPartitionSize: Option[Long] = None,
    isDistributionStrictlyRequired: Boolean = true,
    numRowsPerSplit: Int = Int.MaxValue)
  extends InMemoryTableWithV2Filter(name, columns, partitioning, properties, constraints,
    distribution, ordering, numPartitions, advisoryPartitionSize, isDistributionStrictlyRequired,
    numRowsPerSplit) {

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

    /** Partition source columns that are present in the scan read schema. */
    private def partitionAttrs: Array[NamedReference] = {
      partitioning.flatMap(_.references()).distinct
        .filter(ref => readSchema.findNestedField(
          ref.fieldNames.toImmutableArraySeq, resolver = SQLConf.get.resolver).isDefined)
    }

    override def filterAttributes(): Array[NamedReference] = {
      partitionAttrs.filter { ref =>
        restrictedFilterAttrs.forall(_.contains(ref.fieldNames.mkString(".")))
      }
    }

    // Not intersected with `filterAttributes()`, so a table can declare a fully pushed attribute
    // that is not a filter attribute, a combination the interface forbids.
    override def fullyPushedFilterAttributes(): Array[NamedReference] = {
      partitionAttrs.filter(ref => fullyPushedFilterAttrs.contains(ref.fieldNames.mkString(".")))
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
