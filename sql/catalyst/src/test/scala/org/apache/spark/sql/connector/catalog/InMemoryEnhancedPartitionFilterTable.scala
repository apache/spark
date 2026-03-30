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

import scala.collection.mutable.{ArrayBuffer, Buffer}

import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.expressions.filter.{PartitionPredicate, Predicate}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * In-memory table whose scan builder implements enhanced partition filtering using
 * PartitionPredicates pushed in a second pass.
 */
class InMemoryEnhancedPartitionFilterTable(
    name: String,
    columns: Array[Column],
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryTable(name, columns, partitioning, properties) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryEnhancedPartitionFilterScanBuilder(schema())
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    InMemoryBaseTable.maybeSimulateFailedTableWrite(new CaseInsensitiveStringMap(properties))
    InMemoryBaseTable.maybeSimulateFailedTableWrite(info.options)
    new InMemoryWriterBuilderWithOverWrite(info)
  }

  class InMemoryEnhancedPartitionFilterScanBuilder(
      tableSchema: StructType)
    extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownRequiredColumns {

    private var readSchema: StructType = tableSchema
    private val partitionPredicates: Buffer[PartitionPredicate] = ArrayBuffer.empty
    private val firstPassPushedPredicates: Buffer[Predicate] = ArrayBuffer.empty

    // Default true: accept (push down) partition predicates.
    // Set to false to return them for post-scan.
    private val acceptPartitionPredicates =
      InMemoryEnhancedPartitionFilterTable.this.properties.getOrDefault(
        InMemoryEnhancedPartitionFilterTable.AcceptPartitionPredicatesKey, "true")
        .toBoolean

    private val acceptDataPredicates =
      InMemoryEnhancedPartitionFilterTable.this.properties.getOrDefault(
        InMemoryEnhancedPartitionFilterTable.AcceptDataPredicatesKey, "false")
        .toBoolean

    override def supportsIterativePushdown(): Boolean = true

    override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
      val partPaths = InMemoryEnhancedPartitionFilterTable.this.partCols.map(_.mkString(".")).toSet
      def referencesOnlyPartitionCols(p: Predicate): Boolean =
        p.references().forall(ref => partPaths.contains(ref.fieldNames().mkString(".")))
      def referencesOnlyDataCols(p: Predicate): Boolean =
        p.references().forall(ref => !partPaths.contains(ref.fieldNames().mkString(".")))

      val returned = ArrayBuffer.empty[Predicate]

      predicates.foreach {
        case p: PartitionPredicate =>
          if (acceptPartitionPredicates) {
            partitionPredicates += p
          } else {
            returned += p
          }
        case p if referencesOnlyPartitionCols(p) &&
            InMemoryTableWithV2Filter.supportsPredicates(Array(p)) =>
          if (acceptPartitionPredicates) {
            firstPassPushedPredicates += p
          } else {
            returned += p
          }
        case p if acceptDataPredicates && referencesOnlyDataCols(p) =>
          // Accept: we are mocking a data source that can evaluate this data predicate
          firstPassPushedPredicates += p
        case p =>
          returned += p
      }

      returned.toArray
    }

    override def pushedPredicates(): Array[Predicate] =
      (firstPassPushedPredicates ++ partitionPredicates.map(p => p: Predicate)).toArray

    override def pruneColumns(requiredSchema: StructType): Unit = {
      readSchema = requiredSchema
    }

    override def build(): Scan = {
      val allPartitions = data.map(_.asInstanceOf[InputPartition]).toImmutableArraySeq
      val partNames =
        InMemoryEnhancedPartitionFilterTable.this.partCols.map(_.toSeq.quoted)
          .toImmutableArraySeq
      val partPathSet =
        InMemoryEnhancedPartitionFilterTable.this.partCols.map(_.mkString(".")).toSet
      // Only partition predicates can be used for partition key filtering (filtersToKeys).
      val firstPassPartitionPredicates = firstPassPushedPredicates.filter { p =>
        p.references().forall(ref => partPathSet.contains(ref.fieldNames().mkString(".")))
      }
      val allKeys = allPartitions.map(_.asInstanceOf[BufferedRows].key)
      val matchingKeys = InMemoryTableWithV2Filter.filtersToKeys(
        allKeys, partNames, firstPassPartitionPredicates.toArray).toSet
      val filteredByFirstPass = allPartitions.filter(p =>
        matchingKeys.contains(p.asInstanceOf[BufferedRows].key))
      val filteredBySecondPass = filteredByFirstPass.filter { p =>
        val partRow = p.asInstanceOf[BufferedRows].partitionKey()
        partitionPredicates.forall(_.eval(partRow))
      }
      InMemoryEnhancedPartitionFilterBatchScan(
        filteredBySecondPass, readSchema, tableSchema, partitionPredicates.toSeq)
    }

  }

  /**
   * Batch scan that stores pushed PartitionPredicates.
   */
  case class InMemoryEnhancedPartitionFilterBatchScan(
      _data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType,
      pushedPartitionPredicates: Seq[PartitionPredicate] = Seq.empty)
    extends BatchScanBaseClass(_data, readSchema, tableSchema) {

    def getPushedPartitionPredicates: Seq[PartitionPredicate] = pushedPartitionPredicates
  }
}

object InMemoryEnhancedPartitionFilterTable {
  /**
   * Table property: when "true", accept (do not return) all PartitionPredicates for pushdown.
   */
  private[catalog] val AcceptPartitionPredicatesKey = "accept-partition-predicates"

  /**
   * Table property: when "true", accept (do not return) data predicates for pushdown (we are
   * mocking a data source that can evaluate this particular data predicate).
   */
  private[catalog] val AcceptDataPredicatesKey = "accept-data-predicates"
}
