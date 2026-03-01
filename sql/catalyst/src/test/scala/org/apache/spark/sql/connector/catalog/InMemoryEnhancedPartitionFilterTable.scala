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
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate
import org.apache.spark.sql.connector.expressions.filter.Predicate
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

    override def supportsEnhancedPartitionFiltering(): Boolean = true

    override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
      val partNames = InMemoryEnhancedPartitionFilterTable.this.partCols.flatMap(_.toSeq).toSet
      def referencesOnlyPartitionCols(p: Predicate): Boolean =
        p.references().forall(ref => partNames.contains(ref.fieldNames().mkString(".")))

      val returned = ArrayBuffer.empty[Predicate]

      predicates.foreach {
        case p: PartitionPredicate =>
          partitionPredicates += p
        case p if referencesOnlyPartitionCols(p) &&
            InMemoryTableWithV2Filter.supportsPredicates(Array(p)) =>
          firstPassPushedPredicates += p
        case p =>
          returned += p
      }

      if (partitionPredicates.nonEmpty) Array.empty[Predicate]
      else returned.toArray
    }

    override def pushedPredicates(): Array[Predicate] =
      (firstPassPushedPredicates ++ partitionPredicates.map(p => p: Predicate)).toArray

    override def pruneColumns(requiredSchema: StructType): Unit = {
      readSchema = requiredSchema
    }

    override def build(): Scan = {
      val allPartitions = data.map(_.asInstanceOf[InputPartition]).toImmutableArraySeq
      val filteredByFirstPass = if (firstPassPushedPredicates.isEmpty) {
        allPartitions
      } else {
        val partNames =
          InMemoryEnhancedPartitionFilterTable.this.partCols.map(_.toSeq.quoted)
            .toImmutableArraySeq
        val allKeys = allPartitions.map(_.asInstanceOf[BufferedRows].key)
        val matchingKeys = InMemoryTableWithV2Filter.filtersToKeys(
          allKeys, partNames, firstPassPushedPredicates.toArray).toSet
        allPartitions.filter(p =>
          matchingKeys.contains(p.asInstanceOf[BufferedRows].key))
      }
      val filtered = filteredByFirstPass.filter { p =>
        val partRow = p.asInstanceOf[BufferedRows].partitionKey()
        partitionPredicates.forall(_.accept(partRow))
      }
      InMemoryEnhancedPartitionFilterBatchScan(filtered, readSchema, tableSchema)
    }
  }

  case class InMemoryEnhancedPartitionFilterBatchScan(
      _data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType)
    extends BatchScanBaseClass(_data, readSchema, tableSchema)
}
