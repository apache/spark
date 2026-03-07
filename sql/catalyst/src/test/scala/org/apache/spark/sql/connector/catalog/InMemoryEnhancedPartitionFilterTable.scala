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
import org.apache.spark.sql.connector.expressions.filter.{And, PartitionPredicate, Predicate}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * Trait for test scans that expose pushed partition predicates (e.g. to verify
 * PartitionPredicate.referencedPartitionColumnOrdinals). Used by DataSourceV2 suites.
 */
trait TestPartitionPredicateScan {
  def getPushedPartitionPredicates: Seq[PartitionPredicate]
}

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

    private val rejectPartitionPredicates =
      InMemoryEnhancedPartitionFilterTable.this.properties.getOrDefault(
        InMemoryEnhancedPartitionFilterTable.RejectPartitionPredicatesKey, "false")
        .toBoolean

    override def supportsEnhancedPartitionFiltering(): Boolean = true

    override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
      val partNames = InMemoryEnhancedPartitionFilterTable.this.partCols.flatMap(_.toSeq).toSet
      def referencesOnlyPartitionCols(p: Predicate): Boolean =
        p.references().forall(ref => partNames.contains(ref.fieldNames().mkString(".")))

      val returned = ArrayBuffer.empty[Predicate]

      predicates.foreach {
        case p: PartitionPredicate =>
          if (rejectPartitionPredicates) {
            returned += p
          } else {
            partitionPredicates += p
          }
        case p if referencesOnlyPartitionCols(p) &&
            supportsPredicatesForFirstPass(Array(p)) =>
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
      val partNames =
        InMemoryEnhancedPartitionFilterTable.this.partCols.map(_.toSeq.quoted)
          .toImmutableArraySeq
      val allKeys = allPartitions.map(_.asInstanceOf[BufferedRows].key)
      val matchingKeys = filtersToKeysForFirstPass(
        allKeys, partNames, firstPassPushedPredicates.toArray).toSet
      val filteredByFirstPass = allPartitions.filter(p =>
        matchingKeys.contains(p.asInstanceOf[BufferedRows].key))
      val filteredBySecondPass = filteredByFirstPass.filter { p =>
        val partRow = p.asInstanceOf[BufferedRows].partitionKey()
        partitionPredicates.forall(_.accept(partRow))
      }
      InMemoryEnhancedPartitionFilterBatchScan(
        filteredBySecondPass, readSchema, tableSchema, partitionPredicates.toSeq)
    }

    /**
     * Like InMemoryTableWithV2Filter.supportsPredicatesForFirstPass but
     * accept predicate names as produced by V2ExpressionBuilder (eg, "IS_NOT_NULL", "IS_NULL").
     */
    private def supportsPredicatesForFirstPass(predicates: Array[Predicate]): Boolean = {
      predicates.flatMap(splitAndForFirstPass).forall {
        case p: Predicate if p.name().equals("=") => true
        case p: Predicate if p.name().equals("<=>") => true
        case p: Predicate if p.name().equals("IS NULL") || p.name().equals("IS_NULL") => true
        case p: Predicate if p.name().equals("IS NOT NULL") || p.name().equals("IS_NOT_NULL") =>
          true
        case p: Predicate if p.name().equals("ALWAYS_TRUE") => true
        case _ => false
      }
    }

    private def splitAndForFirstPass(filter: Predicate): Seq[Predicate] = {
      filter match {
        case and: And =>
          splitAndForFirstPass(and.left()) ++ splitAndForFirstPass(and.right())
        case _ => filter :: Nil
      }
    }

    /**
     * Like InMemoryTableWithV2Filter.filtersToKeys but accepts predicate names
     * as produced by V2ExpressionBuilder (e.g. "IS_NOT_NULL", "IS_NULL").
     */
    private def filtersToKeysForFirstPass(
        keys: Iterable[Seq[Any]],
        partitionNames: Seq[String],
        filters: Array[Predicate]): Iterable[Seq[Any]] = {
      keys.filter { partValues =>
        filters.flatMap(splitAndForFirstPass).forall {
          case p: Predicate if p.name().equals("=") =>
            p.children()(1).asInstanceOf[org.apache.spark.sql.connector.expressions.LiteralValue[_]]
              .value == InMemoryBaseTable.extractValue(
              p.children()(0).toString, partitionNames, partValues)
          case p: Predicate if p.name().equals("<=>") =>
            val attrVal = InMemoryBaseTable.extractValue(
              p.children()(0).toString, partitionNames, partValues)
            val value = p.children()(1)
              .asInstanceOf[org.apache.spark.sql.connector.expressions.LiteralValue[_]].value
            if (attrVal == null && value == null) true
            else if (attrVal == null || value == null) false
            else value == attrVal
          case p: Predicate if p.name().equals("IS NULL") || p.name().equals("IS_NULL") =>
            val attr = p.children()(0).toString
            null == InMemoryBaseTable.extractValue(attr, partitionNames, partValues)
          case p: Predicate if p.name().equals("IS NOT NULL") || p.name().equals("IS_NOT_NULL") =>
            val attr = p.children()(0).toString
            null != InMemoryBaseTable.extractValue(attr, partitionNames, partValues)
          case p: Predicate if p.name().equals("ALWAYS_TRUE") => true
          case p: Predicate if p.name().equals("IN") =>
            if (p.children().length > 1) {
              val filterRef = p.children()(0).toString
              val matchingValues = p.children().drop(1).map(
                _.asInstanceOf[org.apache.spark.sql.connector.expressions.LiteralValue[_]].value
              ).toSet
              val partVal = InMemoryBaseTable.extractValue(
                filterRef, partitionNames, partValues)
              matchingValues.contains(partVal)
            } else {
              true
            }
          case f =>
            throw new IllegalArgumentException(s"Unsupported filter type: $f")
        }
      }
    }
  }

  /**
   * Batch scan that stores pushed partition predicates for test inspection
   * (e.g. to verify PartitionPredicate.referencedPartitionColumnOrdinals).
   */
  case class InMemoryEnhancedPartitionFilterBatchScan(
      _data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType,
      pushedPartitionPredicates: Seq[PartitionPredicate] = Seq.empty)
    extends BatchScanBaseClass(_data, readSchema, tableSchema)
    with TestPartitionPredicateScan {

    override def getPushedPartitionPredicates: Seq[PartitionPredicate] = pushedPartitionPredicates
  }
}

object InMemoryEnhancedPartitionFilterTable {
  /**
   * Table property: when "true", reject all PartitionPredicates (for testing).
   */
  private[catalog] val RejectPartitionPredicatesKey = "reject-partition-predicates"
}
