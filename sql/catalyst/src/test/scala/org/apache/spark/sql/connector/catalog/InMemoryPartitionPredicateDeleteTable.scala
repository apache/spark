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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.expressions.filter.{PartitionPredicate, Predicate}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._

/**
 * In-memory table that supports row-level operations and accepts [[PartitionPredicate]]s
 * in V2 [[canDeleteWhere]]/[[deleteWhere]] for metadata-only deletes.
 *
 * Contains some knobs to control acceptance of various partition and data predicates.
 */
class InMemoryPartitionPredicateDeleteTable(
    name: String,
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryRowLevelOperationTable(name, schema, partitioning, properties) {

  private val acceptPartitionPredicates: Boolean =
    properties.getOrDefault(
      InMemoryPartitionPredicateDeleteTable.AcceptPartitionPredicatesKey, "true").toBoolean

  private val acceptDataPredicates: Boolean =
    properties.getOrDefault(
      InMemoryPartitionPredicateDeleteTable.AcceptDataPredicatesKey, "false").toBoolean

  private val partPaths = partCols.map(_.mkString(".")).toSet

  private def refsOnlyPartCols(p: Predicate): Boolean =
    p.references().forall(ref => partPaths.contains(ref.fieldNames().mkString(".")))

  override def canDeleteWhere(predicates: Array[Predicate]): Boolean = {
    predicates.forall {
      case _: PartitionPredicate => acceptPartitionPredicates
      case p =>
        InMemoryTableWithV2Filter.supportsPredicates(Array(p)) &&
          (acceptDataPredicates || refsOnlyPartCols(p))
    }
  }

  override def deleteWhere(predicates: Array[Predicate]): Unit = dataMap.synchronized {
    val (partPreds, standardPreds) = predicates.partition(_.isInstanceOf[PartitionPredicate])
    val (partStdPreds, dataStdPreds) = standardPreds.partition(refsOnlyPartCols)

    val candidateKeys = if (partStdPreds.nonEmpty) {
      InMemoryTableWithV2Filter.filtersToKeys(
        dataMap.keys, partCols.map(_.toSeq.quoted).toImmutableArraySeq, partStdPreds)
    } else {
      dataMap.keys
    }

    // Handle partition predicates.
    val keysToProcess = if (partPreds.nonEmpty) {
      val pArr = partPreds.map(_.asInstanceOf[PartitionPredicate])
      candidateKeys.filter { key =>
        val partRow = PartitionInternalRow(key.toArray)
        pArr.forall(_.eval(partRow))
      }
    } else {
      candidateKeys
    }

    // Handle data predicates (simulate a data source handling data filters with data statistics)
    if (dataStdPreds.isEmpty) {
      dataMap --= keysToProcess
    } else {
      for (key <- keysToProcess.toSeq) {
        dataMap.get(key).foreach { splits =>
          val filtered = splits.map { buffered =>
            val kept = new BufferedRows(key, buffered.schema)
            buffered.rows
              .filterNot(rowMatchesAll(_, dataStdPreds, buffered.schema))
              .foreach(kept.withRow)
            kept
          }
          if (filtered.forall(_.rows.isEmpty)) {
            dataMap.remove(key)
          } else {
            dataMap.update(key, filtered)
          }
        }
      }
    }
  }

  private def rowMatchesAll(
      row: InternalRow,
      preds: Array[Predicate],
      rowSchema: StructType): Boolean = {
    val resolve: String => Any = colName => {
      val idx = rowSchema.fieldIndex(colName)
      row.get(idx, rowSchema(idx).dataType)
    }
    preds.forall(
      InMemoryTableWithV2Filter.evalPredicate(_, resolve))
  }
}

object InMemoryPartitionPredicateDeleteTable {
  private[catalog] val AcceptPartitionPredicatesKey = "accept-partition-predicates"
  private[catalog] val AcceptDataPredicatesKey = "accept-data-predicates"
}

class InMemoryPartitionPredicateDeleteCatalog extends InMemoryTableCatalog {
  import CatalogV2Implicits._

  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident.asMultipartIdentifier)
    }

    InMemoryTableCatalog.maybeSimulateFailedTableCreation(tableInfo.properties)

    val tableName = s"$name.${ident.quoted}"
    val schema = CatalogV2Util.v2ColumnsToStructType(tableInfo.columns)
    val table = new InMemoryPartitionPredicateDeleteTable(
      tableName, schema, tableInfo.partitions, tableInfo.properties)
    tables.put(ident, table)
    namespaces.putIfAbsent(ident.namespace.toList, Map())
    table
  }
}
