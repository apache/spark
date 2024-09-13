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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionsException, ResolvedPartitionSpec}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{SupportsAtomicPartitionManagement, SupportsPartitionManagement}
import org.apache.spark.sql.errors.QueryExecutionErrors

/**
 * Physical plan node for dropping partitions of table.
 */
case class DropPartitionExec(
    table: SupportsPartitionManagement,
    partSpecs: Seq[ResolvedPartitionSpec],
    ignoreIfNotExists: Boolean,
    purge: Boolean,
    refreshCache: () => Unit) extends LeafV2CommandExec {
  import DataSourceV2Implicits._

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val existsPartIdents: mutable.Set[InternalRow] = mutable.Set()
    val notExistsPartIdents: mutable.Set[InternalRow] = mutable.Set()
    partSpecs.foreach(partSpec => {
      val partIdents = table.listPartitionIdentifiers(partSpec.names.toArray, partSpec.ident)
      if (partIdents.nonEmpty) {
        existsPartIdents.addAll(partIdents)
      } else {
        notExistsPartIdents.add(partSpec.ident)
      }
    })

    if (notExistsPartIdents.nonEmpty && !ignoreIfNotExists) {
      throw new NoSuchPartitionsException(
        table.name(), notExistsPartIdents.toSeq, table.partitionSchema())
    }

    val isTableAltered = existsPartIdents.toSeq match {
      case Seq() => false // Nothing will be done
      case Seq(partIdent) =>
        if (purge) table.purgePartition(partIdent) else table.dropPartition(partIdent)
      case _ if table.isInstanceOf[SupportsAtomicPartitionManagement] =>
        val idents = existsPartIdents.toArray
        val atomicTable = table.asAtomicPartitionable
        if (purge) atomicTable.purgePartitions(idents) else atomicTable.dropPartitions(idents)
      case _ =>
        throw QueryExecutionErrors.cannotDropMultiPartitionsOnNonatomicPartitionTableError(
          table.name())
    }
    if (isTableAltered) refreshCache()
    Seq.empty
  }
}
