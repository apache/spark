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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionsException, ResolvedPartitionSpec}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{SupportsAtomicPartitionManagement, SupportsPartitionManagement}

/**
 * Physical plan node for dropping partitions of table.
 */
case class AlterTableDropPartitionExec(
    table: SupportsPartitionManagement,
    partSpecs: Seq[ResolvedPartitionSpec],
    ignoreIfNotExists: Boolean) extends V2CommandExec {
  import DataSourceV2Implicits._

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val (existsPartIdents, notExistsPartIdents) =
      partSpecs.map(_.ident).partition(table.partitionExists)

    if (notExistsPartIdents.nonEmpty && !ignoreIfNotExists) {
      throw new NoSuchPartitionsException(
        table.name(), notExistsPartIdents, table.partitionSchema())
    }

    existsPartIdents match {
      case Seq() => // Nothing will be done
      case Seq(partIdent) =>
        table.dropPartition(partIdent)
      case _ if table.isInstanceOf[SupportsAtomicPartitionManagement] =>
        table.asAtomicPartitionable.dropPartitions(existsPartIdents.toArray)
      case _ =>
        throw new UnsupportedOperationException(
          s"Nonatomic partition table ${table.name()} can not drop multiple partitions.")
    }
    Seq.empty
  }
}
