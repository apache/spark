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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{PartitionsAlreadyExistException, ResolvedPartitionSpec}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{SupportsAtomicPartitionManagement, SupportsPartitionManagement}
import org.apache.spark.sql.errors.QueryExecutionErrors

/**
 * Physical plan node for adding partitions of table.
 */
case class AddPartitionExec(
    table: SupportsPartitionManagement,
    partSpecs: Seq[ResolvedPartitionSpec],
    ignoreIfExists: Boolean,
    refreshCache: () => Unit) extends LeafV2CommandExec {
  import DataSourceV2Implicits._

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val (existsParts, notExistsParts) =
      partSpecs.partition(p => table.partitionExists(p.ident))

    if (existsParts.nonEmpty && !ignoreIfExists) {
      throw new PartitionsAlreadyExistException(
        table.name(), existsParts.map(_.ident), table.partitionSchema())
    }

    val isTableAltered = notExistsParts match {
      case Seq() => false // Nothing will be done
      case Seq(partitionSpec) =>
        val partProp = partitionSpec.location.map(loc => "location" -> loc).toMap
        table.createPartition(partitionSpec.ident, partProp.asJava)
        true
      case _ if table.isInstanceOf[SupportsAtomicPartitionManagement] =>
        val partIdents = notExistsParts.map(_.ident)
        val partProps = notExistsParts.map(_.location.map(loc => "location" -> loc).toMap)
        table.asAtomicPartitionable
          .createPartitions(
            partIdents.toArray,
            partProps.map(_.asJava).toArray)
        true
      case _ =>
        throw QueryExecutionErrors.cannotAddMultiPartitionsOnNonatomicPartitionTableError(
          table.name())
    }
    if (isTableAltered) refreshCache()
    Seq.empty
  }
}
