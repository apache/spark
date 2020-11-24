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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.plans.logical.{AlterTableAddPartition, AlterTableDropPartition, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement
import org.apache.spark.sql.util.PartitioningUtils.{castPartitionValues, normalizePartitionSpec}

/**
 * Resolve [[UnresolvedPartitionSpec]] to [[ResolvedPartitionSpec]] in partition related commands.
 */
object ResolvePartitionSpec extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case r @ AlterTableAddPartition(
        ResolvedTable(_, _, table: SupportsPartitionManagement), partitionSpec, _) =>
      r.copy(parts = resolvePartitionSpecs(table, partitionSpec))

    case r @ AlterTableDropPartition(
        ResolvedTable(_, _, table: SupportsPartitionManagement), partitionSpec, _, _, _) =>
      r.copy(parts = resolvePartitionSpecs(table, partitionSpec))
  }

  private def resolvePartitionSpecs(
      table: SupportsPartitionManagement,
      partitionSpec: Seq[PartitionSpec]): Seq[ResolvedPartitionSpec] =
    partitionSpec.map {
      case unresolvedPartSpec: UnresolvedPartitionSpec =>
        ResolvedPartitionSpec(
          convertToPartIdent(table, unresolvedPartSpec.spec),
          unresolvedPartSpec.location)
      case resolvedPartitionSpec: ResolvedPartitionSpec =>
        resolvedPartitionSpec
    }

  private def convertToPartIdent(
      table: SupportsPartitionManagement,
      partitionSpec: TablePartitionSpec): InternalRow = {
    val partitionSchema = table.partitionSchema()
    val normalizedSpec = normalizePartitionSpec(
      partitionSpec,
      partitionSchema.map(_.name),
      table.name,
      conf.resolver)

    castPartitionValues(
      normalizedSpec,
      partitionSchema,
      table.properties().asScala.toMap,
      conf.sessionLocalTimeZone)
  }
}
