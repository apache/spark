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

import org.apache.spark.sql.catalyst.catalog.{ResolvedPartitionSpec, UnresolvedPartitionSpec}
import org.apache.spark.sql.catalyst.plans.logical.{AlterTableAddPartition, AlterTableDropPartition, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits

/**
 * Analyze PartitionSpecs in datasource v2 commands.
 */
object ResolvePartitionSpec extends Rule[LogicalPlan] {
  import DataSourceV2Implicits._

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case r@ AlterTableAddPartition(
        ResolvedTable(_, _, table: SupportsPartitionManagement), parts, _) =>
      val resolvedParts = parts.map {
        case unresolvedPartSpec: UnresolvedPartitionSpec =>
          ResolvedPartitionSpec(
            unresolvedPartSpec.spec.asPartitionIdentifier(table.partitionSchema()),
            unresolvedPartSpec.location)
        case resolvedPartSpec: ResolvedPartitionSpec =>
          resolvedPartSpec
      }
      r.copy(parts = resolvedParts)

    case r@ AlterTableDropPartition(
        ResolvedTable(_, _, table: SupportsPartitionManagement), parts, _, _, _) =>
      val resolvedParts = parts.map {
        case unresolvedPartSpec: UnresolvedPartitionSpec =>
          ResolvedPartitionSpec(
            unresolvedPartSpec.spec.asPartitionIdentifier(table.partitionSchema()),
            unresolvedPartSpec.location)
        case resolvedPartSpec: ResolvedPartitionSpec =>
          resolvedPartSpec
      }
      r.copy(parts = resolvedParts)
  }
}
