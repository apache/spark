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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, V2PartitionCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement
import org.apache.spark.sql.types.StructField

/**
 * Resolve [[UnresolvedPartitionSpec]] to [[ResolvedPartitionSpec]] in partition related commands.
 */
object ResolvePartitionSpec extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(COMMAND)) {
    case command: V2PartitionCommand if command.childrenResolved && !command.resolved =>
      command.table match {
        case r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _) =>
          command.transformExpressions {
            case partSpecs: UnresolvedPartitionSpec =>
              val partitionSchema = table.partitionSchema()
              PartitionSpecResolution.resolvePartitionSpec(
                r.name,
                partSpecs,
                partitionSchema,
                command.allowPartialPartitionSpec,
                conf)
          }
        case _ => command
      }
  }

  private[sql] def convertToPartIdent(
      partitionSpec: TablePartitionSpec,
      schema: Seq[StructField]): InternalRow = {
    PartitionSpecResolution.convertToPartIdent(partitionSpec, schema, conf)
  }
}
