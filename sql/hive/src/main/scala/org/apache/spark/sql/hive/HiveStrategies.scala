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

package org.apache.spark.sql.hive

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{CreateTableUsing, CreateTableUsingAsSelect, DescribeCommand}
import org.apache.spark.sql.execution.{DescribeCommand => RunnableDescribeCommand, _}
import org.apache.spark.sql.hive.execution._


private[hive] trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SQLContext#SparkPlanner =>

  val hiveContext: HiveContext

  object Scripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ScriptTransformation(input, script, output, child, schema: HiveScriptIOSchema) =>
        ScriptTransformation(input, script, output, planLater(child), schema)(hiveContext) :: Nil
      case _ => Nil
    }
  }

  object DataSinks extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.InsertIntoTable(
          table: MetastoreRelation, partition, child, overwrite, ifNotExists) =>
        execution.InsertIntoHiveTable(
          table, partition, planLater(child), overwrite, ifNotExists) :: Nil
      case hive.InsertIntoHiveTable(
          table: MetastoreRelation, partition, child, overwrite, ifNotExists) =>
        execution.InsertIntoHiveTable(
          table, partition, planLater(child), overwrite, ifNotExists) :: Nil
      case _ => Nil
    }
  }

  /**
   * Retrieves data using a HiveTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: MetastoreRelation) =>
        // Filter out all predicates that only deal with partition keys, these are given to the
        // hive table scan operator to be used for partition pruning.
        val partitionKeyIds = AttributeSet(relation.partitionKeys)
        val (pruningPredicates, otherPredicates) = predicates.partition { predicate =>
          !predicate.references.isEmpty &&
          predicate.references.subsetOf(partitionKeyIds)
        }

        pruneFilterProject(
          projectList,
          otherPredicates,
          identity[Seq[Expression]],
          HiveTableScan(_, relation, pruningPredicates)(hiveContext)) :: Nil
      case _ =>
        Nil
    }
  }

  object HiveDDLStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case CreateTableUsing(
        tableIdent, userSpecifiedSchema, provider, false, opts, allowExisting, managedIfNoPath) =>
        val cmd =
          CreateMetastoreDataSource(
            tableIdent, userSpecifiedSchema, provider, opts, allowExisting, managedIfNoPath)
        ExecutedCommand(cmd) :: Nil

      case CreateTableUsingAsSelect(
        tableIdent, provider, false, partitionCols, mode, opts, query) =>
        val cmd =
          CreateMetastoreDataSourceAsSelect(tableIdent, provider, partitionCols, mode, opts, query)
        ExecutedCommand(cmd) :: Nil

      case _ => Nil
    }
  }

  case class HiveCommandStrategy(context: HiveContext) extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case describe: DescribeCommand =>
        val resolvedTable = context.executePlan(describe.table).analyzed
        resolvedTable match {
          case t: MetastoreRelation =>
            ExecutedCommand(
              DescribeHiveTableCommand(t, describe.output, describe.isExtended)) :: Nil

          case o: LogicalPlan =>
            val resultPlan = context.executePlan(o).executedPlan
            ExecutedCommand(RunnableDescribeCommand(
              resultPlan, describe.output, describe.isExtended)) :: Nil
        }

      case _ => Nil
    }
  }
}
