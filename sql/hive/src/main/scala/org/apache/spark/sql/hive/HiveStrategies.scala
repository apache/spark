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

package org.apache.spark.sql
package hive

import catalyst.expressions._
import catalyst.planning._
import catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{BaseRelation, LogicalPlan}

import org.apache.spark.sql.execution._

trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkSqlContext#SparkPlanner =>

  val hiveContext: HiveContext

  object Scripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ScriptTransformation(input, script, output, child) =>
        ScriptTransformation(input, script, output, planLater(child))(hiveContext) :: Nil
      case _ => Nil
    }
  }

  object DataSinks extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.InsertIntoTable(table: MetastoreRelation, partition, child, overwrite) =>
        InsertIntoHiveTable(table, partition, planLater(child), overwrite)(hiveContext) :: Nil
      case logical.InsertIntoTable(table: ParquetRelation, partition, child, overwrite) =>
        InsertIntoParquetTable(table, planLater(child))(hiveContext.sparkContext) :: Nil
      case _ => Nil
    }
  }

  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      // Push attributes into table scan when possible.
      case p @ logical.Project(projectList, m: MetastoreRelation) if isSimpleProject(projectList) =>
        HiveTableScan(projectList.asInstanceOf[Seq[Attribute]], m, None)(hiveContext) :: Nil
      case p @ logical.Project(projectList, r: ParquetRelation) if isSimpleProject(projectList) =>
        ParquetTableScan(
          projectList.asInstanceOf[Seq[Attribute]],
          r,
          None)(hiveContext.sparkContext) :: Nil
      case m: MetastoreRelation =>
        HiveTableScan(m.output, m, None)(hiveContext) :: Nil
      case p: ParquetRelation =>
        ParquetTableScan(p.output, p, None)(hiveContext.sparkContext) :: Nil
      case _ => Nil
    }
  }

  /**
   * A strategy used to detect filtering predicates on top of a partitioned relation to help
   * partition pruning.
   *
   * This strategy itself doesn't perform partition pruning, it just collects and combines all the
   * partition pruning predicates and pass them down to the underlying [[HiveTableScan]] operator,
   * which does the actual pruning work.
   */
  object PartitionPrunings extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case p @ FilteredOperation(predicates, relation: MetastoreRelation)
        if relation.isPartitioned =>

        val partitionKeyIds = relation.partitionKeys.map(_.id).toSet

        // Filter out all predicates that only deal with partition keys
        val (pruningPredicates, otherPredicates) = predicates.partition {
          _.references.map(_.id).subsetOf(partitionKeyIds)
        }

        val scan = HiveTableScan(
          relation.output, relation, pruningPredicates.reduceLeftOption(And))(hiveContext)

        otherPredicates
          .reduceLeftOption(And)
          .map(Filter(_, scan))
          .getOrElse(scan) :: Nil

      case _ =>
        Nil
    }
  }

  /**
   * A strategy that detects projects and filters over some relation and applies column pruning if
   * possible.  Partition pruning is applied first if the relation is partitioned.
   */
  object ColumnPrunings extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: BaseRelation) =>
        val predicateOpt = predicates.reduceOption(And)
        val predicateRefs = predicateOpt.map(_.references).getOrElse(Set.empty)
        val projectRefs = projectList.flatMap(_.references)

        // To figure out what columns to preserve after column pruning, we need to consider:
        //
        // 1. Columns referenced by the project list (order preserved)
        // 2. Columns referenced by filtering predicates but not by project list
        // 3. Relation output
        //
        // Then the final result is ((1 union 2) intersect 3)
        val prunedCols = (projectRefs ++ (predicateRefs -- projectRefs)).intersect(relation.output)

        val filteredScans =
          if (relation.isPartitioned) {  // from here on relation must be a [[MetaStoreRelation]]
            // Applies partition pruning first for partitioned table
            val filteredRelation = predicateOpt.map(logical.Filter(_, relation)).getOrElse(relation)
            PartitionPrunings(filteredRelation).view.map(_.transform {
              case scan: HiveTableScan =>
                scan.copy(attributes = prunedCols)(hiveContext)
            })
          } else {
            val scan = relation match {
              case MetastoreRelation(_, _, _) => {
                HiveTableScan(
                  prunedCols,
                  relation.asInstanceOf[MetastoreRelation],
                  None)(hiveContext)
              }
              case ParquetRelation(_, _) => {
                ParquetTableScan(
                  relation.output,
                  relation.asInstanceOf[ParquetRelation],
                  None)(hiveContext.sparkContext)
                  .pruneColumns(prunedCols)
              }
            }
            predicateOpt.map(execution.Filter(_, scan)).getOrElse(scan) :: Nil
          }

        if (isSimpleProject(projectList) && prunedCols == projectRefs) {
          filteredScans
        } else {
          filteredScans.view.map(execution.Project(projectList, _))
        }

      case _ =>
        Nil
    }
  }

  /**
   * Returns true if `projectList` only performs column pruning and does not evaluate other
   * complex expressions.
   */
  def isSimpleProject(projectList: Seq[NamedExpression]) = {
    projectList.forall(_.isInstanceOf[Attribute])
  }
}
