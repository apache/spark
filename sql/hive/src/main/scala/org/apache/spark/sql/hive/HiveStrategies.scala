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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive.execution._

private[hive] trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkPlanner =>

  val sparkSession: SparkSession

  object Scripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ScriptTransformation(input, script, output, child, ioschema) =>
        val hiveIoSchema = HiveScriptIOSchema(ioschema)
        ScriptTransformation(input, script, output, planLater(child), hiveIoSchema) :: Nil
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
      case p @Project(projectList, relation: MetastoreRelation) =>
        val projectSet = AttributeSet(projectList.flatMap(_.references))
        if (AttributeSet(projectList.map(_.toAttribute)) == projectSet) {
          HiveTableScanExec(projectList.asInstanceOf[Seq[Attribute]], relation,
            relation.partitionPruningPred)(sparkSession) :: Nil
        } else {
          ProjectExec(projectList, HiveTableScanExec(projectSet.toSeq, relation,
            relation.partitionPruningPred)(sparkSession)) :: Nil
        }
      case p @Project(projectList, filter@Filter(condition, relation: MetastoreRelation)) =>
        val projectSet = AttributeSet(projectList.flatMap(_.references))
        val filterSet = AttributeSet(condition.flatMap(_.references))
        if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
          filterSet.subsetOf(projectSet)) {
          FilterExec(condition, HiveTableScanExec(projectList.asInstanceOf[Seq[Attribute]],
            relation, relation.partitionPruningPred)(sparkSession)) :: Nil
        } else {
          val scan = HiveTableScanExec((projectSet ++ filterSet).toSeq, relation,
            relation.partitionPruningPred)(sparkSession)
          ProjectExec(projectList, FilterExec(condition, scan)) :: Nil
        }
      case relation: MetastoreRelation =>
        HiveTableScanExec(relation.output, relation, relation.partitionPruningPred)(
          sparkSession) :: Nil
      case _ =>
        Nil
    }
  }
}
