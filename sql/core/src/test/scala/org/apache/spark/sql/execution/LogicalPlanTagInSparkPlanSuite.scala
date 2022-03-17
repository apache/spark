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

package org.apache.spark.sql.execution

import scala.reflect.ClassTag

import org.apache.spark.sql.TPCDSQuerySuite
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Final}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Generate, Join, LocalRelation, LogicalPlan, Range, Sample, Union, Window}
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecutionSuite
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec

// Disable AQE because AdaptiveSparkPlanExec does not have a logical plan link
class LogicalPlanTagInSparkPlanSuite extends TPCDSQuerySuite with DisableAdaptiveExecutionSuite {

  override protected def checkGeneratedCode(
      plan: SparkPlan, checkMethodCodeSize: Boolean = true): Unit = {
    super.checkGeneratedCode(plan, checkMethodCodeSize)
    checkLogicalPlanTag(plan)
  }

  private def isFinalAgg(aggExprs: Seq[AggregateExpression]): Boolean = {
    // TODO: aggregate node without aggregate expressions can also be a final aggregate, but
    // currently the aggregate node doesn't have a final/partial flag.
    aggExprs.nonEmpty && aggExprs.forall(ae => ae.mode == Complete || ae.mode == Final)
  }

  // A scan plan tree is a plan tree that has a leaf node under zero or more Project/Filter nodes.
  // We may add `ColumnarToRowExec` and `InputAdapter` above the scan node after planning.
  @scala.annotation.tailrec
  private def isScanPlanTree(plan: SparkPlan): Boolean = plan match {
    case ColumnarToRowExec(i: InputAdapter) => isScanPlanTree(i.child)
    case p: ProjectExec => isScanPlanTree(p.child)
    case f: FilterExec => isScanPlanTree(f.child)
    case _: LeafExecNode => true
    case _ => false
  }

  private def checkLogicalPlanTag(plan: SparkPlan): Unit = {
    plan match {
      case _: HashJoin | _: BroadcastNestedLoopJoinExec | _: CartesianProductExec
           | _: ShuffledHashJoinExec | _: SortMergeJoinExec =>
        assertLogicalPlanType[Join](plan)

      // There is no corresponding logical plan for the physical partial aggregate.
      case agg: HashAggregateExec if isFinalAgg(agg.aggregateExpressions) =>
        assertLogicalPlanType[Aggregate](plan)
      case agg: ObjectHashAggregateExec if isFinalAgg(agg.aggregateExpressions) =>
        assertLogicalPlanType[Aggregate](plan)
      case agg: SortAggregateExec if isFinalAgg(agg.aggregateExpressions) =>
        assertLogicalPlanType[Aggregate](plan)

      case _: WindowExec =>
        assertLogicalPlanType[Window](plan)

      case _: UnionExec =>
        assertLogicalPlanType[Union](plan)

      case _: SampleExec =>
        assertLogicalPlanType[Sample](plan)

      case _: GenerateExec =>
        assertLogicalPlanType[Generate](plan)

      // The exchange related nodes are created after the planning, they don't have corresponding
      // logical plan.
      case _: ShuffleExchangeExec | _: BroadcastExchangeExec | _: ReusedExchangeExec =>
        assert(plan.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).isEmpty)

      // The subquery exec nodes are just wrappers of the actual nodes, they don't have
      // corresponding logical plan.
      case _: SubqueryExec | _: ReusedSubqueryExec =>
        assert(plan.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).isEmpty)

      case _ if isScanPlanTree(plan) =>
        // `ColumnarToRowExec` and `InputAdapter` are added outside of the planner, which doesn't
        // have the logical plan tag.
        val actualPlan = plan match {
          case ColumnarToRowExec(i: InputAdapter) => i.child
          case _ => plan
        }

        // The strategies for planning scan can remove or add FilterExec/ProjectExec nodes,
        // so it's not simple to check. Instead, we only check that the origin LogicalPlan
        // contains the corresponding leaf node of the SparkPlan.
        // a strategy might remove the filter if it's totally pushed down, e.g.:
        //   logical = Project(Filter(Scan A))
        //   physical = ProjectExec(ScanExec A)
        // we only check that leaf modes match between logical and physical plan.
        val logicalLeaves = getLogicalPlan(actualPlan).collectLeaves()
        val physicalLeaves = plan.collectLeaves()
        assert(logicalLeaves.length == 1)
        assert(physicalLeaves.length == 1)
        physicalLeaves.head match {
          case _: RangeExec => logicalLeaves.head.isInstanceOf[Range]
          case _: DataSourceScanExec => logicalLeaves.head.isInstanceOf[LogicalRelation]
          case _: InMemoryTableScanExec => logicalLeaves.head.isInstanceOf[InMemoryRelation]
          case _: LocalTableScanExec => logicalLeaves.head.isInstanceOf[LocalRelation]
          case _: ExternalRDDScanExec[_] => logicalLeaves.head.isInstanceOf[ExternalRDD[_]]
          case _: BatchScanExec => logicalLeaves.head.isInstanceOf[DataSourceV2Relation]
          case _ =>
        }
        // Do not need to check the children recursively.
        return

      case _ =>
    }

    plan.children.foreach(checkLogicalPlanTag)
    plan.subqueries.foreach(checkLogicalPlanTag)
  }

  private def getLogicalPlan(node: SparkPlan): LogicalPlan = {
    node.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).getOrElse {
      fail(node.getClass.getSimpleName + " does not have a logical plan link")
    }
  }

  private def assertLogicalPlanType[T <: LogicalPlan : ClassTag](node: SparkPlan): Unit = {
    val logicalPlan = getLogicalPlan(node)
    val expectedCls = implicitly[ClassTag[T]].runtimeClass
    assert(expectedCls == logicalPlan.getClass)
  }
}
