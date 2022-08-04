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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{Literal, SortOrder}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractTopK}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LocalLimit, LogicalPlan, Project, RebalancePartitions, Repartition, RepartitionByExpression, Sort, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{LIMIT, OUTER_JOIN, SORT}
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule supports push down local limit and local sort from TopK through outer join:
 *   - for a left outer join, the references of ordering of TopK come from the left side and
 *     the limits of TopK is smaller than left side max rows
 *   - for a right outer join, the references of ordering of TopK come from the right side and
 *     the limits of TopK is smaller than right side max rows
 *
 * Note that, this rule only push down local topK to the bottom outer join which is different with
 * [[LimitPushDown]]. This is to avoid regression due to the overhead of local sort.
 */
object PushLocalTopKThroughOuterJoin extends Rule[LogicalPlan] {
  private def smallThan(limits: Int, maxRowsOpt: Option[Long]): Boolean = maxRowsOpt match {
    case Some(maxRows) => limits < maxRows
    case _ => true
  }

  private def canPushThroughOuterJoin(
      joinType: JoinType,
      order: Seq[SortOrder],
      leftChild: LogicalPlan,
      rightChild: LogicalPlan,
      limits: Int): Boolean = joinType match {
    case LeftOuter =>
      order.forall(_.references.subsetOf(leftChild.outputSet)) &&
        smallThan(limits, leftChild.maxRowsPerPartition)
    case RightOuter =>
      order.forall(_.references.subsetOf(rightChild.outputSet)) &&
        smallThan(limits, rightChild.maxRowsPerPartition)
    case _ => false
  }

  private def findOuterJoin(limits: Int, order: Seq[SortOrder], child: LogicalPlan): Seq[Join] = {
    child match {
      case j @ ExtractEquiJoinKeys(joinType, _, _, _, _, leftChild, rightChild, _)
          if canPushThroughOuterJoin(joinType, order, leftChild, rightChild, limits) =>
        // find the bottom outer join to push down local topK
        val childOuterJoins = joinType match {
          case LeftOuter => findOuterJoin(limits, order, leftChild)
          case RightOuter => findOuterJoin(limits, order, rightChild)
          case _ => Seq.empty
        }
        if (childOuterJoins.nonEmpty) {
          childOuterJoins
        } else {
          j :: Nil
        }
      case u: Union => u.children.flatMap(child => findOuterJoin(limits, order, child))
      case p: Project if p.projectList.forall(_.deterministic) =>
        findOuterJoin(limits, order, p.child)
      case r: RepartitionByExpression if r.partitionExpressions.forall(_.deterministic) =>
        findOuterJoin(limits, order, r.child)
      case r: RebalancePartitions if r.partitionExpressions.forall(_.deterministic) =>
        findOuterJoin(limits, order, r.child)
      case r: Repartition => findOuterJoin(limits, order, r.child)
      case _ => Seq.empty
    }
  }

  private def pushLocalTopK(limits: Int, order: Seq[SortOrder], join: Join): Join = {
    val (newLeft, newRight) = join.joinType match {
      case LeftOuter =>
        (LocalLimit(Literal(limits), Sort(order, false, join.left)), join.right)

      case RightOuter =>
        (join.left, LocalLimit(Literal(limits), Sort(order, false, join.right)))

      case _ => (join.left, join.right)
    }

    Join(newLeft, newRight, join.joinType, join.condition, join.hint)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformWithPruning(_.containsAllPatterns(LIMIT, SORT, OUTER_JOIN), ruleId) {
      case topK @ ExtractTopK(limits, order, _, child, _)
          if limits <= conf.getConf(SQLConf.PUSH_DOWN_LOCAL_TOPK_LIMIT_THRESHOLD) =>
        val outerJoins = findOuterJoin(limits, order, child)
        if (outerJoins.nonEmpty) {
          val identifierMap = new java.util.IdentityHashMap[Join, Join]
          outerJoins.foreach { j =>
            identifierMap.put(j, pushLocalTopK(limits, order, j))
          }

          topK.transformWithPruning(_.containsPattern(OUTER_JOIN)) {
            case j: Join if identifierMap.containsKey(j) => identifierMap.get(j)
          }
        } else {
          topK
        }
    }
  }
}
