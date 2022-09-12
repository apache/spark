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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.trees.UnaryLike

trait DynamicPruning extends Predicate

/**
 * The DynamicPruningSubquery expression is only used in join operations to prune one side of the
 * join with a filter from the other side of the join. It is inserted in cases where partition
 * pruning can be applied.
 *
 * @param pruningKey the filtering key of the plan to be pruned.
 * @param buildQuery the build side of the join.
 * @param buildKeys the join keys corresponding to the build side of the join
 * @param onlyInBroadcast when set to false it indicates that the pruning filter is likely to be
 *  beneficial and so it should be executed even if it cannot reuse the results of the
 *  broadcast through ReuseExchange; otherwise, it will use the filter only if it
 *  can reuse the results of the broadcast through ReuseExchange
 * @param broadcastKeyIndex the index of the filtering key collected from the broadcast
 */
case class DynamicPruningSubquery(
    pruningKey: Expression,
    buildQuery: LogicalPlan,
    buildKeys: Seq[Expression],
    broadcastKeyIndex: Int,
    onlyInBroadcast: Boolean,
    exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression(buildQuery, Seq(pruningKey), exprId)
  with DynamicPruning
  with Unevaluable
  with UnaryLike[Expression] {

  override def child: Expression = pruningKey

  override def plan: LogicalPlan = buildQuery

  override def nullable: Boolean = false

  override def withNewPlan(plan: LogicalPlan): DynamicPruningSubquery = copy(buildQuery = plan)

  override lazy val resolved: Boolean = {
    pruningKey.resolved &&
      buildQuery.resolved &&
      buildKeys.nonEmpty &&
      buildKeys.forall(_.resolved) &&
      broadcastKeyIndex >= 0 &&
      broadcastKeyIndex < buildKeys.size &&
      buildKeys.forall(_.references.subsetOf(buildQuery.outputSet)) &&
      pruningKey.dataType == buildKeys(broadcastKeyIndex).dataType
  }

  final override def nodePatternsInternal: Seq[TreePattern] = Seq(DYNAMIC_PRUNING_SUBQUERY)

  override def toString: String = s"dynamicpruning#${exprId.id} $conditionString"

  override lazy val preCanonicalized: DynamicPruning = {
    copy(
      pruningKey = pruningKey.preCanonicalized,
      buildQuery = buildQuery.canonicalized,
      buildKeys = buildKeys.map(_.preCanonicalized),
      exprId = ExprId(0))
  }

  override protected def withNewChildInternal(newChild: Expression): DynamicPruningSubquery =
    copy(pruningKey = newChild)
}

/**
 * Marker for a planned [[DynamicPruning]] expression.
 * The expression is created during planning, and it defers to its child for evaluation.
 *
 * @param child underlying predicate.
 */
case class DynamicPruningExpression(child: Expression)
  extends UnaryExpression
  with DynamicPruning {
  override def eval(input: InternalRow): Any = child.eval(input)
  final override val nodePatterns: Seq[TreePattern] = Seq(DYNAMIC_PRUNING_EXPRESSION)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.genCode(ctx)
  }

  override protected def withNewChildInternal(newChild: Expression): DynamicPruningExpression =
    copy(child = newChild)
}
