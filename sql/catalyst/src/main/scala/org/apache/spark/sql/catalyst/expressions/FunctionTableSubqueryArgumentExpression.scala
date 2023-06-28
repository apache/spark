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

import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern.{FUNCTION_TABLE_RELATION_ARGUMENT_EXPRESSION, TreePattern}
import org.apache.spark.sql.types.DataType

/**
 * This is the parsed representation of a relation argument for a TableValuedFunction call.
 * The syntax supports passing such relations one of two ways:
 *
 * 1. SELECT ... FROM tvf_call(TABLE t)
 * 2. SELECT ... FROM tvf_call(TABLE (<query>))
 *
 * In the former case, the relation argument directly refers to the name of a
 * table in the catalog. In the latter case, the relation argument comprises
 * a table subquery that may itself refer to one or more tables in its own
 * FROM clause.
 */
case class FunctionTableSubqueryArgumentExpression(
    plan: LogicalPlan,
    outerAttrs: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression(plan, outerAttrs, exprId, Seq.empty, None) with Unevaluable {

  override def dataType: DataType = plan.schema
  override def nullable: Boolean = false
  override def withNewPlan(plan: LogicalPlan): FunctionTableSubqueryArgumentExpression =
    copy(plan = plan)
  override def hint: Option[HintInfo] = None
  override def withNewHint(hint: Option[HintInfo]): FunctionTableSubqueryArgumentExpression =
    copy()
  override def toString: String = s"table-argument#${exprId.id} $conditionString"
  override lazy val canonicalized: Expression = {
    FunctionTableSubqueryArgumentExpression(
      plan.canonicalized,
      outerAttrs.map(_.canonicalized),
      ExprId(0))
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): FunctionTableSubqueryArgumentExpression =
    copy(outerAttrs = newChildren)

  final override def nodePatternsInternal: Seq[TreePattern] =
    Seq(FUNCTION_TABLE_RELATION_ARGUMENT_EXPRESSION)

  lazy val evaluable: LogicalPlan = Project(Seq(Alias(CreateStruct(plan.output), "c")()), plan)
}
