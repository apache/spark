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

import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, LogicalPlan, Project, Repartition, RepartitionByExpression, Sort}
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
 *
 * Each TABLE argument may also optionally include a PARTITION BY clause. If present, these indicate
 * how to logically split up the input relation such that the table-valued function evaluates
 * exactly once for each partition, and returns the union of all results. If no partitioning list is
 * present, this splitting of the input relation is undefined. Furthermore, if the PARTITION BY
 * clause includes a following ORDER BY clause, Catalyst will sort the rows in each partition such
 * that the table-valued function receives them one-by-one in the requested order. Otherwise, if no
 * such ordering is specified, the ordering of rows within each partition is undefined.
 *
 * @param plan the logical plan provided as input for the table argument as either a logical
 *             relation or as a more complex logical plan in the event of a table subquery.
 * @param outerAttrs outer references of this subquery plan, generally empty since these table
 *                   arguments do not allow correlated references currently
 * @param exprId expression ID of this subquery expression, generally generated afresh each time
 * @param partitionByExpressions if non-empty, the TABLE argument included the PARTITION BY clause
 *                               to indicate that the input relation should be repartitioned by the
 *                               hash of the provided expressions, such that all the rows with each
 *                               unique combination of values of the partitioning expressions will
 *                               be consumed by exactly one instance of the table function class.
 * @param withSinglePartition if true, the TABLE argument included the WITH SINGLE PARTITION clause
 *                            to indicate that the entire input relation should be repartitioned to
 *                            one worker for consumption by exactly one instance of the table
 *                            function class.
 * @param orderByExpressions if non-empty, the TABLE argument included the ORDER BY clause to
 *                           indicate that the rows within each partition of the table function are
 *                           to arrive in the provided order.
 */
case class FunctionTableSubqueryArgumentExpression(
    plan: LogicalPlan,
    outerAttrs: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId,
    partitionByExpressions: Seq[Expression] = Seq.empty,
    withSinglePartition: Boolean = false,
    orderByExpressions: Seq[SortOrder] = Seq.empty)
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
      ExprId(0),
      partitionByExpressions,
      withSinglePartition,
      orderByExpressions)
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): FunctionTableSubqueryArgumentExpression =
    copy(outerAttrs = newChildren)

  final override def nodePatternsInternal: Seq[TreePattern] =
    Seq(FUNCTION_TABLE_RELATION_ARGUMENT_EXPRESSION)

  def hasRepartitioning: Boolean = withSinglePartition || partitionByExpressions.nonEmpty

  lazy val evaluable: LogicalPlan = {
    // If the TABLE argument includes the WITH SINGLE PARTITION or PARTITION BY or ORDER BY
    // clause(s), add a corresponding logical operator to represent the repartitioning operation in
    // the query plan.
    var subquery = plan
    if (partitionByExpressions.nonEmpty) {
      subquery = RepartitionByExpression(
        partitionExpressions = partitionByExpressions,
        child = subquery,
        optNumPartitions = None)
    }
    if (withSinglePartition) {
      subquery = Repartition(
        numPartitions = 1,
        shuffle = true,
        child = subquery)
    }
    if (orderByExpressions.nonEmpty) {
      subquery = Sort(
        order = orderByExpressions,
        global = false,
        child = subquery)
    }
    Project(Seq(Alias(CreateStruct(subquery.output), "c")()), subquery)
  }
}
