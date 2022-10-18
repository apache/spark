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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, ExprId, InSet, ListQuery, Literal, PlanExpression, Predicate, SupportQueryContext}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.{LeafLike, SQLQueryContext, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * The base class for subquery that is used in SparkPlan.
 */
abstract class ExecSubqueryExpression extends PlanExpression[BaseSubqueryExec] {
  /**
   * Fill the expression with collected result from executed plan.
   */
  def updateResult(): Unit

  /** Updates the expression with a new plan. */
  override def withNewPlan(plan: BaseSubqueryExec): ExecSubqueryExpression
}

object ExecSubqueryExpression {
  /**
   * Returns true when an expression contains a subquery
   */
  def hasSubquery(e: Expression): Boolean = {
    e.exists {
      case _: ExecSubqueryExpression => true
      case _ => false
    }
  }
}

/**
 * A subquery that will return only one row and one column.
 *
 * This is the physical copy of ScalarSubquery to be used inside SparkPlan.
 */
case class ScalarSubquery(
    plan: BaseSubqueryExec,
    exprId: ExprId)
  extends ExecSubqueryExpression with LeafLike[Expression] with SupportQueryContext {

  override def dataType: DataType = plan.schema.fields.head.dataType
  override def nullable: Boolean = true
  override def toString: String = plan.simpleString(SQLConf.get.maxToStringFields)
  override def withNewPlan(query: BaseSubqueryExec): ScalarSubquery = copy(plan = query)
  def initQueryContext(): Option[SQLQueryContext] = Some(origin.context)

  override lazy val canonicalized: Expression = {
    ScalarSubquery(plan.canonicalized.asInstanceOf[BaseSubqueryExec], ExprId(0))
  }

  // the first column in first row from `query`.
  @volatile private var result: Any = _
  @volatile private var updated: Boolean = false

  def updateResult(): Unit = {
    val rows = plan.executeCollect()
    if (rows.length > 1) {
      throw QueryExecutionErrors.multipleRowSubqueryError(getContextOrNull())
    }
    if (rows.length == 1) {
      assert(rows(0).numFields == 1,
        s"Expects 1 field, but got ${rows(0).numFields}; something went wrong in analysis")
      result = rows(0).get(0, dataType)
    } else {
      // If there is no rows returned, the result should be null.
      result = null
    }
    updated = true
  }

  override def eval(input: InternalRow): Any = {
    require(updated, s"$this has not finished")
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    require(updated, s"$this has not finished")
    Literal.create(result, dataType).doGenCode(ctx, ev)
  }
}

/**
 * The physical node of in-subquery. When this is used for Dynamic Partition Pruning, as the pruning
 * happens at the driver side, we don't broadcast subquery result.
 */
case class InSubqueryExec(
    child: Expression,
    plan: BaseSubqueryExec,
    exprId: ExprId,
    shouldBroadcast: Boolean = false,
    private var resultBroadcast: Broadcast[Array[Any]] = null,
    @transient private var result: Array[Any] = null)
  extends ExecSubqueryExpression with UnaryLike[Expression] with Predicate {

  @transient private lazy val inSet = InSet(child, result.toSet)

  override def nullable: Boolean = child.nullable
  override def toString: String = s"$child IN ${plan.name}"
  override def withNewPlan(plan: BaseSubqueryExec): InSubqueryExec = copy(plan = plan)
  final override def nodePatternsInternal: Seq[TreePattern] = Seq(IN_SUBQUERY_EXEC)

  def updateResult(): Unit = {
    val rows = plan.executeCollect()
    result = if (plan.output.length > 1) {
      rows.asInstanceOf[Array[Any]]
    } else {
      rows.map(_.get(0, child.dataType))
    }
    if (shouldBroadcast) {
      resultBroadcast = plan.session.sparkContext.broadcast(result)
    }
  }

  // This is used only by DPP where we don't need broadcast the result.
  def values(): Option[Array[Any]] = Option(result)

  private def prepareResult(): Unit = {
    require(result != null || resultBroadcast != null, s"$this has not finished")
    if (result == null && resultBroadcast != null) {
      result = resultBroadcast.value
    }
  }

  override def eval(input: InternalRow): Any = {
    prepareResult()
    inSet.eval(input)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    prepareResult()
    inSet.doGenCode(ctx, ev)
  }

  override lazy val canonicalized: InSubqueryExec = {
    copy(
      child = child.canonicalized,
      plan = plan.canonicalized.asInstanceOf[BaseSubqueryExec],
      exprId = ExprId(0),
      resultBroadcast = null,
      result = null)
  }

  override protected def withNewChildInternal(newChild: Expression): InSubqueryExec =
    copy(child = newChild)
}

/**
 * Plans subqueries that are present in the given [[SparkPlan]].
 */
case class PlanSubqueries(sparkSession: SparkSession) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY, IN_SUBQUERY)) {
      case subquery: expressions.ScalarSubquery =>
        val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, subquery.plan)
        ScalarSubquery(
          SubqueryExec.createForScalarSubquery(
            s"scalar-subquery#${subquery.exprId.id}", executedPlan),
          subquery.exprId)
      case expressions.InSubquery(values, ListQuery(query, _, exprId, _, _)) =>
        val expr = if (values.length == 1) {
          values.head
        } else {
          CreateNamedStruct(
            values.zipWithIndex.flatMap { case (v, index) =>
              Seq(Literal(s"col_$index"), v)
            }
          )
        }
        val executedPlan = QueryExecution.prepareExecutedPlan(sparkSession, query)
        InSubqueryExec(expr, SubqueryExec(s"subquery#${exprId.id}", executedPlan), exprId)
    }
  }
}
