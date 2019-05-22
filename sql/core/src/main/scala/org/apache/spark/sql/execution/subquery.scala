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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{AttributeSeq, Expression, ExprId, InSet, Literal, PlanExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}

/**
 * The base class for subquery that is used in SparkPlan.
 */
abstract class ExecSubqueryExpression extends PlanExpression[BaseSubqueryExec] {
  /**
   * Fill the expression with collected result from executed plan.
   */
  def updateResult(): Unit

  override def canonicalize(attrs: AttributeSeq): ExecSubqueryExpression = {
    withNewPlan(plan.canonicalized.asInstanceOf[BaseSubqueryExec])
      .asInstanceOf[ExecSubqueryExpression]
  }
}

object ExecSubqueryExpression {
  /**
   * Returns true when an expression contains a subquery
   */
  def hasSubquery(e: Expression): Boolean = {
    e.find {
      case _: ExecSubqueryExpression => true
      case _ => false
    }.isDefined
  }
}

/**
 * Exists is used to test for the existence of any record in a subquery.
 *
 * This is the physical copy of Exists to be used inside SparkPlan.
 */
case class Exists(
    plan: BaseSubqueryExec,
    exprId: ExprId)
  extends ExecSubqueryExpression {

  override def dataType: DataType = BooleanType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = false
  override def toString: String = plan.simpleString(SQLConf.get.maxToStringFields)
  override def withNewPlan(plan: BaseSubqueryExec): Exists = copy(plan = plan)

  // Whether the subquery returns one or more records
  @volatile private var result: Boolean = _
  @volatile private var updated: Boolean = false

  def updateResult(): Unit = {
    val rows = plan.executeCollect()
    result = rows.nonEmpty
    updated = true
  }

  override def eval(input: InternalRow): Boolean = {
    require(updated, s"$this has not finished")
    result
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    require(updated, s"$this has not finished")
    Literal.create(result, BooleanType).doGenCode(ctx, ev)
  }
}

/**
 * Evaluates to `true` if `values` are returned in the subquery's result set.
 * If `values` are not found in the subquery's result set, and there are nulls in
 * `values` or the result set, it should return null.
 * This is the physical copy of InSubquery to be used inside SparkPlan.
 */
case class InSubquery(
    values: Seq[Literal],
    plan: BaseSubqueryExec,
    exprId: ExprId)
  extends ExecSubqueryExpression {
  override def dataType: DataType = BooleanType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = true
  override def toString: String = plan.simpleString(SQLConf.get.maxToStringFields)
  override def withNewPlan(plan: BaseSubqueryExec): InSubquery = copy(plan = plan)

  @volatile private var result: Boolean = false
  @volatile private var isNull: Boolean = false
  @volatile private var updated: Boolean = false

  def updateResult(): Unit = {
    // The semantic of '(a,b) in ((x1, y1), (x2, y2), ...)' is
    // '(a = x1 and b = y1) or (a = x2 and b = y2) or ...'
    val rows = plan.executeCollect()
    var hasNullInDisjunction = false
    val leftValues = values.map(_.value)
    result = rows.exists(row => {
      var hasNullInConjunction = false
      val rowValues = row.toSeq(plan.schema)
      val allTrueInConjunction = leftValues.zip(rowValues).forall({
        case (left, right) =>
          if (left == null) {
            hasNullInConjunction = true
            true
          } else {
            if (right == null) {
              hasNullInConjunction = true
              true
            } else {
              left == right
            }
          }
      })
      if (allTrueInConjunction && !hasNullInConjunction) {
        true
      } else {
        if (allTrueInConjunction && hasNullInConjunction) {
          hasNullInDisjunction = true
        }
        false
      }
    })
    if (!result && hasNullInDisjunction) {
      isNull = true
    }
    updated = true
  }

  override def eval(input: InternalRow): Any = {
    require(updated, s"$this has not finished")
    if (isNull) {
      null
    } else {
      result
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    require(updated, s"$this has not finished")
    if (isNull) {
      Literal.create(null, BooleanType).doGenCode(ctx, ev)
    } else {
      Literal.create(result, BooleanType).doGenCode(ctx, ev)
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
  extends ExecSubqueryExpression {

  override def dataType: DataType = plan.schema.fields.head.dataType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = true
  override def toString: String = plan.simpleString(SQLConf.get.maxToStringFields)
  override def withNewPlan(query: BaseSubqueryExec): ScalarSubquery = copy(plan = query)

  override def semanticEquals(other: Expression): Boolean = other match {
    case s: ScalarSubquery => plan.sameResult(s.plan)
    case _ => false
  }

  // the first column in first row from `query`.
  @volatile private var result: Any = _
  @volatile private var updated: Boolean = false

  def updateResult(): Unit = {
    val rows = plan.executeCollect()
    if (rows.length > 1) {
      sys.error(s"more than one row returned by a subquery used as an expression:\n$plan")
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
 * Plans scalar subqueries from that are present in the given [[SparkPlan]].
 */
case class PlanSubqueries(sparkSession: SparkSession) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressions {
      case subquery: expressions.ScalarSubquery =>
        val executedPlan = new QueryExecution(sparkSession, subquery.plan).executedPlan
        ScalarSubquery(
          SubqueryExec(s"subquery${subquery.exprId.id}", executedPlan),
          subquery.exprId)
      case subquery: expressions.Exists =>
        val executedPlan = new QueryExecution(sparkSession, subquery.plan).executedPlan
        Exists(
          SubqueryExec(s"exists${subquery.exprId.id}", executedPlan),
          subquery.exprId
        )
      case expressions.InSubquery(values, subquery) =>
        val executedPlan = new QueryExecution(sparkSession, subquery.plan).executedPlan
        InSubquery(
          values.map(_.asInstanceOf[Literal]),
          SubqueryExec(s"in${subquery.exprId.id}", executedPlan),
          subquery.exprId
        )
    }
  }
}


/**
 * Find out duplicated subqueries in the spark plan, then use the same subquery result for all the
 * references.
 */
case class ReuseSubquery(conf: SQLConf) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.subqueryReuseEnabled) {
      return plan
    }
    // Build a hash map using schema of subqueries to avoid O(N*N) sameResult calls.
    val subqueries = mutable.HashMap[StructType, ArrayBuffer[BaseSubqueryExec]]()
    plan transformAllExpressions {
      case sub: ExecSubqueryExpression =>
        val sameSchema =
          subqueries.getOrElseUpdate(sub.plan.schema, ArrayBuffer[BaseSubqueryExec]())
        val sameResult = sameSchema.find(_.sameResult(sub.plan))
        if (sameResult.isDefined) {
          sub.withNewPlan(ReusedSubqueryExec(sameResult.get))
        } else {
          sameSchema += sub.plan
          sub
        }
    }
  }
}
