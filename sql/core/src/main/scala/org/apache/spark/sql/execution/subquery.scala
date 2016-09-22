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
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId, InSet, Literal, PlanExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, StructType}

/**
 * The base class for subquery that is used in SparkPlan.
 */
abstract class ExecSubqueryExpression extends PlanExpression[SubqueryExec] {
  /**
   * Fill the expression with collected result from executed plan.
   */
  def updateResult(): Unit
}

/**
 * A subquery that will return only one row and one column.
 *
 * This is the physical copy of ScalarSubquery to be used inside SparkPlan.
 */
case class ScalarSubquery(
    plan: SubqueryExec,
    exprId: ExprId)
  extends ExecSubqueryExpression {

  override def dataType: DataType = plan.schema.fields.head.dataType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = true
  override def toString: String = plan.simpleString
  override def withNewPlan(query: SubqueryExec): ScalarSubquery = copy(plan = query)

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
 * A subquery that will check the value of `child` whether is in the result of a query or not.
 */
case class InSubquery(
    child: Expression,
    plan: SubqueryExec,
    exprId: ExprId,
    private var result: Array[Any] = null,
    private var updated: Boolean = false) extends ExecSubqueryExpression {

  override def dataType: DataType = BooleanType
  override def children: Seq[Expression] = child :: Nil
  override def nullable: Boolean = child.nullable
  override def toString: String = s"$child IN ${plan.name}"
  override def withNewPlan(plan: SubqueryExec): InSubquery = copy(plan = plan)

  override def semanticEquals(other: Expression): Boolean = other match {
    case in: InSubquery => child.semanticEquals(in.child) && plan.sameResult(in.plan)
    case _ => false
  }

  def updateResult(): Unit = {
    val rows = plan.executeCollect()
    result = rows.map(_.get(0, child.dataType)).asInstanceOf[Array[Any]]
    updated = true
  }

  override def eval(input: InternalRow): Any = {
    require(updated, s"$this has not finished")
    val v = child.eval(input)
    if (v == null) {
      null
    } else {
      result.contains(v)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    require(updated, s"$this has not finished")
    InSet(child, result.toSet).doGenCode(ctx, ev)
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
      case expressions.PredicateSubquery(query, Seq(e: Expression), _, exprId) =>
        val executedPlan = new QueryExecution(sparkSession, query).executedPlan
        InSubquery(e, SubqueryExec(s"subquery${exprId.id}", executedPlan), exprId)
    }
  }
}


/**
 * Find out duplicated exchanges in the spark plan, then use the same exchange for all the
 * references.
 */
case class ReuseSubquery(conf: SQLConf) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.exchangeReuseEnabled) {
      return plan
    }
    // Build a hash map using schema of exchanges to avoid O(N*N) sameResult calls.
    val subqueries = mutable.HashMap[StructType, ArrayBuffer[SubqueryExec]]()
    plan transformAllExpressions {
      case sub: ExecSubqueryExpression =>
        val sameSchema = subqueries.getOrElseUpdate(sub.plan.schema, ArrayBuffer[SubqueryExec]())
        val sameResult = sameSchema.find(_.sameResult(sub.plan))
        if (sameResult.isDefined) {
          sub.withNewPlan(sameResult.get)
        } else {
          sameSchema += sub.plan
          sub
        }
    }
  }
}
