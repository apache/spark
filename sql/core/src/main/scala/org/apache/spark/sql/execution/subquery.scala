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
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A subquery that will return only one row and one column.
 *
 * This is the physical copy of ScalarSubquery to be used inside SparkPlan.
 */
case class ScalarSubquery(
    executedPlan: SparkPlan,
    exprId: ExprId)
  extends SubqueryExpression {

  override def query: LogicalPlan = throw new UnsupportedOperationException
  override def withNewPlan(plan: LogicalPlan): SubqueryExpression = {
    throw new UnsupportedOperationException
  }
  override def plan: SparkPlan = SubqueryExec(simpleString, executedPlan)

  override def dataType: DataType = executedPlan.schema.fields.head.dataType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = true
  override def toString: String = s"subquery#${exprId.id}"

  // the first column in first row from `query`.
  @volatile private var result: Any = null
  @volatile private var updated: Boolean = false
  @volatile private var executed: Boolean = false

  def isExecuted: Boolean = executed

  def updateExecutedState() : Unit = {
    executed = true
  }

  def updateResult(v: Any): Unit = {
    result = v
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

  override def equals(o: Any): Boolean = o match {
    case other: ScalarSubquery => this.eq(other)
    case _ => false
  }

  override def hashCode: Int = exprId.hashCode()
}

/**
 * A wrapper for reused uncorrelated ScalarSubquery to avoid the re-computing for subqueries with
 * the same "canonical" logical plan in a query, because uncorrelated subqueries with the same
 * "canonical" logical plan always produce the same results.
 */
case class ReusedScalarSubquery(
    exprId: ExprId,
    child: ScalarSubquery) extends UnaryExpression {

  override def dataType: DataType = child.dataType
  override def toString: String = s"ReusedSubquery#${exprId.id}($child)"

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => c)

  protected override def nullSafeEval(input: Any): Any = input
}

/**
 * Plans scalar subqueries from that are present in the given [[SparkPlan]].
 */
case class PlanSubqueries(sparkSession: SparkSession) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    // Build a hash map using schema of subquery's logical plan to avoid O(N*N) sameResult calls.
    val subqueryMap = mutable.HashMap[StructType, ArrayBuffer[(LogicalPlan, ScalarSubquery)]]()
    plan.transformAllExpressions {
      case subquery: expressions.ScalarSubquery =>
        val sameSchema = subqueryMap.getOrElseUpdate(
          subquery.query.schema, ArrayBuffer[(LogicalPlan, ScalarSubquery)]())
        val samePlan = sameSchema.find { case (e, _) =>
          subquery.query.sameResult(e)
        }
        if (samePlan.isDefined) {
          // Subqueries that have the same logical plan can be reused the same results.
          ReusedScalarSubquery(subquery.exprId, samePlan.get._2)
        } else {
          val executedPlan = new QueryExecution(sparkSession, subquery.plan).executedPlan
          val physicalSubquery = new ScalarSubquery(executedPlan, subquery.exprId)
          sameSchema += ((subquery.plan, physicalSubquery))
          physicalSubquery
        }
    }
  }
}
