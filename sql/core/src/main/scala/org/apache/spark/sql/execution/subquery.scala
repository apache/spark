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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{ExprId, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.DataType

/**
 * A subquery that will return only one row and one column.
 *
 * This is the physical copy of ScalarSubquery to be used inside SparkPlan.
 */
case class ScalarSubquery(
    @transient executedPlan: SparkPlan,
    exprId: ExprId)
  extends SubqueryExpression {

  override def query: LogicalPlan = throw new UnsupportedOperationException
  override def withNewPlan(plan: LogicalPlan): SubqueryExpression = {
    throw new UnsupportedOperationException
  }
  override def plan: SparkPlan = Subquery(simpleString, executedPlan)

  override def dataType: DataType = executedPlan.schema.fields.head.dataType
  override def nullable: Boolean = true
  override def toString: String = s"subquery#${exprId.id}"

  // the first column in first row from `query`.
  private var result: Any = null

  def updateResult(v: Any): Unit = {
    result = v
  }

  override def eval(input: InternalRow): Any = result

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): String = {
    Literal.create(result, dataType).doGenCode(ctx, ev)
  }
}

/**
 * Plans scalar subqueries from that are present in the given [[SparkPlan]].
 */
case class PlanSubqueries(sqlContext: SQLContext) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressions {
      case subquery: expressions.ScalarSubquery =>
        val executedPlan = new QueryExecution(sqlContext, subquery.plan).executedPlan
        ScalarSubquery(executedPlan, subquery.exprId)
    }
  }
}
