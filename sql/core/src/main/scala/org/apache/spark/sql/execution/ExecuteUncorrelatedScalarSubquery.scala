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

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.SubExprUtils.hasOuterReferences
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

class ExecuteUncorrelatedScalarSubquery(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case subquery: expressions.ScalarSubquery if !hasOuterReferences(subquery.plan) =>
      val result = SubqueryEvaluation.ifEnabled(spark) {
        evaluate(subquery)
      }
      result.getOrElse(subquery)
  }

  private def evaluate(subquery: expressions.ScalarSubquery): Literal = {
    val qe = new QueryExecution(spark, subquery.plan)
    val (resultType, rows) = SQLExecution.withNewExecutionId(qe) {
      val physicalPlan = qe.executedPlan
      (physicalPlan.schema.fields.head.dataType, physicalPlan.executeCollect())
    }

    if (rows.length > 1) {
      throw new AnalysisException(
        s"More than one row returned by a subquery used as an expression:\n${subquery.plan}")
    }

    if (rows.length == 1) {
      assert(rows(0).numFields == 1,
        s"Expects 1 field, but got ${rows(0).numFields}; something went wrong in analysis")
      Literal(rows(0).get(0, resultType), resultType)
    } else {
      // If there is no rows returned, the result should be null.
      Literal(null, resultType)
    }
  }
}

object SubqueryEvaluation {
  private val SUBQUERY_EVAL_KEY = "spark.sql.subquery.eval.enabled"

  private def enabled(spark: SparkSession): Boolean = {
    val enabled = spark.sparkContext.getLocalProperty(SUBQUERY_EVAL_KEY)
    if (enabled != null) {
      enabled.toBoolean
    } else {
      spark.sessionState.conf.enableSubqueryEvaluation
    }
  }

  def ifEnabled[T](spark: SparkSession)(body: => T): Option[T] = {
    if (enabled(spark)) {
      Some(body)
    } else {
      None
    }
  }

  def withoutSubqueryEvaluation[T](spark: SparkSession)(body: => T): T = {
    val sc = spark.sparkContext
    val originalValue = sc.getLocalProperty(SUBQUERY_EVAL_KEY)
    try {
      sc.setLocalProperty(SUBQUERY_EVAL_KEY, "false")
      body
    } finally {
      sc.setLocalProperty(SUBQUERY_EVAL_KEY, originalValue)
    }
  }
}
