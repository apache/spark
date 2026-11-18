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

package org.apache.spark.sql.execution.reuse

import java.util.IdentityHashMap

import scala.collection.mutable

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, ReusedSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}

/**
 * Find out duplicated exchanges and subqueries in the whole spark plan including subqueries, then
 * use the same exchange or subquery for all the references.
 *
 * Note that the Spark plan is a mutually recursive data structure:
 * SparkPlan -> Expr -> Subquery -> SparkPlan -> Expr -> Subquery -> ...
 * Therefore, in this rule, we recursively rewrite the exchanges and subqueries in a bottom-up way,
 * in one go.
 */
case object ReuseExchangeAndSubquery extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (conf.exchangeReuseEnabled || conf.subqueryReuseEnabled) {
      val exchanges = mutable.Map.empty[SparkPlan, Exchange]
      val subqueries = mutable.Map.empty[SparkPlan, BaseSubqueryExec]
      // Memoize completed recursive results by identity within this rule invocation, avoiding
      // repeated visits to shared subquery roots when subquery reuse is enabled.
      val rewrittenPlans = new IdentityHashMap[SparkPlan, SparkPlan]

      def reuse(plan: SparkPlan): SparkPlan = {
        if (conf.subqueryReuseEnabled) {
          val cached = rewrittenPlans.get(plan)
          if (cached != null) return cached
        }
        val result = plan.transformUpWithPruning(_.containsAnyPattern(EXCHANGE, PLAN_EXPRESSION)) {
          case exchange: Exchange if conf.exchangeReuseEnabled =>
            val cachedExchange = exchanges.getOrElseUpdate(exchange.canonicalized, exchange)
            if (cachedExchange.ne(exchange)) {
              ReusedExchangeExec(exchange.output, cachedExchange)
            } else {
              cachedExchange
            }

          case other =>
            other.transformExpressionsUpWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
              case sub: ExecSubqueryExpression =>
                val subquery = reuse(sub.plan).asInstanceOf[BaseSubqueryExec]
                val newSubquery = if (conf.subqueryReuseEnabled) {
                  val cachedSubquery = subqueries.getOrElseUpdate(subquery.canonicalized, subquery)
                  if (cachedSubquery.ne(subquery)) {
                    ReusedSubqueryExec(cachedSubquery)
                  } else {
                    cachedSubquery
                  }
                } else {
                  subquery
                }
                sub.withNewPlan(newSubquery)
            }
        }
        if (conf.subqueryReuseEnabled) {
          rewrittenPlans.put(plan, result)
        }
        result
      }

      reuse(plan)
    } else {
      plan
    }
  }
}
