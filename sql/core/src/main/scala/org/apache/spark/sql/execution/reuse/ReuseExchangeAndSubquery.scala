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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, ReusedSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.util.ReuseMap

/**
 * Find out duplicated exchanges and subqueries in the whole spark plan including subqueries, then
 * use the same exchange or subquery for all the references.
 */
case object ReuseExchangeAndSubquery extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (conf.exchangeReuseEnabled || conf.subqueryReuseEnabled) {
      val exchanges = new ReuseMap[Exchange, SparkPlan]()
      val subqueries = new ReuseMap[BaseSubqueryExec, SparkPlan]()

      def reuse(plan: SparkPlan): SparkPlan = plan.transformUp {
        case exchange: Exchange if conf.exchangeReuseEnabled =>
          exchanges.reuseOrElseAdd(exchange, ReusedExchangeExec(exchange.output, _))

        case other => other.transformExpressionsUp {
          case sub: ExecSubqueryExpression =>
            val subquery = reuse(sub.plan).asInstanceOf[BaseSubqueryExec]
            sub.withNewPlan(
              if (conf.subqueryReuseEnabled) {
                subqueries.reuseOrElseAdd(subquery, ReusedSubqueryExec(_))
              } else {
                subquery
              }
            )
        }
      }

      reuse(plan)
    } else {
      plan
    }
  }
}
