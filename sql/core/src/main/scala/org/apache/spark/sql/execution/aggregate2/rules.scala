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

package org.apache.spark.sql.execution.aggregate2

import org.apache.spark.sql.{SQLConf, AnalysisException, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{Average => Average1, AggregateExpression1}
import org.apache.spark.sql.catalyst.expressions.aggregate2.{Average => Average2, DistinctAggregateExpression1, AggregateExpression2, Complete}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

case class ConvertAggregateFunction(context: SQLContext) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case p: LogicalPlan if !p.childrenResolved => p

    case p: Aggregate if context.conf.useSqlAggregate2 => p.transformExpressionsDown {
      case DistinctAggregateExpression1(Average1(child)) =>
        AggregateExpression2(Average2(child), Complete, true)
      case Average1(child) => AggregateExpression2(Average2(child), Complete, false)
    }
    case p: Aggregate if !context.conf.useSqlAggregate2 => p.transformExpressionsDown {
      // If aggregate2 is not enabled, just remove DistinctAggregateExpression1.
      case DistinctAggregateExpression1(agg1) => agg1
    }
  }
}

case class CheckAggregateFunction(context: SQLContext) extends (LogicalPlan => Unit) {
  def failAnalysis(msg: String): Nothing = { throw new AnalysisException(msg) }

  def apply(plan: LogicalPlan): Unit = plan.foreachUp {
    case p: Aggregate if context.conf.useSqlAggregate2 => {
      p.transformExpressionsUp {
        case agg: AggregateExpression1 =>
          failAnalysis(
            s"${SQLConf.USE_SQL_AGGREGATE2.key} is enabled. Please disable it to use $agg.")
        case DistinctAggregateExpression1(agg: AggregateExpression1) =>
          failAnalysis(
            s"${SQLConf.USE_SQL_AGGREGATE2.key} is enabled. " +
              s"Please disable it to use $agg with DISTINCT keyword.")
      }

      val distinctColumnSets = p.aggregateExpressions.flatMap { expr =>
        expr.collect {
          case AggregateExpression2(func, mode, isDistinct) if isDistinct => func.children
        }
      }.distinct
      if (distinctColumnSets.length > 1) {
        // TODO: Provide more information in the error message.
        // There are more than one distinct column sets. For example, sum(distinct a) and
        // sum(distinct b) will generate two distinct column sets, {a} and {b}.
        failAnalysis(s"When ${SQLConf.USE_SQL_AGGREGATE2.key} is enabled, " +
          s"only a single distinct column set is supported.")
      }
    }
    case p: Aggregate if !context.conf.useSqlAggregate2 => p.transformExpressionsUp {
      case agg: AggregateExpression2 =>
        failAnalysis(
          s"${SQLConf.USE_SQL_AGGREGATE2.key} is disabled. Please enable it to use $agg.")
    }
    case _ =>
  }
}
