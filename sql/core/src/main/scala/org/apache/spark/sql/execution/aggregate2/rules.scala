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
import org.apache.spark.sql.catalyst.expressions.{Average => Average1, AggregateExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate2.{Average => Average2, AggregateExpression2, Complete}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class ConvertAggregateFunction(context: SQLContext) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case p: LogicalPlan if !p.childrenResolved => p

    case p if context.conf.useSqlAggregate2 => p.transformExpressionsUp {
      case Average1(child) => AggregateExpression2(Average2(child), Complete, false)
    }
  }
}

case class CheckAggregateFunction(context: SQLContext) extends (LogicalPlan => Unit) {
  def failAnalysis(msg: String): Nothing = { throw new AnalysisException(msg) }

  def apply(plan: LogicalPlan): Unit = plan.foreachUp {
    case p if context.conf.useSqlAggregate2 => p.transformExpressionsUp {
      case agg: AggregateExpression =>
        failAnalysis(s"${SQLConf.USE_SQL_AGGREGATE2} is enabled. Please disable it to use $agg.")
    }
    case p if !context.conf.useSqlAggregate2 => p.transformExpressionsUp {
      case agg: AggregateExpression2 =>
        failAnalysis(s"${SQLConf.USE_SQL_AGGREGATE2} is disabled. Please enable it to use $agg.")
    }
  }
}
