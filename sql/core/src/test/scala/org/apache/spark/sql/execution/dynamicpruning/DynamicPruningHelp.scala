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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}
import org.apache.spark.sql.execution.{BloomFilterSubqueryExec, FileSourceScanExec, FilterExec, InSubqueryExec, SparkPlan, SubqueryBroadcastExec, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}

trait DynamicPruningHelp extends QueryTest with AdaptiveSparkPlanHelper {

  /**
   * Check if the query plan has a in filter predicate inserted as
   * a subquery duplicate or as a custom broadcast exchange.
   */
  protected def checkInSubqueryPredicate(
      df: DataFrame,
      withSubquery: Boolean,
      withBroadcast: Boolean): Unit = {
    val plan = df.queryExecution.executedPlan
    val dpExprs = collectDynamicPruningExpressions(plan)
    val hasSubquery = dpExprs.exists {
      case InSubqueryExec(_, _: SubqueryExec, _, _) => true
      case _ => false
    }
    val subqueryBroadcast = dpExprs.collect {
      case InSubqueryExec(_, b: SubqueryBroadcastExec, _, _) => b
    }

    val name = "trigger shuffle pruning"
    val hasFilter = if (withSubquery) "Should" else "Shouldn't"
    assert(hasSubquery == withSubquery,
      s"$hasFilter $name with a subquery duplicate:\n${df.queryExecution}")
    val hasBroadcast = if (withBroadcast) "Should" else "Shouldn't"
    assert(subqueryBroadcast.nonEmpty == withBroadcast,
      s"$hasBroadcast $name with a reused broadcast exchange:\n${df.queryExecution}")

    subqueryBroadcast.foreach { s =>
      s.child match {
        case _: ReusedExchangeExec => // reuse check ok.
        case b: BroadcastExchangeExec =>
          val hasReuse = plan.find {
            case ReusedExchangeExec(_, e) => e eq b
            case _ => false
          }.isDefined
          assert(hasReuse, s"$s\nshould have been reused in\n$plan")
        case _ =>
          fail(s"Invalid child node found in\n$s")
      }
    }

    val isMainQueryAdaptive = plan.isInstanceOf[AdaptiveSparkPlanExec]
    subqueriesAll(plan).filterNot(subqueryBroadcast.contains).foreach { s =>
      assert(s.find(_.isInstanceOf[AdaptiveSparkPlanExec]).isDefined == isMainQueryAdaptive)
    }
  }

  /**
   * Check if the query plan has a bloom filter predicate inserted as
   * a subquery duplicate or as a custom broadcast exchange.
   */
  protected def checkBloomFilterSubqueryPredicate(
      df: DataFrame,
      withSubquery: Boolean,
      withBroadcast: Boolean): Unit = {
    val plan = df.queryExecution.executedPlan
    val dpExprs = collectDynamicPruningExpressions(plan)
    val hasSubquery = dpExprs.exists {
      case BloomFilterSubqueryExec(_, _: SubqueryExec, _, _) => true
      case _ => false
    }
    val subqueryBroadcast = dpExprs.collect {
      case BloomFilterSubqueryExec(_, b: SubqueryBroadcastExec, _, _) => b
    }

    val name = "trigger shuffle pruning"
    val hasFilter = if (withSubquery) "Should" else "Shouldn't"
    assert(hasSubquery == withSubquery,
      s"$hasFilter $name with a subquery duplicate:\n${df.queryExecution}")
    val hasBroadcast = if (withBroadcast) "Should" else "Shouldn't"
    assert(subqueryBroadcast.nonEmpty == withBroadcast,
      s"$hasBroadcast $name with a reused broadcast exchange:\n${df.queryExecution}")

    subqueryBroadcast.foreach { s =>
      s.child match {
        case _: ReusedExchangeExec => // reuse check ok.
        case b: BroadcastExchangeExec =>
          val hasReuse = plan.find {
            case ReusedExchangeExec(_, e) => e eq b
            case _ => false
          }.isDefined
          assert(hasReuse, s"$s\nshould have been reused in\n$plan")
        case _ =>
          fail(s"Invalid child node found in\n$s")
      }
    }

    val isMainQueryAdaptive = plan.isInstanceOf[AdaptiveSparkPlanExec]
    subqueriesAll(plan).filterNot(subqueryBroadcast.contains).foreach { s =>
      assert(s.find(_.isInstanceOf[AdaptiveSparkPlanExec]).isDefined == isMainQueryAdaptive)
    }
  }

  /**
   * Check if the plan has the given number of distinct broadcast exchange subqueries.
   */
  protected def checkDistinctSubqueries(df: DataFrame, n: Int): Unit = {
    val buf = collectDynamicPruningExpressions(df.queryExecution.executedPlan).collect {
      case InSubqueryExec(_, b: SubqueryBroadcastExec, _, _) =>
        b.index
      case BloomFilterSubqueryExec(_, b: SubqueryBroadcastExec, _, _) =>
        b.index
    }
    assert(buf.distinct.size == n)
  }

  /**
   * Collect the children of all correctly pushed down dynamic pruning expressions in a spark plan.
   */
  protected def collectDynamicPruningExpressions(plan: SparkPlan): Seq[Expression] = {
    plan.flatMap {
      case s: FileSourceScanExec => s.partitionFilters.collect {
        case d: DynamicPruningExpression => d.child
      }
      case s: FilterExec => s.condition.collect {
        case d: DynamicPruningExpression => d.child
      }
      case _ => Nil
    }
  }

  protected def getTableScan(plan: SparkPlan, tableName: String): SparkPlan = {
    val scanOption =
      find(plan) {
        case s: FileSourceScanExec =>
          s.tableIdentifier.exists(_.table.equals(tableName))
        case _ => false
      }
    assert(scanOption.isDefined)
    scanOption.get
  }

}
