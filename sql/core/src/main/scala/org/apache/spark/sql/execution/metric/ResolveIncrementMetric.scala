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

package org.apache.spark.sql.execution.metric

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.{UnresolvedIncrementMetricIf, UnresolvedIncrementMetricIfThenReturn}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.write.RowLevelOperation
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{ReplaceDataExec, V2ExistingTableWriteExec, WriteDeltaExec}

/**
 * Resolves [[UnresolvedIncrementMetricIf]] and [[UnresolvedIncrementMetricIfThenReturn]]
 * expressions in a subtree into their resolved counterparts using a provided metrics map.
 *
 * Used as an AQE preprocessing rule so that resolution survives AQE replanning (which
 * re-creates physical plans from logical plans).
 *
 * @param metricsMap mapping from metric name to SQLMetric accumulator.
 */
case class ResolveIncrementMetric(metricsMap: Map[String, SQLMetric])
  extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (metricsMap.isEmpty) return plan
    plan.transformAllExpressions {
      case UnresolvedIncrementMetricIf(cond, name) =>
        IncrementMetricIf(cond, metricsMap(name))
      case UnresolvedIncrementMetricIfThenReturn(cond, ret, name) =>
        IncrementMetricIfThenReturn(cond, ret, metricsMap(name))
    }
  }
}

/**
 * Top-level preparation rule that finds V2 write exec nodes with a `rowLevelCommand`,
 * creates operation SQLMetrics, resolves [[UnresolvedIncrementMetricIf]] and
 * [[UnresolvedIncrementMetricIfThenReturn]] expressions in the child plan, and stores the
 * metrics on the exec node.
 */
object ResolveIncrementMetrics extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case w: V2ExistingTableWriteExec if w.rowLevelCommand.isDefined && w.operationMetrics.isEmpty =>
      val metricsMap = createOperationMetrics(w.rowLevelCommand.get)
      val resolved = ResolveIncrementMetric(metricsMap).apply(w.child)
      val withChild = w.withNewChildren(Seq(resolved))
      setOperationMetrics(withChild, metricsMap)
  }

  private def createOperationMetrics(cmd: RowLevelOperation.Command): Map[String, SQLMetric] = {
    val sc = SparkContext.getOrCreate()
    cmd match {
      case RowLevelOperation.Command.UPDATE =>
        Seq(
          "numUpdatedRows",
          "numCopiedRows"
        ).map { name =>
          name -> SQLMetrics.createMetric(sc, name)
        }.toMap
      case _ => Map.empty
    }
  }

  private def setOperationMetrics(
      plan: SparkPlan,
      metricsMap: Map[String, SQLMetric]): SparkPlan = plan match {
    case r: ReplaceDataExec => r.copy(operationMetrics = metricsMap)
    case d: WriteDeltaExec => d.copy(operationMetrics = metricsMap)
    case other => other
  }
}
