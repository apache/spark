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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.plans.logical.{EmptyRelation, LogicalPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.adaptive.LogicalQueryStage
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.internal.SQLConf

/**
 * :: DeveloperApi ::
 * Stores information about a SQL SparkPlan.
 */
@DeveloperApi
class SparkPlanInfo(
    val nodeName: String,
    val simpleString: String,
    val children: Seq[SparkPlanInfo],
    val metadata: Map[String, String],
    val metrics: Seq[SQLMetricInfo]) {

  override def hashCode(): Int = {
    // hashCode of simpleString should be good enough to distinguish the plans from each other
    // within a plan
    simpleString.hashCode
  }

  override def equals(other: Any): Boolean = other match {
    case o: SparkPlanInfo =>
      nodeName == o.nodeName && simpleString == o.simpleString && children == o.children
    case _ => false
  }
}

private[execution] object SparkPlanInfo {

  private def fromLogicalPlan(plan: LogicalPlan): SparkPlanInfo = {
    val childrenInfo = plan match {
      case LogicalQueryStage(_, physical) => Seq(fromSparkPlan(physical))
      case EmptyRelation(logical) => Seq(fromLogicalPlan(logical))
      case _ => (plan.children ++ plan.subqueries).map(fromLogicalPlan)
    }
    new SparkPlanInfo(
      plan.nodeName,
      plan.simpleString(SQLConf.get.maxToStringFields),
      childrenInfo,
      Map[String, String](),
      Seq.empty)
  }

  def fromSparkPlan(plan: SparkPlan): SparkPlanInfo = {
    val children = plan match {
      case ReusedExchangeExec(_, child) => child :: Nil
      case ReusedSubqueryExec(child) => child :: Nil
      case a: AdaptiveSparkPlanExec => a.executedPlan :: Nil
      case stage: QueryStageExec => stage.plan :: Nil
      case inMemTab: InMemoryTableScanExec => inMemTab.relation.cachedPlan :: Nil
      case EmptyRelationExec(logical) => (logical :: Nil)
      case _ => plan.children ++ plan.subqueries
    }
    val metrics = plan.metrics.toSeq.map { case (key, metric) =>
      new SQLMetricInfo(metric.name.getOrElse(key), metric.id, metric.metricType)
    }

    // dump the file scan metadata (e.g file path) to event log
    val metadata = plan match {
      case fileScan: FileSourceScanLike => fileScan.metadata
      case _ => Map[String, String]()
    }
    val childrenInfo = children.flatMap {
      case child: SparkPlan =>
        Some(fromSparkPlan(child))
      case child: LogicalPlan =>
        Some(fromLogicalPlan(child))
      case _ => None
    }
    new SparkPlanInfo(
      plan.nodeName,
      plan.simpleString(SQLConf.get.maxToStringFields),
      childrenInfo,
      metadata,
      metrics)
  }

  final lazy val EMPTY: SparkPlanInfo = new SparkPlanInfo("", "", Nil, Map.empty, Nil)
}
