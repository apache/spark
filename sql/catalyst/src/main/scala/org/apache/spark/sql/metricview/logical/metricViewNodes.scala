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

package org.apache.spark.sql.metricview.logical

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.trees.TreePattern.{METRIC_VIEW_PLACEHOLDER, RESOLVED_METRIC_VIEW, TreePattern}
import org.apache.spark.sql.metricview.serde.MetricView

case class MetricViewPlaceholder(
    metadata: CatalogTable,
    desc: MetricView,
    outputMetrics: Seq[Attribute],
    child: LogicalPlan,
    isCreate: Boolean = false) extends UnaryNode {
  final override val nodePatterns: Seq[TreePattern] = Seq(METRIC_VIEW_PLACEHOLDER)
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
  override def output: Seq[Attribute] = outputMetrics
  override lazy val resolved: Boolean = child.resolved
  override def simpleString(maxFields: Int): String =
    s"$nodeName ${output.mkString("[", ", ", "]")}".trim

  override def producedAttributes: AttributeSet = AttributeSet(outputMetrics)
}

case class ResolvedMetricView(
    identifier: TableIdentifier,
    child: LogicalPlan) extends UnaryNode {
  override def output: scala.Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
  override lazy val resolved: Boolean = child.resolved
  final override val nodePatterns: Seq[TreePattern] = Seq(RESOLVED_METRIC_VIEW)
}
