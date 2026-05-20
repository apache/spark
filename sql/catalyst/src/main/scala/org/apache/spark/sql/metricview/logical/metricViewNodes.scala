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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryCommand, UnaryNode}
import org.apache.spark.sql.catalyst.trees.TreePattern.{METRIC_VIEW_PLACEHOLDER, RESOLVED_METRIC_VIEW, TreePattern}
import org.apache.spark.sql.types.Metadata

/**
 * A parsed metric-view column, populated by [[org.apache.spark.sql.metricview.util.MetricViewPlanner]]
 * from the YAML definition before the placeholder is handed to the analyzer. Carrying the parsed
 * [[Expression]] (rather than the raw YAML descriptor) lets downstream resolution rules read
 * a stable, analyzer-friendly representation without re-parsing.
 */
sealed trait InputColumn {
  def name: String
  def expr: Expression
  def metadata: Metadata
}

case class DimensionInputColumn(
    name: String,
    expr: Expression,
    metadata: Metadata) extends InputColumn

case class MeasureInputColumn(
    name: String,
    expr: Expression,
    metadata: Metadata) extends InputColumn

/**
 * Logical plan for `CREATE VIEW ... WITH METRICS`. This is the v1/v2-agnostic representation
 * the parser returns; downstream analysis decides which runnable form it becomes:
 *  - For the session catalog: [[org.apache.spark.sql.execution.command.CreateMetricViewCommand]]
 *    via an analyzer rule that fires once the identifier is resolved.
 *  - For non-session v2 [[org.apache.spark.sql.connector.catalog.ViewCatalog]]s: a
 *    `CreateV2MetricViewExec` produced by `DataSourceV2Strategy`.
 *
 * Splitting this from the runnable command lets the parser return a single logical shape
 * regardless of target catalog (instead of pre-committing to a runnable command at parse
 * time), and gives downstream rules a single match target to dispatch from.
 */
case class CreateMetricView(
    child: LogicalPlan,
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    properties: Map[String, String],
    originalText: String,
    allowExisting: Boolean,
    replace: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}

case class MetricViewPlaceholder(
    metadata: CatalogTable,
    inputColumns: Seq[InputColumn],
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
