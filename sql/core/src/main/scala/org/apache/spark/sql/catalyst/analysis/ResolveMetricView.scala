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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Measure}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.METRIC_VIEW_PLACEHOLDER
import org.apache.spark.sql.metricview.logical.{MetricViewPlaceholder, ResolvedMetricView}
import org.apache.spark.sql.metricview.serde.{Column => CanonicalColumn, DimensionExpression, JsonUtils, MeasureExpression, MetricView => CanonicalMetricView}
import org.apache.spark.sql.types.{DataType, Metadata, MetadataBuilder}

/**
 * Analysis rule for resolving metric view operations (CREATE and SELECT).
 *
 * == Background ==
 * A metric view is a special type of view that defines a semantic layer over raw data by
 * declaring dimensions (grouping columns) and measures (pre-aggregated metrics). Users can
 * query metric views using the MEASURE() function to access pre-defined aggregations without
 * needing to know the underlying aggregation logic.
 *
 * == Metric View Definition (YAML) ==
 * A metric view is defined using YAML syntax that specifies:
 * - source: The underlying table or SQL query
 * - where: Optional filter condition applied to the source
 * - select: List of columns, each being either a dimension or measure
 *   - Dimensions: Expressions used for grouping (e.g., "region", "upper(region)")
 *   - Measures: Aggregate expressions (e.g., "sum(count)", "avg(price)")
 *
 * Example YAML definition:
 * {{{
 *   version: "0.1"
 *   source:
 *     asset: "sales_table"
 *   where: "product = 'product_1'"
 *   select:
 *     - name: region
 *       expression: dimension(region)
 *     - name: region_upper
 *       expression: dimension(upper(region))
 *     - name: total_sales
 *       expression: measure(sum(amount))
 *     - name: avg_price
 *       expression: measure(avg(price))
 * }}}
 *
 * This rule handles two distinct workflows:
 *
 * == Workflow 1: CREATE METRIC VIEW ==
 * Purpose: Analyze the metric view definition and derive the output schema for catalog storage.
 *
 * SQL Example:
 * {{{
 *   CREATE VIEW sales_metrics
 *   WITH METRICS
 *   LANGUAGE YAML
 *   AS $$<yaml definition>$$
 * }}}
 *
 * Processing steps:
 * 1. Detect [[MetricViewPlaceholder]] nodes marked for creation (isCreate = true)
 * 2. Parse the YAML definition to extract dimensions and measures
 * 3. Build an [[Aggregate]] logical plan:
 *    {{{
 *      Aggregate(
 *        groupingExpressions = [region, upper(region)],  // all dimensions
 *        aggregateExpressions = [
 *          region,                // dimensions become output columns
 *          upper(region) AS region_upper,
 *          sum(amount) AS total_sales,    // measures with their aggregations
 *          avg(price) AS avg_price
 *        ],
 *        child = Filter(product = 'product_1', sales_table)
 *      )
 *    }}}
 * 4. The analyzer resolves this plan to derive column data types
 * 5. The resolved schema (with metadata about dimensions/measures) is stored in the catalog
 *
 * Key insight: We construct an Aggregate node even though it won't be executed. This allows
 * the analyzer to infer proper data types for measures (e.g., sum(int) -> long).
 *
 * == Workflow 2: SELECT FROM METRIC VIEW ==
 * Purpose: Rewrite user queries to replace MEASURE() function calls with actual aggregations.
 *
 * SQL Example:
 * {{{
 *   SELECT region, MEASURE(total_sales), MEASURE(avg_price)
 *   FROM sales_metrics
 *   WHERE region_upper = 'REGION_1'
 *   GROUP BY region
 * }}}
 *
 * Processing steps:
 * 1. Detect queries against metric views (identified by [[MetricViewReadOperation]])
 * 2. Load and parse the stored metric view definition from catalog metadata
 * 3. Build a [[Project]] node that:
 *    - Projects dimension expressions: [region, upper(region) AS region_upper]
 *    - Includes non-conflicting source columns for measure aggregate functions to reference
 *    - Result: The metric view now exposes dimensions as queryable columns
 * 4. Locate [[Aggregate]] nodes containing MEASURE() function calls
 * 5. Substitute each MEASURE() call with its corresponding aggregate expression:
 *    {{{
 *      Before substitution:
 *        Aggregate(
 *          groupingExpressions = [region],
 *          aggregateExpressions = [region, MEASURE(total_sales), MEASURE(avg_price)],
 *          child = Filter(region_upper = 'REGION_1', sales_metrics)
 *        )
 *
 *      After substitution:
 *        Aggregate(
 *          groupingExpressions = [region],
 *          aggregateExpressions = [region, sum(amount), avg(price)],
 *          child = Filter(region_upper = 'REGION_1',
 *                   Project([upper(region) AS region_upper, region, amount, price],
 *                     Filter(product = 'product_1', sales_table)))
 *        )
 *    }}}
 * 6. Return the rewritten plan for further optimization and execution
 *
 * Key behaviors:
 * - Dimensions can be used directly in SELECT, WHERE, GROUP BY, ORDER BY
 * - Measures must be accessed via MEASURE() function and can only appear in aggregate context
 * - The WHERE clause from the metric view definition is automatically applied
 * - Source table columns are hidden from the metric view
 *
 * Example query patterns:
 * {{{
 *   -- Dimension only (no aggregation needed)
 *   SELECT region_upper FROM sales_metrics GROUP BY 1
 *   => SELECT upper(region) FROM sales_table WHERE product = 'product_1' GROUP BY 1
 *
 *   -- Measure only (aggregates entire dataset)
 *   SELECT MEASURE(total_sales) FROM sales_metrics
 *   => SELECT sum(amount) FROM sales_table WHERE product = 'product_1'
 *
 *   -- Dimensions + Measures (group by dimensions)
 *   SELECT region, MEASURE(total_sales) FROM sales_metrics GROUP BY region
 *   => SELECT region, sum(amount) FROM sales_table
 *      WHERE product = 'product_1' GROUP BY region
 * }}}
 *
 * The rule operates on unresolved plans and transforms [[MetricViewPlaceholder]] nodes
 * into resolved logical plans that can be further optimized and executed.
 */
case class ResolveMetricView(session: SparkSession) extends Rule[LogicalPlan] {
  private def parser: ParserInterface = session.sessionState.sqlParser
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.containsPattern(METRIC_VIEW_PLACEHOLDER)) {
      return plan
    }
    plan.resolveOperatorsUp {
      // CREATE PATH: to create a metric view, we need to analyze the metric view
      // definition and get the output schema (with column metadata). Since the measures
      // are aggregate functions, we need to use an Aggregate node and group by all
      // dimensions to get the output schema.
      case mvp: MetricViewPlaceholder if mvp.isCreate && mvp.child.resolved =>
        val (dimensions, measures) = buildMetricViewOutput(mvp.desc)
        Aggregate(
          // group by all dimensions
          dimensions.map(_.toAttribute).toSeq,
          // select all dimensions and measures to get the final output (mostly data types)
          (dimensions ++ measures).toSeq,
          mvp.child
        )

      // SELECT PATH: to read a metric view, user will use the `MEASURE` aggregate function
      // to read the measures, so it'll lead to an Aggregate node. This way, we only need to
      // Resolve the Aggregate node based on the metric view output and then replace
      // the AttributeReference of the metric view output to the actual expressions.
      case node @ MetricViewReadOperation(metricView) =>
        // step 1: parse the metric view definition
        val (dimensions, measures) =
          parseMetricViewColumns(metricView.outputMetrics, metricView.desc.select)

        // step 2: build the Project node containing the dimensions
        val dimensionExprs = dimensions.map(_.namedExpr)
        // Drop the source columns if it conflicts with dimensions
        val sourceOutput = metricView.child.output
        // 1. hide the column conflict with dimensions
        // 2. add an alias to the source column so they are stable with DeduplicateRelation
        // 3. metric view output should use the same exprId
        val dimensionAttrs = metricView.outputMetrics.filter(a =>
          dimensions.exists(_.exprId == a.exprId)
        )
        val sourceProjList = sourceOutput.filterNot { attr =>
          // conflict with dimensions
          dimensionAttrs
            .resolve(Seq(attr.name), session.sessionState.conf.resolver)
            .nonEmpty
        }.map { attr =>
          // add an alias to the source column so they are stable with DeduplicateRelation
          Alias(attr, attr.name)()
        }
        val withDimensions = node.transformDownWithPruning(
          _.containsPattern(METRIC_VIEW_PLACEHOLDER)) {
          case mv: MetricViewPlaceholder
            if mv.metadata.identifier == metricView.metadata.identifier =>
            ResolvedMetricView(
              mv.metadata.identifier,
              Project(sourceProjList ++ dimensionExprs, mv.child)
            )
        }

        // step 3: resolve the measure references in Aggregate node
        withDimensions match {
          case aggregate: Aggregate => transformAggregateWithMeasures(
            aggregate,
            measures
          )
          case other =>
            throw SparkException.internalError("ran into unexpected node: " + other)
        }
    }
  }

  private def buildMetricViewOutput(metricView: CanonicalMetricView)
  : (Seq[NamedExpression], Seq[NamedExpression]) = {
    val dimensions = new mutable.ArrayBuffer[NamedExpression]()
    val measures = new mutable.ArrayBuffer[NamedExpression]()
    metricView.select.foreach { col =>
      val metadata = new MetadataBuilder()
        .withMetadata(Metadata.fromJson(JsonUtils.toJson(col.getColumnMetadata)))
        .build()
      col.expression match {
        case DimensionExpression(expr) =>
          dimensions.append(
            Alias(parser.parseExpression(expr), col.name)(explicitMetadata = Some(metadata)))
        case MeasureExpression(expr) =>
          measures.append(
            Alias(parser.parseExpression(expr), col.name)(explicitMetadata = Some(metadata)))
      }
    }
    (dimensions.toSeq, measures.toSeq)
  }

  private def parseMetricViewColumns(
      metricViewOutput: Seq[Attribute],
      columns: Seq[CanonicalColumn]
  ): (Seq[MetricViewDimension], Seq[MetricViewMeasure]) = {
    val dimensions = new mutable.ArrayBuffer[MetricViewDimension]()
    val measures = new mutable.ArrayBuffer[MetricViewMeasure]()
    metricViewOutput.zip(columns).foreach { case (attr, column) =>
      column.expression match {
        case DimensionExpression(expr) =>
          dimensions.append(
            MetricViewDimension(
              attr.name,
              parser.parseExpression(expr),
              attr.exprId,
              attr.dataType)
          )
        case MeasureExpression(expr) =>
          measures.append(
            MetricViewMeasure(
              attr.name,
              parser.parseExpression(expr),
              attr.exprId,
              attr.dataType)
          )
      }
    }
    (dimensions.toSeq, measures.toSeq)
  }

  private def transformAggregateWithMeasures(
      aggregate: Aggregate,
      measures: Seq[MetricViewMeasure]): LogicalPlan = {
    val measuresMap = measures.map(m => m.exprId -> m).toMap
    val newAggExprs = aggregate.aggregateExpressions.map { expr =>
      expr.transform {
        case AggregateExpression(Measure(a: AttributeReference), _, _, _, _) =>
          measuresMap(a.exprId).expr
      }.asInstanceOf[NamedExpression]
    }
    aggregate.copy(aggregateExpressions = newAggExprs)
  }
}

object MetricViewReadOperation {
  def unapply(plan: LogicalPlan): Option[MetricViewPlaceholder] = {
    plan match {
      case a: Aggregate if a.resolved && a.containsPattern(METRIC_VIEW_PLACEHOLDER) =>
        collectMetricViewNode(a.child)
      case _ =>
        None
    }
  }

  @scala.annotation.tailrec
  private def collectMetricViewNode(plan: LogicalPlan): Option[MetricViewPlaceholder] = {
    plan match {
      case f: Filter => collectMetricViewNode(f.child)
      case s: Expand => collectMetricViewNode(s.child)
      case s: Project => collectMetricViewNode(s.child)
      case s: SubqueryAlias => collectMetricViewNode(s.child)
      case m: MetricViewPlaceholder => Some(m)
      case _ => None
    }
  }
}

sealed trait MetricViewColumn {
  def name: String
  def expr: Expression
  def exprId: ExprId
  def dataType: DataType
  def namedExpr: NamedExpression = {
    Alias(UpCast(expr, dataType), name)(exprId = exprId)
  }
}

case class MetricViewDimension(
    name: String,
    expr: Expression,
    exprId: ExprId,
    dataType: DataType) extends MetricViewColumn

case class MetricViewMeasure(
    name: String,
    expr: Expression,
    exprId: ExprId,
    dataType: DataType) extends MetricViewColumn
