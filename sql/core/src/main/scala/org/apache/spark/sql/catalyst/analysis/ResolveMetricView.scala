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
import org.apache.spark.sql.metricview.serde.{Column => CanonicalColumn, Constants => MetricViewConstants, Expression => CanonicalExpression, JsonUtils, MetricView => CanonicalMetricView, _}
import org.apache.spark.sql.types.{DataType, Metadata, MetadataBuilder}

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
        val sourceProjList = sourceOutput.filterNot { attr =>
          // conflict with dimensions
          metricView.outputMetrics
            .resolve(Seq(attr.name), session.sessionState.conf.resolver)
            .exists(a => dimensions.exists(_.exprId == a.exprId))
        }.map { attr =>
          if (attr.metadata.contains(MetricViewConstants.COLUMN_TYPE_PROPERTY_KEY)) {
            // no alias for metric view column since the measure reference needs to use the
            // measure column in MetricViewPlaceholder, but an alias will change the exprId
            attr
          } else {
            // add an alias to the source column so they are stable with DeduplicateRelation
            Alias(attr, attr.name)()
          }
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
        val res = withDimensions match {
          case aggregate: Aggregate => transformAggregateWithMeasures(
            aggregate,
            measures
          )
          case other =>
            throw SparkException.internalError("ran into unexpected node: " + other)
        }
        res
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
      columns: Seq[CanonicalColumn[_ <: CanonicalExpression]]
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
