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

package org.apache.spark.sql.execution.datasources.v2

import java.util.Locale

import scala.collection.mutable.ArrayBuilder

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, V1Scan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.{AggregateFunc, Aggregation}
import org.apache.spark.sql.types.StructType

object V2ScanRelationPushDown extends Rule[LogicalPlan] {

  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case Aggregate(groupingExpressions, resultExpressions, child) =>
      child match {
        case ScanOperation(project, filters, relation: DataSourceV2Relation) =>
          val scanBuilder = relation.table.asReadable.newScanBuilder(relation.options)
          val aggregates = resultExpressions.flatMap { expr =>
            expr.collect {
              case agg: AggregateExpression => agg
            }
          }.distinct

          val aggregation = PushDownUtils.pushAggregates(scanBuilder, aggregates,
            groupingExpressions)

          val (pushedFilters, postScanFilters, scan, output, normalizedProjects) =
            processFilerAndColumn(scanBuilder, project, filters, relation)

          logInfo(
            s"""
               |Pushing operators to ${relation.name}
               |Pushed Filters: ${pushedFilters.mkString(", ")}
               |Post-Scan Filters: ${postScanFilters.mkString(",")}
               |Pushed Aggregate Functions: ${aggregation.aggregateExpressions.mkString(", ")}
               |Pushed Groupby: ${aggregation.groupByExpressions.mkString(", ")}
               |Output: ${output.mkString(", ")}
             """.stripMargin)

          val wrappedScan = scan match {
            case v1: V1Scan =>
              val translated = filters.flatMap(DataSourceStrategy.translateFilter(_, true))
              V1ScanWrapper(v1, translated, pushedFilters, aggregation)
            case _ => scan
          }

          if (aggregation.aggregateExpressions.isEmpty) {
            val plan = buildLogicalPlan(project, relation, wrappedScan, output, normalizedProjects,
              postScanFilters)
            Aggregate(groupingExpressions, resultExpressions, plan)
          } else {
            val aggOutputBuilder = ArrayBuilder.make[AttributeReference]
            for (i <- 0 until aggregates.length) {
              aggOutputBuilder += AttributeReference(
                  aggregation.aggregateExpressions(i).toString, aggregates(i).dataType)()
            }
            val aggOutput = aggOutputBuilder.result

            val newOutputBuilder = ArrayBuilder.make[AttributeReference]
            for (col <- aggOutput) {
              newOutputBuilder += col
            }
            for (groupBy <- groupingExpressions) {
                newOutputBuilder += groupBy.asInstanceOf[AttributeReference]
            }
            val newOutput = newOutputBuilder.result

            val r = buildLogicalPlan(newOutput, relation, wrappedScan, newOutput,
              normalizedProjects, postScanFilters)
            val plan = Aggregate(groupingExpressions, resultExpressions, r)

            var i = 0
            plan.transformExpressions {
              case agg: AggregateExpression =>
                i += 1
                val aggFunction: aggregate.AggregateFunction = {
                  if (agg.aggregateFunction.isInstanceOf[aggregate.Max]) {
                    aggregate.Max(aggOutput(i - 1))
                  } else if (agg.aggregateFunction.isInstanceOf[aggregate.Min]) {
                    aggregate.Min(aggOutput(i - 1))
                  } else if (agg.aggregateFunction.isInstanceOf[aggregate.Average]) {
                    aggregate.Average(aggOutput(i - 1))
                  } else if (agg.aggregateFunction.isInstanceOf[aggregate.Sum]) {
                    aggregate.Sum(aggOutput(i - 1))
                  } else {
                    agg.aggregateFunction
                  }
                }
                // Aggregate filter is pushed to datasource
                agg.copy(aggregateFunction = aggFunction, filter = None)
            }
          }

        case _ =>
          Aggregate(groupingExpressions, resultExpressions, child)
      }
    case ScanOperation(project, filters, relation: DataSourceV2Relation) =>
      val scanBuilder = relation.table.asReadable.newScanBuilder(relation.options)

      val (pushedFilters, postScanFilters, scan, output, normalizedProjects) =
        processFilerAndColumn(scanBuilder, project, filters, relation)

      logInfo(
        s"""
           |Pushing operators to ${relation.name}
           |Pushed Filters: ${pushedFilters.mkString(", ")}
           |Post-Scan Filters: ${postScanFilters.mkString(",")}
           |Output: ${output.mkString(", ")}
         """.stripMargin)

      val wrappedScan = scan match {
        case v1: V1Scan =>
          val translated = filters.flatMap(DataSourceStrategy.translateFilter(_, true))
          V1ScanWrapper(v1, translated, pushedFilters,
            Aggregation(Seq.empty[AggregateFunc], Seq.empty[String]))

        case _ => scan
      }

      buildLogicalPlan(project, relation, wrappedScan, output, normalizedProjects, postScanFilters)
  }

  private def processFilerAndColumn(
      scanBuilder: ScanBuilder,
      project: Seq[NamedExpression],
      filters: Seq[Expression],
      relation: DataSourceV2Relation):
  (Seq[sources.Filter], Seq[Expression], Scan, Seq[AttributeReference], Seq[NamedExpression]) = {
    val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, relation.output)
    val (normalizedFiltersWithSubquery, normalizedFiltersWithoutSubquery) =
      normalizedFilters.partition(SubqueryExpression.hasSubquery)

    // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
    // `postScanFilters` need to be evaluated after the scan.
    // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
    val (pushedFilters, postScanFiltersWithoutSubquery) = PushDownUtils.pushFilters(
      scanBuilder, normalizedFiltersWithoutSubquery)
    val postScanFilters = postScanFiltersWithoutSubquery ++ normalizedFiltersWithSubquery

    val normalizedProjects = DataSourceStrategy
      .normalizeExprs(project, relation.output)
      .asInstanceOf[Seq[NamedExpression]]
    val (scan, output) = PushDownUtils.pruneColumns(
      scanBuilder, relation, normalizedProjects, postScanFilters)
    (pushedFilters, postScanFilters, scan, output, normalizedProjects)
  }

  private def buildLogicalPlan(
      project: Seq[NamedExpression],
      relation: DataSourceV2Relation,
      wrappedScan: Scan,
      output: Seq[AttributeReference],
      normalizedProjects: Seq[NamedExpression],
      postScanFilters: Seq[Expression]): LogicalPlan = {
    val scanRelation = DataSourceV2ScanRelation(relation.table, wrappedScan, output)
    val projectionOverSchema = ProjectionOverSchema(output.toStructType)
    val projectionFunc = (expr: Expression) => expr transformDown {
      case projectionOverSchema(newExpr) => newExpr
    }

    val filterCondition = postScanFilters.reduceLeftOption(And)
    val newFilterCondition = filterCondition.map(projectionFunc)
    val withFilter = newFilterCondition.map(Filter(_, scanRelation)).getOrElse(scanRelation)

    val withProjection = if (withFilter.output != project) {
      val newProjects = normalizedProjects
        .map(projectionFunc)
        .asInstanceOf[Seq[NamedExpression]]
      Project(newProjects, withFilter)
    } else {
      withFilter
    }
    withProjection
  }
}

// A wrapper for v1 scan to carry the translated filters and the handled ones. This is required by
// the physical v1 scan node.
case class V1ScanWrapper(
    v1Scan: V1Scan,
    translatedFilters: Seq[sources.Filter],
    handledFilters: Seq[sources.Filter],
    pushedAggregates: sources.Aggregation) extends Scan {
  override def readSchema(): StructType = v1Scan.readSchema()
}
