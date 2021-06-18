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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning.{OperationHelper, ScanOperation}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.connector.expressions.Aggregation
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, V1Scan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType

object V2ScanRelationPushDown extends Rule[LogicalPlan] with AliasHelper
  with OperationHelper with PredicateHelper {
  import DataSourceV2Implicits._

  def apply(plan: LogicalPlan): LogicalPlan = {
    applyColumnPruning(pushdownAggregate(pushDownFilters(createScanBuilder(plan))))
  }

  private def createScanBuilder(plan: LogicalPlan) = plan.transform {
    case r: DataSourceV2Relation =>
      ScanBuilderHolder(r.output, r, r.table.asReadable.newScanBuilder(r.options))
  }

  private def pushDownFilters(plan: LogicalPlan) = plan.transform {
    // update the scan builder with filter push down and return a new plan with filter pushed
    case filter @ Filter(_, sHolder: ScanBuilderHolder) =>
      val (filters, _, _) = collectFilters(filter).get

      val normalizedFilters =
        DataSourceStrategy.normalizeExprs(filters, sHolder.relation.output)
      val (normalizedFiltersWithSubquery, normalizedFiltersWithoutSubquery) =
        normalizedFilters.partition(SubqueryExpression.hasSubquery)

      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (pushedFilters, postScanFiltersWithoutSubquery) = PushDownUtils.pushFilters(
        sHolder.builder, normalizedFiltersWithoutSubquery)
      val postScanFilters = postScanFiltersWithoutSubquery ++ normalizedFiltersWithSubquery

      logInfo(
        s"""
           |Pushing operators to ${sHolder.relation.name}
           |Pushed Filters: ${pushedFilters.mkString(", ")}
           |Post-Scan Filters: ${postScanFilters.mkString(",")}
         """.stripMargin)

      val filterCondition = postScanFilters.reduceLeftOption(And)
      filterCondition.map(Filter(_, sHolder)).getOrElse(sHolder)
  }

  def pushdownAggregate(plan: LogicalPlan): LogicalPlan = plan.transform {
    // update the scan builder with agg pushdown and return a new plan with agg pushed
    case aggNode @ Aggregate(groupingExpressions, resultExpressions, child) =>
      child match {
        case ScanOperation(project, filters, sHolder: ScanBuilderHolder) =>
          sHolder.builder match {
            case r: SupportsPushDownAggregates =>
              if (filters.length == 0) {  // can't push down aggregate if postScanFilters exist
                if (r.supportsGlobalAggregatePushDownOnly() && groupingExpressions.nonEmpty) {
                  aggNode // return original plan node
                } else {
                  val aggregates = getAggregateExpression(resultExpressions, project, sHolder)
                  val pushedAggregates = PushDownUtils
                    .pushAggregates(sHolder.builder, aggregates, groupingExpressions)
                  if (pushedAggregates.aggregateExpressions.isEmpty) {
                    aggNode // return original plan node
                  } else {
                    // use the aggregate columns as the output columns
                    // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
                    // SELECT min(c1), max(c1) FROM t;
                    // Use min(c1), max(c1) as output for DataSourceV2ScanRelation
                    // We want to have the following logical plan:
                    // == Optimized Logical Plan ==
                    // Aggregate [min(min(c1)#21) AS min(c1)#17, max(max(c1)#22) AS max(c1)#18]
                    // +- RelationV2[min(c1)#21, max(c1)#22] parquet file ...
                    val output = aggregates.map {
                      case agg: AggregateExpression =>
                        AttributeReference(toPrettySQL(agg), agg.dataType)()
                    }

                    // No need to do column pruning because only the aggregate columns are used as
                    // DataSourceV2ScanRelation output columns. All the other columns are not
                    // included in the output. Since PushDownUtils.pruneColumns is not called,
                    // ScanBuilder.requiredSchema is not pruned, but ScanBuilder.requiredSchema is
                    // not used anyways. The schema for aggregate columns will be built in Scan.
                    val scan = sHolder.builder.build()

                    logInfo(
                      s"""
                         |Pushing operators to ${sHolder.relation.name}
                         |Pushed Aggregate Functions:
                         | ${pushedAggregates.aggregateExpressions.mkString(", ")}
                         |Output: ${output.mkString(", ")}
                      """.stripMargin)

                    val scanRelation = DataSourceV2ScanRelation(sHolder.relation, scan, output)
                    val plan = Aggregate(groupingExpressions, resultExpressions, scanRelation)

                    // Change the optimized logical plan to reflect the pushed down aggregate
                    // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
                    // SELECT min(c1), max(c1) FROM t;
                    // The original logical plan is
                    // Aggregate [min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
                    // +- RelationV2[c1#9] parquet ...
                    //
                    // After change the V2ScanRelation output to [min(_1)#21, max(_1)#22]
                    // we have the following
                    // !Aggregate [min(_1#9) AS min(_1)#17, max(_1#9) AS max(_1)#18]
                    // +- RelationV2[min(_1)#21, max(_1)#22] parquet ...
                    //
                    // We want to change it to
                    // == Optimized Logical Plan ==
                    // Aggregate [min(min(c1)#21) AS min(c1)#17, max(max(c1)#22) AS max(c1)#18]
                    // +- RelationV2[min(c1)#21, max(c1)#22] parquet file ...
                    var i = 0
                    plan.transformExpressions {
                      case agg: AggregateExpression =>
                        i += 1
                        val aggFunction: aggregate.AggregateFunction =
                          agg.aggregateFunction match {
                            case _: aggregate.Max => aggregate.Max(output(i - 1))
                            case _: aggregate.Min => aggregate.Min(output(i - 1))
                            case _: aggregate.Sum => aggregate.Sum(output(i - 1))
                            case _: aggregate.Count => aggregate.Sum(output(i - 1))
                            case _ => agg.aggregateFunction
                          }
                        agg.copy(aggregateFunction = aggFunction, filter = None)
                    }
                  }
                }
              } else {
                aggNode
              }
            case _ => aggNode
          }
        case _ => aggNode
      }
  }

  def applyColumnPruning(plan: LogicalPlan): LogicalPlan = plan.transform {
    case ScanOperation(project, filters, sHolder: ScanBuilderHolder) =>
      // column pruning
      val normalizedProjects = DataSourceStrategy
        .normalizeExprs(project, sHolder.output)
        .asInstanceOf[Seq[NamedExpression]]
      val (scan, output) = PushDownUtils.pruneColumns(
        sHolder.builder, sHolder.relation, normalizedProjects, filters)

      logInfo(
        s"""
           |Output: ${output.mkString(", ")}
         """.stripMargin)

      val wrappedScan = scan match {
        case v1: V1Scan =>
          val translated = filters.flatMap(DataSourceStrategy.translateFilter(_, true))
          val pushedFilters = sHolder.builder match {
            case f: SupportsPushDownFilters =>
              f.pushedFilters()
            case _ => Array.empty[sources.Filter]
          }
          V1ScanWrapper(v1, translated, pushedFilters, Aggregation.empty)
        case _ => scan
      }

      val scanRelation = DataSourceV2ScanRelation(sHolder.relation, wrappedScan, output)

      val projectionOverSchema = ProjectionOverSchema(output.toStructType)
      val projectionFunc = (expr: Expression) => expr transformDown {
        case projectionOverSchema(newExpr) => newExpr
      }

      val filterCondition = filters.reduceLeftOption(And)
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

  private def getAggregateExpression(
      resultExpressions: Seq[NamedExpression],
      project: Seq[NamedExpression],
      sHolder: ScanBuilderHolder): Seq[AggregateExpression] = {
    val aggregates = resultExpressions.flatMap { expr =>
      expr.collect {
        case agg: AggregateExpression =>
          replaceAlias(agg, getAliasMap(project)).asInstanceOf[AggregateExpression]
      }
    }
    DataSourceStrategy.normalizeExprs(aggregates, sHolder.relation.output)
      .asInstanceOf[Seq[AggregateExpression]]
  }

  private def collectFilters(plan: LogicalPlan):
    Option[(Seq[Expression], LogicalPlan, AttributeMap[Expression])] = {
    plan match {
      case Filter(condition, child) =>
        collectFilters(child) match {
          case Some((filters, other, aliases)) =>
            // Follow CombineFilters and only keep going if 1) the collected Filters
            // and this filter are all deterministic or 2) if this filter is the first
            // collected filter and doesn't have common non-deterministic expressions
            // with lower Project.
            val substitutedCondition = substitute(aliases)(condition)
            val canCombineFilters = (filters.nonEmpty && filters.forall(_.deterministic) &&
              substitutedCondition.deterministic) || filters.isEmpty
            if (canCombineFilters && !hasCommonNonDeterministic(Seq(condition), aliases)) {
              Some((filters ++ splitConjunctivePredicates(substitutedCondition),
                other, aliases))
            } else {
              None
            }
          case None => None
        }

      case other =>
        Some((Nil, other, AttributeMap(Seq())))
    }
  }
}

case class ScanBuilderHolder(
    output: Seq[AttributeReference],
    relation: DataSourceV2Relation,
    builder: ScanBuilder) extends LeafNode

// A wrapper for v1 scan to carry the translated filters and the handled ones. This is required by
// the physical v1 scan node.
case class V1ScanWrapper(
    v1Scan: V1Scan,
    translatedFilters: Seq[sources.Filter],
    handledFilters: Seq[sources.Filter],
    pushedAggregates: Aggregation) extends Scan {
  override def readSchema(): StructType = v1Scan.readSchema()
}
