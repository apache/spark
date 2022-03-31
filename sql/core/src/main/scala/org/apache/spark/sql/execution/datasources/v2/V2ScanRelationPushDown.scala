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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, AliasHelper, And, Attribute, AttributeReference, Cast, Expression, IntegerLiteral, NamedExpression, PredicateHelper, ProjectionOverSchema, SortOrder, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LeafNode, Limit, LocalLimit, LogicalPlan, Project, Sample, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.expressions.{SortOrder => V2SortOrder}
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Avg, Count, GeneralAggregateFunc, Sum}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, V1Scan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{DataType, LongType, StructType}
import org.apache.spark.sql.util.SchemaUtils._

object V2ScanRelationPushDown extends Rule[LogicalPlan] with PredicateHelper with AliasHelper {
  import DataSourceV2Implicits._

  def apply(plan: LogicalPlan): LogicalPlan = {
    val pushdownRules = Seq[LogicalPlan => LogicalPlan] (
      createScanBuilder,
      pushDownSample,
      pushDownFilters,
      pushDownAggregates,
      pushDownLimits,
      pruneColumns)

    pushdownRules.foldLeft(plan) { (newPlan, pushDownRule) =>
      pushDownRule(newPlan)
    }
  }

  private def createScanBuilder(plan: LogicalPlan) = plan.transform {
    case r: DataSourceV2Relation =>
      ScanBuilderHolder(r.output, r, r.table.asReadable.newScanBuilder(r.options))
  }

  private def pushDownFilters(plan: LogicalPlan) = plan.transform {
    // update the scan builder with filter push down and return a new plan with filter pushed
    case Filter(condition, sHolder: ScanBuilderHolder) =>
      val filters = splitConjunctivePredicates(condition)
      val normalizedFilters =
        DataSourceStrategy.normalizeExprs(filters, sHolder.relation.output)
      val (normalizedFiltersWithSubquery, normalizedFiltersWithoutSubquery) =
        normalizedFilters.partition(SubqueryExpression.hasSubquery)

      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (pushedFilters, postScanFiltersWithoutSubquery) = PushDownUtils.pushFilters(
        sHolder.builder, normalizedFiltersWithoutSubquery)
      val pushedFiltersStr = if (pushedFilters.isLeft) {
        pushedFilters.left.get.mkString(", ")
      } else {
        sHolder.pushedPredicates = pushedFilters.right.get
        pushedFilters.right.get.mkString(", ")
      }

      val postScanFilters = postScanFiltersWithoutSubquery ++ normalizedFiltersWithSubquery

      logInfo(
        s"""
           |Pushing operators to ${sHolder.relation.name}
           |Pushed Filters: $pushedFiltersStr
           |Post-Scan Filters: ${postScanFilters.mkString(",")}
         """.stripMargin)

      val filterCondition = postScanFilters.reduceLeftOption(And)
      filterCondition.map(Filter(_, sHolder)).getOrElse(sHolder)
  }

  def pushDownAggregates(plan: LogicalPlan): LogicalPlan = plan.transform {
    // update the scan builder with agg pushdown and return a new plan with agg pushed
    case aggNode @ Aggregate(groupingExpressions, resultExpressions, child) =>
      child match {
        case ScanOperation(project, filters, sHolder: ScanBuilderHolder)
          if filters.isEmpty && CollapseProject.canCollapseExpressions(
            resultExpressions, project, alwaysInline = true) =>
          sHolder.builder match {
            case r: SupportsPushDownAggregates =>
              val aliasMap = getAliasMap(project)
              val actualResultExprs = resultExpressions.map(replaceAliasButKeepName(_, aliasMap))
              val actualGroupExprs = groupingExpressions.map(replaceAlias(_, aliasMap))

              val aggExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
              val aggregates = collectAggregates(actualResultExprs, aggExprToOutputOrdinal)
              val normalizedAggregates = DataSourceStrategy.normalizeExprs(
                aggregates, sHolder.relation.output).asInstanceOf[Seq[AggregateExpression]]
              val normalizedGroupingExpressions = DataSourceStrategy.normalizeExprs(
                actualGroupExprs, sHolder.relation.output)
              val translatedAggregates = DataSourceStrategy.translateAggregation(
                normalizedAggregates, normalizedGroupingExpressions)
              val (finalResultExpressions, finalAggregates, finalTranslatedAggregates) = {
                if (translatedAggregates.isEmpty ||
                  r.supportCompletePushDown(translatedAggregates.get) ||
                  translatedAggregates.get.aggregateExpressions().forall(!_.isInstanceOf[Avg])) {
                  (actualResultExprs, aggregates, translatedAggregates)
                } else {
                  // scalastyle:off
                  // The data source doesn't support the complete push-down of this aggregation.
                  // Here we translate `AVG` to `SUM / COUNT`, so that it's more likely to be
                  // pushed, completely or partially.
                  // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
                  // SELECT avg(c1) FROM t GROUP BY c2;
                  // The original logical plan is
                  // Aggregate [c2#10],[avg(c1#9) AS avg(c1)#19]
                  // +- ScanOperation[...]
                  //
                  // After convert avg(c1#9) to sum(c1#9)/count(c1#9)
                  // we have the following
                  // Aggregate [c2#10],[sum(c1#9)/count(c1#9) AS avg(c1)#19]
                  // +- ScanOperation[...]
                  // scalastyle:on
                  val newResultExpressions = actualResultExprs.map { expr =>
                    expr.transform {
                      case AggregateExpression(avg: aggregate.Average, _, isDistinct, _, _) =>
                        val sum = aggregate.Sum(avg.child).toAggregateExpression(isDistinct)
                        val count = aggregate.Count(avg.child).toAggregateExpression(isDistinct)
                        avg.evaluateExpression transform {
                          case a: Attribute if a.semanticEquals(avg.sum) =>
                            addCastIfNeeded(sum, avg.sum.dataType)
                          case a: Attribute if a.semanticEquals(avg.count) =>
                            addCastIfNeeded(count, avg.count.dataType)
                        }
                    }
                  }.asInstanceOf[Seq[NamedExpression]]
                  // Because aggregate expressions changed, translate them again.
                  aggExprToOutputOrdinal.clear()
                  val newAggregates =
                    collectAggregates(newResultExpressions, aggExprToOutputOrdinal)
                  val newNormalizedAggregates = DataSourceStrategy.normalizeExprs(
                    newAggregates, sHolder.relation.output).asInstanceOf[Seq[AggregateExpression]]
                  (newResultExpressions, newAggregates, DataSourceStrategy.translateAggregation(
                    newNormalizedAggregates, normalizedGroupingExpressions))
                }
              }

              if (finalTranslatedAggregates.isEmpty) {
                aggNode // return original plan node
              } else if (!r.supportCompletePushDown(finalTranslatedAggregates.get) &&
                !supportPartialAggPushDown(finalTranslatedAggregates.get)) {
                aggNode // return original plan node
              } else {
                val pushedAggregates = finalTranslatedAggregates.filter(r.pushAggregation)
                if (pushedAggregates.isEmpty) {
                  aggNode // return original plan node
                } else {
                  // No need to do column pruning because only the aggregate columns are used as
                  // DataSourceV2ScanRelation output columns. All the other columns are not
                  // included in the output.
                  val scan = sHolder.builder.build()

                  // scalastyle:off
                  // use the group by columns and aggregate columns as the output columns
                  // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
                  // SELECT min(c1), max(c1) FROM t GROUP BY c2;
                  // Use c2, min(c1), max(c1) as output for DataSourceV2ScanRelation
                  // We want to have the following logical plan:
                  // == Optimized Logical Plan ==
                  // Aggregate [c2#10], [min(min(c1)#21) AS min(c1)#17, max(max(c1)#22) AS max(c1)#18]
                  // +- RelationV2[c2#10, min(c1)#21, max(c1)#22]
                  // scalastyle:on
                  val newOutput = scan.readSchema().toAttributes
                  assert(newOutput.length == groupingExpressions.length + finalAggregates.length)
                  val groupAttrs = normalizedGroupingExpressions.zip(newOutput).map {
                    case (a: Attribute, b: Attribute) => b.withExprId(a.exprId)
                    case (_, b) => b
                  }
                  val aggOutput = newOutput.drop(groupAttrs.length)
                  val output = groupAttrs ++ aggOutput

                  logInfo(
                    s"""
                       |Pushing operators to ${sHolder.relation.name}
                       |Pushed Aggregate Functions:
                       | ${pushedAggregates.get.aggregateExpressions.mkString(", ")}
                       |Pushed Group by:
                       | ${pushedAggregates.get.groupByColumns.mkString(", ")}
                       |Output: ${output.mkString(", ")}
                      """.stripMargin)

                  val wrappedScan = getWrappedScan(scan, sHolder, pushedAggregates)
                  val scanRelation =
                    DataSourceV2ScanRelation(sHolder.relation, wrappedScan, output)
                  if (r.supportCompletePushDown(pushedAggregates.get)) {
                    val projectExpressions = finalResultExpressions.map { expr =>
                      // TODO At present, only push down group by attribute is supported.
                      // In future, more attribute conversion is extended here. e.g. GetStructField
                      expr.transform {
                        case agg: AggregateExpression =>
                          val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
                          val child =
                            addCastIfNeeded(aggOutput(ordinal), agg.resultAttribute.dataType)
                          Alias(child, agg.resultAttribute.name)(agg.resultAttribute.exprId)
                      }
                    }.asInstanceOf[Seq[NamedExpression]]
                    Project(projectExpressions, scanRelation)
                  } else {
                    val plan = Aggregate(output.take(groupingExpressions.length),
                      finalResultExpressions, scanRelation)

                    // scalastyle:off
                    // Change the optimized logical plan to reflect the pushed down aggregate
                    // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
                    // SELECT min(c1), max(c1) FROM t GROUP BY c2;
                    // The original logical plan is
                    // Aggregate [c2#10],[min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
                    // +- RelationV2[c1#9, c2#10] ...
                    //
                    // After change the V2ScanRelation output to [c2#10, min(c1)#21, max(c1)#22]
                    // we have the following
                    // !Aggregate [c2#10], [min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
                    // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
                    //
                    // We want to change it to
                    // == Optimized Logical Plan ==
                    // Aggregate [c2#10], [min(min(c1)#21) AS min(c1)#17, max(max(c1)#22) AS max(c1)#18]
                    // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
                    // scalastyle:on
                    plan.transformExpressions {
                      case agg: AggregateExpression =>
                        val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
                        val aggAttribute = aggOutput(ordinal)
                        val aggFunction: aggregate.AggregateFunction =
                          agg.aggregateFunction match {
                            case max: aggregate.Max =>
                              max.copy(child = addCastIfNeeded(aggAttribute, max.child.dataType))
                            case min: aggregate.Min =>
                              min.copy(child = addCastIfNeeded(aggAttribute, min.child.dataType))
                            case sum: aggregate.Sum =>
                              sum.copy(child = addCastIfNeeded(aggAttribute, sum.child.dataType))
                            case _: aggregate.Count =>
                              aggregate.Sum(addCastIfNeeded(aggAttribute, LongType))
                            case other => other
                          }
                        agg.copy(aggregateFunction = aggFunction)
                    }
                  }
                }
              }
            case _ => aggNode
          }
        case _ => aggNode
      }
  }

  private def collectAggregates(resultExpressions: Seq[NamedExpression],
      aggExprToOutputOrdinal: mutable.HashMap[Expression, Int]): Seq[AggregateExpression] = {
    var ordinal = 0
    resultExpressions.flatMap { expr =>
      expr.collect {
        // Do not push down duplicated aggregate expressions. For example,
        // `SELECT max(a) + 1, max(a) + 2 FROM ...`, we should only push down one
        // `max(a)` to the data source.
        case agg: AggregateExpression
          if !aggExprToOutputOrdinal.contains(agg.canonicalized) =>
          aggExprToOutputOrdinal(agg.canonicalized) = ordinal
          ordinal += 1
          agg
      }
    }
  }

  private def supportPartialAggPushDown(agg: Aggregation): Boolean = {
    // We don't know the agg buffer of `GeneralAggregateFunc`, so can't do partial agg push down.
    // If `Sum`, `Count`, `Avg` with distinct, can't do partial agg push down.
    agg.aggregateExpressions().exists {
      case sum: Sum => !sum.isDistinct
      case count: Count => !count.isDistinct
      case avg: Avg => !avg.isDistinct
      case _: GeneralAggregateFunc => false
      case _ => true
    }
  }

  private def addCastIfNeeded(expression: Expression, expectedDataType: DataType) =
    if (expression.dataType == expectedDataType) {
      expression
    } else {
      Cast(expression, expectedDataType)
    }

  def pruneColumns(plan: LogicalPlan): LogicalPlan = plan.transform {
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

      val wrappedScan = getWrappedScan(scan, sHolder, Option.empty[Aggregation])

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
        Project(restoreOriginalOutputNames(newProjects, project.map(_.name)), withFilter)
      } else {
        withFilter
      }
      withProjection
  }

  def pushDownSample(plan: LogicalPlan): LogicalPlan = plan.transform {
    case sample: Sample => sample.child match {
      case ScanOperation(_, filter, sHolder: ScanBuilderHolder) if filter.isEmpty =>
        val tableSample = TableSampleInfo(
          sample.lowerBound,
          sample.upperBound,
          sample.withReplacement,
          sample.seed)
        val pushed = PushDownUtils.pushTableSample(sHolder.builder, tableSample)
        if (pushed) {
          sHolder.pushedSample = Some(tableSample)
          sample.child
        } else {
          sample
        }

      case _ => sample
    }
  }

  private def pushDownLimit(plan: LogicalPlan, limit: Int): LogicalPlan = plan match {
    case operation @ ScanOperation(_, filter, sHolder: ScanBuilderHolder) if filter.isEmpty =>
      val limitPushed = PushDownUtils.pushLimit(sHolder.builder, limit)
      if (limitPushed) {
        sHolder.pushedLimit = Some(limit)
      }
      operation
    case s @ Sort(order, _, operation @ ScanOperation(project, filter, sHolder: ScanBuilderHolder))
        if filter.isEmpty && CollapseProject.canCollapseExpressions(
          order, project, alwaysInline = true) =>
      val aliasMap = getAliasMap(project)
      val newOrder = order.map(replaceAlias(_, aliasMap)).asInstanceOf[Seq[SortOrder]]
      val orders = DataSourceStrategy.translateSortOrders(newOrder)
      if (orders.length == order.length) {
        val (isPushed, isPartiallyPushed) =
          PushDownUtils.pushTopN(sHolder.builder, orders.toArray, limit)
        if (isPushed) {
          sHolder.pushedLimit = Some(limit)
          sHolder.sortOrders = orders
          if (isPartiallyPushed) {
            s
          } else {
            operation
          }
        } else {
          s
        }
      } else {
        s
      }
    case p: Project =>
      val newChild = pushDownLimit(p.child, limit)
      p.withNewChildren(Seq(newChild))
    case other => other
  }

  def pushDownLimits(plan: LogicalPlan): LogicalPlan = plan.transform {
    case globalLimit @ Limit(IntegerLiteral(limitValue), child) =>
      val newChild = pushDownLimit(child, limitValue)
      val newLocalLimit = globalLimit.child.asInstanceOf[LocalLimit].withNewChildren(Seq(newChild))
      globalLimit.withNewChildren(Seq(newLocalLimit))
  }

  private def getWrappedScan(
      scan: Scan,
      sHolder: ScanBuilderHolder,
      aggregation: Option[Aggregation]): Scan = {
    scan match {
      case v1: V1Scan =>
        val pushedFilters = sHolder.builder match {
          case f: SupportsPushDownFilters =>
            f.pushedFilters()
          case _ => Array.empty[sources.Filter]
        }
        val pushedDownOperators = PushedDownOperators(aggregation, sHolder.pushedSample,
          sHolder.pushedLimit, sHolder.sortOrders, sHolder.pushedPredicates)
        V1ScanWrapper(v1, pushedFilters, pushedDownOperators)
      case _ => scan
    }
  }
}

case class ScanBuilderHolder(
    output: Seq[AttributeReference],
    relation: DataSourceV2Relation,
    builder: ScanBuilder) extends LeafNode {
  var pushedLimit: Option[Int] = None

  var sortOrders: Seq[V2SortOrder] = Seq.empty[V2SortOrder]

  var pushedSample: Option[TableSampleInfo] = None

  var pushedPredicates: Seq[Predicate] = Seq.empty[Predicate]
}


// A wrapper for v1 scan to carry the translated filters and the handled ones, along with
// other pushed down operators. This is required by the physical v1 scan node.
case class V1ScanWrapper(
    v1Scan: V1Scan,
    handledFilters: Seq[sources.Filter],
    pushedDownOperators: PushedDownOperators) extends Scan {
  override def readSchema(): StructType = v1Scan.readSchema()
}
