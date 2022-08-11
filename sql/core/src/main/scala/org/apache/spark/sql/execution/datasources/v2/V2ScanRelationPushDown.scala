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

import org.apache.spark.sql.catalyst.expressions.{aggregate, Alias, And, Attribute, AttributeMap, AttributeReference, AttributeSet, Cast, Expression, IntegerLiteral, Literal, NamedExpression, PredicateHelper, ProjectionOverSchema, SortOrder, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LeafNode, Limit, LimitAndOffset, LocalLimit, LogicalPlan, Offset, OffsetAndLimit, Project, Sample, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.expressions.{SortOrder => V2SortOrder}
import org.apache.spark.sql.connector.expressions.aggregate.{Aggregation, Avg, Count, CountStar, Max, Min, Sum}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, V1Scan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{DataType, DecimalType, IntegerType, StructType}
import org.apache.spark.sql.util.SchemaUtils._

object V2ScanRelationPushDown extends Rule[LogicalPlan] with PredicateHelper {
  import DataSourceV2Implicits._

  def apply(plan: LogicalPlan): LogicalPlan = {
    val pushdownRules = Seq[LogicalPlan => LogicalPlan] (
      createScanBuilder,
      pushDownSample,
      pushDownFilters,
      pushDownAggregates,
      pushDownLimitAndOffset,
      buildScanWithPushedAggregate,
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
    case agg: Aggregate => rewriteAggregate(agg)
  }

  private def rewriteAggregate(agg: Aggregate): LogicalPlan = agg.child match {
    case PhysicalOperation(project, Nil, holder @ ScanBuilderHolder(_, _,
        r: SupportsPushDownAggregates)) if CollapseProject.canCollapseExpressions(
        agg.aggregateExpressions, project, alwaysInline = true) =>
      val aliasMap = getAliasMap(project)
      val actualResultExprs = agg.aggregateExpressions.map(replaceAliasButKeepName(_, aliasMap))
      val actualGroupExprs = agg.groupingExpressions.map(replaceAlias(_, aliasMap))

      val aggExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
      val aggregates = collectAggregates(actualResultExprs, aggExprToOutputOrdinal)
      val normalizedAggExprs = DataSourceStrategy.normalizeExprs(
        aggregates, holder.relation.output).asInstanceOf[Seq[AggregateExpression]]
      val normalizedGroupingExpr = DataSourceStrategy.normalizeExprs(
        actualGroupExprs, holder.relation.output)
      val translatedAggOpt = DataSourceStrategy.translateAggregation(
        normalizedAggExprs, normalizedGroupingExpr)
      if (translatedAggOpt.isEmpty) {
        // Cannot translate the catalyst aggregate, return the query plan unchanged.
        return agg
      }

      val (finalResultExprs, finalAggExprs, translatedAgg, canCompletePushDown) = {
        if (r.supportCompletePushDown(translatedAggOpt.get)) {
          (actualResultExprs, normalizedAggExprs, translatedAggOpt.get, true)
        } else if (!translatedAggOpt.get.aggregateExpressions().exists(_.isInstanceOf[Avg])) {
          (actualResultExprs, normalizedAggExprs, translatedAggOpt.get, false)
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
          val newNormalizedAggExprs = DataSourceStrategy.normalizeExprs(
            newAggregates, holder.relation.output).asInstanceOf[Seq[AggregateExpression]]
          val newTranslatedAggOpt = DataSourceStrategy.translateAggregation(
            newNormalizedAggExprs, normalizedGroupingExpr)
          if (newTranslatedAggOpt.isEmpty) {
            // Ideally we should never reach here. But if we end up with not able to translate
            // new aggregate with AVG replaced by SUM/COUNT, revert to the original one.
            (actualResultExprs, normalizedAggExprs, translatedAggOpt.get, false)
          } else {
            (newResultExpressions, newNormalizedAggExprs, newTranslatedAggOpt.get,
              r.supportCompletePushDown(newTranslatedAggOpt.get))
          }
        }
      }

      if (!canCompletePushDown && !supportPartialAggPushDown(translatedAgg)) {
        return agg
      }
      if (!r.pushAggregation(translatedAgg)) {
        return agg
      }

      // scalastyle:off
      // We name the output columns of group expressions and aggregate functions by
      // ordinal: `group_col_0`, `group_col_1`, ..., `agg_func_0`, `agg_func_1`, ...
      // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
      // SELECT min(c1), max(c1) FROM t GROUP BY c2;
      // Use group_col_0, agg_func_0, agg_func_1 as output for ScanBuilderHolder.
      // We want to have the following logical plan:
      // == Optimized Logical Plan ==
      // Aggregate [group_col_0#10], [min(agg_func_0#21) AS min(c1)#17, max(agg_func_1#22) AS max(c1)#18]
      // +- ScanBuilderHolder[group_col_0#10, agg_func_0#21, agg_func_1#22]
      // Later, we build the `Scan` instance and convert ScanBuilderHolder to DataSourceV2ScanRelation.
      // scalastyle:on
      val groupOutputMap = normalizedGroupingExpr.zipWithIndex.map { case (e, i) =>
        AttributeReference(s"group_col_$i", e.dataType)() -> e
      }
      val groupOutput = groupOutputMap.unzip._1
      val aggOutputMap = finalAggExprs.zipWithIndex.map { case (e, i) =>
        AttributeReference(s"agg_func_$i", e.dataType)() -> e
      }
      val aggOutput = aggOutputMap.unzip._1
      val newOutput = groupOutput ++ aggOutput
      val groupByExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
      normalizedGroupingExpr.zipWithIndex.foreach { case (expr, ordinal) =>
        if (!groupByExprToOutputOrdinal.contains(expr.canonicalized)) {
          groupByExprToOutputOrdinal(expr.canonicalized) = ordinal
        }
      }

      holder.pushedAggregate = Some(translatedAgg)
      holder.pushedAggOutputMap = AttributeMap(groupOutputMap ++ aggOutputMap)
      holder.output = newOutput
      logInfo(
        s"""
           |Pushing operators to ${holder.relation.name}
           |Pushed Aggregate Functions:
           | ${translatedAgg.aggregateExpressions().mkString(", ")}
           |Pushed Group by:
           | ${translatedAgg.groupByExpressions.mkString(", ")}
         """.stripMargin)

      if (canCompletePushDown) {
        val projectExpressions = finalResultExprs.map { expr =>
          expr.transformDown {
            case agg: AggregateExpression =>
              val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
              Alias(aggOutput(ordinal), agg.resultAttribute.name)(agg.resultAttribute.exprId)
            case expr if groupByExprToOutputOrdinal.contains(expr.canonicalized) =>
              val ordinal = groupByExprToOutputOrdinal(expr.canonicalized)
              expr match {
                case ne: NamedExpression => Alias(groupOutput(ordinal), ne.name)(ne.exprId)
                case _ => groupOutput(ordinal)
              }
          }
        }.asInstanceOf[Seq[NamedExpression]]
        Project(projectExpressions, holder)
      } else {
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
        val aggExprs = finalResultExprs.map(_.transform {
          case agg: AggregateExpression =>
            val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
            val aggAttribute = aggOutput(ordinal)
            val aggFunction: aggregate.AggregateFunction =
              agg.aggregateFunction match {
                case max: aggregate.Max =>
                  max.copy(child = aggAttribute)
                case min: aggregate.Min =>
                  min.copy(child = aggAttribute)
                case sum: aggregate.Sum =>
                  // To keep the dataType of `Sum` unchanged, we need to cast the
                  // data-source-aggregated result to `Sum.child.dataType` if it's decimal.
                  // See `SumBase.resultType`
                  val newChild = if (sum.dataType.isInstanceOf[DecimalType]) {
                    addCastIfNeeded(aggAttribute, sum.child.dataType)
                  } else {
                    aggAttribute
                  }
                  sum.copy(child = newChild)
                case _: aggregate.Count =>
                  aggregate.Sum(aggAttribute)
                case other => other
              }
            agg.copy(aggregateFunction = aggFunction)
          case expr if groupByExprToOutputOrdinal.contains(expr.canonicalized) =>
            val ordinal = groupByExprToOutputOrdinal(expr.canonicalized)
            expr match {
              case ne: NamedExpression => Alias(groupOutput(ordinal), ne.name)(ne.exprId)
              case _ => groupOutput(ordinal)
            }
        }).asInstanceOf[Seq[NamedExpression]]
        Aggregate(groupOutput, aggExprs, holder)
      }

    case _ => agg
  }

  private def collectAggregates(
      resultExpressions: Seq[NamedExpression],
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
    // We can only partially push down min/max/sum/count without DISTINCT.
    agg.aggregateExpressions().isEmpty || agg.aggregateExpressions().forall {
      case sum: Sum => !sum.isDistinct
      case count: Count => !count.isDistinct
      case _: Min | _: Max | _: CountStar => true
      case _ => false
    }
  }

  private def addCastIfNeeded(expression: Expression, expectedDataType: DataType) =
    if (expression.dataType == expectedDataType) {
      expression
    } else {
      Cast(expression, expectedDataType)
    }

  def buildScanWithPushedAggregate(plan: LogicalPlan): LogicalPlan = plan.transform {
    case holder: ScanBuilderHolder if holder.pushedAggregate.isDefined =>
      // No need to do column pruning because only the aggregate columns are used as
      // DataSourceV2ScanRelation output columns. All the other columns are not
      // included in the output.
      val scan = holder.builder.build()
      val realOutput = scan.readSchema().toAttributes
      assert(realOutput.length == holder.output.length,
        "The data source returns unexpected number of columns")
      val wrappedScan = getWrappedScan(scan, holder)
      val scanRelation = DataSourceV2ScanRelation(holder.relation, wrappedScan, realOutput)
      val projectList = realOutput.zip(holder.output).map { case (a1, a2) =>
        // The data source may return columns with arbitrary data types and it's safer to cast them
        // to the expected data type.
        assert(Cast.canCast(a1.dataType, a2.dataType))
        Alias(addCastIfNeeded(a1, a2.dataType), a2.name)(a2.exprId)
      }
      Project(projectList, scanRelation)
  }

  def pruneColumns(plan: LogicalPlan): LogicalPlan = plan.transform {
    case PhysicalOperation(project, filters, sHolder: ScanBuilderHolder) =>
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

      val wrappedScan = getWrappedScan(scan, sHolder)

      val scanRelation = DataSourceV2ScanRelation(sHolder.relation, wrappedScan, output)

      val projectionOverSchema =
        ProjectionOverSchema(output.toStructType, AttributeSet(output))
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
      case PhysicalOperation(_, filter, sHolder: ScanBuilderHolder) if filter.isEmpty =>
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

  private def pushDownLimit(plan: LogicalPlan, limit: Int): (LogicalPlan, Boolean) = plan match {
    case operation @ PhysicalOperation(_, filter, sHolder: ScanBuilderHolder) if filter.isEmpty =>
      val (isPushed, isPartiallyPushed) = PushDownUtils.pushLimit(sHolder.builder, limit)
      if (isPushed) {
        sHolder.pushedLimit = Some(limit)
      }
      (operation, isPushed && !isPartiallyPushed)
    case s @ Sort(order, _, operation @ PhysicalOperation(project, Nil, sHolder: ScanBuilderHolder))
      if CollapseProject.canCollapseExpressions(order, project, alwaysInline = true) =>
      val aliasMap = getAliasMap(project)
      val aliasReplacedOrder = order.map(replaceAlias(_, aliasMap))
      val newOrder = if (sHolder.pushedAggregate.isDefined) {
        // `ScanBuilderHolder` has different output columns after aggregate push-down. Here we
        // replace the attributes in ordering expressions with the original table output columns.
        aliasReplacedOrder.map {
          _.transform {
            case a: Attribute => sHolder.pushedAggOutputMap.getOrElse(a, a)
          }.asInstanceOf[SortOrder]
        }
      } else {
        aliasReplacedOrder.asInstanceOf[Seq[SortOrder]]
      }
      val normalizedOrders = DataSourceStrategy.normalizeExprs(
        newOrder, sHolder.relation.output).asInstanceOf[Seq[SortOrder]]
      val orders = DataSourceStrategy.translateSortOrders(normalizedOrders)
      if (orders.length == order.length) {
        val (isPushed, isPartiallyPushed) =
          PushDownUtils.pushTopN(sHolder.builder, orders.toArray, limit)
        if (isPushed) {
          sHolder.pushedLimit = Some(limit)
          sHolder.sortOrders = orders
          if (isPartiallyPushed) {
            (s, false)
          } else {
            (operation, true)
          }
        } else {
          (s, false)
        }
      } else {
        (s, false)
      }
    case p: Project =>
      val (newChild, isPartiallyPushed) = pushDownLimit(p.child, limit)
      (p.withNewChildren(Seq(newChild)), isPartiallyPushed)
    case other => (other, false)
  }

  private def pushDownOffset(
      plan: LogicalPlan,
      offset: Int): Boolean = plan match {
    case sHolder: ScanBuilderHolder =>
      val isPushed = PushDownUtils.pushOffset(sHolder.builder, offset)
      if (isPushed) {
        sHolder.pushedOffset = Some(offset)
      }
      isPushed
    case Project(projectList, child) if projectList.forall(_.deterministic) =>
      pushDownOffset(child, offset)
    case _ => false
  }

  def pushDownLimitAndOffset(plan: LogicalPlan): LogicalPlan = plan.transform {
    case offset @ LimitAndOffset(limit, offsetValue, child) =>
      val (newChild, canRemoveLimit) = pushDownLimit(child, limit)
      if (canRemoveLimit) {
        // Try to push down OFFSET only if the LIMIT operator has been pushed and can be removed.
        val isPushed = pushDownOffset(newChild, offsetValue)
        if (isPushed) {
          newChild
        } else {
          // Keep the OFFSET operator if we failed to push down OFFSET to the data source.
          offset.withNewChildren(Seq(newChild))
        }
      } else {
        // Keep the OFFSET operator if we can't remove LIMIT operator.
        offset
      }
    case globalLimit @ OffsetAndLimit(offset, limit, child) =>
      // For `df.offset(n).limit(m)`, we can push down `limit(m + n)` first.
      val (newChild, canRemoveLimit) = pushDownLimit(child, limit + offset)
      if (canRemoveLimit) {
        // Try to push down OFFSET only if the LIMIT operator has been pushed and can be removed.
        val isPushed = pushDownOffset(newChild, offset)
        if (isPushed) {
          newChild
        } else {
          // Still keep the OFFSET operator if we can't push it down.
          Offset(Literal(offset), newChild)
        }
      } else {
        // For `df.offset(n).limit(m)`, since we can't push down `limit(m + n)`,
        // try to push down `offset(n)` here.
        val isPushed = pushDownOffset(child, offset)
        if (isPushed) {
          // Keep the LIMIT operator if we can't push it down.
          Limit(Literal(limit, IntegerType), child)
        } else {
          // Keep the origin plan if we can't push OFFSET operator and LIMIT operator.
          globalLimit
        }
      }
    case globalLimit @ Limit(IntegerLiteral(limitValue), child) =>
      val (newChild, canRemoveLimit) = pushDownLimit(child, limitValue)
      if (canRemoveLimit) {
        newChild
      } else {
        val newLocalLimit =
          globalLimit.child.asInstanceOf[LocalLimit].withNewChildren(Seq(newChild))
        globalLimit.withNewChildren(Seq(newLocalLimit))
      }
    case offset @ Offset(IntegerLiteral(n), child) =>
      val isPushed = pushDownOffset(child, n)
      if (isPushed) {
        child
      } else {
        offset
      }
  }

  private def getWrappedScan(scan: Scan, sHolder: ScanBuilderHolder): Scan = {
    scan match {
      case v1: V1Scan =>
        val pushedFilters = sHolder.builder match {
          case f: SupportsPushDownFilters =>
            f.pushedFilters()
          case _ => Array.empty[sources.Filter]
        }
        val pushedDownOperators = PushedDownOperators(sHolder.pushedAggregate, sHolder.pushedSample,
          sHolder.pushedLimit, sHolder.pushedOffset, sHolder.sortOrders, sHolder.pushedPredicates)
        V1ScanWrapper(v1, pushedFilters, pushedDownOperators)
      case _ => scan
    }
  }
}

case class ScanBuilderHolder(
    var output: Seq[AttributeReference],
    relation: DataSourceV2Relation,
    builder: ScanBuilder) extends LeafNode {
  var pushedLimit: Option[Int] = None

  var pushedOffset: Option[Int] = None

  var sortOrders: Seq[V2SortOrder] = Seq.empty[V2SortOrder]

  var pushedSample: Option[TableSampleInfo] = None

  var pushedPredicates: Seq[Predicate] = Seq.empty[Predicate]

  var pushedAggregate: Option[Aggregation] = None

  var pushedAggOutputMap: AttributeMap[Expression] = AttributeMap.empty[Expression]
}

// A wrapper for v1 scan to carry the translated filters and the handled ones, along with
// other pushed down operators. This is required by the physical v1 scan node.
case class V1ScanWrapper(
    v1Scan: V1Scan,
    handledFilters: Seq[sources.Filter],
    pushedDownOperators: PushedDownOperators) extends Scan {
  override def readSchema(): StructType = v1Scan.readSchema()
}
