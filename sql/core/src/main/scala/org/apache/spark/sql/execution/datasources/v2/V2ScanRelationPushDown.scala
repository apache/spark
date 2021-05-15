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
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownAggregates, V1Scan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.{AggregateFunc, Aggregation}
import org.apache.spark.sql.types.StructType

object V2ScanRelationPushDown extends Rule[LogicalPlan] with AliasHelper {
  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case aggNode@Aggregate(groupingExpressions, resultExpressions, child) =>
      child match {
        case ScanOperation(project, filters, relation: DataSourceV2Relation) =>
          val scanBuilder = relation.table.asReadable.newScanBuilder(relation.options)
          scanBuilder match {
            case r: SupportsPushDownAggregates if r.supportsPushDownAggregateWithFilter =>
              // todo: need to change this for JDBC aggregate push down
              // for (pushDownOrder <- ScanBuilder.PUSH_DOWN_ORDERS) {
              // if (pushDownOrder == orders.FILTER) {
              //  pushdown filter
              // } else if(pushDownOrder == orders.AGGREGATE) {
              //  pushdown aggregate
              // }
              aggNode
            case r: SupportsPushDownAggregates =>
              if (filters.isEmpty) {
                if (r.supportsGlobalAggregatePushDownOnly() && groupingExpressions.nonEmpty) {
                  aggNode // return original plan node
                } else {
                  var aggregates = resultExpressions.flatMap { expr =>
                    expr.collect {
                      case agg: AggregateExpression =>
                        replaceAlias(agg, getAliasMap(project)).asInstanceOf[AggregateExpression]
                    }
                  }
                  aggregates = DataSourceStrategy.normalizeExprs(aggregates, relation.output)
                    .asInstanceOf[Seq[AggregateExpression]]
                  val aggregation = PushDownUtils
                    .pushAggregates(scanBuilder, aggregates, groupingExpressions)
                  if (aggregation.aggregateExpressions.isEmpty) {
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
                    val scan = scanBuilder.build()

                    logInfo(
                      s"""
                         |Pushing operators to ${relation.name}
                         |Pushed Aggregates: ${aggregation.aggregateExpressions.mkString(", ")}
                         |Output: ${output.mkString(", ")}
                      """.stripMargin)
                    val wrappedScan = scan match {
                      case v1: V1Scan =>
                        V1ScanWrapper(v1, Seq.empty[sources.Filter], Seq.empty[sources.Filter],
                          aggregation)
                      case _ => scan
                    }

                    val scanRelation = DataSourceV2ScanRelation(relation, wrappedScan, output)
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
          }

        case _ => aggNode // return original plan node
      }
    case ScanOperation(project, filters, relation: DataSourceV2Relation) =>
      val scanBuilder = relation.table.asReadable.newScanBuilder(relation.options)

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
            Aggregation(Seq.empty[Seq[AggregateFunc]], Seq.empty[String]))

        case _ => scan
      }

      val scanRelation = DataSourceV2ScanRelation(relation, wrappedScan, output)

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
