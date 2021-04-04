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

import scala.collection.mutable.ArrayBuilder

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, V1Scan}
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

          val aliasMap = getAliasMap(project)
          var aggregates = resultExpressions.flatMap { expr =>
            expr.collect {
              case agg: AggregateExpression =>
                replaceAlias(agg, aliasMap).asInstanceOf[AggregateExpression]
            }
          }
          aggregates = DataSourceStrategy.normalizeExprs(aggregates, relation.output)
            .asInstanceOf[Seq[AggregateExpression]]

          val groupingExpressionsWithoutAlias = groupingExpressions.flatMap{ expr =>
            expr.collect {
              case e: Expression => replaceAlias(e, aliasMap)
            }
          }
          val normalizedGroupingExpressions =
            DataSourceStrategy.normalizeExprs(groupingExpressionsWithoutAlias, relation.output)

          var newFilters = filters
          aggregates.foreach(agg =>
            if (agg.filter.nonEmpty)  {
              // handle agg filter the same way as other filters
              newFilters = newFilters :+ agg.filter.get
            }
          )

          val (pushedFilters, postScanFilters) = pushDownFilter(scanBuilder, newFilters, relation)
          if (postScanFilters.nonEmpty) {
            aggNode // return original plan node
          } else { // only push down aggregate if all the filers can be push down
            val aggregation = PushDownUtils.pushAggregates(scanBuilder, aggregates,
              normalizedGroupingExpressions)

            val (scan, output, normalizedProjects) =
              processFilterAndColumn(scanBuilder, project, postScanFilters, relation)

            logInfo(
              s"""
                 |Pushing operators to ${relation.name}
                 |Pushed Filters: ${pushedFilters.mkString(", ")}
                 |Post-Scan Filters: ${postScanFilters.mkString(",")}
                 |Pushed Aggregate Functions: ${aggregation.aggregateExpressions.mkString(", ")}
                 |Pushed Groupby: ${aggregation.groupByColumns.mkString(", ")}
                 |Output: ${output.mkString(", ")}
             """.stripMargin)

            val wrappedScan = scan match {
              case v1: V1Scan =>
                val translated = newFilters.flatMap(DataSourceStrategy.translateFilter(_, true))
                V1ScanWrapper(v1, translated, pushedFilters, aggregation)
              case _ => scan
            }

            if (aggregation.aggregateExpressions.isEmpty) {
              aggNode // return original plan node
            } else {
              // build the aggregate expressions + groupby expressions
              val aggOutputBuilder = ArrayBuilder.make[AttributeReference]
              for (i <- 0 until aggregates.length) {
                aggOutputBuilder += AttributeReference(
                  aggregation.aggregateExpressions(i).toString, aggregates(i).dataType)()
              }
              groupingExpressions.foreach{
                case a@AttributeReference(_, _, _, _) => aggOutputBuilder += a
                case _ =>
              }
              val aggOutput = aggOutputBuilder.result

              val r = buildLogicalPlan(aggOutput, relation, wrappedScan, aggOutput,
                normalizedProjects, postScanFilters)
              val plan = Aggregate(groupingExpressions, resultExpressions, r)

              var i = 0
              // scalastyle:off line.size.limit
              // change the original optimized logical plan to reflect the pushed down aggregate
              // e.g. sql("select max(id), min(id) FROM h2.test.people")
              // the the original optimized logical plan is
              // == Optimized Logical Plan ==
              // Aggregate [max(ID#35) AS max(ID)#38, min(ID#35) AS min(ID)#39]
              // +- RelationV2[ID#35] test.people
              // We want to change it to the following
              // == Optimized Logical Plan ==
              // Aggregate [max(Max(ID,IntegerType)#298) AS max(ID)#293, min(Min(ID,IntegerType)#299) AS min(ID)#294]
              //   +- RelationV2[Max(ID,IntegerType)#298, Min(ID,IntegerType)#299] test.people
              // scalastyle:on line.size.limit
              plan.transformExpressions {
                case agg: AggregateExpression =>
                  i += 1
                  val aggFunction: aggregate.AggregateFunction = agg.aggregateFunction match {
                    case max: aggregate.Max => aggregate.Max(aggOutput(i - 1))
                    case min: aggregate.Min => aggregate.Min(aggOutput(i - 1))
                    case sum: aggregate.Sum => aggregate.Sum(aggOutput(i - 1))
                    case avg: aggregate.Average => aggregate.Average(aggOutput(i - 1))
                    case count: aggregate.Count => aggregate.PushDownCount(aggOutput(i - 1), true)
                    case _ => agg.aggregateFunction
                  }
                  agg.copy(aggregateFunction = aggFunction, filter = None)
              }
            }
          }

        case _ => aggNode // return original plan node
      }
    case ScanOperation(project, filters, relation: DataSourceV2Relation) =>
      val scanBuilder = relation.table.asReadable.newScanBuilder(relation.options)
      val (pushedFilters, postScanFilters) = pushDownFilter (scanBuilder, filters, relation)
      val (scan, output, normalizedProjects) =
        processFilterAndColumn(scanBuilder, project, postScanFilters, relation)

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

  private def pushDownFilter(
      scanBuilder: ScanBuilder,
      filters: Seq[Expression],
      relation: DataSourceV2Relation): (Seq[sources.Filter], Seq[Expression]) = {
    val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, relation.output)
    val (normalizedFiltersWithSubquery, normalizedFiltersWithoutSubquery) =
      normalizedFilters.partition(SubqueryExpression.hasSubquery)

    // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
    // `postScanFilters` need to be evaluated after the scan.
    // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
    val (pushedFilters, postScanFiltersWithoutSubquery) = PushDownUtils.pushFilters(
      scanBuilder, normalizedFiltersWithoutSubquery)
    val postScanFilters = postScanFiltersWithoutSubquery ++ normalizedFiltersWithSubquery
    (pushedFilters, postScanFilters)
  }

  private def processFilterAndColumn(
      scanBuilder: ScanBuilder,
      project: Seq[NamedExpression],
      postScanFilters: Seq[Expression],
      relation: DataSourceV2Relation):
  (Scan, Seq[AttributeReference], Seq[NamedExpression]) = {
    val normalizedProjects = DataSourceStrategy
      .normalizeExprs(project, relation.output)
      .asInstanceOf[Seq[NamedExpression]]
    val (scan, output) = PushDownUtils.pruneColumns(
      scanBuilder, relation, normalizedProjects, postScanFilters)
    (scan, output, normalizedProjects)
  }

  private def buildLogicalPlan(
      project: Seq[NamedExpression],
      relation: DataSourceV2Relation,
      wrappedScan: Scan,
      output: Seq[AttributeReference],
      normalizedProjects: Seq[NamedExpression],
      postScanFilters: Seq[Expression]): LogicalPlan = {
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
