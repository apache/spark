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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.connector.read.{Scan, V1Scan}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Implicits, DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.Aggregation
import org.apache.spark.sql.types.StructType

/**
 * Push down partial Aggregate to datasource for better performance
 */
object PartialAggregatePushDown extends Rule[LogicalPlan]  with AliasHelper {
  import DataSourceV2Implicits._

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Pattern matching for partial aggregate push down
    // For parquet, footer only has statistics information for max/min/count.
    // It doesn't handle max/min/count associated with filter or group by.
    // ORC is similar. If JDBC partial aggregate push down is added later,
    // these condition checks need to be changed.
    case aggNode@Aggregate(groupingExpressions, resultExpressions, child)
      if (groupingExpressions.isEmpty) =>
      child match {
        case ScanOperation(project, filters, relation@DataSourceV2Relation(table, _, _, _, _))
          if (filters.isEmpty) && table.isInstanceOf[ParquetTable] =>
          val scanBuilder = relation.table.asReadable.newScanBuilder(relation.options)
          var aggregates = resultExpressions.flatMap { expr =>
            expr.collect {
              case agg: AggregateExpression =>
                replaceAlias(agg, getAliasMap(project)).asInstanceOf[AggregateExpression]
            }
          }
          aggregates = DataSourceStrategy.normalizeExprs(aggregates, relation.output)
            .asInstanceOf[Seq[AggregateExpression]]

          val scan = scanBuilder.build()
          val translatedAggregates = aggregates.map(DataSourceStrategy
            .translateAggregate(_, PushableColumn(false)))
          if (translatedAggregates.exists(_.isEmpty)) {
            aggNode // return original plan node
          } else {
            val aggregation = Aggregation(translatedAggregates.flatten, Seq.empty)
            scan.pushAggregation(aggregation)
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


            logInfo(
              s"""
                 |Pushing operators to ${relation.name}
                 |Pushed Aggregate Functions: ${aggregation.aggregateExpressions.mkString(", ")}
                 |Output: ${output.mkString(", ")}
              """.stripMargin)
            val wrappedScan = scan match {
              case v1: V1Scan =>
                V1ScanWrapper(v1, Seq.empty[sources.Filter], Seq.empty[sources.Filter], aggregation)
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
                val aggFunction: aggregate.AggregateFunction = agg.aggregateFunction match {
                  case _: aggregate.Max => aggregate.Max(output(i - 1))
                  case _: aggregate.Min => aggregate.Min(output(i - 1))
                  case _: aggregate.Sum => aggregate.Sum(output(i - 1))
                  case _: aggregate.Average => aggregate.Average(output(i - 1))
                  case _: aggregate.Count => aggregate.Sum(output(i - 1))
                  case _ => agg.aggregateFunction
                }
                agg.copy(aggregateFunction = aggFunction, filter = None)
            }
          }

        case _ => aggNode // return original plan node
      }
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
