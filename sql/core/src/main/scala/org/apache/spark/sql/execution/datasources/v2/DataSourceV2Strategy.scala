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

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader._

object DataSourceV2Strategy extends Strategy {
  // TODO: write path
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projects, filters, DataSourceV2Relation(output, reader)) =>
      val stayUpFilters: Seq[Expression] = reader match {
        case r: SupportsPushDownCatalystFilters =>
          r.pushCatalystFilters(filters.toArray)

        case r: SupportsPushDownFilters =>
          // A map from original Catalyst expressions to corresponding translated data source
          // filters. If a predicate is not in this map, it means it cannot be pushed down.
          val translatedMap: Map[Expression, Filter] = filters.flatMap { p =>
            DataSourceStrategy.translateFilter(p).map(f => p -> f)
          }.toMap

          // Catalyst predicate expressions that cannot be converted to data source filters.
          val nonConvertiblePredicates = filters.filterNot(translatedMap.contains)

          // Data source filters that cannot be pushed down. An unhandled filter means
          // the data source cannot guarantee the rows returned can pass the filter.
          // As a result we must return it so Spark can plan an extra filter operator.
          val unhandledFilters = r.pushFilters(translatedMap.values.toArray).toSet
          val unhandledPredicates = translatedMap.filter { case (_, f) =>
            unhandledFilters.contains(f)
          }.keys

          nonConvertiblePredicates ++ unhandledPredicates

        case _ => filters
      }

      val attrMap = AttributeMap(output.zip(output))
      val projectSet = AttributeSet(projects.flatMap(_.references))
      val filterSet = AttributeSet(stayUpFilters.flatMap(_.references))

      // Match original case of attributes.
      // TODO: nested fields pruning
      val requiredColumns = (projectSet ++ filterSet).toSeq.map(attrMap)
      reader match {
        case r: SupportsPushDownRequiredColumns =>
          r.pruneColumns(requiredColumns.toStructType)
        case _ =>
      }

      val scan = DataSourceV2ScanExec(
        output.toArray,
        reader,
        reader.readSchema(),
        ExpressionSet(filters),
        Nil)

      val filterCondition = stayUpFilters.reduceLeftOption(And)
      val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

      val withProject = if (projects == withFilter.output) {
        withFilter
      } else {
        ProjectExec(projects, withFilter)
      }

      withProject :: Nil

    case _ => Nil
  }
}
