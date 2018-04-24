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

import org.apache.spark.sql.catalyst.expressions.{And, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.v2.reader.{SupportsPushDownCatalystFilters, SupportsPushDownFilters, SupportsPushDownRequiredColumns}

object PushDownOperatorsToDataSource extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    // PhysicalOperation guarantees that filters are deterministic; no need to check
    case PhysicalOperation(project, filters, relation: DataSourceV2Relation) =>
      val newReader = relation.createFreshReader
      var newRelation = relation.copy(optimizedReader = Some(newReader))

      val postScanFilters: Seq[Expression] = newReader match {
        case r: SupportsPushDownCatalystFilters =>
          val postScanFilters = r.pushCatalystFilters(filters.toArray)
          newRelation.copy(pushedFilters = r.pushedCatalystFilters())
          postScanFilters

        case r: SupportsPushDownFilters =>
          // A map from translated data source filters to original catalyst filter expressions.
          val translatedFilterToExpr = mutable.HashMap.empty[sources.Filter, Expression]
          // Catalyst filter expression that can't be translated to data source filters.
          val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

          for (filterExpr <- filters) {
            val translated = DataSourceStrategy.translateFilter(filterExpr)
            if (translated.isDefined) {
              translatedFilterToExpr(translated.get) = filterExpr
            } else {
              untranslatableExprs += filterExpr
            }
          }

          // Data source filters that need to be evaluated again after scanning. which means
          // the data source cannot guarantee the rows returned can pass these filters.
          // As a result we must return it so Spark can plan an extra filter operator.
          val postScanFilters =
          r.pushFilters(translatedFilterToExpr.keys.toArray).map(translatedFilterToExpr)
          // The filters which are marked as pushed to this data source
          val pushedFilters = r.pushedFilters().map(translatedFilterToExpr)
          newRelation = newRelation.copy(pushedFilters = pushedFilters)
          untranslatableExprs ++ postScanFilters

        case _ => filters
      }

      newReader match {
        case r: SupportsPushDownRequiredColumns =>
          val requiredColumns = AttributeSet(
            project.flatMap(_.references) ++ postScanFilters.flatMap(_.references))
          val neededOutput = relation.output.filter(requiredColumns.contains)
          if (neededOutput != relation.output) {
            r.pruneColumns(neededOutput.toStructType)
            val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
            val newOutput = r.readSchema().toAttributes.map {
              // We have to keep the attribute id during transformation.
              a => a.withExprId(nameToAttr(a.name).exprId)
            }
            newRelation = newRelation.copy(output = newOutput)
          }

        case _ =>
      }

      val filterCondition = postScanFilters.reduceLeftOption(And)
      val withFilter = filterCondition.map(Filter(_, newRelation)).getOrElse(newRelation)
      if (withFilter.output == project) {
        withFilter
      } else {
        Project(project, withFilter)
      }

    case other => other.mapChildren(apply)
  }
}
