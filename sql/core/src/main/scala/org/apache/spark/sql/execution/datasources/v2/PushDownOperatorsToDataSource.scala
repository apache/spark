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

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeSet, Expression, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.RemoveRedundantProject
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.v2.reader._

/**
 * Pushes down various operators to the underlying data source for better performance. Operators are
 * being pushed down with a specific order. As an example, given a LIMIT has a FILTER child, you
 * can't push down LIMIT if FILTER is not completely pushed down. When both are pushed down, the
 * data source should execute FILTER before LIMIT. And required columns are calculated at the end,
 * because when more operators are pushed down, we may need less columns at Spark side.
 */
object PushDownOperatorsToDataSource extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Note that, we need to collect the target operator along with PROJECT node, as PROJECT may
    // appear in many places for column pruning.
    // TODO: Ideally column pruning should be implemented via a plan property that is propagated
    // top-down, then we can simplify the logic here and only collect target operators.
    val filterPushed = plan transformUp {
      case FilterAndProject(fields, condition, r @ DataSourceV2Relation(_, reader)) =>
        val (candidates, nonDeterministic) =
          splitConjunctivePredicates(condition).partition(_.deterministic)

        val stayUpFilters: Seq[Expression] = reader match {
          case r: SupportsPushDownCatalystFilters =>
            r.pushCatalystFilters(candidates.toArray)

          case r: SupportsPushDownFilters =>
            // A map from original Catalyst expressions to corresponding translated data source
            // filters. If a predicate is not in this map, it means it cannot be pushed down.
            val translatedMap: Map[Expression, sources.Filter] = candidates.flatMap { p =>
              DataSourceStrategy.translateFilter(p).map(f => p -> f)
            }.toMap

            // Catalyst predicate expressions that cannot be converted to data source filters.
            val nonConvertiblePredicates = candidates.filterNot(translatedMap.contains)

            // Data source filters that cannot be pushed down. An unhandled filter means
            // the data source cannot guarantee the rows returned can pass the filter.
            // As a result we must return it so Spark can plan an extra filter operator.
            val unhandledFilters = r.pushFilters(translatedMap.values.toArray).toSet
            val unhandledPredicates = translatedMap.filter { case (_, f) =>
              unhandledFilters.contains(f)
            }.keys

            nonConvertiblePredicates ++ unhandledPredicates

          case _ => candidates
        }

        val filterCondition = (stayUpFilters ++ nonDeterministic).reduceLeftOption(And)
        val withFilter = filterCondition.map(Filter(_, r)).getOrElse(r)
        if (withFilter.output == fields) {
          withFilter
        } else {
          Project(fields, withFilter)
        }
    }

    // TODO: add more push down rules.

    val columnPruned = pushDownRequiredColumns(filterPushed, filterPushed.outputSet)
    // After column pruning, we may have redundant PROJECT nodes in the query plan, remove them.
    RemoveRedundantProject(columnPruned)
  }

  // TODO: nested fields pruning
  private def pushDownRequiredColumns(
      plan: LogicalPlan, requiredByParent: AttributeSet): LogicalPlan = {
    plan match {
      case p @ Project(projectList, child) =>
        val required = projectList.flatMap(_.references)
        p.copy(child = pushDownRequiredColumns(child, AttributeSet(required)))

      case f @ Filter(condition, child) =>
        val required = requiredByParent ++ condition.references
        f.copy(child = pushDownRequiredColumns(child, required))

      case relation: DataSourceV2Relation => relation.reader match {
        case reader: SupportsPushDownRequiredColumns =>
          // TODO: Enable the below assert after we make `DataSourceV2Relation` immutable. Fow now
          // it's possible that the mutable reader being updated by someone else, and we need to
          // always call `reader.pruneColumns` here to correct it.
          // assert(relation.output.toStructType == reader.readSchema(),
          //  "Schema of data source reader does not match the relation plan.")

          val requiredColumns = relation.output.filter(requiredByParent.contains)
          reader.pruneColumns(requiredColumns.toStructType)

          val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
          val newOutput = reader.readSchema().map(_.name).map(nameToAttr)
          relation.copy(output = newOutput)

        case _ => relation
      }

      // TODO: there may be more operators that can be used to calculate the required columns. We
      // can add more and more in the future.
      case _ => plan.mapChildren(c => pushDownRequiredColumns(c, c.outputSet))
    }
  }

  /**
   * Finds a Filter node(with an optional Project child) above data source relation.
   */
  object FilterAndProject {
    // returns the project list, the filter condition and the data source relation.
    def unapply(plan: LogicalPlan)
        : Option[(Seq[NamedExpression], Expression, DataSourceV2Relation)] = plan match {

      case Filter(condition, r: DataSourceV2Relation) => Some((r.output, condition, r))

      case Filter(condition, Project(fields, r: DataSourceV2Relation))
          if fields.forall(_.deterministic) =>
        val attributeMap = AttributeMap(fields.map(e => e.toAttribute -> e))
        val substituted = condition.transform {
          case a: Attribute => attributeMap.getOrElse(a, a)
        }
        Some((fields, substituted, r))

      case _ => None
    }
  }
}
