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

package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

/**
 * A Strategy for planning scans over data sources defined using the sources API.
 */
private[sql] object DataSourceStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(projectList, filters, l @ LogicalRelation(t: PrunedFilteredScan)) =>
      pruneFilterProject(
        l,
        projectList,
        filters,
        (a, f) => t.buildScan(a, f)) :: Nil

    case PhysicalOperation(projectList, filters, l @ LogicalRelation(t: PrunedScan)) =>
      pruneFilterProject(
        l,
        projectList,
        filters,
        (a, _) => t.buildScan(a)) :: Nil

    case l @ LogicalRelation(t: TableScan) =>
      execution.PhysicalRDD(l.output, t.buildScan()) :: Nil

    case _ => Nil
  }

  protected def pruneFilterProject(
    relation: LogicalRelation,
    projectList: Seq[NamedExpression],
    filterPredicates: Seq[Expression],
    scanBuilder: (Array[String], Array[Filter]) => RDD[Row]) = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filterCondition = filterPredicates.reduceLeftOption(And)

    val pushedFilters = selectFilters(filterPredicates.map { _ transform {
      case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
    }}).toArray

    if (projectList.map(_.toAttribute) == projectList &&
        projectSet.size == projectList.size &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns =
        projectList.asInstanceOf[Seq[Attribute]] // Safe due to if above.
          .map(relation.attributeMap)            // Match original case of attributes.
          .map(_.name)
          .toArray

      val scan =
        execution.PhysicalRDD(
          projectList.map(_.toAttribute),
          scanBuilder(requestedColumns, pushedFilters))
      filterCondition.map(execution.Filter(_, scan)).getOrElse(scan)
    } else {
      val requestedColumns = (projectSet ++ filterSet).map(relation.attributeMap).toSeq
      val columnNames = requestedColumns.map(_.name).toArray

      val scan = execution.PhysicalRDD(requestedColumns, scanBuilder(columnNames, pushedFilters))
      execution.Project(projectList, filterCondition.map(execution.Filter(_, scan)).getOrElse(scan))
    }
  }

  protected def selectFilters(filters: Seq[Expression]): Seq[Filter] = filters.collect {
    case expressions.EqualTo(a: Attribute, Literal(v, _)) => EqualTo(a.name, v)
    case expressions.EqualTo(Literal(v, _), a: Attribute) => EqualTo(a.name, v)

    case expressions.GreaterThan(a: Attribute, Literal(v, _)) => GreaterThan(a.name, v)
    case expressions.GreaterThan(Literal(v, _), a: Attribute) => LessThan(a.name, v)

    case expressions.LessThan(a: Attribute, Literal(v, _)) => LessThan(a.name, v)
    case expressions.LessThan(Literal(v, _), a: Attribute) => GreaterThan(a.name, v)

    case expressions.GreaterThanOrEqual(a: Attribute, Literal(v, _)) =>
      GreaterThanOrEqual(a.name, v)
    case expressions.GreaterThanOrEqual(Literal(v, _), a: Attribute) =>
      LessThanOrEqual(a.name, v)

    case expressions.LessThanOrEqual(a: Attribute, Literal(v, _)) => LessThanOrEqual(a.name, v)
    case expressions.LessThanOrEqual(Literal(v, _), a: Attribute) => GreaterThanOrEqual(a.name, v)
  }
}
