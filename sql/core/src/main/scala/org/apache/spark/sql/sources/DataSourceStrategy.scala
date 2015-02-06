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
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Row, Strategy, execution}

/**
 * A Strategy for planning scans over data sources defined using the sources API.
 */
private[sql] object DataSourceStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {
    case PhysicalOperation(projectList, filters, l @ LogicalRelation(t: CatalystScan)) =>
      pruneFilterProjectRaw(
        l,
        projectList,
        filters,
        (a, f) => t.buildScan(a, f)) :: Nil

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

    case i @ logical.InsertIntoTable(
      l @ LogicalRelation(t: InsertableRelation), partition, query, overwrite) =>
      if (partition.nonEmpty) {
        sys.error(s"Insert into a partition is not allowed because $l is not partitioned.")
      }
      execution.ExecutedCommand(InsertIntoDataSource(l, query, overwrite)) :: Nil

    case _ => Nil
  }

  // Based on Public API.
  protected def pruneFilterProject(
      relation: LogicalRelation,
      projectList: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Array[String], Array[Filter]) => RDD[Row]) = {
    pruneFilterProjectRaw(
      relation,
      projectList,
      filterPredicates,
      (requestedColumns, pushedFilters) => {
        scanBuilder(requestedColumns.map(_.name).toArray, selectFilters(pushedFilters).toArray)
      })
  }

  // Based on Catalyst expressions.
  protected def pruneFilterProjectRaw(
      relation: LogicalRelation,
      projectList: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Seq[Attribute], Seq[Expression]) => RDD[Row]) = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filterCondition = filterPredicates.reduceLeftOption(And)

    val pushedFilters = filterPredicates.map { _ transform {
      case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
    }}

    if (projectList.map(_.toAttribute) == projectList &&
        projectSet.size == projectList.size &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val requestedColumns =
        projectList.asInstanceOf[Seq[Attribute]] // Safe due to if above.
          .map(relation.attributeMap)            // Match original case of attributes.

      val scan =
        execution.PhysicalRDD(
          projectList.map(_.toAttribute),
          scanBuilder(requestedColumns, pushedFilters))
      filterCondition.map(execution.Filter(_, scan)).getOrElse(scan)
    } else {
      val requestedColumns = (projectSet ++ filterSet).map(relation.attributeMap).toSeq

      val scan =
        execution.PhysicalRDD(requestedColumns, scanBuilder(requestedColumns, pushedFilters))
      execution.Project(projectList, filterCondition.map(execution.Filter(_, scan)).getOrElse(scan))
    }
  }

  /** Turn Catalyst [[Expression]]s into data source [[Filter]]s. */
  protected[sql] def selectFilters(filters: Seq[Expression]): Seq[Filter] = filters.collect {
    case expressions.EqualTo(a: Attribute, expressions.Literal(v, _)) => EqualTo(a.name, v)
    case expressions.EqualTo(expressions.Literal(v, _), a: Attribute) => EqualTo(a.name, v)

    case expressions.GreaterThan(a: Attribute, expressions.Literal(v, _)) => GreaterThan(a.name, v)
    case expressions.GreaterThan(expressions.Literal(v, _), a: Attribute) => LessThan(a.name, v)

    case expressions.LessThan(a: Attribute, expressions.Literal(v, _)) => LessThan(a.name, v)
    case expressions.LessThan(expressions.Literal(v, _), a: Attribute) => GreaterThan(a.name, v)

    case expressions.GreaterThanOrEqual(a: Attribute, expressions.Literal(v, _)) =>
      GreaterThanOrEqual(a.name, v)
    case expressions.GreaterThanOrEqual(expressions.Literal(v, _), a: Attribute) =>
      LessThanOrEqual(a.name, v)

    case expressions.LessThanOrEqual(a: Attribute, expressions.Literal(v, _)) =>
      LessThanOrEqual(a.name, v)
    case expressions.LessThanOrEqual(expressions.Literal(v, _), a: Attribute) =>
      GreaterThanOrEqual(a.name, v)

    case expressions.InSet(a: Attribute, set) => In(a.name, set.toArray)
  }
}
