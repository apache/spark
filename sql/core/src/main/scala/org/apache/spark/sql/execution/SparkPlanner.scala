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

package org.apache.spark.sql.execution

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.DataSourceStrategy

class SparkPlanner(val sqlContext: SQLContext) extends SparkStrategies {
  val sparkContext: SparkContext = sqlContext.sparkContext

  def numPartitions: Int = sqlContext.conf.numShufflePartitions

  def strategies: Seq[Strategy] =
    sqlContext.experimental.extraStrategies ++ (
      DataSourceStrategy ::
      DDLStrategy ::
      TakeOrderedAndProject ::
      Aggregation ::
      LeftSemiJoin ::
      EquiJoinSelection ::
      InMemoryScans ::
      BasicOperators ::
      BroadcastNestedLoop ::
      CartesianProduct ::
      DefaultJoin :: Nil)

  /**
   * Used to build table scan operators where complex projection and filtering are done using
   * separate physical operators.  This function returns the given scan operator with Project and
   * Filter nodes added only when needed.  For example, a Project operator is only used when the
   * final desired output requires complex expressions to be evaluated or when columns can be
   * further eliminated out after filtering has been done.
   *
   * The `prunePushedDownFilters` parameter is used to remove those filters that can be optimized
   * away by the filter pushdown optimization.
   *
   * The required attributes for both filtering and expression evaluation are passed to the
   * provided `scanBuilder` function so that it can avoid unnecessary column materialization.
   */
  def pruneFilterProject(
      projectList: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      prunePushedDownFilters: Seq[Expression] => Seq[Expression],
      scanBuilder: Seq[Attribute] => SparkPlan): SparkPlan = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
    val filterCondition: Option[Expression] =
      prunePushedDownFilters(filterPredicates).reduceLeftOption(catalyst.expressions.And)

    // Right now we still use a projection even if the only evaluation is applying an alias
    // to a column.  Since this is a no-op, it could be avoided. However, using this
    // optimization with the current implementation would change the output schema.
    // TODO: Decouple final output schema from expression evaluation so this copy can be
    // avoided safely.

    if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val scan = scanBuilder(projectList.asInstanceOf[Seq[Attribute]])
      filterCondition.map(Filter(_, scan)).getOrElse(scan)
    } else {
      val scan = scanBuilder((projectSet ++ filterSet).toSeq)
      Project(projectList, filterCondition.map(Filter(_, scan)).getOrElse(scan))
    }
  }
}
