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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.adaptive.LogicalQueryStageStrategy
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FileSourceStrategy}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy

class SparkPlanner(val session: SparkSession, val experimentalMethods: ExperimentalMethods)
  extends SparkStrategies with SQLConfHelper {

  def numPartitions: Int = conf.numShufflePartitions

  override def strategies: Seq[Strategy] =
    experimentalMethods.extraStrategies ++
      extraPlanningStrategies ++ (
      LogicalQueryStageStrategy ::
      PythonEvals ::
      new DataSourceV2Strategy(session) ::
      FileSourceStrategy ::
      DataSourceStrategy ::
      SpecialLimits ::
      Aggregation ::
      Window ::
      JoinSelection ::
      InMemoryScans ::
      SparkScripts ::
      WithCTEStrategy ::
      BasicOperators :: Nil)

  /**
   * Override to add extra planning strategies to the planner. These strategies are tried after
   * the strategies defined in [[ExperimentalMethods]], and before the regular strategies.
   */
  def extraPlanningStrategies: Seq[Strategy] = Nil

  override protected def collectPlaceholders(plan: SparkPlan): Seq[(SparkPlan, LogicalPlan)] = {
    plan.collect {
      case placeholder @ PlanLater(logicalPlan) => placeholder -> logicalPlan
    }
  }

  override protected def prunePlans(plans: Iterator[SparkPlan]): Iterator[SparkPlan] = {
    // TODO: We will need to prune bad plans when we improve plan space exploration
    //       to prevent combinatorial explosion.
    plans
  }

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
      filterCondition.map(FilterExec(_, scan)).getOrElse(scan)
    } else {
      val scan = scanBuilder((projectSet ++ filterSet).toSeq)
      ProjectExec(projectList, filterCondition.map(FilterExec(_, scan)).getOrElse(scan))
    }
  }
}
