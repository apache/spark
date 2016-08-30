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

package org.apache.spark.sql.execution.subquery

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.SparkSession

/** Holds a subquery logical plan and its data */
private[sql] case class CommonSubqueryItem(plan: LogicalPlan, subquery: CommonSubquery)

/**
 * Provides support to eliminate unnecessary subqueries execution. Unlike the queries that need to
 * be cached with [[CacheManager]], the subqueries are only used in a single query but could be
 * executed multiple times during the query execution. [[CommonSubquery]] is used to keep the
 * common [[SparkPlan]] for the duplicate subqueries in a query.
 */
private[sql] class SubqueryDedup {

  private val subqueryData = new scala.collection.mutable.ArrayBuffer[CommonSubqueryItem]

  /**
   * Creates a [[CommonSubquery]] for the given logical plan. A [[CommonSubquery]]
   * wraps the output of the logical plan, the [[SparkPlan]] of the logical plan and its statistic.
   * This method will first look up if there is already logical plan with the same results in the
   * subqueries list. If so, it returns the previously created [[CommonSubquery]]. If not,
   * this method will create a new [[CommonSubquery]]. Thus, all the logical plans which
   * produce the same results, will refer to the same [[SparkPlan]] which will be executed
   * only once at running stage.
   */
  private[sql] def createCommonSubquery(
      sparkSession: SparkSession,
      planToDedup: LogicalPlan): CommonSubquery = {
    val execution = sparkSession.sessionState.executePlan(planToDedup)
    lookupCommonSubquery(planToDedup).map(_.subquery).getOrElse {
      val common =
        CommonSubqueryItem(
          planToDedup,
          CommonSubquery(planToDedup.output,
            execution.executedPlan)
            (execution.optimizedPlan,
            planToDedup.statistics))
      subqueryData += common
      common.subquery
    }.withOutput(planToDedup.output)
  }

  /** Optionally returns common subquery for the given [[LogicalPlan]]. */
  private[sql] def lookupCommonSubquery(plan: LogicalPlan): Option[CommonSubqueryItem] = {
    subqueryData.find(cd => plan.sameResult(cd.plan))
  }
}
