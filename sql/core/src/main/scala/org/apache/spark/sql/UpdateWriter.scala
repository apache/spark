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

package org.apache.spark.sql

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, UpdateTable}
import org.apache.spark.sql.functions.expr

/**
 * This class defines methods to specify a condition an an update operation
 * or directly executing it.
 *
 * @param dataset DataSet representing table to update.
 * @param assignments A Map of column names to Column expressions representing the updates
 *     to be applied.
 * @param T type of dataset
 *
 * @since 4.0.0
 */
@Experimental
class UpdateWriter[T](dataset: Dataset[T], assignments: Map[String, Column]) {

  /**
   * Limits the update to rows matching the specified condition.
   *
   * @param condition the update condition
   * @return
   */
  def where(condition: Column): UpdateWithCondition[T] = {
    new UpdateWithCondition(dataset, assignments, Some(condition))
  }

  /**
   * Executes the update operation.
   */
  def execute(): Unit = {
    new UpdateWithCondition(dataset, assignments, None).execute()
  }
}

/**
 * A class for executing an update operation.
 *
 * @param dataset Dataset representing table to update.
 * @param assignments A Map of column names to Column expressions representing the updates
 *     to be applied.
 * @param condition the update condition
 * @since 4.0.0
 */
@Experimental
class UpdateWithCondition[T](
    dataset: Dataset[T],
    assignments: Map[String, Column],
    condition: Option[Column]) {

  private val sparkSession = dataset.sparkSession
  private val logicalPlan = dataset.queryExecution.logical

  /**
   * Executes the update operation.
   */
  def execute(): Unit = {
    val update = UpdateTable(
      logicalPlan,
      assignments.map(x => Assignment(expr(x._1).expr, x._2.expr)).toSeq,
      condition.map(_.expr))
    val qe = sparkSession.sessionState.executePlan(update)
    qe.assertCommandExecuted()
  }
}
