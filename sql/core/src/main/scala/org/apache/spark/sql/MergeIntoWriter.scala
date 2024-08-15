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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, InsertStarAction, MergeAction, MergeIntoTable, UpdateAction, UpdateStarAction}
import org.apache.spark.sql.functions.expr

/**
 * `MergeIntoWriter` provides methods to define and execute merge actions based
 * on specified conditions.
 *
 * @tparam T the type of data in the Dataset.
 * @param table the name of the target table for the merge operation.
 * @param ds the source Dataset to merge into the target table.
 * @param on the merge condition.
 * @param schemaEvolutionEnabled whether to enable automatic schema evolution for this merge
 *                               operation. Default is `false`.
 *
 * @since 4.0.0
 */
@Experimental
class MergeIntoWriter[T] private[sql] (
    table: String,
    ds: Dataset[T],
    on: Column,
    private[sql] val schemaEvolutionEnabled: Boolean = false) {

  private val df: DataFrame = ds.toDF()

  private val sparkSession = ds.sparkSession

  private val tableName = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(table)

  private val logicalPlan = df.queryExecution.logical

  private[sql] var matchedActions: Seq[MergeAction] = Seq.empty[MergeAction]
  private[sql] var notMatchedActions: Seq[MergeAction] = Seq.empty[MergeAction]
  private[sql] var notMatchedBySourceActions: Seq[MergeAction] = Seq.empty[MergeAction]

  /**
   * Initialize a `WhenMatched` action without any condition.
   *
   * This `WhenMatched` action will be executed when a source row matches a target table row based
   * on the merge condition.
   *
   * This `WhenMatched` can be followed by one of the following merge actions:
   *   - `updateAll`: Update all the matched target table rows with source dataset rows.
   *   - `update(Map)`: Update all the matched target table rows while changing only
   *     a subset of columns based on the provided assignment.
   *   - `delete`: Delete all target rows that have a match in the source table.
   *
   * @return a new `WhenMatched` object.
   */
  def whenMatched(): WhenMatched[T] = {
    new WhenMatched[T](this, None)
  }

  /**
   * Initialize a `WhenMatched` action with a condition.
   *
   * This `WhenMatched` action will be executed when a source row matches a target table row based
   * on the merge condition and the specified `condition` is satisfied.
   *
   * This `WhenMatched` can be followed by one of the following merge actions:
   *   - `updateAll`: Update all the matched target table rows with source dataset rows.
   *   - `update(Map)`: Update all the matched target table rows while changing only
   *     a subset of columns based on the provided assignment.
   *   - `delete`: Delete all target rows that have a match in the source table.
   *
   * @param condition a `Column` representing the condition to be evaluated for the action.
   * @return a new `WhenMatched` object configured with the specified condition.
   */
  def whenMatched(condition: Column): WhenMatched[T] = {
    new WhenMatched[T](this, Some(condition.expr))
  }

  /**
   * Initialize a `WhenNotMatched` action without any condition.
   *
   * This `WhenNotMatched` action will be executed when a source row does not match any target row
   * based on the merge condition.
   *
   * This `WhenNotMatched` can be followed by one of the following merge actions:
   *   - `insertAll`: Insert all rows from the source that are not already in the target table.
   *   - `insert(Map)`: Insert all rows from the source that are not already in the target table,
   *      with the specified columns based on the provided assignment.
   *
   * @return a new `WhenNotMatched` object.
   */
  def whenNotMatched(): WhenNotMatched[T] = {
    new WhenNotMatched[T](this, None)
  }

  /**
   * Initialize a `WhenNotMatched` action with a condition.
   *
   * This `WhenNotMatched` action will be executed when a source row does not match any target row
   * based on the merge condition and the specified `condition` is satisfied.
   *
   * This `WhenNotMatched` can be followed by one of the following merge actions:
   *   - `insertAll`: Insert all rows from the source that are not already in the target table.
   *   - `insert(Map)`: Insert all rows from the source that are not already in the target table,
   *     with the specified columns based on the provided assignment.
   *
   * @param condition a `Column` representing the condition to be evaluated for the action.
   * @return a new `WhenNotMatched` object configured with the specified condition.
   */
  def whenNotMatched(condition: Column): WhenNotMatched[T] = {
    new WhenNotMatched[T](this, Some(condition.expr))
  }

  /**
   * Initialize a `WhenNotMatchedBySource` action without any condition.
   *
   * This `WhenNotMatchedBySource` action will be executed when a target row does not match any
   * rows in the source table based on the merge condition.
   *
   * This `WhenNotMatchedBySource` can be followed by one of the following merge actions:
   *   - `updateAll`: Update all the not matched target table rows with source dataset rows.
   *   - `update(Map)`: Update all the not matched target table rows while changing only
   *     the specified columns based on the provided assignment.
   *   - `delete`: Delete all target rows that have no matches in the source table.
   *
   * @return a new `WhenNotMatchedBySource` object.
   */
  def whenNotMatchedBySource(): WhenNotMatchedBySource[T] = {
    new WhenNotMatchedBySource[T](this, None)
  }

  /**
   * Initialize a `WhenNotMatchedBySource` action with a condition.
   *
   * This `WhenNotMatchedBySource` action will be executed when a target row does not match any
   * rows in the source table based on the merge condition and the specified `condition`
   * is satisfied.
   *
   * This `WhenNotMatchedBySource` can be followed by one of the following merge actions:
   *   - `updateAll`: Update all the not matched target table rows with source dataset rows.
   *   - `update(Map)`: Update all the not matched target table rows while changing only
   *     the specified columns based on the provided assignment.
   *   - `delete`: Delete all target rows that have no matches in the source table.
   *
   * @param condition a `Column` representing the condition to be evaluated for the action.
   * @return a new `WhenNotMatchedBySource` object configured with the specified condition.
   */
  def whenNotMatchedBySource(condition: Column): WhenNotMatchedBySource[T] = {
    new WhenNotMatchedBySource[T](this, Some(condition.expr))
  }

  /**
   * Enable automatic schema evolution for this merge operation.
   * @return A `MergeIntoWriter` instance with schema evolution enabled.
   */
  def withSchemaEvolution(): MergeIntoWriter[T] = {
    new MergeIntoWriter[T](this.table, this.ds, this.on, schemaEvolutionEnabled = true)
      .withNewMatchedActions(this.matchedActions: _*)
      .withNewNotMatchedActions(this.notMatchedActions: _*)
      .withNewNotMatchedBySourceActions(this.notMatchedBySourceActions: _*)
  }

  /**
   * Executes the merge operation.
   */
  def merge(): Unit = {
    if (matchedActions.isEmpty && notMatchedActions.isEmpty && notMatchedBySourceActions.isEmpty) {
      throw new SparkRuntimeException(
        errorClass = "NO_MERGE_ACTION_SPECIFIED",
        messageParameters = Map.empty)
    }

    val merge = MergeIntoTable(
      UnresolvedRelation(tableName).forWrite,
      logicalPlan,
      on.expr,
      matchedActions,
      notMatchedActions,
      notMatchedBySourceActions,
      schemaEvolutionEnabled)
    val qe = sparkSession.sessionState.executePlan(merge)
    qe.assertCommandExecuted()
  }

  private[sql] def withNewMatchedActions(actions: MergeAction*): MergeIntoWriter[T] = {
    this.matchedActions ++= actions
    this
  }

  private[sql] def withNewNotMatchedActions(actions: MergeAction*): MergeIntoWriter[T] = {
    this.notMatchedActions ++= actions
    this
  }

  private[sql] def withNewNotMatchedBySourceActions(actions: MergeAction*): MergeIntoWriter[T] = {
    this.notMatchedBySourceActions ++= actions
    this
  }
}

/**
 * A class for defining actions to be taken when matching rows in a DataFrame during
 * a merge operation.
 *
 * @param mergeIntoWriter   The MergeIntoWriter instance responsible for writing data to a
 *                          target DataFrame.
 * @param condition         An optional condition Expression that specifies when the actions
 *                          should be applied.
 *                          If the condition is None, the actions will be applied to all matched
 *                          rows.
 *
 * @tparam T                The type of data in the MergeIntoWriter.
 */
case class WhenMatched[T] private[sql](
    mergeIntoWriter: MergeIntoWriter[T],
    condition: Option[Expression]) {
  /**
   * Specifies an action to update all matched rows in the DataFrame.
   *
   * @return The MergeIntoWriter instance with the update all action configured.
   */
  def updateAll(): MergeIntoWriter[T] = {
    mergeIntoWriter.withNewMatchedActions(UpdateStarAction(condition))
  }

  /**
   * Specifies an action to update matched rows in the DataFrame with the provided column
   * assignments.
   *
   * @param map A Map of column names to Column expressions representing the updates to be applied.
   * @return The MergeIntoWriter instance with the update action configured.
   */
  def update(map: Map[String, Column]): MergeIntoWriter[T] = {
    mergeIntoWriter.withNewMatchedActions(
      UpdateAction(condition, map.map(x => Assignment(expr(x._1).expr, x._2.expr)).toSeq))
  }

  /**
   * Specifies an action to delete matched rows from the DataFrame.
   *
   * @return The MergeIntoWriter instance with the delete action configured.
   */
  def delete(): MergeIntoWriter[T] = {
    mergeIntoWriter.withNewMatchedActions(DeleteAction(condition))
  }
}

/**
 * A class for defining actions to be taken when no matching rows are found in a DataFrame
 * during a merge operation.
 *
 * @param MergeIntoWriter   The MergeIntoWriter instance responsible for writing data to a
 *                          target DataFrame.
 * @param condition         An optional condition Expression that specifies when the actions
 *                          defined in this configuration should be applied.
 *                          If the condition is None, the actions will be applied when there
 *                          are no matching rows.
 * @tparam T                The type of data in the MergeIntoWriter.
 */
case class WhenNotMatched[T] private[sql](
    mergeIntoWriter: MergeIntoWriter[T],
    condition: Option[Expression]) {

  /**
   * Specifies an action to insert all non-matched rows into the DataFrame.
   *
   * @return The MergeIntoWriter instance with the insert all action configured.
   */
  def insertAll(): MergeIntoWriter[T] = {
    mergeIntoWriter.withNewNotMatchedActions(InsertStarAction(condition))
  }

  /**
   * Specifies an action to insert non-matched rows into the DataFrame with the provided
   * column assignments.
   *
   * @param map A Map of column names to Column expressions representing the values to be inserted.
   * @return The MergeIntoWriter instance with the insert action configured.
   */
  def insert(map: Map[String, Column]): MergeIntoWriter[T] = {
    mergeIntoWriter.withNewNotMatchedActions(
      InsertAction(condition, map.map(x => Assignment(expr(x._1).expr, x._2.expr)).toSeq))
  }
}


/**
 * A class for defining actions to be performed when there is no match by source
 * during a merge operation in a MergeIntoWriter.
 *
 * @param MergeIntoWriter the MergeIntoWriter instance to which the merge actions will be applied.
 * @param condition       an optional condition to be used with the merge actions.
 * @tparam T the type parameter for the MergeIntoWriter.
 */
case class WhenNotMatchedBySource[T] private[sql](
    mergeIntoWriter: MergeIntoWriter[T],
    condition: Option[Expression]) {

  /**
   * Specifies an action to update all non-matched rows in the target DataFrame when
   * not matched by the source.
   *
   * @return The MergeIntoWriter instance with the update all action configured.
   */
  def updateAll(): MergeIntoWriter[T] = {
    mergeIntoWriter.withNewNotMatchedBySourceActions(UpdateStarAction(condition))
  }

  /**
   * Specifies an action to update non-matched rows in the target DataFrame with the provided
   * column assignments when not matched by the source.
   *
   * @param map A Map of column names to Column expressions representing the updates to be applied.
   * @return The MergeIntoWriter instance with the update action configured.
   */
  def update(map: Map[String, Column]): MergeIntoWriter[T] = {
    mergeIntoWriter.withNewNotMatchedBySourceActions(
      UpdateAction(condition, map.map(x => Assignment(expr(x._1).expr, x._2.expr)).toSeq))
  }

  /**
   * Specifies an action to delete non-matched rows from the target DataFrame when not matched by
   * the source.
   *
   * @return The MergeIntoWriter instance with the delete action configured.
   */
  def delete(): MergeIntoWriter[T] = {
    mergeIntoWriter.withNewNotMatchedBySourceActions(DeleteAction(condition))
  }
}
