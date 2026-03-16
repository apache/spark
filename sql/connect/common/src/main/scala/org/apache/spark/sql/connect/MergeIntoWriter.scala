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

package org.apache.spark.sql.connect

import org.apache.spark.SparkRuntimeException
import org.apache.spark.annotation.Experimental
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{Expression, MergeAction, MergeIntoTableCommand}
import org.apache.spark.connect.proto.MergeAction.ActionType._
import org.apache.spark.sql
import org.apache.spark.sql.Column
import org.apache.spark.sql.connect.ColumnNodeToProtoConverter.toExpr
import org.apache.spark.sql.functions.expr

/**
 * `MergeIntoWriter` provides methods to define and execute merge actions based on specified
 * conditions.
 *
 * @tparam T
 *   the type of data in the Dataset.
 * @param table
 *   the name of the target table for the merge operation.
 * @param ds
 *   the source Dataset to merge into the target table.
 * @param on
 *   the merge condition.
 *
 * @since 4.0.0
 */
@Experimental
class MergeIntoWriter[T] private[sql] (table: String, ds: Dataset[T], on: Column)
    extends sql.MergeIntoWriter[T] {

  private val builder = MergeIntoTableCommand
    .newBuilder()
    .setTargetTableName(table)
    .setSourceTablePlan(ds.plan.getRoot)
    .setMergeCondition(toExpr(on))

  /**
   * Executes the merge operation.
   */
  def merge(): Unit = {
    if (builder.getMatchActionsCount == 0 &&
      builder.getNotMatchedActionsCount == 0 &&
      builder.getNotMatchedBySourceActionsCount == 0) {
      throw new SparkRuntimeException(
        errorClass = "NO_MERGE_ACTION_SPECIFIED",
        messageParameters = Map.empty)
    }
    ds.sparkSession.execute(
      proto.Command
        .newBuilder()
        .setMergeIntoTableCommand(builder.setWithSchemaEvolution(schemaEvolutionEnabled))
        .build())
  }

  override protected[sql] def insertAll(condition: Option[Column]): MergeIntoWriter[T] = {
    builder.addNotMatchedActions(buildMergeAction(ACTION_TYPE_INSERT_STAR, condition))
    this
  }

  override protected[sql] def insert(
      condition: Option[Column],
      map: Map[String, Column]): MergeIntoWriter[T] = {
    builder.addNotMatchedActions(buildMergeAction(ACTION_TYPE_INSERT, condition, map))
    this
  }

  override protected[sql] def updateAll(
      condition: Option[Column],
      notMatchedBySource: Boolean): MergeIntoWriter[T] = {
    appendUpdateDeleteAction(
      buildMergeAction(ACTION_TYPE_UPDATE_STAR, condition),
      notMatchedBySource)
  }

  override protected[sql] def update(
      condition: Option[Column],
      map: Map[String, Column],
      notMatchedBySource: Boolean): MergeIntoWriter[T] = {
    appendUpdateDeleteAction(
      buildMergeAction(ACTION_TYPE_UPDATE, condition, map),
      notMatchedBySource)
  }

  override protected[sql] def delete(
      condition: Option[Column],
      notMatchedBySource: Boolean): MergeIntoWriter[T] = {
    appendUpdateDeleteAction(buildMergeAction(ACTION_TYPE_DELETE, condition), notMatchedBySource)
  }

  private def appendUpdateDeleteAction(
      action: Expression,
      notMatchedBySource: Boolean): MergeIntoWriter[T] = {
    if (notMatchedBySource) {
      builder.addNotMatchedBySourceActions(action)
    } else {
      builder.addMatchActions(action)
    }
    this
  }

  private def buildMergeAction(
      actionType: MergeAction.ActionType,
      condition: Option[Column],
      assignments: Map[String, Column] = Map.empty): Expression = {
    val builder = proto.MergeAction.newBuilder().setActionType(actionType)
    condition.foreach(c => builder.setCondition(toExpr(c)))
    assignments.foreach { case (k, v) =>
      builder
        .addAssignmentsBuilder()
        .setKey(toExpr(expr(k)))
        .setValue(toExpr(v))
    }
    Expression
      .newBuilder()
      .setMergeAction(builder)
      .build()
  }
}
