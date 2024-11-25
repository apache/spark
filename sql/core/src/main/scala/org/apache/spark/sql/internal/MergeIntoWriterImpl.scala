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

package org.apache.spark.sql.internal

import scala.collection.mutable

import org.apache.spark.SparkRuntimeException
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{Column, DataFrame, Dataset, MergeIntoWriter}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.functions.expr

/**
 * `MergeIntoWriter` provides methods to define and execute merge actions based
 * on specified conditions.
 *
 * @tparam T the type of data in the Dataset.
 * @param table the name of the target table for the merge operation.
 * @param ds the source Dataset to merge into the target table.
 * @param on the merge condition.
 *
 * @since 4.0.0
 */
@Experimental
class MergeIntoWriterImpl[T] private[sql] (table: String, ds: Dataset[T], on: Column)
  extends MergeIntoWriter[T] {

  private val df: DataFrame = ds.toDF()

  private[sql] val sparkSession = ds.sparkSession
  import sparkSession.RichColumn

  private val tableName = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(table)

  private val logicalPlan = df.queryExecution.logical

  private[sql] val matchedActions = mutable.Buffer.empty[MergeAction]
  private[sql] val notMatchedActions = mutable.Buffer.empty[MergeAction]
  private[sql] val notMatchedBySourceActions = mutable.Buffer.empty[MergeAction]

  /** @inheritdoc */
  def merge(): Unit = {
    if (matchedActions.isEmpty && notMatchedActions.isEmpty && notMatchedBySourceActions.isEmpty) {
      throw new SparkRuntimeException(
        errorClass = "NO_MERGE_ACTION_SPECIFIED",
        messageParameters = Map.empty)
    }

    val merge = MergeIntoTable(
      UnresolvedRelation(tableName).requireWritePrivileges(MergeIntoTable.getWritePrivileges(
        matchedActions, notMatchedActions, notMatchedBySourceActions)),
      logicalPlan,
      on.expr,
      matchedActions.toSeq,
      notMatchedActions.toSeq,
      notMatchedBySourceActions.toSeq,
      schemaEvolutionEnabled)
    val qe = sparkSession.sessionState.executePlan(merge)
    qe.assertCommandExecuted()
  }

  override protected[sql] def insertAll(condition: Option[Column]): MergeIntoWriter[T] = {
    this.notMatchedActions += InsertStarAction(condition.map(_.expr))
    this
  }

  override protected[sql] def insert(
    condition: Option[Column],
    map: Map[String, Column]): MergeIntoWriter[T] = {
    this.notMatchedActions += InsertAction(condition.map(_.expr), mapToAssignments(map))
    this
  }

  override protected[sql] def updateAll(
      condition: Option[Column],
      notMatchedBySource: Boolean): MergeIntoWriter[T] = {
    appendUpdateDeleteAction(UpdateStarAction(condition.map(_.expr)), notMatchedBySource)
  }

  override protected[sql] def update(
      condition: Option[Column],
      map: Map[String, Column],
      notMatchedBySource: Boolean): MergeIntoWriter[T] = {
    appendUpdateDeleteAction(
      UpdateAction(condition.map(_.expr), mapToAssignments(map)),
      notMatchedBySource)
  }

  override protected[sql] def delete(
      condition: Option[Column],
      notMatchedBySource: Boolean): MergeIntoWriter[T] = {
    appendUpdateDeleteAction(DeleteAction(condition.map(_.expr)), notMatchedBySource)
  }

  private def appendUpdateDeleteAction(
      action: MergeAction,
      notMatchedBySource: Boolean): MergeIntoWriter[T] = {
    if (notMatchedBySource) {
      notMatchedBySourceActions += action
    } else {
      matchedActions += action
    }
    this
  }

  private def mapToAssignments(map: Map[String, Column]): Seq[Assignment] = {
    map.map(x => Assignment(expr(x._1).expr, x._2.expr)).toSeq
  }
}
