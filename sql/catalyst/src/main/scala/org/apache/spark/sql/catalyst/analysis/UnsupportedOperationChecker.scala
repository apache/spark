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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.{AnalysisException, InternalOutputModes}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.OutputMode

/**
 * Analyzes the presence of unsupported operations in a logical plan.
 */
object UnsupportedOperationChecker {

  def checkForBatch(plan: LogicalPlan): Unit = {
    plan.foreachUp {
      case p if p.isStreaming =>
        throwError("Queries with streaming sources must be executed with writeStream.start()")(p)

      case _ =>
    }
  }

  def checkForStreaming(plan: LogicalPlan, outputMode: OutputMode): Unit = {

    if (!plan.isStreaming) {
      throwError(
        "Queries without streaming sources cannot be executed with writeStream.start()")(plan)
    }

    // Disallow multiple streaming aggregations
    val aggregates = plan.collect { case a@Aggregate(_, _, _, _) if a.isStreaming => a }

    if (aggregates.size > 1) {
      throwError(
        "Multiple streaming aggregations are not supported with " +
          "streaming DataFrames/Datasets")(plan)
    }

    // Disallow some output mode
    outputMode match {
      case InternalOutputModes.Append if aggregates.nonEmpty =>
        throwError(
          s"$outputMode output mode not supported when there are streaming aggregations on " +
            s"streaming DataFrames/DataSets")(plan)

      case InternalOutputModes.Complete | InternalOutputModes.Update if aggregates.isEmpty =>
        throwError(
          s"$outputMode output mode not supported when there are no streaming aggregations on " +
            s"streaming DataFrames/Datasets")(plan)

      case _ =>
    }

    /**
     * Whether the subplan will contain complete data or incremental data in every incremental
     * execution. Some operations may be allowed only when the child logical plan gives complete
     * data.
     */
    def containsCompleteData(subplan: LogicalPlan): Boolean = {
      val aggs = plan.collect { case a @ Aggregate(_, _, _, _) if a.isStreaming => a }
      // Either the subplan has no streaming source, or it has aggregation with Complete mode
      !subplan.isStreaming || (aggs.nonEmpty && outputMode == InternalOutputModes.Complete)
    }

    plan.foreachUp { implicit subPlan =>

      // Operations that cannot exists anywhere in a streaming plan
      subPlan match {

        case _: Command =>
          throwError("Commands like CreateTable*, AlterTable*, Show* are not supported with " +
            "streaming DataFrames/Datasets")

        case _: InsertIntoTable =>
          throwError("InsertIntoTable is not supported with streaming DataFrames/Datasets")

        case Join(left, right, joinType, _) =>

          joinType match {

            case _: InnerLike =>
              if (left.isStreaming && right.isStreaming) {
                throwError("Inner join between two streaming DataFrames/Datasets is not supported")
              }

            case FullOuter =>
              if (left.isStreaming || right.isStreaming) {
                throwError("Full outer joins with streaming DataFrames/Datasets are not supported")
              }


            case LeftOuter | LeftSemi | LeftAnti =>
              if (right.isStreaming) {
                throwError("Left outer/semi/anti joins with a streaming DataFrame/Dataset " +
                    "on the right is not supported")
              }

            case RightOuter =>
              if (left.isStreaming) {
                throwError("Right outer join with a streaming DataFrame/Dataset on the left is " +
                    "not supported")
              }

            case NaturalJoin(_) | UsingJoin(_, _) =>
              // They should not appear in an analyzed plan.

            case _ =>
              throwError(s"Join type $joinType is not supported with streaming DataFrame/Dataset")
          }

        case c: CoGroup if c.children.exists(_.isStreaming) =>
          throwError("CoGrouping with a streaming DataFrame/Dataset is not supported")

        case u: Union if u.children.map(_.isStreaming).distinct.size == 2 =>
          throwError("Union between streaming and batch DataFrames/Datasets is not supported")

        case Except(left, right) if right.isStreaming =>
          throwError("Except with a streaming DataFrame/Dataset on the right is not supported")

        case Intersect(left, right) if left.isStreaming && right.isStreaming =>
          throwError("Intersect between two streaming DataFrames/Datasets is not supported")

        case GroupingSets(_, _, child, _) if child.isStreaming =>
          throwError("GroupingSets is not supported on streaming DataFrames/Datasets")

        case GlobalLimit(_, _) | LocalLimit(_, _) if subPlan.children.forall(_.isStreaming) =>
          throwError("Limits are not supported on streaming DataFrames/Datasets")

        case Sort(_, _, _) | SortPartitions(_, _) if !containsCompleteData(subPlan) =>
          throwError("Sorting is not supported on streaming DataFrames/Datasets, unless it is on" +
            "aggregated DataFrame/Dataset in Complete mode")

        case Sample(_, _, _, _, child) if child.isStreaming =>
          throwError("Sampling is not supported on streaming DataFrames/Datasets")

        case Window(_, _, _, child) if child.isStreaming =>
          throwError("Non-time-based windows are not supported on streaming DataFrames/Datasets")

        case ReturnAnswer(child) if child.isStreaming =>
          throwError("Cannot return immediate result on streaming DataFrames/Dataset. Queries " +
            "with streaming DataFrames/Datasets must be executed with writeStream.start().")

        case _ =>
      }
    }
  }

  private def throwErrorIf(
      condition: Boolean,
      msg: String)(implicit operator: LogicalPlan): Unit = {
    if (condition) {
      throwError(msg)
    }
  }

  private def throwError(msg: String)(implicit operator: LogicalPlan): Nothing = {
    throw new AnalysisException(
      msg, operator.origin.line, operator.origin.startPosition, Some(operator))
  }
}
