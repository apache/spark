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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Analyzes the presence of unsupported operations in a logical plan.
 */
object UnsupportedOperationChecker {

  def checkForBatch(plan: LogicalPlan): Unit = {
    plan.foreachUp {
      case p if p.isStreaming =>
        throwError(
          "Queries with streaming sources must be executed with write.startStream()")(p)

      case _ =>
    }
  }

  def checkForStreaming(plan: LogicalPlan, outputMode: OutputMode): Unit = {

    if (!plan.isStreaming) {
      throwError(
        "Queries without streaming sources cannot be executed with write.startStream()")(plan)
    }

    plan.foreachUp { implicit plan =>

      // Operations that cannot exists anywhere in a streaming plan
      plan match {

        case _: Command =>
          throwError("Commands like CreateTable*, AlterTable*, Show* are not supported with " +
            "streaming DataFrames/Datasets")

        case _: InsertIntoTable =>
          throwError("InsertIntoTable is not supported with streaming DataFrames/Datasets")

        case Aggregate(_, _, child) if child.isStreaming =>
          if (outputMode == Append) {
            throwError(
              "Aggregations are not supported on streaming DataFrames/Datasets in " +
                "Append output mode. Consider changing output mode to Update.")
          }
          val moreStreamingAggregates = child.find {
            case Aggregate(_, _, grandchild) if grandchild.isStreaming => true
            case _ => false
          }
          if (moreStreamingAggregates.nonEmpty) {
            throwError("Multiple streaming aggregations are not supported with " +
              "streaming DataFrames/Datasets")
          }

        case Join(left, right, joinType, _) =>

          joinType match {

            case Inner =>
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

        case GlobalLimit(_, _) | LocalLimit(_, _) if plan.children.forall(_.isStreaming) =>
          throwError("Limits are not supported on streaming DataFrames/Datasets")

        case Sort(_, _, _) | SortPartitions(_, _) if plan.children.forall(_.isStreaming) =>
          throwError("Sorting is not supported on streaming DataFrames/Datasets")

        case Sample(_, _, _, _, child) if child.isStreaming =>
          throwError("Sampling is not supported on streaming DataFrames/Datasets")

        case Window(_, _, _, child) if child.isStreaming =>
          throwError("Non-time-based windows are not supported on streaming DataFrames/Datasets")

        case ReturnAnswer(child) if child.isStreaming =>
          throwError("Cannot return immediate result on streaming DataFrames/Dataset. Queries " +
            "with streaming DataFrames/Datasets must be executed with write.startStream().")

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
