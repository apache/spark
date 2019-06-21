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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, CurrentDate, CurrentTimestamp, MonotonicallyIncreasingID}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.internal.SQLConf
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

    /** Collect all the streaming aggregates in a sub plan */
    def collectStreamingAggregates(subplan: LogicalPlan): Seq[Aggregate] = {
      subplan.collect { case a: Aggregate if a.isStreaming => a }
    }

    val mapGroupsWithStates = plan.collect {
      case f: FlatMapGroupsWithState if f.isStreaming && f.isMapGroupsWithState => f
    }

    // Disallow multiple `mapGroupsWithState`s.
    if (mapGroupsWithStates.size >= 2) {
      throwError(
        "Multiple mapGroupsWithStates are not supported on a streaming DataFrames/Datasets")(plan)
    }

    val flatMapGroupsWithStates = plan.collect {
      case f: FlatMapGroupsWithState if f.isStreaming && !f.isMapGroupsWithState => f
    }

    // Disallow mixing `mapGroupsWithState`s and `flatMapGroupsWithState`s
    if (mapGroupsWithStates.nonEmpty && flatMapGroupsWithStates.nonEmpty) {
      throwError(
        "Mixing mapGroupsWithStates and flatMapGroupsWithStates are not supported on a " +
          "streaming DataFrames/Datasets")(plan)
    }

    // Only allow multiple `FlatMapGroupsWithState(Append)`s in append mode.
    if (flatMapGroupsWithStates.size >= 2 && (
      outputMode != InternalOutputModes.Append ||
        flatMapGroupsWithStates.exists(_.outputMode != InternalOutputModes.Append)
      )) {
      throwError(
        "Multiple flatMapGroupsWithStates are not supported when they are not all in append mode" +
          " or the output mode is not append on a streaming DataFrames/Datasets")(plan)
    }

    // Disallow multiple streaming aggregations
    val aggregates = collectStreamingAggregates(plan)

    if (aggregates.size > 1) {
      throwError(
        "Multiple streaming aggregations are not supported with " +
          "streaming DataFrames/Datasets")(plan)
    }

    // Disallow some output mode
    outputMode match {
      case InternalOutputModes.Append if aggregates.nonEmpty =>
        val aggregate = aggregates.head

        // Find any attributes that are associated with an eventTime watermark.
        val watermarkAttributes = aggregate.groupingExpressions.collect {
          case a: Attribute if a.metadata.contains(EventTimeWatermark.delayKey) => a
        }

        // We can append rows to the sink once the group is under the watermark. Without this
        // watermark a group is never "finished" so we would never output anything.
        if (watermarkAttributes.isEmpty) {
          throwError(
            s"$outputMode output mode not supported when there are streaming aggregations on " +
                s"streaming DataFrames/DataSets without watermark")(plan)
        }

      case InternalOutputModes.Complete if aggregates.isEmpty =>
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
      val aggs = subplan.collect { case a@Aggregate(_, _, _) if a.isStreaming => a }
      // Either the subplan has no streaming source, or it has aggregation with Complete mode
      !subplan.isStreaming || (aggs.nonEmpty && outputMode == InternalOutputModes.Complete)
    }

    def checkUnsupportedExpressions(implicit operator: LogicalPlan): Unit = {
      val unsupportedExprs = operator.expressions.flatMap(_.collect {
        case m: MonotonicallyIncreasingID => m
      }).distinct
      if (unsupportedExprs.nonEmpty) {
        throwError("Expression(s): " + unsupportedExprs.map(_.sql).mkString(", ") +
          " is not supported with streaming DataFrames/Datasets")
      }
    }

    plan.foreachUp { implicit subPlan =>

      // Operations that cannot exists anywhere in a streaming plan
      subPlan match {

        case Aggregate(_, aggregateExpressions, child) =>
          val distinctAggExprs = aggregateExpressions.flatMap { expr =>
            expr.collect { case ae: AggregateExpression if ae.isDistinct => ae }
          }
          throwErrorIf(
            child.isStreaming && distinctAggExprs.nonEmpty,
            "Distinct aggregations are not supported on streaming DataFrames/Datasets. Consider " +
              "using approx_count_distinct() instead.")

        case _: Command =>
          throwError("Commands like CreateTable*, AlterTable*, Show* are not supported with " +
            "streaming DataFrames/Datasets")

        case _: InsertIntoDir =>
          throwError("InsertIntoDir is not supported with streaming DataFrames/Datasets")

        // mapGroupsWithState and flatMapGroupsWithState
        case m: FlatMapGroupsWithState if m.isStreaming =>

          // Check compatibility with output modes and aggregations in query
          val aggsAfterFlatMapGroups = collectStreamingAggregates(plan)

          if (m.isMapGroupsWithState) {                       // check mapGroupsWithState
            // allowed only in update query output mode and without aggregation
            if (aggsAfterFlatMapGroups.nonEmpty) {
              throwError(
                "mapGroupsWithState is not supported with aggregation " +
                  "on a streaming DataFrame/Dataset")
            } else if (outputMode != InternalOutputModes.Update) {
              throwError(
                "mapGroupsWithState is not supported with " +
                  s"$outputMode output mode on a streaming DataFrame/Dataset")
            }
          } else {                                           // check latMapGroupsWithState
            if (aggsAfterFlatMapGroups.isEmpty) {
              // flatMapGroupsWithState without aggregation: operation's output mode must
              // match query output mode
              m.outputMode match {
                case InternalOutputModes.Update if outputMode != InternalOutputModes.Update =>
                  throwError(
                    "flatMapGroupsWithState in update mode is not supported with " +
                      s"$outputMode output mode on a streaming DataFrame/Dataset")

                case InternalOutputModes.Append if outputMode != InternalOutputModes.Append =>
                  throwError(
                    "flatMapGroupsWithState in append mode is not supported with " +
                      s"$outputMode output mode on a streaming DataFrame/Dataset")

                case _ =>
              }
            } else {
              // flatMapGroupsWithState with aggregation: update operation mode not allowed, and
              // *groupsWithState after aggregation not allowed
              if (m.outputMode == InternalOutputModes.Update) {
                throwError(
                  "flatMapGroupsWithState in update mode is not supported with " +
                    "aggregation on a streaming DataFrame/Dataset")
              } else if (collectStreamingAggregates(m).nonEmpty) {
                throwError(
                  "flatMapGroupsWithState in append mode is not supported after " +
                    s"aggregation on a streaming DataFrame/Dataset")
              }
            }
          }

          // Check compatibility with timeout configs
          if (m.timeout == EventTimeTimeout) {
            // With event time timeout, watermark must be defined.
            val watermarkAttributes = m.child.output.collect {
              case a: Attribute if a.metadata.contains(EventTimeWatermark.delayKey) => a
            }
            if (watermarkAttributes.isEmpty) {
              throwError(
                "Watermark must be specified in the query using " +
                  "'[Dataset/DataFrame].withWatermark()' for using event-time timeout in a " +
                  "[map|flatMap]GroupsWithState. Event-time timeout not supported without " +
                  "watermark.")(plan)
            }
          }

        case d: Deduplicate if collectStreamingAggregates(d).nonEmpty =>
          throwError("dropDuplicates is not supported after aggregation on a " +
            "streaming DataFrame/Dataset")

        case Join(left, right, joinType, condition, _) =>

          joinType match {

            case _: InnerLike =>
              if (left.isStreaming && right.isStreaming &&
                outputMode != InternalOutputModes.Append) {
                throwError("Inner join between two streaming DataFrames/Datasets is not supported" +
                  s" in ${outputMode} output mode, only in Append output mode")
              }

            case FullOuter =>
              if (left.isStreaming || right.isStreaming) {
                throwError("Full outer joins with streaming DataFrames/Datasets are not supported")
              }

            case LeftSemi | LeftAnti =>
              if (right.isStreaming) {
                throwError("Left semi/anti joins with a streaming DataFrame/Dataset " +
                    "on the right are not supported")
              }

            // We support streaming left outer joins with static on the right always, and with
            // stream on both sides under the appropriate conditions.
            case LeftOuter =>
              if (!left.isStreaming && right.isStreaming) {
                throwError("Left outer join with a streaming DataFrame/Dataset " +
                  "on the right and a static DataFrame/Dataset on the left is not supported")
              } else if (left.isStreaming && right.isStreaming) {
                val watermarkInJoinKeys = StreamingJoinHelper.isWatermarkInJoinKeys(subPlan)

                val hasValidWatermarkRange =
                  StreamingJoinHelper.getStateValueWatermark(
                    left.outputSet, right.outputSet, condition, Some(1000000)).isDefined

                if (!watermarkInJoinKeys && !hasValidWatermarkRange) {
                  throwError("Stream-stream outer join between two streaming DataFrame/Datasets " +
                    "is not supported without a watermark in the join keys, or a watermark on " +
                    "the nullable side and an appropriate range condition")
                }
              }

            // We support streaming right outer joins with static on the left always, and with
            // stream on both sides under the appropriate conditions.
            case RightOuter =>
              if (left.isStreaming && !right.isStreaming) {
                throwError("Right outer join with a streaming DataFrame/Dataset on the left and " +
                    "a static DataFrame/DataSet on the right not supported")
              } else if (left.isStreaming && right.isStreaming) {
                val isWatermarkInJoinKeys = StreamingJoinHelper.isWatermarkInJoinKeys(subPlan)

                // Check if the nullable side has a watermark, and there's a range condition which
                // implies a state value watermark on the first side.
                val hasValidWatermarkRange =
                    StreamingJoinHelper.getStateValueWatermark(
                      right.outputSet, left.outputSet, condition, Some(1000000)).isDefined

                if (!isWatermarkInJoinKeys && !hasValidWatermarkRange) {
                  throwError("Stream-stream outer join between two streaming DataFrame/Datasets " +
                    "is not supported without a watermark in the join keys, or a watermark on " +
                    "the nullable side and an appropriate range condition")
                }
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

        case Except(left, right, _) if right.isStreaming =>
          throwError("Except on a streaming DataFrame/Dataset on the right is not supported")

        case Intersect(left, right, _) if left.isStreaming && right.isStreaming =>
          throwError("Intersect between two streaming DataFrames/Datasets is not supported")

        case GroupingSets(_, _, child, _) if child.isStreaming =>
          throwError("GroupingSets is not supported on streaming DataFrames/Datasets")

        case GlobalLimit(_, _) | LocalLimit(_, _)
            if subPlan.children.forall(_.isStreaming) && outputMode == InternalOutputModes.Update =>
          throwError("Limits are not supported on streaming DataFrames/Datasets in Update " +
            "output mode")

        case Sort(_, _, _) if !containsCompleteData(subPlan) =>
          throwError("Sorting is not supported on streaming DataFrames/Datasets, unless it is on " +
            "aggregated DataFrame/Dataset in Complete output mode")

        case Sample(_, _, _, _, child) if child.isStreaming =>
          throwError("Sampling is not supported on streaming DataFrames/Datasets")

        case Window(_, _, _, child) if child.isStreaming =>
          throwError("Non-time-based windows are not supported on streaming DataFrames/Datasets")

        case ReturnAnswer(child) if child.isStreaming =>
          throwError("Cannot return immediate result on streaming DataFrames/Dataset. Queries " +
            "with streaming DataFrames/Datasets must be executed with writeStream.start().")

        case _ =>
      }

      // Check if there are unsupported expressions in streaming query plan.
      checkUnsupportedExpressions(subPlan)
    }
  }

  def checkForContinuous(plan: LogicalPlan, outputMode: OutputMode): Unit = {
    checkForStreaming(plan, outputMode)

    plan.foreachUp { implicit subPlan =>
      subPlan match {
        case (_: Project | _: Filter | _: MapElements | _: MapPartitions |
              _: DeserializeToObject | _: SerializeFromObject | _: SubqueryAlias |
              _: TypedFilter) =>
        case node if node.nodeName == "StreamingRelationV2" =>
        case Repartition(1, false, _) =>
        case node: Aggregate =>
          val aboveSinglePartitionCoalesce = node.find {
            case Repartition(1, false, _) => true
            case _ => false
          }.isDefined

          if (!aboveSinglePartitionCoalesce) {
            throwError(s"In continuous processing mode, coalesce(1) must be called before " +
              s"aggregate operation ${node.nodeName}.")
          }
        case node =>
          throwError(s"Continuous processing does not support ${node.nodeName} operations.")
      }

      subPlan.expressions.foreach { e =>
        if (e.collectLeaves().exists {
          case (_: CurrentTimestamp | _: CurrentDate) => true
          case _ => false
        }) {
          throwError(s"Continuous processing does not support current time operations.")
        }
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
