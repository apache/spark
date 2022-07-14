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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, CurrentDate, CurrentTimestampLike, GroupingSets, LocalTimestamp, MonotonicallyIncreasingID, SessionWindow}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode

/**
 * Analyzes the presence of unsupported operations in a logical plan.
 */
object UnsupportedOperationChecker extends Logging {

  def checkForBatch(plan: LogicalPlan): Unit = {
    plan.foreachUp {
      case p if p.isStreaming =>
        throwError("Queries with streaming sources must be executed with writeStream.start()")(p)

      case _ =>
    }
  }

  /**
   * Checks for possible correctness issue in chained stateful operators. The behavior is
   * controlled by SQL config `spark.sql.streaming.statefulOperator.checkCorrectness.enabled`.
   * Once it is enabled, an analysis exception will be thrown. Otherwise, Spark will just
   * print a warning message.
   */
  def checkStreamingQueryGlobalWatermarkLimit(
      plan: LogicalPlan,
      outputMode: OutputMode): Unit = {
    def isStatefulOperationPossiblyEmitLateRows(p: LogicalPlan): Boolean = p match {
      case s: Aggregate
        if s.isStreaming && outputMode == InternalOutputModes.Append => true
      case Join(left, right, joinType, _, _)
        if left.isStreaming && right.isStreaming && joinType != Inner => true
      case f: FlatMapGroupsWithState
        if f.isStreaming && f.outputMode == OutputMode.Append() => true
      case _ => false
    }

    def isStatefulOperation(p: LogicalPlan): Boolean = p match {
      case s: Aggregate if s.isStreaming => true
      case _ @ Join(left, right, _, _, _) if left.isStreaming && right.isStreaming => true
      case f: FlatMapGroupsWithState if f.isStreaming => true
      case d: Deduplicate if d.isStreaming => true
      case _ => false
    }

    val failWhenDetected = SQLConf.get.statefulOperatorCorrectnessCheckEnabled

    try {
      plan.foreach { subPlan =>
        if (isStatefulOperation(subPlan)) {
          subPlan.find { p =>
            (p ne subPlan) && isStatefulOperationPossiblyEmitLateRows(p)
          }.foreach { _ =>
            val errorMsg = "Detected pattern of possible 'correctness' issue " +
              "due to global watermark. " +
              "The query contains stateful operation which can emit rows older than " +
              "the current watermark plus allowed late record delay, which are \"late rows\"" +
              " in downstream stateful operations and these rows can be discarded. " +
              "Please refer the programming guide doc for more details. If you understand " +
              "the possible risk of correctness issue and still need to run the query, " +
              "you can disable this check by setting the config " +
              "`spark.sql.streaming.statefulOperator.checkCorrectness.enabled` to false."
            throwError(errorMsg)(plan)
          }
        }
      }
    } catch {
      case e: AnalysisException if !failWhenDetected => logWarning(s"${e.message};\n$plan")
    }
  }

  def checkForStreaming(plan: LogicalPlan, outputMode: OutputMode): Unit = {
    if (!plan.isStreaming) {
      throwError(
        "Queries without streaming sources cannot be executed with writeStream.start()")(plan)
    }

    /** Collect all the streaming aggregates in a sub plan */
    def collectStreamingAggregates(subplan: LogicalPlan): Seq[Aggregate] = {
      subplan.collect {
        case a: Aggregate if a.isStreaming => a
        // Since the Distinct node will be replaced to Aggregate in the optimizer rule
        // [[ReplaceDistinctWithAggregate]], here we also need to check all Distinct node by
        // assuming it as Aggregate.
        case d @ Distinct(c: LogicalPlan) if d.isStreaming => Aggregate(c.output, c.output, c)
      }
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

      case InternalOutputModes.Update if aggregates.nonEmpty =>
        val aggregate = aggregates.head

        val existingSessionWindow = aggregate.groupingExpressions.exists {
          case attr: AttributeReference
            if attr.metadata.contains(SessionWindow.marker) &&
               attr.metadata.getBoolean(SessionWindow.marker) => true
          case _ => false
        }

        if (existingSessionWindow) {
          throwError(s"$outputMode output mode not supported for session window on " +
            "streaming DataFrames/DataSets")(plan)
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

        case Aggregate(groupingExpressions, aggregateExpressions, child) =>
          val distinctAggExprs = aggregateExpressions.flatMap { expr =>
            expr.collect { case ae: AggregateExpression if ae.isDistinct => ae }
          }
          val haveGroupingSets = groupingExpressions.exists(_.isInstanceOf[GroupingSets])

          throwErrorIf(
            child.isStreaming && distinctAggExprs.nonEmpty,
            "Distinct aggregations are not supported on streaming DataFrames/Datasets. Consider " +
              "using approx_count_distinct() instead.")

          throwErrorIf(
            child.isStreaming && haveGroupingSets,
            "Grouping Sets is not supported on streaming DataFrames/Datasets"
          )

        case _: Command =>
          throwError("Commands like CreateTable*, AlterTable*, Show* are not supported with " +
            "streaming DataFrames/Datasets")

        case _: InsertIntoDir =>
          throwError("InsertIntoDir is not supported with streaming DataFrames/Datasets")

        // mapGroupsWithState and flatMapGroupsWithState
        case m: FlatMapGroupsWithState if m.isStreaming =>

          // Check compatibility with output modes and aggregations in query
          val aggsInQuery = collectStreamingAggregates(plan)

          if (m.initialState.isStreaming) {
            // initial state has to be a batch relation
            throwError("Non-streaming DataFrame/Dataset is not supported as the" +
              " initial state in [flatMap|map]GroupsWithState operation on a streaming" +
              " DataFrame/Dataset")
          }
          if (m.isMapGroupsWithState) {                       // check mapGroupsWithState
            // allowed only in update query output mode and without aggregation
            if (aggsInQuery.nonEmpty) {
              throwError(
                "mapGroupsWithState is not supported with aggregation " +
                  "on a streaming DataFrame/Dataset")
            } else if (outputMode != InternalOutputModes.Update) {
              throwError(
                "mapGroupsWithState is not supported with " +
                  s"$outputMode output mode on a streaming DataFrame/Dataset")
            }
          } else {                                           // check flatMapGroupsWithState
            if (aggsInQuery.isEmpty) {
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
                    "aggregation on a streaming DataFrame/Dataset")
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

        case j @ Join(left, right, joinType, condition, _) =>
          if (left.isStreaming && right.isStreaming && outputMode != InternalOutputModes.Append) {
            throwError("Join between two streaming DataFrames/Datasets is not supported" +
              s" in ${outputMode} output mode, only in Append output mode")
          }

          joinType match {
            case _: InnerLike =>
              // no further validations needed

            case FullOuter =>
              if (left.isStreaming && !right.isStreaming) {
                throwError("FullOuter joins with streaming DataFrames/Datasets on the left " +
                  "and a static DataFrame/Dataset on the right is not supported")
              } else if (!left.isStreaming && right.isStreaming) {
                throwError("FullOuter joins with streaming DataFrames/Datasets on the right " +
                  "and a static DataFrame/Dataset on the left is not supported")
              } else if (left.isStreaming && right.isStreaming) {
                checkForStreamStreamJoinWatermark(j)
              }

            case LeftAnti =>
              if (right.isStreaming) {
                throwError(s"$LeftAnti joins with a streaming DataFrame/Dataset " +
                    "on the right are not supported")
              }

            // We support streaming left outer and left semi joins with static on the right always,
            // and with stream on both sides under the appropriate conditions.
            case LeftOuter | LeftSemi =>
              if (!left.isStreaming && right.isStreaming) {
                throwError(s"$joinType join with a streaming DataFrame/Dataset " +
                  "on the right and a static DataFrame/Dataset on the left is not supported")
              } else if (left.isStreaming && right.isStreaming) {
                checkForStreamStreamJoinWatermark(j)
              }

            // We support streaming right outer joins with static on the left always, and with
            // stream on both sides under the appropriate conditions.
            case RightOuter =>
              if (left.isStreaming && !right.isStreaming) {
                throwError("RightOuter join with a streaming DataFrame/Dataset on the left and " +
                    "a static DataFrame/DataSet on the right not supported")
              } else if (left.isStreaming && right.isStreaming) {
                checkForStreamStreamJoinWatermark(j)
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

        case Intersect(left, right, _) if left.isStreaming || right.isStreaming =>
          throwError("Intersect of streaming DataFrames/Datasets is not supported")

        case GlobalLimit(_, _) | LocalLimit(_, _)
            if subPlan.children.forall(_.isStreaming) && outputMode == InternalOutputModes.Update =>
          throwError("Limits are not supported on streaming DataFrames/Datasets in Update " +
            "output mode")

        case Offset(_, _) => throwError("Offset is not supported on streaming DataFrames/Datasets")

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

    checkStreamingQueryGlobalWatermarkLimit(plan, outputMode)
  }

  def checkForContinuous(plan: LogicalPlan, outputMode: OutputMode): Unit = {
    checkForStreaming(plan, outputMode)

    plan.foreachUp { implicit subPlan =>
      subPlan match {
        case (_: Project | _: Filter | _: MapElements | _: MapPartitions |
              _: DeserializeToObject | _: SerializeFromObject | _: SubqueryAlias |
              _: TypedFilter) =>
        case v: View if v.isTempViewStoringAnalyzedPlan =>
        case node if node.nodeName == "StreamingRelationV2" =>
        case node =>
          throwError(s"Continuous processing does not support ${node.nodeName} operations.")
      }

      subPlan.expressions.foreach { e =>
        if (e.collectLeaves().exists {
          case (_: CurrentTimestampLike | _: CurrentDate | _: LocalTimestamp) => true
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

  private def checkForStreamStreamJoinWatermark(join: Join): Unit = {
    val watermarkInJoinKeys = StreamingJoinHelper.isWatermarkInJoinKeys(join)

    // Check if the nullable side has a watermark, and there's a range condition which
    // implies a state value watermark on the first side.
    val hasValidWatermarkRange = join.joinType match {
      case LeftOuter | LeftSemi => StreamingJoinHelper.getStateValueWatermark(
        join.left.outputSet, join.right.outputSet, join.condition, Some(1000000)).isDefined
      case RightOuter => StreamingJoinHelper.getStateValueWatermark(
        join.right.outputSet, join.left.outputSet, join.condition, Some(1000000)).isDefined
      case FullOuter =>
        Seq((join.left.outputSet, join.right.outputSet),
          (join.right.outputSet, join.left.outputSet)).exists {
          case (attributesToFindStateWatermarkFor, attributesWithEventWatermark) =>
            StreamingJoinHelper.getStateValueWatermark(attributesToFindStateWatermarkFor,
              attributesWithEventWatermark, join.condition, Some(1000000)).isDefined
        }
      case _ =>
        throwError(
          s"Join type ${join.joinType} is not supported with streaming DataFrame/Dataset")(join)
    }

    if (!watermarkInJoinKeys && !hasValidWatermarkRange) {
      throwError(
        s"Stream-stream ${join.joinType} join between two streaming DataFrame/Datasets " +
          "is not supported without a watermark in the join keys, or a watermark on " +
          "the nullable side and an appropriate range condition")(join)
    }
  }
}
