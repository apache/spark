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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.{Utils => CUtils}

/**
 * Utility functions used by the query planner to convert our plan to new aggregation code path.
 */
object AggUtils {

  private def mayRemoveAggFilters(exprs: Seq[AggregateExpression]): Seq[AggregateExpression] = {
    exprs.map { ae =>
      if (ae.filter.isDefined) {
        ae.mode match {
          // Aggregate filters are applicable only in partial/complete modes;
          // this method filters out them, otherwise.
          case Partial | Complete => ae
          case _ => ae.copy(filter = None)
        }
      } else {
        ae
      }
    }
  }

  private def createStreamingAggregate(
      requiredChildDistributionExpressions: Option[Seq[Expression]] = None,
      groupingExpressions: Seq[NamedExpression] = Nil,
      aggregateExpressions: Seq[AggregateExpression] = Nil,
      aggregateAttributes: Seq[Attribute] = Nil,
      initialInputBufferOffset: Int = 0,
      resultExpressions: Seq[NamedExpression] = Nil,
      child: SparkPlan): SparkPlan = {
    createAggregate(
      requiredChildDistributionExpressions,
      isStreaming = true,
      groupingExpressions = groupingExpressions,
      aggregateExpressions = aggregateExpressions,
      aggregateAttributes = aggregateAttributes,
      initialInputBufferOffset = initialInputBufferOffset,
      resultExpressions = resultExpressions,
      child = child)
  }

  private def createAggregate(
      requiredChildDistributionExpressions: Option[Seq[Expression]] = None,
      isStreaming: Boolean = false,
      groupingExpressions: Seq[NamedExpression] = Nil,
      aggregateExpressions: Seq[AggregateExpression] = Nil,
      aggregateAttributes: Seq[Attribute] = Nil,
      initialInputBufferOffset: Int = 0,
      resultExpressions: Seq[NamedExpression] = Nil,
      child: SparkPlan): SparkPlan = {
    val useHash = Aggregate.supportsHashAggregate(
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes))
    val forceSortAggregate = forceApplySortAggregate(child.conf)

    if (useHash && !forceSortAggregate) {
      HashAggregateExec(
        requiredChildDistributionExpressions = requiredChildDistributionExpressions,
        isStreaming = isStreaming,
        numShufflePartitions = None,
        groupingExpressions = groupingExpressions,
        aggregateExpressions = mayRemoveAggFilters(aggregateExpressions),
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = initialInputBufferOffset,
        resultExpressions = resultExpressions,
        child = child)
    } else {
      val objectHashEnabled = child.conf.useObjectHashAggregation
      val useObjectHash = Aggregate.supportsObjectHashAggregate(aggregateExpressions)

      if (objectHashEnabled && useObjectHash && !forceSortAggregate) {
        ObjectHashAggregateExec(
          requiredChildDistributionExpressions = requiredChildDistributionExpressions,
          isStreaming = isStreaming,
          numShufflePartitions = None,
          groupingExpressions = groupingExpressions,
          aggregateExpressions = mayRemoveAggFilters(aggregateExpressions),
          aggregateAttributes = aggregateAttributes,
          initialInputBufferOffset = initialInputBufferOffset,
          resultExpressions = resultExpressions,
          child = child)
      } else {
        SortAggregateExec(
          requiredChildDistributionExpressions = requiredChildDistributionExpressions,
          isStreaming = isStreaming,
          numShufflePartitions = None,
          groupingExpressions = groupingExpressions,
          aggregateExpressions = mayRemoveAggFilters(aggregateExpressions),
          aggregateAttributes = aggregateAttributes,
          initialInputBufferOffset = initialInputBufferOffset,
          resultExpressions = resultExpressions,
          child = child)
      }
    }
  }

  def planAggregateWithoutDistinct(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {
    // Check if we can use HashAggregate.

    // 1. Create an Aggregate Operator for partial aggregations.

    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val partialAggregateExpressions = aggregateExpressions.map(_.copy(mode = Partial))
    val partialAggregateAttributes =
      partialAggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
    val partialResultExpressions =
      groupingAttributes ++
        partialAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

    val partialAggregate = createAggregate(
        requiredChildDistributionExpressions = None,
        groupingExpressions = groupingExpressions,
        aggregateExpressions = partialAggregateExpressions,
        aggregateAttributes = partialAggregateAttributes,
        initialInputBufferOffset = 0,
        resultExpressions = partialResultExpressions,
        child = child)

    // If we have session window expression in aggregation, we add MergingSessionExec to
    // merge sessions with calculating aggregation values.
    val interExec: SparkPlan = mayAppendMergingSessionExec(groupingExpressions,
      aggregateExpressions, partialAggregate)

    // 2. Create an Aggregate Operator for final aggregations.
    val finalAggregateExpressions = aggregateExpressions.map(_.copy(mode = Final))
    // The attributes of the final aggregation buffer, which is presented as input to the result
    // projection:
    val finalAggregateAttributes = finalAggregateExpressions.map(_.resultAttribute)

    val finalAggregate = createAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = finalAggregateExpressions,
        aggregateAttributes = finalAggregateAttributes,
        initialInputBufferOffset = groupingExpressions.length,
        resultExpressions = resultExpressions,
        child = interExec)

    finalAggregate :: Nil
  }

  def planAggregateWithOneDistinct(
      groupingExpressions: Seq[NamedExpression],
      functionsWithDistinct: Seq[AggregateExpression],
      functionsWithoutDistinct: Seq[AggregateExpression],
      distinctExpressions: Seq[Expression],
      normalizedNamedDistinctExpressions: Seq[NamedExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {

    // If we have session window expression in aggregation, we add UpdatingSessionsExec to
    // calculate sessions for input rows and update rows' session column, so that further
    // aggregations can aggregate input rows for the same session.
    val maySessionChild = mayAppendUpdatingSessionExec(groupingExpressions, child)

    val distinctAttributes = normalizedNamedDistinctExpressions.map(_.toAttribute)
    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    // 1. Create an Aggregate Operator for partial aggregations.
    val partialAggregate: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Partial))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      // We will group by the original grouping expression, plus an additional expression for the
      // DISTINCT column. For example, for AVG(DISTINCT value) GROUP BY key, the grouping
      // expressions will be [key, value].
      createAggregate(
        groupingExpressions = groupingExpressions ++ normalizedNamedDistinctExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        resultExpressions = groupingAttributes ++ distinctAttributes ++
          aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = maySessionChild)
    }

    // 2. Create an Aggregate Operator for partial merge aggregations.
    val partialMergeAggregate: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = PartialMerge))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      createAggregate(
        requiredChildDistributionExpressions =
          Some(groupingAttributes ++ distinctAttributes),
        groupingExpressions = groupingAttributes ++ distinctAttributes,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = (groupingAttributes ++ distinctAttributes).length,
        resultExpressions = groupingAttributes ++ distinctAttributes ++
          aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = partialAggregate)
    }

    // 3. Create an Aggregate operator for partial aggregation (for distinct)
    val distinctColumnAttributeLookup = CUtils.toMap(distinctExpressions, distinctAttributes)
    val rewrittenDistinctFunctions = functionsWithDistinct.map {
      // Children of an AggregateFunction with DISTINCT keyword has already
      // been evaluated. At here, we need to replace original children
      // to AttributeReferences.
      case agg @ AggregateExpression(aggregateFunction, mode, true, _, _) =>
        aggregateFunction.transformDown(distinctColumnAttributeLookup)
          .asInstanceOf[AggregateFunction]
      case agg =>
        throw new IllegalArgumentException(
          "Non-distinct aggregate is found in functionsWithDistinct " +
          s"at planAggregateWithOneDistinct: $agg")
    }

    val partialDistinctAggregate: SparkPlan = {
      val mergeAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = PartialMerge))
      // The attributes of the final aggregation buffer, which is presented as input to the result
      // projection:
      val mergeAggregateAttributes = mergeAggregateExpressions.map(_.resultAttribute)
      val (distinctAggregateExpressions, distinctAggregateAttributes) =
        rewrittenDistinctFunctions.zipWithIndex.map { case (func, i) =>
          // We rewrite the aggregate function to a non-distinct aggregation because
          // its input will have distinct arguments.
          // We just keep the isDistinct setting to true, so when users look at the query plan,
          // they still can see distinct aggregations.
          val expr = AggregateExpression(func, Partial, isDistinct = true)
          // Use original AggregationFunction to lookup attributes, which is used to build
          // aggregateFunctionToAttribute
          val attr = functionsWithDistinct(i).resultAttribute
          (expr, attr)
      }.unzip

      val partialAggregateResult = groupingAttributes ++
          mergeAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes) ++
          distinctAggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)
      createAggregate(
        groupingExpressions = groupingAttributes,
        aggregateExpressions = mergeAggregateExpressions ++ distinctAggregateExpressions,
        aggregateAttributes = mergeAggregateAttributes ++ distinctAggregateAttributes,
        initialInputBufferOffset = (groupingAttributes ++ distinctAttributes).length,
        resultExpressions = partialAggregateResult,
        child = partialMergeAggregate)
    }

    // 4. Create an Aggregate Operator for the final aggregation.
    val finalAndCompleteAggregate: SparkPlan = {
      val finalAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Final))
      // The attributes of the final aggregation buffer, which is presented as input to the result
      // projection:
      val finalAggregateAttributes = finalAggregateExpressions.map(_.resultAttribute)

      val (distinctAggregateExpressions, distinctAggregateAttributes) =
        rewrittenDistinctFunctions.zipWithIndex.map { case (func, i) =>
          // We rewrite the aggregate function to a non-distinct aggregation because
          // its input will have distinct arguments.
          // We just keep the isDistinct setting to true, so when users look at the query plan,
          // they still can see distinct aggregations.
          val expr = AggregateExpression(func, Final, isDistinct = true)
          // Use original AggregationFunction to lookup attributes, which is used to build
          // aggregateFunctionToAttribute
          val attr = functionsWithDistinct(i).resultAttribute
          (expr, attr)
      }.unzip

      createAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = finalAggregateExpressions ++ distinctAggregateExpressions,
        aggregateAttributes = finalAggregateAttributes ++ distinctAggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = resultExpressions,
        child = partialDistinctAggregate)
    }

    finalAndCompleteAggregate :: Nil
  }

  /**
   * Plans a streaming aggregation using the following progression:
   *  - Partial Aggregation
   *  - Shuffle
   *  - Partial Merge (now there is at most 1 tuple per group)
   *  - StateStoreRestore (now there is 1 tuple from this batch + optionally one from the previous)
   *  - PartialMerge (now there is at most 1 tuple per group)
   *  - StateStoreSave (saves the tuple for the next batch)
   *  - Complete (output the current result of the aggregation)
   */
  def planStreamingAggregation(
      groupingExpressions: Seq[NamedExpression],
      functionsWithoutDistinct: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      stateFormatVersion: Int,
      child: SparkPlan): Seq[SparkPlan] = {

    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    val partialAggregate: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Partial))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      createStreamingAggregate(
        groupingExpressions = groupingExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        resultExpressions = groupingAttributes ++
            aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = child)
    }

    val partialMerged1: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = PartialMerge))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      createStreamingAggregate(
        requiredChildDistributionExpressions =
            Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = groupingAttributes ++
            aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = partialAggregate)
    }

    val restored = StateStoreRestoreExec(groupingAttributes, None, stateFormatVersion,
      partialMerged1)

    val partialMerged2: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = PartialMerge))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      createStreamingAggregate(
        requiredChildDistributionExpressions =
            Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = groupingAttributes ++
            aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = restored)
    }
    // Note: stateId and returnAllStates are filled in later with preparation rules
    // in IncrementalExecution.
    val saved =
      StateStoreSaveExec(
        groupingAttributes,
        stateInfo = None,
        outputMode = None,
        eventTimeWatermark = None,
        stateFormatVersion = stateFormatVersion,
        partialMerged2)

    val finalAndCompleteAggregate: SparkPlan = {
      val finalAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Final))
      // The attributes of the final aggregation buffer, which is presented as input to the result
      // projection:
      val finalAggregateAttributes = finalAggregateExpressions.map(_.resultAttribute)

      createStreamingAggregate(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = finalAggregateExpressions,
        aggregateAttributes = finalAggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = resultExpressions,
        child = saved)
    }

    finalAndCompleteAggregate :: Nil
  }

  /**
   * Plans a streaming session aggregation using the following progression:
   *
   *  - Partial Aggregation
   *    - all tuples will have aggregated columns with initial value
   *  - (If "spark.sql.streaming.sessionWindow.merge.sessions.in.local.partition" is enabled)
   *    - Sort within partition (sort: all keys)
   *    - MergingSessionExec
   *      - calculate session among tuples, and aggregate tuples in session with partial merge
   *  - Shuffle & Sort (distribution: keys "without" session, sort: all keys)
   *  - SessionWindowStateStoreRestore (group: keys "without" session)
   *    - merge input tuples with stored tuples (sessions) respecting sort order
   *  - MergingSessionExec
   *    - calculate session among tuples, and aggregate tuples in session with partial merge
   *    - NOTE: it leverages the fact that the output of SessionWindowStateStoreRestore is sorted
   *    - now there is at most 1 tuple per group, key with session
   *  - SessionWindowStateStoreSave (group: keys "without" session)
   *    - saves tuple(s) for the next batch (multiple sessions could co-exist at the same time)
   *  - Complete (output the current result of the aggregation)
   */
  def planStreamingAggregationForSession(
      groupingExpressions: Seq[NamedExpression],
      sessionExpression: NamedExpression,
      functionsWithoutDistinct: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      stateFormatVersion: Int,
      mergeSessionsInLocalPartition: Boolean,
      child: SparkPlan): Seq[SparkPlan] = {

    val groupWithoutSessionExpression = groupingExpressions.filterNot { p =>
      p.semanticEquals(sessionExpression)
    }

    if (groupWithoutSessionExpression.isEmpty) {
      throw new AnalysisException("Global aggregation with session window in streaming query" +
        " is not supported.")
    }

    val groupingWithoutSessionAttributes = groupWithoutSessionExpression.map(_.toAttribute)

    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    // Here doing partial merge is to have aggregated columns with default value for each row.
    val partialAggregate: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Partial))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      createStreamingAggregate(
        groupingExpressions = groupingExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        resultExpressions = groupingAttributes ++
          aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = child)
    }

    val partialMerged1: SparkPlan = if (mergeSessionsInLocalPartition) {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = PartialMerge))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)

      // sort happens here to merge sessions on each partition
      // this is to reduce amount of rows to shuffle
      MergingSessionsExec(
        requiredChildDistributionExpressions = None,
        isStreaming = true,
        numShufflePartitions = None,
        groupingExpressions = groupingAttributes,
        sessionExpression = sessionExpression,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = groupingAttributes ++
          aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = partialAggregate
      )
    } else {
      partialAggregate
    }

    // shuffle & sort happens here: most of details are also handled in this physical plan
    val restored = SessionWindowStateStoreRestoreExec(groupingWithoutSessionAttributes,
      sessionExpression.toAttribute, stateInfo = None, eventTimeWatermark = None,
      stateFormatVersion, partialMerged1)

    val mergedSessions = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = PartialMerge))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      MergingSessionsExec(
        requiredChildDistributionExpressions = Some(groupingWithoutSessionAttributes),
        isStreaming = true,
        // This will be replaced with actual value in state rule.
        numShufflePartitions = None,
        groupingExpressions = groupingAttributes,
        sessionExpression = sessionExpression,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = groupingAttributes ++
          aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
        child = restored
      )
    }

    // Note: stateId and returnAllStates are filled in later with preparation rules
    // in IncrementalExecution.
    val saved = SessionWindowStateStoreSaveExec(
      groupingWithoutSessionAttributes,
      sessionExpression.toAttribute,
      stateInfo = None,
      outputMode = None,
      eventTimeWatermark = None,
      stateFormatVersion, mergedSessions)

    val finalAndCompleteAggregate: SparkPlan = {
      val finalAggregateExpressions = functionsWithoutDistinct.map(_.copy(mode = Final))
      // The attributes of the final aggregation buffer, which is presented as input to the result
      // projection:
      val finalAggregateAttributes = finalAggregateExpressions.map(_.resultAttribute)

      createStreamingAggregate(
        requiredChildDistributionExpressions = Some(groupingWithoutSessionAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = finalAggregateExpressions,
        aggregateAttributes = finalAggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        resultExpressions = resultExpressions,
        child = saved)
    }

    finalAndCompleteAggregate :: Nil
  }

  private def mayAppendUpdatingSessionExec(
      groupingExpressions: Seq[NamedExpression],
      maybeChildPlan: SparkPlan,
      isStreaming: Boolean = false): SparkPlan = {
    groupingExpressions.find(_.metadata.contains(SessionWindow.marker)) match {
      case Some(sessionExpression) =>
        UpdatingSessionsExec(
          isStreaming = isStreaming,
          // numShufflePartitions will be set to None, and replaced to the actual value in the
          // state rule if the query is streaming.
          numShufflePartitions = None,
          groupingExpressions.map(_.toAttribute),
          sessionExpression.toAttribute,
          maybeChildPlan)

      case None => maybeChildPlan
    }
  }

  private def mayAppendMergingSessionExec(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      partialAggregate: SparkPlan,
      isStreaming: Boolean = false): SparkPlan = {
    groupingExpressions.find(_.metadata.contains(SessionWindow.marker)) match {
      case Some(sessionExpression) =>
        val aggExpressions = aggregateExpressions.map(_.copy(mode = PartialMerge))
        val aggAttributes = aggregateExpressions.map(_.resultAttribute)

        val groupingAttributes = groupingExpressions.map(_.toAttribute)
        val groupingWithoutSessionExpressions = groupingExpressions.diff(Seq(sessionExpression))
        val groupingWithoutSessionsAttributes = groupingWithoutSessionExpressions
          .map(_.toAttribute)

        MergingSessionsExec(
          requiredChildDistributionExpressions = Some(groupingWithoutSessionsAttributes),
          isStreaming = isStreaming,
          // numShufflePartitions will be set to None, and replaced to the actual value in the
          // state rule if the query is streaming.
          numShufflePartitions = None,
          groupingExpressions = groupingAttributes,
          sessionExpression = sessionExpression,
          aggregateExpressions = aggExpressions,
          aggregateAttributes = aggAttributes,
          initialInputBufferOffset = groupingAttributes.length,
          resultExpressions = groupingAttributes ++
            aggExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes),
          child = partialAggregate
        )

      case None => partialAggregate
    }
  }

  /**
   * Returns whether a sort aggregate should be force applied.
   * The config key is hard-coded because it's testing only and should not be exposed.
   */
  private def forceApplySortAggregate(conf: SQLConf): Boolean = {
    Utils.isTesting &&
      conf.getConfString("spark.sql.test.forceApplySortAggregate", "false") == "true"
  }
}
