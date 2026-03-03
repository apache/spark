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

package org.apache.spark.sql.execution.streaming.runtime

import scala.collection.mutable

import org.apache.spark.sql.catalyst.plans.logical.{
  LogicalPlan,
  SequentialStreamingUnion
}
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.read.streaming.{
  SparkDataStream,
  SupportsTriggerAvailableNow
}
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2ScanRelation
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.util.Clock

/**
 * Streaming execution for queries containing SequentialStreamingUnion.
 *
 * This execution mode processes children sequentially - each child is drained completely
 * before moving to the next. Only the currently active child's sources receive new data;
 * all other sources get endOffset = startOffset (no new data).
 *
 * Key responsibilities:
 * - Track which child index is currently active
 * - Control which sources are active per batch (offset manipulation)
 * - Detect when active child's sources are exhausted
 * - Transition to next child when current is exhausted
 * - Prepare non-final children with AvailableNow semantics
 * - Persist sequential union state in checkpoint
 */
class SequentialUnionExecution(
    sparkSession: SparkSession,
    trigger: Trigger,
    triggerClock: Clock,
    extraOptions: Map[String, String],
    plan: WriteToStream)
  extends MicroBatchExecution(sparkSession, trigger, triggerClock, extraOptions, plan) {

  // Tracks which child is currently active (initialized lazily)
  @volatile private var activeChildIndex: Int = 0

  // Maps child index to the set of sources belonging to that child
  @volatile private var childToSourcesMap: Map[Int, Set[SparkDataStream]] = Map.empty

  // The original SequentialStreamingUnion node from the logical plan
  @volatile private var sequentialUnion: Option[SequentialStreamingUnion] = None

  // Flag to track if we should transition at the start of the next batch
  @volatile private var shouldTransitionNext: Boolean = false

  /**
   * Initialize the child-to-source mapping by traversing the logical plan.
   * Extracts sources from each child of the SequentialStreamingUnion.
   */
  private def initializeChildToSourcesMap(plan: LogicalPlan): Unit = {
    if (childToSourcesMap.nonEmpty) {
      return  // Already initialized
    }

    plan.collectFirst {
      case union: SequentialStreamingUnion => union
    }.foreach { union =>
      sequentialUnion = Some(union)

      // Extract sources from each child
      val mapping = mutable.Map[Int, Set[SparkDataStream]]()

      union.children.zipWithIndex.foreach { case (child, childIdx) =>
        val childSources = child.collect {
          case s: StreamingExecutionRelation =>
            s.source
          case r: StreamingDataSourceV2ScanRelation =>
            r.stream
        }.toSet

        if (childSources.nonEmpty) {
          mapping(childIdx) = childSources
        }
      }

      childToSourcesMap = mapping.toMap
    }
  }

  /**
   * Checks if a source is active in the sequential union.
   * This is called from constructNextBatch to determine which
   * sources should receive offsets.
   *
   * Only sources from the currently active child are considered active.
   * Inactive sources get startOffset==endOffset (no new data).
   */
  def isSourceActive(source: SparkDataStream): Boolean = {
    val activeChildSources = getActiveChildSources()
    activeChildSources.contains(source)
  }

  /**
   * Returns the sources that belong to the specified child index.
   */
  private def getSourcesForChild(childIndex: Int): Set[SparkDataStream] = {
    childToSourcesMap.getOrElse(childIndex, Set.empty)
  }

  /**
   * Returns the sources that belong to the currently active child.
   */
  private def getActiveChildSources(): Set[SparkDataStream] = {
    getSourcesForChild(activeChildIndex)
  }

  /**
   * Gets the start offset for a source from the execution context.
   */
  private def getStartOffsetForSource(
      execCtx: MicroBatchExecutionContext,
      source: SparkDataStream): Any = {
    execCtx.startOffsets.get(source) match {
      case Some(off) => off
      case None => "None"
    }
  }

  /**
   * Checks if the active child's sources are exhausted (no new data available).
   * A source is considered exhausted when endOffset == startOffset.
   */
  private def isActiveChildExhausted(execCtx: MicroBatchExecutionContext): Boolean = {
    val activeChildSources = getActiveChildSources()

    val hasNewData = activeChildSources.exists { source =>
      (execCtx.endOffsets.get(source), execCtx.startOffsets.get(source)) match {
        case (Some(end), Some(start)) =>
          start != end
        case (Some(end), None) =>
          true  // First batch has data
        case _ =>
          false
      }
    }

    !hasNewData
  }

  /**
   * Returns true if we're currently on the final child.
   */
  private def isOnFinalChild: Boolean = {
    val numChildren = sequentialUnion.map(_.children.size).getOrElse(0)
    activeChildIndex >= numChildren - 1
  }

  /**
   * Prepares the active source with AvailableNow semantics to bound it.
   * Called when transitioning to a new non-final child.
   *
   * This is key to completion detection:
   * - Non-final children: call prepareForTriggerAvailableNow() to bound them
   * - After bounding, startOffset==endOffset means "truly exhausted, transition to next"
   * - Final child: never prepared, runs with user's trigger (unbounded)
   */
  private def prepareActiveSourceForAvailableNow(): Unit = {
    if (isOnFinalChild) {
      return
    }

    val activeChildSources = getActiveChildSources()
    activeChildSources.foreach {
      case s: SupportsTriggerAvailableNow =>
        s.prepareForTriggerAvailableNow()
      case _ =>
        // Source does not support AvailableNow
    }
  }

  /**
   * Transitions to the next child. Should only be called after the current child is exhausted.
   */
  private def transitionToNextChild(): Unit = {
    require(!isOnFinalChild, "Cannot transition past final child")

    val previousChild = activeChildIndex
    activeChildIndex += 1

    // Prepare the new active child with AvailableNow semantics (if not final)
    prepareActiveSourceForAvailableNow()
  }

  /**
   * Override to skip offset collection for inactive sources.
   * This is the key method that enforces sequential semantics.
   */
  override protected def constructNextBatch(
      execCtx: MicroBatchExecutionContext,
      noDataBatchesEnabled: Boolean): Boolean = {
    // Initialize mapping on first use using the logical plan
    if (childToSourcesMap.isEmpty) {
      initializeChildToSourcesMap(logicalPlan)
      // Prepare the initial (first) child with AvailableNow semantics
      prepareActiveSourceForAvailableNow()
    }

    // If we flagged a transition in the previous batch, do it now BEFORE constructing
    // This ensures the transition happens after the previous batch was fully executed
    if (shouldTransitionNext) {
      transitionToNextChild()
      shouldTransitionNext = false
    }

    // Let parent construct the batch
    val batchConstructed = super.constructNextBatch(execCtx, noDataBatchesEnabled)

    if (batchConstructed) {
      // Check if active child is exhausted and queue transition for next batch
      // Auto-transition works with any trigger type - no MultiBatchExecutor requirement
      val exhausted = isActiveChildExhausted(execCtx)

      if (!isOnFinalChild && exhausted) {
        shouldTransitionNext = true
      }
    } else {
      // No batch constructed - check if we should transition to next child
      val exhausted = isActiveChildExhausted(execCtx)

      if (!isOnFinalChild && exhausted) {
        // Active child is exhausted and we're not on the final child
        // Transition immediately to the next child and try constructing a batch for it
        transitionToNextChild()

        // Now try to construct a batch for the new child
        return constructNextBatch(execCtx, noDataBatchesEnabled)
      }
    }

    batchConstructed
  }
}
