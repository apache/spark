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

    // scalastyle:off println
    println("[SEQEXEC] === Initializing ChildToSourcesMap ===")
    plan.collectFirst {
      case union: SequentialStreamingUnion => union
    }.foreach { union =>
      sequentialUnion = Some(union)

      // Extract sources from each child
      val mapping = mutable.Map[Int, Set[SparkDataStream]]()

      union.children.zipWithIndex.foreach { case (child, childIdx) =>
        println(s"[SEQEXEC] Processing child $childIdx:")
        val childSources = child.collect {
          case s: StreamingExecutionRelation =>
            println(s"[SEQEXEC]   Found StreamingExecutionRelation with source: " +
              s"${s.source.getClass.getSimpleName}@${System.identityHashCode(s.source)}")
            s.source
          case r: StreamingDataSourceV2ScanRelation =>
            println(s"[SEQEXEC]   Found StreamingDataSourceV2ScanRelation with stream: " +
              s"${r.stream.getClass.getSimpleName}@${System.identityHashCode(r.stream)}")
            r.stream
        }.toSet

        if (childSources.nonEmpty) {
          mapping(childIdx) = childSources
          val sourceNames = childSources.map(_.getClass.getSimpleName).mkString(", ")
          val srcMsg = s"[SEQEXEC] Found ${childSources.size} source(s) for " +
            s"child $childIdx: $sourceNames"
          println(srcMsg)
        } else {
          println(s"[SEQEXEC] No sources found for child $childIdx")
        }
      }

      childToSourcesMap = mapping.toMap

      val numChildren = union.children.size
      println(s"[SEQEXEC] Initialized SequentialUnionExecution with $numChildren children:")
      childToSourcesMap.foreach { case (idx, srcs) =>
        val srcDescr = srcs.map { s =>
          s"${s.getClass.getSimpleName}@${System.identityHashCode(s)}"
        }.mkString(", ")
        println(s"[SEQEXEC]   Child $idx has ${srcs.size} source(s): $srcDescr")
      }

      // Note: We will prepare and collect offsets for non-final children when they become active.
      // This is done in transitionToNextChild() for the active child, and in constructNextBatch()
      // for the first call to ensure proper initialization.
    }
    // scalastyle:on println
  }

  /**
   * Checks if a source is active in the sequential union.
   * This is called from constructNextBatch to determine which
   * sources should receive offsets.
   *
   * Note: We also consider the NEXT child's sources as "active" if we're about to transition.
   * This ensures isNewDataAvailable() will return true and trigger another batch construction.
   */
  def isSourceActive(source: SparkDataStream): Boolean = {
    val activeChildSources = getActiveChildSources()
    var isActive = activeChildSources.contains(source)

    // Also mark next child's sources as active to collect their offsets early
    // This ensures they have valid offsets ready when auto-transition happens
    if (!isActive && !isOnFinalChild) {
      val nextChildSources = getSourcesForChild(activeChildIndex + 1)
      isActive = nextChildSources.contains(source)
      if (isActive) {
        // scalastyle:off println
        println(s"[SEQEXEC] Source from next child marked as active for offset collection")
        // scalastyle:on println
      }
    }

    val srcId = s"${source.getClass.getSimpleName}@${System.identityHashCode(source)}"
    val activeSrcIds = getActiveChildSources().map { s =>
      s"${s.getClass.getSimpleName}@${System.identityHashCode(s)}"
    }.mkString(",")
    val msg = s"[SEQEXEC] isSourceActive: source=$srcId, activeChild=$activeChildIndex, " +
      s"isActive=$isActive, activeSources=$activeSrcIds"
    // scalastyle:off println
    println(msg)
    // scalastyle:on println
    isActive
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
          val different = start != end
          // scalastyle:off println
          val srcId = s"${source.getClass.getSimpleName}@${System.identityHashCode(source)}"
          println(s"[SEQEXEC] Checking exhaustion for $srcId: start=$start, end=$end, " +
            s"hasData=$different")
          // scalastyle:on println
          different
        case (Some(end), None) =>
          // scalastyle:off println
          val srcId = s"${source.getClass.getSimpleName}@${System.identityHashCode(source)}"
          println(s"[SEQEXEC] Checking exhaustion for $srcId: start=None, end=$end, " +
            s"hasData=true (first batch)")
          // scalastyle:on println
          true  // First batch has data
        case _ =>
          // scalastyle:off println
          val srcId = s"${source.getClass.getSimpleName}@${System.identityHashCode(source)}"
          println(s"[SEQEXEC] Checking exhaustion for $srcId: offsets missing, hasData=false")
          // scalastyle:on println
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
    // scalastyle:off println
    println("[SEQEXEC] >>> prepareActiveSourceForAvailableNow ENTER")
    if (isOnFinalChild) {
      println(s"[SEQEXEC] Child $activeChildIndex is final - not preparing with AvailableNow")
      println("[SEQEXEC] >>> prepareActiveSourceForAvailableNow EXIT (final child)")
      return
    }

    val activeChildSources = getActiveChildSources()
    val msg1 = s"[SEQEXEC] Preparing ${activeChildSources.size} source(s) for " +
      s"child $activeChildIndex with AvailableNow semantics"
    println(msg1)

    activeChildSources.foreach {
      case s: SupportsTriggerAvailableNow =>
        val srcId = s"${s.getClass.getSimpleName}@${System.identityHashCode(s)}"
        println(s"[SEQEXEC]   >>> BEFORE prepareForTriggerAvailableNow on $srcId")
        s.prepareForTriggerAvailableNow()
        println(s"[SEQEXEC]   >>> AFTER prepareForTriggerAvailableNow on $srcId")
      case s =>
        val msg2 = s"[SEQEXEC]   WARNING: Source ${s.getClass.getSimpleName} " +
          s"does not support AvailableNow"
        println(msg2)
    }
    println("[SEQEXEC] >>> prepareActiveSourceForAvailableNow EXIT (all sources prepared)")
    // scalastyle:on println
  }

  /**
   * Transitions to the next child. Should only be called after the current child is exhausted.
   */
  private def transitionToNextChild(): Unit = {
    require(!isOnFinalChild, "Cannot transition past final child")

    val previousChild = activeChildIndex
    activeChildIndex += 1

    // scalastyle:off println
    println(s"[SEQEXEC] *** TRANSITIONING from child $previousChild to child $activeChildIndex ***")
    // scalastyle:on println

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
    // scalastyle:off println
    println("[SEQEXEC] === CONSTRUCT BATCH START (child=" + activeChildIndex + ") ===")
    // scalastyle:on println

    // Initialize mapping on first use using the logical plan
    if (childToSourcesMap.isEmpty) {
      // scalastyle:off println
      println("[SEQEXEC] === INITIALIZING (first batch) ===")
      // scalastyle:on println
      initializeChildToSourcesMap(logicalPlan)
      // Prepare the initial (first) child with AvailableNow semantics
      // scalastyle:off println
      println("[SEQEXEC] === CALLING prepareActiveSourceForAvailableNow (initial) ===")
      // scalastyle:on println
      prepareActiveSourceForAvailableNow()
      // scalastyle:off println
      println("[SEQEXEC] === DONE prepareActiveSourceForAvailableNow (initial) ===")
      // scalastyle:on println
    }

    // If we flagged a transition in the previous batch, do it now BEFORE constructing
    // This ensures the transition happens after the previous batch was fully executed
    if (shouldTransitionNext) {
      // scalastyle:off println
      println(s"[SEQEXEC] === TRANSITION START ===")
      println(s"[SEQEXEC] Executing queued transition from previous batch")
      // scalastyle:on println
      transitionToNextChild()
      shouldTransitionNext = false
      // scalastyle:off println
      println(s"[SEQEXEC] === TRANSITION COMPLETE ===")
      // scalastyle:on println
    }

    // Let parent construct the batch
    // scalastyle:off println
    println("[SEQEXEC] === CALLING SUPER.CONSTRUCT ===")
    // scalastyle:on println
    val batchConstructed = super.constructNextBatch(execCtx, noDataBatchesEnabled)
    // scalastyle:off println
    println("[SEQEXEC] === SUPER.CONSTRUCT RETURNED: " + batchConstructed + " ===")
    // scalastyle:on println

    if (batchConstructed) {
      // Check if active child is exhausted and queue transition for next batch
      // Auto-transition works with any trigger type - no MultiBatchExecutor requirement
      // scalastyle:off println
      println("[SEQEXEC] === CHECKING IF EXHAUSTED ===")
      // scalastyle:on println
      val exhausted = isActiveChildExhausted(execCtx)
      // scalastyle:off println
      val msg3 = s"[SEQEXEC] Batch constructed: activeChild=$activeChildIndex, " +
        s"exhausted=$exhausted, isOnFinalChild=$isOnFinalChild"
      println(msg3)
      // scalastyle:on println

      if (!isOnFinalChild && exhausted) {
        // scalastyle:off println
        println(s"[SEQEXEC] Child $activeChildIndex EXHAUSTED, will transition after this batch")
        // scalastyle:on println
        shouldTransitionNext = true
      }
    } else {
      // No batch constructed - check if we should transition to next child
      // scalastyle:off println
      println("[SEQEXEC] === NO BATCH CONSTRUCTED, CHECKING IF SHOULD TRANSITION ===")
      // scalastyle:on println
      val exhausted = isActiveChildExhausted(execCtx)
      // scalastyle:off println
      val msg4 = s"[SEQEXEC] No batch: activeChild=$activeChildIndex, " +
        s"exhausted=$exhausted, isOnFinalChild=$isOnFinalChild"
      println(msg4)
      // scalastyle:on println

      if (!isOnFinalChild && exhausted) {
        // Active child is exhausted and we're not on the final child
        // Transition immediately to the next child and try constructing a batch for it
        // scalastyle:off println
        println(s"[SEQEXEC] Child $activeChildIndex exhausted, transitioning immediately")
        // scalastyle:on println
        transitionToNextChild()

        // Now try to construct a batch for the new child
        // scalastyle:off println
        println(s"[SEQEXEC] === RECURSIVE CONSTRUCT AFTER TRANSITION ===")
        // scalastyle:on println
        return constructNextBatch(execCtx, noDataBatchesEnabled)
      }
    }

    // scalastyle:off println
    println("[SEQEXEC] === CONSTRUCT BATCH END (returning " + batchConstructed + ") ===")
    // scalastyle:on println
    batchConstructed
  }
}
