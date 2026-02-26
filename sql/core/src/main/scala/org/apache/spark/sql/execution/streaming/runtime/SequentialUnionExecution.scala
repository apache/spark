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

import org.apache.spark.sql.catalyst.plans.logical.SequentialStreamingUnion
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
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

  /**
   * Checks if a source is active in the sequential union.
   * This is called from constructNextBatch to determine which sources should receive offsets.
   */
  def isSourceActive(source: SparkDataStream): Boolean = {
    initializeChildMapping()
    getActiveChildSources().contains(source)
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
   * Initializes the child-to-sources mapping by traversing the logical plan.
   * This is called lazily since logicalPlan may not be available during construction.
   */
  private def initializeChildMapping(): Unit = {
    if (childToSourcesMap.isEmpty) {
      try {
        // Find the SequentialStreamingUnion node in the plan
        val unionOpt = logicalPlan.collectFirst {
          case union: SequentialStreamingUnion => union
        }

        val union = unionOpt.getOrElse {
          throw new IllegalStateException(
            "SequentialUnionExecution requires a SequentialStreamingUnion in the logical plan")
        }

        sequentialUnion = Some(union)

        // For each child, extract the sources it contains
        val mapping = union.children.zipWithIndex.map { case (child, childIdx) =>
          val childSources = child.collect {
            case s: StreamingExecutionRelation => s.source
            case r: StreamingDataSourceV2ScanRelation => r.stream
          }.toSet

          childIdx -> childSources
        }.toMap

        childToSourcesMap = mapping

        logInfo(s"Initialized SequentialUnionExecution with ${union.children.size} children:")
        childToSourcesMap.foreach { case (idx, srcs) =>
          logInfo(s"  Child $idx has ${srcs.size} source(s)")
        }
      } catch {
        case e: Exception =>
          logError(s"Error initializing SequentialUnionExecution: ${e.getMessage}", e)
          throw e
      }
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
        case (Some(end), Some(start)) => start != end
        case (Some(_), None) => true  // First batch has data
        case _ => false
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
   * Transitions to the next child. Should only be called after the current child is exhausted.
   */
  private def transitionToNextChild(): Unit = {
    require(!isOnFinalChild, "Cannot transition past final child")

    val previousChild = activeChildIndex
    activeChildIndex += 1

    logInfo(s"Sequential union transitioning from child $previousChild to child $activeChildIndex")
  }

  /**
   * Override to skip offset collection for inactive sources.
   * This is the key method that enforces sequential semantics.
   */
  override protected def constructNextBatch(
      execCtx: MicroBatchExecutionContext,
      noDataBatchesEnabled: Boolean): Boolean = {
    // Initialize child mapping on first call
    initializeChildMapping()

    // Let parent construct the batch
    val batchConstructed = super.constructNextBatch(execCtx, noDataBatchesEnabled)

    if (batchConstructed) {
      // Check if active child is exhausted and transition if needed
      if (!isOnFinalChild && isActiveChildExhausted(execCtx)) {
        logInfo(s"Transitioning from child $activeChildIndex to ${activeChildIndex + 1}")
        transitionToNextChild()
      }

      logDebug(s"Active child: $activeChildIndex, active sources: ${getActiveChildSources().size}")
    }

    batchConstructed
  }
}
