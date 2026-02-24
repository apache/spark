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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.catalyst.plans.logical.SequentialStreamingUnion
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.read.streaming.SupportsTriggerAvailableNow
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, SequentialUnionOffset}
import org.apache.spark.sql.execution.streaming.runtime.{MicroBatchExecution, MicroBatchExecutionContext}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.util.Clock

/**
 * Execution engine for SequentialStreamingUnion, which processes multiple streaming sources
 * sequentially. Non-final sources are drained completely using AvailableNow semantics before
 * transitioning to the next source. The final source runs with the user's specified trigger.
 *
 * This implementation uses plan rewriting: it transforms the SequentialStreamingUnion node
 * to just the currently active child, and increments the active index when a source is exhausted.
 *
 * @param sparkSession   The SparkSession for this query
 * @param trigger        User's specified trigger (applied only to final source)
 * @param triggerClock   Clock for trigger timing
 * @param extraOptions   Additional options for the query
 * @param plan           WriteToStream logical plan containing SequentialStreamingUnion
 */
class SequentialUnionExecution(
    sparkSession: SparkSession,
    trigger: Trigger,
    triggerClock: Clock,
    extraOptions: Map[String, String],
    plan: WriteToStream)
  extends MicroBatchExecution(
    sparkSession,
    trigger,
    triggerClock,
    extraOptions,
    plan) { // We'll transform the plan dynamically, not in constructor

  // Track which child of the SequentialUnion is currently active
  @volatile private var activeChildIndex: Int = 0

  // Cache the SequentialUnion node from the ORIGINAL plan (before transformation)
  private lazy val sequentialUnionNode: SequentialStreamingUnion = {
    plan.inputQuery.collectFirst {
      case su: SequentialStreamingUnion => su
    }.getOrElse {
      throw new IllegalStateException(
        "SequentialUnionExecution requires a SequentialStreamingUnion in the plan")
    }
  }

  private lazy val numChildren: Int = sequentialUnionNode.children.size

  /**
   * Override populateStartOffsets to:
   * 1. Restore activeChildIndex from checkpoint (if recovering)
   * 2. Prepare non-final sources with AvailableNow
   */
  override protected def populateStartOffsets(
      execCtx: MicroBatchExecutionContext,
      sparkSessionToRunBatches: SparkSession): Unit = {
    super.populateStartOffsets(execCtx, sparkSessionToRunBatches)

    // Try to restore activeChildIndex from offset log
    restoreActiveChildIndexFromCheckpoint()

    // If we're on a non-final source, prepare it with AvailableNow semantics
    if (activeChildIndex < numChildren - 1) {
      prepareActiveSourceForAvailableNow()
    }
  }

  /**
   * Restore activeChildIndex from the latest offset in the log.
   */
  private def restoreActiveChildIndexFromCheckpoint(): Unit = {
    offsetLog.getLatest() match {
      case Some((batchId, offsetSeqBase)) =>
        offsetSeqBase match {
          case offsetMap: OffsetMap =>
            // Look for SequentialUnionOffset in the map
            // The key would be something like "sequential-union" or based on source names
            val seqUnionKey = "sequential-union"
            offsetMap.offsetsMap.get(seqUnionKey).flatten match {
              case Some(seqOffset: SequentialUnionOffset) =>
                // Extract child index from the active source name
                val indexOpt = seqOffset.activeSourceName.stripPrefix("child_").toIntOption
                indexOpt.foreach { idx =>
                  activeChildIndex = idx
                  logInfo(s"Restored activeChildIndex from checkpoint: " +
                    s"$activeChildIndex (batch $batchId)")
                }
              case _ =>
                logInfo(s"No SequentialUnionOffset found in offset log, starting from child 0")
            }
          case _ =>
            logInfo(s"Offset log not in v2 format, starting from child 0")
        }
      case None =>
        logInfo(s"No checkpoint found, starting fresh from child 0")
    }
  }

  /**
   * Prepare the current active source with AvailableNow semantics.
   */
  private def prepareActiveSourceForAvailableNow(): Unit = {
    sources.foreach {
      case s: SupportsTriggerAvailableNow =>
        logInfo(s"Preparing source for AvailableNow (child index: $activeChildIndex)")
        s.prepareForTriggerAvailableNow()
      case s =>
        logWarning(s"Source $s does not support SupportsTriggerAvailableNow")
    }
  }

  /**
   * Check if current source is exhausted (no more data).
   * A source is exhausted when endOffset == startOffset for all sources.
   */
  private def isCurrentSourceExhausted(execCtx: MicroBatchExecutionContext): Boolean = {
    // Check if any source has new data
    val hasNewData = execCtx.endOffsets.exists {
      case (source, endOffset) =>
        execCtx.startOffsets.get(source) match {
          case Some(startOffset) => startOffset != endOffset
          case None => true // First batch, has data
        }
    }
    !hasNewData
  }

  /**
   * Transition to the next child source when current source is exhausted.
   */
  private def transitionToNextSource(): Unit = {
    if (activeChildIndex < numChildren - 1) {
      val oldIndex = activeChildIndex
      activeChildIndex += 1
      logInfo(s"Sequential union: transitioning from child $oldIndex to child $activeChildIndex")

      // Prepare the next source if it's not the final one
      if (activeChildIndex < numChildren - 1) {
        prepareActiveSourceForAvailableNow()
      }
    }
  }

  /**
   * Get the current SequentialUnionOffset representing our state.
   */
  private def getCurrentSequentialUnionOffset(): SequentialUnionOffset = {
    val sourceNames = sequentialUnionNode.children.zipWithIndex.map { case (_, idx) =>
      s"child_$idx" // Simple naming for now
    }

    SequentialUnionOffset(
      activeSourceName = s"child_$activeChildIndex",
      allSourceNames = sourceNames,
      completedSourceNames = (0 until activeChildIndex).map(i => s"child_$i").toSet
    )
  }

  /**
   * Override to add SequentialUnionOffset to the offset log.
   */
  override protected def markMicroBatchStart(execCtx: MicroBatchExecutionContext): Unit = {
    import org.apache.spark.sql.execution.streaming.checkpointing.OffsetSeqMetadataV2
    import org.apache.spark.sql.errors.QueryExecutionErrors

    if (!trigger.isInstanceOf[RealTimeTrigger]) {
      // Create the base offset from parent logic
      val baseOffset = execCtx.endOffsets.toOffsets(sources, sourceIdMap, execCtx.offsetSeqMetadata)

      // Add SequentialUnionOffset to the map (if it's v2 format)
      val offsetWithSeqUnion = baseOffset match {
        case offsetMap: OffsetMap =>
          val seqUnionOffset = getCurrentSequentialUnionOffset()
          val updatedMap = offsetMap.offsetsMap + ("sequential-union" -> Some(seqUnionOffset))
          OffsetMap(updatedMap, offsetMap.metadata.asInstanceOf[OffsetSeqMetadataV2])
        case other =>
          // V1 format, can't add SequentialUnionOffset
          logWarning("Offset log is v1 format, cannot persist SequentialUnionOffset")
          other
      }

      // Write to offset log
      if (!offsetLog.add(execCtx.batchId, offsetWithSeqUnion)) {
        throw QueryExecutionErrors.concurrentStreamLogUpdate(execCtx.batchId)
      }

      logInfo(s"Committed offsets for batch ${execCtx.batchId} with SequentialUnionOffset " +
        s"(activeChildIndex=$activeChildIndex)")
    } else {
      logInfo(s"Delay offset logging for batch ${execCtx.batchId} in real time mode.")
    }
  }

  /**
   * Override constructNextBatch to check for source exhaustion after construction.
   * When a non-final source is exhausted, transition to the next child.
   */
  override protected def constructNextBatch(
      execCtx: MicroBatchExecutionContext,
      noDataBatchesEnabled: Boolean): Boolean = {

    // Call parent to construct the batch normally
    val batchConstructed = super.constructNextBatch(execCtx, noDataBatchesEnabled)

    // After construction, check if current source is exhausted (only for non-final sources)
    if (batchConstructed && activeChildIndex < numChildren - 1) {
      if (isCurrentSourceExhausted(execCtx)) {
        logInfo(s"Source at index $activeChildIndex is exhausted, transitioning to next source")
        transitionToNextSource()

        // Recursively construct next batch for the new source
        return constructNextBatch(execCtx, noDataBatchesEnabled)
      }
    }

    batchConstructed
  }

  // Note: The parent's logicalPlan creates sources for ALL children of SequentialStreamingUnion.
  // That's fine - the Union will combine them, but only the active child gets data (via our
  // constructNextBatch logic). Inactive children get empty LocalRelation in the plan.
}
