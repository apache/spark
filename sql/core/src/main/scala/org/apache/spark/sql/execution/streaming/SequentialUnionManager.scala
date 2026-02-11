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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.SequentialStreamingUnion
import org.apache.spark.sql.connector.read.streaming.{SparkDataStream, SupportsTriggerAvailableNow}
import org.apache.spark.sql.execution.streaming.checkpointing.SequentialUnionOffset

/**
 * Manages the state and lifecycle of a SequentialStreamingUnion during streaming execution.
 *
 * This manager tracks:
 * - Which source is currently active
 * - Which sources have been completed
 * - Transitions between sources
 * - Just-in-time preparation of sources with AvailableNow semantics
 *
 * Execution model (sequential child consumption):
 * {{{
 * while (cur_child < num_children - 1):
 *   prepare(cur_child)              # Just-in-time AvailableNow preparation
 *   while (child_has_data):         # Inner drain loop
 *     process_batch()
 *   record_completion()             # Atomically record in offset log
 *   cur_child++                     # Move to next source
 * }}}
 *
 * @param sequentialUnion The logical plan node for the sequential union
 * @param sourceNames     Ordered list of source names (must match children order)
 * @param sourceMap       Mapping from source name to SparkDataStream instance
 */
class SequentialUnionManager(
    sequentialUnion: SequentialStreamingUnion,
    sourceNames: Seq[String],
    sourceMap: Map[String, SparkDataStream]) extends Logging {

  // Validate inputs
  require(sourceNames.nonEmpty, "sourceNames must not be empty")
  require(sourceNames.size >= 2, "SequentialStreamingUnion requires at least 2 sources")
  require(sourceNames.size == sequentialUnion.children.size,
    s"Number of source names (${sourceNames.size}) must match number of children " +
      s"(${sequentialUnion.children.size})")
  require(sourceNames.forall(sourceMap.contains),
    s"All source names must have corresponding entries in sourceMap. " +
      s"Missing: ${sourceNames.filterNot(sourceMap.contains).mkString(", ")}")

  // Mutable state tracking
  private var _activeSourceIndex: Int = 0
  private var _completedSources: Set[String] = Set.empty

  /**
   * Get the name of the currently active source.
   */
  def activeSourceName: String = sourceNames(_activeSourceIndex)

  /**
   * Get the SparkDataStream instance for the currently active source.
   */
  def activeSource: SparkDataStream = sourceMap(activeSourceName)

  /**
   * Check if we're currently on the final source.
   * The final source is special - it runs with the user's trigger, not AvailableNow.
   */
  def isOnFinalSource: Boolean = _activeSourceIndex == sourceNames.length - 1

  /**
   * Get the set of all completed source names.
   */
  def completedSources: Set[String] = _completedSources

  /**
   * Check if a specific source is currently active.
   */
  def isSourceActive(name: String): Boolean = name == activeSourceName

  /**
   * Check if a specific source has been completed.
   */
  def isSourceCompleted(name: String): Boolean = _completedSources.contains(name)

  /**
   * Get the index of the currently active source.
   */
  def activeSourceIndex: Int = _activeSourceIndex

  /**
   * Prepare the current active source with AvailableNow semantics.
   * Called just-in-time before draining each non-final source.
   *
   * Just-in-time preparation (vs preparing all upfront):
   * - Each source gets freshest bound point at the time it starts
   * - Simpler recovery - don't need to track which sources were prepared
   * - Natural flow: prepare -> drain -> transition -> prepare next -> drain -> ...
   *
   * How AvailableNow bounding works (source-specific internals):
   * - FileStreamSource: pre-fetches file list, subsequent `latestOffset()` uses bounded list
   * - CloudFiles (AutoLoader): sets `triggerAvailableNow=true` and records trigger time,
   *   async producers discover files up to that timestamp, then signal completion via
   *   `areAllProducersCompleted` / `setAndWaitAllConsumersCompleted()`
   * - Delta: bounds to a specific table version
   * - Kafka: bounds to specific partition offsets
   *
   * All sources eventually manifest completion as `latestOffset == startOffset`
   * (no new data), which is how the execution layer detects exhaustion.
   */
  def prepareActiveSourceForAvailableNow(): Unit = {
    require(!isOnFinalSource, "Final source should not use AvailableNow preparation")

    val source = activeSource
    source match {
      case s: SupportsTriggerAvailableNow =>
        logInfo(s"Preparing source '$activeSourceName' for AvailableNow draining " +
          s"(index: ${_activeSourceIndex})")
        s.prepareForTriggerAvailableNow()
      case _ =>
        // For sources that don't support SupportsTriggerAvailableNow, they should be wrapped
        // with AvailableNowSourceWrapper or AvailableNowMicroBatchStreamWrapper by the caller
        logWarning(s"Source '$activeSourceName' does not implement SupportsTriggerAvailableNow. " +
          s"It should be wrapped with an AvailableNow wrapper before being passed to " +
          s"SequentialUnionManager.")
    }

    logInfo(s"Source '$activeSourceName' prepared for AvailableNow draining")
  }

  /**
   * Mark current source as complete and advance to next.
   * Returns the new SequentialUnionOffset representing the updated state.
   *
   * IMPORTANT: This should only be called after the current source has been exhausted
   * (latestOffset == startOffset, meaning no new data).
   */
  def transitionToNextSource(): SequentialUnionOffset = {
    require(!isOnFinalSource, "Cannot transition past final source")

    val completedName = activeSourceName
    _completedSources = _completedSources + completedName
    _activeSourceIndex += 1

    logInfo(s"Sequential union transitioning from '$completedName' to '$activeSourceName' " +
      s"(completed sources: ${_completedSources.mkString(", ")})")

    currentOffset
  }

  /**
   * Get current offset representing sequential union state.
   * This offset will be persisted in the offset log alongside individual source offsets.
   */
  def currentOffset: SequentialUnionOffset = {
    SequentialUnionOffset(
      activeSourceName = activeSourceName,
      allSourceNames = sourceNames,
      completedSourceNames = _completedSources
    )
  }

  /**
   * Restore state from a previously persisted offset.
   * Called on query restart to resume from the correct point.
   *
   * Recovery follows AvailableNow semantics:
   * - If we crash during a source's processing, we re-prepare that source and continue draining
   * - Completed sources are skipped (they won't be re-prepared or re-processed)
   * - The active source at the time of the checkpoint becomes the active source on restart
   */
  def restoreFromOffset(offset: SequentialUnionOffset): Unit = {
    // Validate that the offset is compatible with current sources
    require(offset.allSourceNames == sourceNames,
      s"Source names in offset (${offset.allSourceNames.mkString(", ")}) do not match " +
        s"current source names (${sourceNames.mkString(", ")}). Cannot restore.")

    _activeSourceIndex = sourceNames.indexOf(offset.activeSourceName)
    require(_activeSourceIndex >= 0,
      s"Active source '${offset.activeSourceName}' from offset not found in current sources")

    _completedSources = offset.completedSourceNames

    logInfo(s"Restored sequential union state from checkpoint: " +
      s"active='${offset.activeSourceName}' (index: ${_activeSourceIndex}), " +
      s"completed=${_completedSources.mkString(", ")}")
  }

  /**
   * Get the initial offset for a new query (no checkpoint).
   * Starts with the first source as active and no completed sources.
   */
  def initialOffset: SequentialUnionOffset = {
    SequentialUnionOffset(
      activeSourceName = sourceNames.head,
      allSourceNames = sourceNames,
      completedSourceNames = Set.empty
    )
  }
}
