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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.streaming.SupportsSequentialExecution
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.streaming.Source

/**
 * A clean, thread-safe tracker for managing sequential source execution.
 * This class encapsulates the logic for determining which source is currently active
 * in a sequential union and when to transition to the next source.
 */
class SequentialUnionSourceTracker(sources: Seq[SparkDataStream]) extends Logging {

  private val currentSourceIndex = new AtomicInteger(0)

  require(sources.nonEmpty, "SequentialUnionSourceTracker requires at least one source")

  /**
   * Returns the currently active source index and the source itself.
   */
  def getCurrentSource: (Int, SparkDataStream) = {
    val currentIndex = currentSourceIndex.get()
    if (currentIndex < sources.length) {
      (currentIndex, sources(currentIndex))
    } else {
      // All sources complete - return last source
      val lastIndex = sources.length - 1
      (lastIndex, sources(lastIndex))
    }
  }

  /**
   * Returns the currently active source, handling transitions automatically.
   * This method checks if the current source is complete and transitions to the next
   * source if necessary.
   */
  def getActiveSource: SparkDataStream = {
    val currentIndex = currentSourceIndex.get()
    logError(s"### getActiveSource: currentIndex=$currentIndex, sources.length=${sources.length}")

    // Check if we need to transition to next source
    if (currentIndex < sources.length - 1) {
      val currentSource = sources(currentIndex)
      val isComplete = isSourceComplete(currentSource)
      logError(s"### Current source ${currentSource.getClass.getSimpleName} complete: $isComplete")

      if (isComplete) {
        val nextIndex = currentIndex + 1
        if (currentSourceIndex.compareAndSet(currentIndex, nextIndex)) {
          logError(s"### Sequential union transitioning from source $currentIndex to $nextIndex")
        } else {
          logError(s"### Failed to transition from source $currentIndex to $nextIndex " +
            s"(race condition)")
        }
      }
    }

    val activeIndex = currentSourceIndex.get()
    val activeSource = if (activeIndex < sources.length) {
      sources(activeIndex)
    } else {
      // All sources complete - return last source
      sources(sources.length - 1)
    }

    logError(s"### Returning active source: index=$activeIndex, " +
      s"source=${activeSource.getClass.getSimpleName}")
    activeSource
  }

  /**
   * Returns all sources that are part of this sequential union.
   */
  def getAllSources: Seq[SparkDataStream] = sources

  /**
   * Determines if a given source is currently active in the sequential execution.
   */
  def isSourceActive(source: SparkDataStream): Boolean = {
    val activeSource = getActiveSource
    val isActive = activeSource == source
    logError(s"### isSourceActive: ${source.getClass.getSimpleName} -> $isActive")
    isActive
  }

  /**
   * Determines if a source has completed processing all its data.
   * This uses the SupportsSequentialExecution interface for sources that can
   * signal completion, and falls back to never complete for unbounded sources.
   */
  private def isSourceComplete(source: SparkDataStream): Boolean = {
    source match {
      case completableSource: Source with SupportsSequentialExecution =>
        try {
          completableSource.isSourceComplete()
        } catch {
          case e: Exception =>
            logWarning(s"Error checking source completion: ${e.getMessage}")
            false
        }
      case completableSource: SupportsSequentialExecution =>
        try {
          completableSource.isSourceComplete()
        } catch {
          case e: Exception =>
            logWarning(s"Error checking completion: ${e.getMessage}")
            false
        }
      case _ =>
        // Sources that don't support completion detection are considered never complete
        // This is the safe default for unbounded sources
        false
    }
  }

  override def toString: String = {
    val activeSource = getCurrentSource
    s"SequentialUnionSourceTracker(active=${activeSource._1}/${sources.length})"
  }
}
