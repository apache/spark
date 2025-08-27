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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.SequentialUnion
import org.apache.spark.sql.connector.read.streaming.SparkDataStream

/**
 * A manager for tracking sequential unions within a streaming query.
 * This class provides a clean abstraction for managing which sources are active
 * in sequential union operations.
 */
class SequentialUnionManager extends Logging {

  private val sequentialUnionTrackers =
    mutable.Map[SequentialUnion, SequentialUnionSourceTracker]()

  /**
   * Register a sequential union for tracking.
   */
  def registerSequentialUnion(
      sequentialUnion: SequentialUnion,
      sources: Seq[SparkDataStream]): Unit = {
    val tracker = new SequentialUnionSourceTracker(sources)
    sequentialUnionTrackers.put(sequentialUnion, tracker)
    logInfo(s"Registered sequential union with ${sources.length} sources")
  }

  /**
   * Check if a source is active in any sequential union.
   * Returns true if the source is active or not part of any sequential union.
   */
  def isSourceActive(source: SparkDataStream): Boolean = {
    sequentialUnionTrackers.values.find(_.getAllSources.contains(source)) match {
      case Some(tracker) => tracker.isSourceActive(source)
      case None => true // Source not part of any sequential union - always active
    }
  }

  /**
   * Get all active sources from sequential unions.
   */
  def getActiveSources: Set[SparkDataStream] = {
    sequentialUnionTrackers.values.map(_.getActiveSource).toSet
  }

  /**
   * Clear all registered sequential unions.
   */
  def clear(): Unit = {
    sequentialUnionTrackers.clear()
  }

  /**
   * Returns debug information about tracked sequential unions.
   */
  def getDebugInfo: String = {
    if (sequentialUnionTrackers.isEmpty) {
      "No sequential unions tracked"
    } else {
      sequentialUnionTrackers.values.map(_.toString).mkString("; ")
    }
  }
}
