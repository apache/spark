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
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.streaming.SupportsSequentialExecution
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.execution.streaming.Source

/**
 * A streaming execution relation that processes multiple sources sequentially.
 * Now that StreamingExecutionRelation is a regular class, we can properly extend it
 * and override the source, output, and catalogTable methods for delegation.
 */
class SequentialUnionExecutionRelation(
    val sourceRelations: Seq[StreamingExecutionRelation],
    session: SparkSession) extends StreamingExecutionRelation(
  sourceRelations.head.source, // Will be overridden by delegation
  sourceRelations.head.output, // Will be overridden by delegation
  None // Will be overridden by delegation
)(session) with Logging {

  require(sourceRelations.nonEmpty, "SequentialUnionExecutionRelation requires at least one source")

  // Create unified schema based on first source (all sources should have compatible schemas)
  private val unifiedOutput: Seq[Attribute] = sourceRelations.head.output

  // Track current active source index
  private val currentSourceIndex = new AtomicInteger(0)

  /**
   * Returns the currently active source relation.
   * Handles transitions between sources as they complete.
   */
  def getActiveSourceRelation(): StreamingExecutionRelation = {
    val currentIndex = currentSourceIndex.get()
    logError(s"### getActiveSourceRelation: currentIndex=$currentIndex," +
      s" total sources=${sourceRelations.length}")

    // Check if we need to transition to next source
    if (currentIndex < sourceRelations.length - 1) {
      val currentRelation = sourceRelations(currentIndex)
      val isComplete = isSourceComplete(currentRelation)
      logError(s"### Source $currentIndex complete: $isComplete")

      if (isComplete) {
        val nextIndex = currentIndex + 1
        if (currentSourceIndex.compareAndSet(currentIndex, nextIndex)) {
          logError(s"### Sequential union transitioning from source $currentIndex to $nextIndex")
        }
        val activeRelation = sourceRelations(currentSourceIndex.get())
        logError(s"### Using source ${currentSourceIndex.get()}:" +
          s" ${activeRelation.source.getClass.getSimpleName}")
        activeRelation
      } else {
        logError(s"### Using current source $currentIndex:" +
          s" ${currentRelation.source.getClass.getSimpleName}")
        currentRelation
      }
    } else {
      // Already at last source
      val activeRelation = sourceRelations(currentIndex)
      logError(s"### Using last source $currentIndex:" +
        s" ${activeRelation.source.getClass.getSimpleName}")
      activeRelation
    }
  }

  // Override to delegate to active source
  override def source: SparkDataStream = {
    val activeSource = getActiveSourceRelation().source
    logError(s"### source() -> ${activeSource.getClass.getSimpleName}")
    activeSource
  }

  // Override to return unified schema (stable across source transitions)
  override def output: Seq[Attribute] = {
    logError(s"### output() -> ${unifiedOutput.map(_.name).mkString(",")}")
    unifiedOutput
  }

  // Override to delegate to active source
  override def catalogTable: Option[CatalogTable] = {
    val activeCatalogTable = getActiveSourceRelation().catalogTable
    logError(s"### catalogTable() -> $activeCatalogTable")
    activeCatalogTable
  }

  /**
   * Determines if a source has completed processing all its data.
   * This uses the SupportsSequentialExecution interface for sources that can
   * signal completion, and falls back to never complete for unbounded sources.
   */
  private def isSourceComplete(relation: StreamingExecutionRelation): Boolean = {
    relation.source match {
      case source: Source with SupportsSequentialExecution =>
        try {
          source.isSourceComplete()
        } catch {
          case e: Exception =>
            logWarning(s"Error checking source completion: ${e.getMessage}")
            false
        }
      case _ =>
        // Sources that don't support completion detection are considered never complete
        // This is the safe default for unbounded sources
        false
    }
  }


  override def toString: String =
    s"SequentialUnion(${sourceRelations.map(_.toString).mkString(" -> ")})"

  // Product methods - required when extending from case class hierarchy
  override def productArity: Int = 1
  override def productElement(n: Int): Any = n match {
    case 0 => sourceRelations
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }
  override def canEqual(that: Any): Boolean = that.isInstanceOf[SequentialUnionExecutionRelation]
}


object SequentialUnionExecutionRelation {
  def apply(
      sourceRelations: Seq[StreamingExecutionRelation],
      session: SparkSession): SequentialUnionExecutionRelation = {
    new SequentialUnionExecutionRelation(sourceRelations, session)
  }
}
