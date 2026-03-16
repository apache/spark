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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * Specifies how we should filter Graph elements.
 */
sealed trait GraphFilter[E] {

  /** Returns the subset of elements provided that match this filter. */
  def filter(elements: Seq[E]): Seq[E]

  /** Returns the subset of elements provided that do not match this filter. */
  def filterNot(elements: Seq[E]): Seq[E]
}

/**
 * Specifies how we should filter Flows.
 */
sealed trait FlowFilter extends GraphFilter[ResolvedFlow]

/**
 * Specifies how we should filter Tables.
 */
sealed trait TableFilter extends GraphFilter[Table] {

  /** Returns whether at least one table will pass the filter. */
  def nonEmpty: Boolean
}

/**
 * Used in full graph update to select all flows.
 */
case object AllFlows extends FlowFilter {
  override def filter(flows: Seq[ResolvedFlow]): Seq[ResolvedFlow] = flows
  override def filterNot(flows: Seq[ResolvedFlow]): Seq[ResolvedFlow] = Seq.empty
}

/**
 * Used in partial graph updates to select flows that flow to "selectedTables".
 */
case class FlowsForTables(selectedTables: Set[TableIdentifier]) extends FlowFilter {

  private def filterCondition(
      flows: Seq[ResolvedFlow],
      useFilterNot: Boolean
  ): Seq[ResolvedFlow] = {
    val (matchingFlows, nonMatchingFlows) = flows.partition { f =>
      selectedTables.contains(f.destinationIdentifier)
    }

    if (useFilterNot) nonMatchingFlows else matchingFlows
  }

  override def filter(flows: Seq[ResolvedFlow]): Seq[ResolvedFlow] = {
    filterCondition(flows, useFilterNot = false)
  }

  override def filterNot(flows: Seq[ResolvedFlow]): Seq[ResolvedFlow] = {
    filterCondition(flows, useFilterNot = true)
  }
}

/** Returns a flow filter that is a union of two flow filters */
case class UnionFlowFilter(oneFilter: FlowFilter, otherFilter: FlowFilter) extends FlowFilter {
  override def filter(flows: Seq[ResolvedFlow]): Seq[ResolvedFlow] = {
    (oneFilter.filter(flows).toSet ++ otherFilter.filter(flows).toSet).toSeq
  }

  override def filterNot(flows: Seq[ResolvedFlow]): Seq[ResolvedFlow] = {
    (flows.toSet -- filter(flows).toSet).toSeq
  }
}

/** Used to specify that no flows should be refreshed. */
case object NoFlows extends FlowFilter {
  override def filter(flows: Seq[ResolvedFlow]): Seq[ResolvedFlow] = Seq.empty
  override def filterNot(flows: Seq[ResolvedFlow]): Seq[ResolvedFlow] = flows
}

/**
 * Used in full graph updates to select all tables.
 */
case object AllTables extends TableFilter {
  override def filter(tables: Seq[Table]): Seq[Table] = tables
  override def filterNot(tables: Seq[Table]): Seq[Table] = Seq.empty

  override def nonEmpty: Boolean = true
}

/**
 * Used to select no tables.
 */
case object NoTables extends TableFilter {
  override def filter(tables: Seq[Table]): Seq[Table] = Seq.empty
  override def filterNot(tables: Seq[Table]): Seq[Table] = tables

  override def nonEmpty: Boolean = false
}

/**
 * Used in partial graph updates to select "selectedTables".
 */
case class SomeTables(selectedTables: Set[TableIdentifier]) extends TableFilter {
  private def filterCondition(tables: Seq[Table], useFilterNot: Boolean): Seq[Table] = {
    tables.filter { t =>
      useFilterNot ^ selectedTables.contains(t.identifier)
    }
  }

  override def filter(tables: Seq[Table]): Seq[Table] =
    filterCondition(tables, useFilterNot = false)

  override def filterNot(tables: Seq[Table]): Seq[Table] =
    filterCondition(tables, useFilterNot = true)

  override def nonEmpty: Boolean = selectedTables.nonEmpty
}
