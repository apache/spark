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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.streaming.{StreamingSourceIdentifyingName, Unassigned, UserProvided}
import org.apache.spark.sql.catalyst.trees.TreePattern.{NAMED_STREAMING_RELATION, TreePattern}

/**
 * A wrapper for streaming relations that carries a source identifying name through analysis.
 *
 * This node is introduced during query parsing/resolution and is removed by the
 * [[NameStreamingSources]] analyzer rule. It serves to:
 * 1. Track user-provided source names from `.name()` API
 * 2. Track flow-assigned names from SDP context
 * 3. Ensure all sources have names before execution planning
 *
 * By extending [[UnaryNode]], this wrapper is transparent to analyzer rules - they naturally
 * descend into the child plan via `mapChildren`, resolve it, and the wrapper persists with the
 * updated child. This eliminates the need for explicit handling in most analyzer rules.
 *
 * The naming happens in the analyzer (before execution) to enable:
 * - Schema lookup at specific offsets during analysis
 * - Stable checkpoint locations for source evolution
 * - SDP flow integration with per-source metadata paths
 *
 * @param child The underlying streaming relation (UnresolvedDataSource, etc.)
 * @param sourceIdentifyingName The source identifying name (UserProvided, FlowAssigned,
 *                              or Unassigned)
 */
object NamedStreamingRelation {
  /**
   * Factory method that creates a NamedStreamingRelation with an optional user-provided name.
   * If nameOpt is Some(name), creates with UserProvided(name).
   * If nameOpt is None, creates with Unassigned.
   *
   * @param child The underlying streaming relation
   * @param nameOpt Optional user-provided source name
   * @return A NamedStreamingRelation with the appropriate name
   */
  def withUserProvidedName(
      child: LogicalPlan,
      nameOpt: Option[String]): NamedStreamingRelation = {
    val name = nameOpt.map(UserProvided(_)).getOrElse(Unassigned)
    NamedStreamingRelation(child, name)
  }
}

case class NamedStreamingRelation(
    child: LogicalPlan,
    sourceIdentifyingName: StreamingSourceIdentifyingName)
  extends UnaryNode {

  override def isStreaming: Boolean = true

  // Delegate output to child for transparent wrapping
  override def output: Seq[Attribute] = child.output

  // Keep unresolved until NameStreamingSources explicitly unwraps this node.
  // This ensures the wrapper persists through analysis until we're ready to
  // propagate the sourceIdentifyingName to the underlying StreamingRelationV2.
  override lazy val resolved: Boolean = false

  override protected def withNewChildInternal(newChild: LogicalPlan): NamedStreamingRelation =
    copy(child = newChild)

  /**
   * Attaches a user-provided name from the `.name()` API.
   * If nameOpt is None, returns this node unchanged.
   *
   * @param nameOpt The user-provided source name
   * @return A new NamedStreamingRelation with the user name attached
   */
  def withUserProvidedName(nameOpt: Option[String]): NamedStreamingRelation = {
    nameOpt.map { n =>
      copy(sourceIdentifyingName = UserProvided(n))
    }.getOrElse {
      this
    }
  }

  override val nodePatterns: Seq[TreePattern] = Seq(NAMED_STREAMING_RELATION)

  override def toString: String = {
    s"NamedStreamingRelation($child, $sourceIdentifyingName)"
  }
}
