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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.trees.BinaryLike

/**
 * Logical plan node for an AUTO CDC INTO operation, used by Spark Declarative Pipelines.
 *
 * This represents a CDC (Change Data Capture) operation that applies an ordered change event
 * stream from [[source]] into [[targetTable]] using SCD Type 1 (upsert) semantics.
 *
 * This node only ever appears as the flow operation of a [[CreateFlowCommand]] (parsed from
 * `CREATE FLOW ... AS AUTO CDC INTO ...`); there is no standalone `AUTO CDC INTO` syntax. It is a
 * plain [[LogicalPlan]] rather than a [[Command]] so that it is never eagerly executed or planned
 * on its own -- it is interpreted by the pipeline submodule during a pipeline execution. Unlike a
 * [[ParsedStatement]], it is not rewritten into another plan during analysis; the pipeline
 * submodule consumes it as-is, so it relies on the default resolution semantics (resolved once its
 * children and expressions are resolved) rather than being forced unresolved. The [[targetTable]]
 * and [[source]] relations are exposed as the node's children (left and right respectively) so the
 * analyzer resolves them through the normal plan resolution path.
 *
 * @param targetTable    The target table to apply changes into, as an `UnresolvedIdentifier`.
 *                       Exposed as the node's left child.
 * @param source         The source relation providing the change events, parsed from a general
 *                       `relationPrimary` (typically a STREAM(...) source marked as a streaming
 *                       read). Exposed as the node's right child.
 * @param keys           Column(s) that uniquely identify a row in the target table.
 * @param deleteCondition An optional expression that marks a source row as a DELETE operation.
 *                        When absent, all source rows are treated as upserts.
 * @param sequenceByExpr Expression that orders CDC events to correctly resolve out-of-order
 *                       arrivals. Must evaluate to a sortable type. Required.
 * @param includeColumns An explicit list of source columns to include in the target table.
 *                       [[None]] when no COLUMNS clause was specified. Mutually exclusive with
 *                       [[excludeColumns]].
 * @param excludeColumns Source columns to exclude from the target table (i.e., all columns
 *                       except these). [[None]] when no COLUMNS clause was specified. Mutually
 *                       exclusive with [[includeColumns]].
 */
case class AutoCdcInto(
    targetTable: LogicalPlan,
    source: LogicalPlan,
    keys: Seq[UnresolvedAttribute],
    deleteCondition: Option[Expression],
    sequenceByExpr: Expression,
    includeColumns: Option[Seq[UnresolvedAttribute]],
    excludeColumns: Option[Seq[UnresolvedAttribute]]
) extends LogicalPlan with BinaryLike[LogicalPlan] {
  override def left: LogicalPlan = targetTable
  override def right: LogicalPlan = source

  // This node is a parse-time placeholder that is never executed or projected from directly; the
  // pipeline submodule reads its fields instead. It therefore produces no output columns.
  override def output: Seq[Attribute] = Nil

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan, newRight: LogicalPlan): AutoCdcInto =
    copy(targetTable = newLeft, source = newRight)
}
