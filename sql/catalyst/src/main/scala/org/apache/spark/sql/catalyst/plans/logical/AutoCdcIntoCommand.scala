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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Logical plan node for an AUTO CDC INTO command, used by Spark Declarative Pipelines.
 *
 * This represents a CDC (Change Data Capture) operation that applies an ordered change event
 * stream from [[sourceTable]] into [[targetTable]] using SCD Type 1 (upsert) semantics.
 *
 * This node serves as a parse-time placeholder for a pipeline CDC definition and cannot be
 * executed directly. It will be interpreted by the pipeline submodule once execution support
 * is added (SPARK-57402).
 *
 * @param targetTable    The target table to apply changes into.
 * @param sourceTable    The source relation providing the change events.
 * @param keys           Column(s) that uniquely identify a row in the target table.
 * @param deleteCondition An optional expression that marks a source row as a DELETE operation.
 *                        When absent, all source rows are treated as upserts.
 * @param sequenceByExpr Expression that orders CDC events to correctly resolve out-of-order
 *                       arrivals. Must evaluate to a sortable type. Required.
 * @param specifiedCols  An explicit list of source columns to include in the target table.
 *                       Mutually exclusive with [[exceptCols]].
 * @param exceptCols     Source columns to exclude from the target table (i.e., all columns
 *                       except these). Mutually exclusive with [[specifiedCols]].
 */
case class AutoCdcIntoCommand(
    targetTable: TableIdentifier,
    sourceTable: LogicalPlan,
    keys: Seq[UnresolvedAttribute],
    deleteCondition: Option[Expression],
    sequenceByExpr: Expression,
    specifiedCols: Seq[UnresolvedAttribute],
    exceptCols: Seq[UnresolvedAttribute]
) extends UnaryCommand {
  override def child: LogicalPlan = sourceTable

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(sourceTable = newChild)
}
