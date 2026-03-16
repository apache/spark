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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.trees.TreePattern.{CURSOR_REFERENCE, TreePattern => TP, UNRESOLVED_CURSOR}
import org.apache.spark.sql.types.{DataType, NullType}

/**
 * Immutable cursor definition containing the cursor's normalized name and SQL query text.
 * This is stored in the scripting context and referenced by CursorReference during analysis.
 * Similar to VariableDefinition, this stores the normalized name for consistent lookups.
 *
 * @param name The normalized cursor name (lowercase if case-insensitive analysis)
 * @param queryText The SQL query text defining the cursor result set
 */
case class CursorDefinition(
    name: String,
    queryText: String)

/**
 * An unresolved reference to a cursor. This is used during parsing and will be resolved
 * to a [[CursorReference]] by the [[ResolveCursors]] analyzer rule.
 *
 * @param nameParts The parts of the cursor name (e.g., Seq("cursor") or Seq("label", "cursor"))
 */
case class UnresolvedCursor(nameParts: Seq[String]) extends LeafExpression with Unevaluable {
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override lazy val resolved: Boolean = false
  override def sql: String = nameParts.mkString(".")
  override def toString: String = s"UnresolvedCursor(${nameParts.mkString(".")})"

  final override val nodePatterns: Seq[TP] = Seq(UNRESOLVED_CURSOR)
}

/**
 * A resolved reference to a cursor. This is created by the [[ResolveCursors]] analyzer rule
 * after normalizing the cursor name parts, checking case sensitivity, and looking up the
 * cursor definition from the scripting context.
 *
 * CursorReference is never actually evaluated - it's only used as a marker in cursor commands
 * (OPEN, FETCH, CLOSE). Therefore, dataType returns NullType to satisfy serialization/profiling
 * requirements without throwing exceptions.
 *
 * @param nameParts The original cursor name parts (unnormalized)
 * @param normalizedName The normalized cursor name (for lookups considering case sensitivity)
 * @param scopeLabel Optional label qualifier for scoped cursors (e.g., Some("label") for
 *                   "label.cursor", None for unqualified cursors)
 * @param definition The cursor definition (CursorDefinition) looked up during analysis.
 */
case class CursorReference(
    nameParts: Seq[String],
    normalizedName: String,
    scopeLabel: Option[String],
    definition: CursorDefinition) extends LeafExpression with Unevaluable {
  // CursorReference is never evaluated, but dataType must return a valid type for serialization
  override def dataType: DataType = NullType
  override def nullable: Boolean = true
  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("CursorReference cannot be evaluated")
  override def sql: String = nameParts.mkString(".")
  override def toString: String = s"CursorReference(${nameParts.mkString(".")})"

  final override val nodePatterns: Seq[TP] = Seq(CURSOR_REFERENCE)
}
