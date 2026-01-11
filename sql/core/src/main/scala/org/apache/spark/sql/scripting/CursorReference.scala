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

package org.apache.spark.sql.scripting

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.trees.TreePattern.{CURSOR_REFERENCE, TreePattern}
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.types.{DataType, NullType}

/**
 * A resolved SQL cursor reference, which contains all the information needed to access
 * the cursor at execution time.
 *
 * Similar to VariableReference, but for cursors. Cursors are looked up in the
 * SqlScriptingExecutionContext at execution time.
 *
 * @param originalNameParts The original name parts as specified in SQL (for display/error messages)
 * @param normalizedName The normalized cursor name used for lookup (respects case sensitivity)
 * @param scopePath The scope path where the cursor was declared (for qualified lookups like label.cursor)
 */
case class CursorReference(
    originalNameParts: Seq[String],
    normalizedName: String,
    scopePath: Seq[String] = Seq.empty)
  extends LeafExpression with Unevaluable {

  override def dataType: DataType = NullType
  override def nullable: Boolean = true

  override def toString: String = {
    val fullName = if (scopePath.nonEmpty) {
      (scopePath :+ normalizedName).map(quoteIfNeeded).mkString(".")
    } else {
      quoteIfNeeded(normalizedName)
    }
    s"CursorReference($fullName)"
  }

  override def sql: String = {
    originalNameParts.map(quoteIfNeeded).mkString(".")
  }

  final override val nodePatterns: Seq[TreePattern] = Seq(CURSOR_REFERENCE)
}

/**
 * An unresolved cursor reference that needs to be resolved by the analyzer.
 *
 * @param nameParts The cursor name parts (unqualified: Seq(name) or qualified: Seq(label, name))
 */
case class UnresolvedCursor(nameParts: Seq[String])
  extends LeafExpression with Unevaluable {

  override def dataType: DataType = NullType
  override def nullable: Boolean = true

  override def toString: String = {
    s"UnresolvedCursor(${nameParts.map(quoteIfNeeded).mkString(".")})"
  }

  override def sql: String = {
    nameParts.map(quoteIfNeeded).mkString(".")
  }
}
