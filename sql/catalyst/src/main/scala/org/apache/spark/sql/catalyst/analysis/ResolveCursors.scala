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

import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SqlScriptingContextManager
import org.apache.spark.sql.catalyst.expressions.{CursorReference, UnresolvedCursor}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_CURSOR
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolves [[UnresolvedCursor]] expressions to [[CursorReference]] expressions.
 * This rule:
 * 1. Normalizes cursor names based on case sensitivity configuration
 * 2. Separates qualified cursor names (label.cursor) into scope label and cursor name
 * 3. Looks up the cursor definition from the scripting context
 * 4. Fails early if cursor is not found
 */
class ResolveCursors extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressionsWithPruning(
    _.containsPattern(UNRESOLVED_CURSOR)) {
    case uc: UnresolvedCursor =>
      resolveCursor(uc)
  }

  private def resolveCursor(uc: UnresolvedCursor): CursorReference = {
    val nameParts = uc.nameParts
    val caseSensitive = SQLConf.get.caseSensitiveAnalysis

    // Parser already validates this, so we can assert
    assert(nameParts.length <= 2,
      s"Cursor reference has too many parts: ${nameParts.mkString(".")}")

    // Split qualified name into scope label and cursor name
    val (scopeLabel, cursorName) = if (nameParts.length == 2) {
      // Qualified cursor: "label.cursor"
      (Some(nameParts.head), nameParts.last)
    } else {
      // Unqualified cursor: "cursor"
      (None, nameParts.head)
    }

    // Normalize cursor name and scope label based on case sensitivity
    val normalizedName = if (caseSensitive) {
      cursorName
    } else {
      cursorName.toLowerCase(Locale.ROOT)
    }

    val normalizedScopeLabel = scopeLabel.map { label =>
      if (caseSensitive) {
        label
      } else {
        label.toLowerCase(Locale.ROOT)
      }
    }

    // Look up cursor definition from scripting context using the extension API
    val context = SqlScriptingContextManager.get()
      .map(_.getContext)
      .getOrElse {
        // Cursors are only allowed within SQL scripts
        throw new AnalysisException(
          errorClass = "CURSOR_OUTSIDE_SCRIPT",
          messageParameters = Map("cursorName" -> nameParts.mkString(".")))
      }

    // Use the SqlScriptingExecutionContextExtension API for cursor lookup
    val cursorDef = normalizedScopeLabel match {
      case Some(label) =>
        // Qualified cursor: look up in specific labeled scope
        context.findCursorInScope(label, normalizedName)
      case None =>
        // Unqualified cursor: search current and parent scopes
        context.findCursorByName(normalizedName)
    }

    // If cursor not found, fail with appropriate error
    cursorDef match {
      case Some(definition) =>
        // Create CursorReference with the cursor definition
        CursorReference(nameParts, normalizedName, normalizedScopeLabel, definition)
      case None =>
        throw new AnalysisException(
          errorClass = "CURSOR_NOT_FOUND",
          messageParameters = Map("cursorName" -> nameParts.mkString(".")))
    }
  }
}
