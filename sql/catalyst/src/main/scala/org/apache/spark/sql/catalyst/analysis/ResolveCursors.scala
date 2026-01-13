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
import org.apache.spark.sql.catalyst.expressions.{CursorDefinition, CursorReference, UnresolvedCursor}
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
    case uc: UnresolvedCursor if !uc.resolved =>
      resolveCursor(uc)
  }

  private def resolveCursor(uc: UnresolvedCursor): CursorReference = {
    val nameParts = uc.nameParts
    val caseSensitive = SQLConf.get.caseSensitiveAnalysis

    // Split qualified name into scope label and cursor name
    val (scopeLabel, cursorName) = if (nameParts.length > 1) {
      // Qualified cursor: "label.cursor"
      (Some(nameParts.head), nameParts.last)
    } else {
      // Unqualified cursor: "cursor"
      (None, nameParts.head)
    }

    // Normalize cursor name based on case sensitivity
    val normalizedName = if (caseSensitive) {
      cursorName
    } else {
      cursorName.toLowerCase(Locale.ROOT)
    }

    // Look up cursor definition from scripting context using reflection
    // Reflection is needed because SqlScriptingExecutionContext is in sql/core
    // which would create a circular dependency if imported into catalyst
    val contextOpt = SqlScriptingContextManager.get().map(_.getContext)

    val cursorDefOpt = contextOpt.flatMap { context =>
      // Get the current frame using reflection
      val currentFrame = context.getClass.getMethod("currentFrame").invoke(context)

      // Call the appropriate lookup method based on whether cursor is qualified
      val result = scopeLabel match {
        case Some(label) =>
          // Qualified cursor: look up in specific labeled scope
          currentFrame.getClass
            .getMethod("findCursorInScope", classOf[String], classOf[String])
            .invoke(currentFrame, label, normalizedName)
        case None =>
          // Unqualified cursor: search current and parent scopes
          currentFrame.getClass
            .getMethod("findCursorByName", classOf[String])
            .invoke(currentFrame, normalizedName)
      }

      // Result is Option[CursorDefinition], but type-erased to Option[Any]
      result.asInstanceOf[Option[Any]]
    }

    // If cursor not found and we're in a scripting context, fail immediately
    if (contextOpt.isDefined && cursorDefOpt.isEmpty) {
      throw new AnalysisException(
        errorClass = "CURSOR_NOT_FOUND",
        messageParameters = Map("cursorName" -> nameParts.mkString(".")))
    }

    // Create CursorReference with the cursor definition
    // Cast needed because reflection returns Any
    val cursorDef = cursorDefOpt.get.asInstanceOf[CursorDefinition]
    CursorReference(nameParts, normalizedName, scopeLabel, cursorDef)
  }
}
