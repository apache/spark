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
import org.apache.spark.sql.catalyst.{SQLConfHelper, SqlScriptingContextManager}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.scripting.{CursorReference, UnresolvedCursor}

/**
 * Resolves cursor references by looking them up in the SQL scripting execution context.
 *
 * This rule converts UnresolvedCursor to CursorReference during the analysis phase,
 * similar to how variable resolution works. This unifies cursor lookup logic and
 * eliminates duplicated code in execution classes.
 */
class ResolveCursors extends Rule[LogicalPlan] with SQLConfHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressionsWithPruning(
    _.containsPattern(org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_ATTRIBUTE)
  ) {
    case UnresolvedCursor(nameParts) if withinSqlScript =>
      lookupCursor(nameParts) match {
        case Some(cursorRef) => cursorRef
        case None =>
          throw new AnalysisException(
            errorClass = "CURSOR_NOT_FOUND",
            messageParameters = Map("cursorName" -> nameParts.mkString(".")))
      }
  }

  /**
   * Looks up a cursor in the SQL scripting execution context.
   *
   * @param nameParts The cursor name parts (unqualified: Seq(name) or qualified: Seq(label, name))
   * @return Some(CursorReference) if found, None otherwise
   */
  private def lookupCursor(nameParts: Seq[String]): Option[CursorReference] = {
    // Normalize name based on case sensitivity
    val normalizedParts = if (conf.caseSensitiveAnalysis) {
      nameParts
    } else {
      nameParts.map(_.toLowerCase(Locale.ROOT))
    }

    // Get the scripting context
    val scriptingContext = SqlScriptingContextManager.get()
      .map(_.getContext.asInstanceOf[org.apache.spark.sql.scripting.SqlScriptingExecutionContext])

    scriptingContext.flatMap { ctx =>
      val (scopePath, cursorName) = if (normalizedParts.length == 1) {
        // Unqualified cursor name - search in current and parent scopes
        (Seq.empty[String], normalizedParts.head)
      } else {
        // Qualified cursor name - label.cursor
        (normalizedParts.init, normalizedParts.last)
      }

      // Find the cursor in the appropriate scope
      val scope = if (scopePath.isEmpty) {
        ctx.currentScope
      } else {
        // Find the labeled scope
        findScopeByLabel(ctx, scopePath.head)
      }

      scope.flatMap { s =>
        s.cursors.get(cursorName).map { _ =>
          CursorReference(
            originalNameParts = nameParts,
            normalizedName = cursorName,
            scopePath = scopePath
          )
        }
      }
    }
  }

  /**
   * Finds a scope by label name.
   */
  private def findScopeByLabel(
      ctx: org.apache.spark.sql.scripting.SqlScriptingExecutionContext,
      label: String): Option[org.apache.spark.sql.scripting.SqlScriptingExecutionScope] = {
    // Search through scope stack for matching label
    var currentOpt = Some(ctx.currentScope)
    while (currentOpt.isDefined) {
      val current = currentOpt.get
      if (current.label.contains(label)) {
        return Some(current)
      }
      currentOpt = current.parent
    }
    None
  }

  /**
   * Check if we are currently within a SQL script.
   */
  private def withinSqlScript: Boolean = {
    SqlScriptingContextManager.get().nonEmpty
  }
}
