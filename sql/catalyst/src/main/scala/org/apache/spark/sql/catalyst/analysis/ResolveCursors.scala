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

import org.apache.spark.sql.catalyst.expressions.{CursorReference, UnresolvedCursor}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_CURSOR
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolves [[UnresolvedCursor]] expressions to [[CursorReference]] expressions.
 * This rule normalizes cursor names based on case sensitivity configuration and
 * separates qualified cursor names (label.cursor) into scope label and cursor name.
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

    CursorReference(nameParts, normalizedName, scopeLabel)
  }
}
