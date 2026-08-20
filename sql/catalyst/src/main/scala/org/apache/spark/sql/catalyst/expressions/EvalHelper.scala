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

import org.apache.spark.sql.catalyst.trees.TreePattern.COMMON_EXPR_REF

/**
 * Helper methods for evaluating expressions.
 */
trait EvalHelper {

  def prepareForEval(e: Expression): Expression = {
    def prepare(expr: Expression): Expression = expr match {
      case r: RuntimeReplaceable => prepare(r.replacement)
      case d: DelegateExpression => prepare(d.definition)
      // Successful markers are removed in a later analysis batch. Constant-expression consumers
      // run before that batch, so unwrap resolved markers here while retaining failed markers for
      // CheckAnalysis to report against the high-level delegate call.
      case m: InputTypeMarker if m.resolved => prepare(m.child)
      case With(child, defs) =>
        val refToExpr = defs.map(d => d.id -> prepare(d.child)).toMap
        prepare(child).transformWithPruning(_.containsPattern(COMMON_EXPR_REF)) {
          // Nested With expressions can contain references defined by an outer scope.
          case ref: CommonExpressionRef if refToExpr.contains(ref.id) => refToExpr(ref.id)
        }
      case other => other.mapChildren(prepare)
    }
    prepare(e)
  }
}
