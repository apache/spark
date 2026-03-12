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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal, VariableReference}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Utility object for handling SQL parameter binding in USING clauses.
 * This provides a single source of truth for parameter binding logic used by:
 * - EXECUTE IMMEDIATE
 * - OPEN CURSOR (parameterized cursors)
 *
 * The implementation is based on EXECUTE IMMEDIATE's parameter handling to ensure
 * consistent behavior across all SQL features that support USING clauses.
 */
object ParameterBindingUtils {

  /**
   * Builds parameter arrays for the session.sql() API from a sequence of expressions.
   * Handles both named parameters (using Alias) and positional parameters.
   *
   * This method supports two modes:
   * 1. Extract names from Alias nodes in args (EXECUTE IMMEDIATE style)
   * 2. Use pre-extracted names provided separately (OPEN CURSOR style, where names are
   *    extracted at parse time to survive analysis phase)
   *
   * Named parameters:
   *   USING x + 1 AS param1, y + 2 AS param2
   *   Result: values=[<value1>, <value2>], names=["param1", "param2"]
   *
   * Positional parameters:
   *   USING x + 1, y + 2
   *   Result: values=[<value1>, <value2>], names=["", ""]
   *
   * @param args Parameter expressions from the USING clause
   * @param preExtractedNames Optional pre-extracted parameter names (for cases where Alias
   *                          nodes don't survive analysis). If provided, must have same length
   *                          as args.
   * @return Tuple of (parameter values as Literals, parameter names)
   */
  def buildUnifiedParameters(
      args: Seq[Expression],
      preExtractedNames: Seq[String] = Seq.empty): (Array[Any], Array[String]) = {
    val values = scala.collection.mutable.ListBuffer[Any]()
    val names = scala.collection.mutable.ListBuffer[String]()

    args.zipWithIndex.foreach { case (expr, idx) =>
      val (paramExpr, paramName) = if (preExtractedNames.nonEmpty) {
        // Use pre-extracted names (OPEN CURSOR style)
        assert(preExtractedNames.length == args.length,
          s"preExtractedNames length (${preExtractedNames.length}) must match " +
          s"args length (${args.length})")
        (expr, preExtractedNames(idx))
      } else {
        // Extract names from Alias nodes (EXECUTE IMMEDIATE style)
        expr match {
          case alias: Alias => (alias.child, alias.name)
          case other => (other, "")
        }
      }

      val paramValue = evaluateParameterExpression(paramExpr)
      values += paramValue
      names += paramName
    }

    (values.toArray, names.toArray)
  }

  /**
   * Evaluates a parameter expression and returns its value wrapped as a Literal.
   * This preserves type information, which is critical for parameters like DATE literals.
   *
   * Supported expressions:
   * - VariableReference: Evaluated to their current value
   * - Foldable expressions: Constants, literals, and compile-time evaluable expressions
   *
   * Unsupported expressions:
   * - Non-foldable expressions (e.g., column references, subqueries)
   *
   * @param expr The expression to evaluate
   * @return Literal with evaluated value and type information
   * @throws QueryCompilationErrors if expression is not foldable
   */
  def evaluateParameterExpression(expr: Expression): Any = {
    expr match {
      case varRef: VariableReference =>
        // Variable references: evaluate to their values and wrap in Literal
        // to preserve type information
        Literal.create(varRef.eval(InternalRow.empty), varRef.dataType)
      case foldable if foldable.foldable =>
        // For foldable expressions, return Literal to preserve type information.
        // This ensures DATE '2023-12-25' remains a DateType literal, not just an Int.
        Literal.create(foldable.eval(InternalRow.empty), foldable.dataType)
      case other =>
        // Expression is not foldable - not supported for parameters
        throw QueryCompilationErrors.unsupportedParameterExpression(other)
    }
  }
}
