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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * Normalizes detached, input-bound HAVING subtrees for complete grouping-expression matches.
 * Supports scalar attributes/literals, casts, numeric arithmetic, and the stock functions
 * lower/lcase, upper/ucase, and substr/substring. Lambdas, subqueries, aggregates, UDFs, and
 * nondeterministic expressions are outside this grammar, even when already resolved.
 *
 * This is not general function resolution: uncertain builtin ownership is a miss. The caller must
 * compare successful candidates with the grouping expressions and keep its ORIGINAL subtree on
 * any miss. For example, normalizing input-bound `lower(name)` does not itself establish that
 * `lower(name)` is a grouping key.
 */
private[analysis] final class HavingGroupingExpressionNormalizer(
    functionResolution: FunctionResolution) extends SQLConfHelper {

  /** Returns a stable resolved candidate, or None without rewriting the supplied expression. */
  def normalize(expression: Expression): Option[Expression] = {
    if (!isSupported(expression)) return None
    val coercionRules = (if (conf.ansiEnabled) {
      AnsiTypeCoercion.typeCoercionRules
    } else {
      TypeCoercion.typeCoercionRules
    }).collect { case rule: TypeCoercionRule => rule }

    try {
      var current = expression.clone()
      var iteration = 0
      while (iteration < conf.analyzerMaxIterations) {
        var unsupportedFunction = false
        val functions = current.transformUp {
          case u: UnresolvedFunction if u.childrenResolved =>
            functionResolution.tryResolveFunctionForGroupingMatch(u).getOrElse {
              unsupportedFunction = true
              u
            }
        }
        if (unsupportedFunction) return None
        val withTimeZone = ResolveTimeZone.resolveTimeZones(functions)
        val arithmetic = withTimeZone.transformUp {
          case e @ (_: Add | _: Subtract | _: Multiply | _: Divide)
              if e.childrenResolved => BinaryArithmeticWithDatetimeResolver.resolve(e)
        }
        val coerced = coercionRules.foldLeft(arithmetic) { (e, rule) =>
          e.transformUp(rule.transform)
        }
        val next = ResolveTimeZone.resolveTimeZones(coerced)
        if (!isSupported(next)) return None
        if (next.fastEquals(current)) {
          return Option.when(next.resolved && next.deterministic)(next)
        }
        current = next
        iteration += 1
      }
      None
    } catch {
      case _: AnalysisException => None
    }
  }

  /** Rejects unsupported nodes before any speculative function construction or type coercion. */
  private def isSupported(expression: Expression): Boolean = expression match {
    case a: AttributeReference => isSupportedType(a.dataType)
    case l: Literal => isSupportedType(l.dataType)
    case c: Cast => isSupportedType(c.dataType) && isSupported(c.child)
    case _: Add | _: Subtract | _: Multiply | _: Divide | _: IntegralDivide | _: Remainder |
         _: UnaryMinus | _: UnaryPositive | _: Lower | _: Upper | _: Substring |
         _: UnresolvedFunction =>
      expression.children.forall(isSupported)
    case _ => false
  }

  /** Limits casts and inputs to native scalar types, excluding complex and user-defined types. */
  private def isSupportedType(dataType: DataType): Boolean = dataType match {
    case _: NumericType | _: StringType | BinaryType | BooleanType | DateType | TimestampType |
         TimestampNTZType | NullType => true
    case _ => false
  }
}
