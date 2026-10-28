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

import org.apache.spark.sql.catalyst.expressions.{Cast, DateAdd, DateSub, Expression, Literal}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * Type coercion helper that matches against [[DateAdd]] and [[DateSub]] expressions in order to
 * type coerce the second argument to [[IntegerType]], if necessary.
 */
object StringLiteralTypeCoercion {
  // Keep cast results boxed: unboxing a NULL as Int yields zero, which would make
  // date_add/date_sub return the input date instead of NULL.
  def apply(expression: Expression): Expression = expression match {
    case DateAdd(l, r) if r.dataType.isInstanceOf[StringType] && r.foldable =>
      val days = try {
        Cast(r, IntegerType, ansiEnabled = true).eval()
      } catch {
        case e: NumberFormatException =>
          throw QueryCompilationErrors.secondArgumentOfFunctionIsNotIntegerError("date_add", e)
      }
      DateAdd(l, Literal.create(days, IntegerType))
    case DateSub(l, r) if r.dataType.isInstanceOf[StringType] && r.foldable =>
      val days = try {
        Cast(r, IntegerType, ansiEnabled = true).eval()
      } catch {
        case e: NumberFormatException =>
          throw QueryCompilationErrors.secondArgumentOfFunctionIsNotIntegerError("date_sub", e)
      }
      DateSub(l, Literal.create(days, IntegerType))
    case other => other
  }
}
