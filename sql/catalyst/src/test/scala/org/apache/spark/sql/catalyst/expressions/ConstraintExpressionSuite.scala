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

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.IntegerType

class ConstraintExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {
  private val boundRef = BoundReference(0, IntegerType, nullable = true)
  private val expr =
    CheckInvariant(GreaterThan(boundRef, Literal(0)), Seq(("a", boundRef)), "c1", "a > 0")

  def expectedMessage(value: String): String =
    s"""|[CHECK_CONSTRAINT_VIOLATION] CHECK constraint c1 a > 0 violated by row with values:
        | - a : $value
        | SQLSTATE: 23001""".stripMargin

  test("CheckInvariant: returns true if column 'a' > 0") {
    checkEvaluation(expr, true, InternalRow(1))
  }

  test("CheckInvariant: return true if column 'a' is null") {
    checkEvaluation(expr, true, InternalRow(null))
  }

  test("CheckInvariant: throws exception if column 'a' <= 0") {
    checkExceptionInExpression[SparkRuntimeException](
      expr, InternalRow(-1), expectedMessage("-1"))
  }
}
