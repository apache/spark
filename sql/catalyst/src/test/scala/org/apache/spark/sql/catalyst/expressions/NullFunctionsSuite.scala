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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class NullFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  def testAllTypes(testFunc: (Any, DataType) => Unit): Unit = {
    testFunc(false, BooleanType)
    testFunc(1.toByte, ByteType)
    testFunc(1.toShort, ShortType)
    testFunc(1, IntegerType)
    testFunc(1L, LongType)
    testFunc(1.0F, FloatType)
    testFunc(1.0, DoubleType)
    testFunc(Decimal(1.5), DecimalType(2, 1))
    testFunc(new java.sql.Date(10), DateType)
    testFunc(new java.sql.Timestamp(10), TimestampType)
    testFunc("abcd", StringType)
  }

  test("isnull and isnotnull") {
    testAllTypes { (value: Any, tpe: DataType) =>
      checkEvaluation(IsNull(Literal.create(value, tpe)), false)
      checkEvaluation(IsNotNull(Literal.create(value, tpe)), true)
      checkEvaluation(IsNull(Literal.create(null, tpe)), true)
      checkEvaluation(IsNotNull(Literal.create(null, tpe)), false)
    }
  }

  test("IsNaN") {
    checkEvaluation(IsNaN(Literal(Double.NaN)), true)
    checkEvaluation(IsNaN(Literal(Float.NaN)), true)
    checkEvaluation(IsNaN(Literal(math.log(-3))), true)
    checkEvaluation(IsNaN(Literal.create(null, DoubleType)), false)
    checkEvaluation(IsNaN(Literal(Double.PositiveInfinity)), false)
    checkEvaluation(IsNaN(Literal(Float.MaxValue)), false)
    checkEvaluation(IsNaN(Literal(5.5f)), false)
  }

  test("nanvl") {
    checkEvaluation(NaNvl(Literal(5.0), Literal.create(null, DoubleType)), 5.0)
    checkEvaluation(NaNvl(Literal.create(null, DoubleType), Literal(5.0)), null)
    checkEvaluation(NaNvl(Literal.create(null, DoubleType), Literal(Double.NaN)), null)
    checkEvaluation(NaNvl(Literal(Double.NaN), Literal(5.0)), 5.0)
    checkEvaluation(NaNvl(Literal(Double.NaN), Literal.create(null, DoubleType)), null)
    assert(NaNvl(Literal(Double.NaN), Literal(Double.NaN)).
      eval(EmptyRow).asInstanceOf[Double].isNaN)
  }

  test("coalesce") {
    testAllTypes { (value: Any, tpe: DataType) =>
      val lit = Literal.create(value, tpe)
      val nullLit = Literal.create(null, tpe)
      checkEvaluation(Coalesce(Seq(nullLit)), null)
      checkEvaluation(Coalesce(Seq(lit)), value)
      checkEvaluation(Coalesce(Seq(nullLit, lit)), value)
      checkEvaluation(Coalesce(Seq(nullLit, lit, lit)), value)
      checkEvaluation(Coalesce(Seq(nullLit, nullLit, lit)), value)
    }
  }

  test("AtLeastNNonNulls") {
    val mix = Seq(Literal("x"),
      Literal.create(null, StringType),
      Literal.create(null, DoubleType),
      Literal(Double.NaN),
      Literal(5f))

    val nanOnly = Seq(Literal("x"),
      Literal(10.0),
      Literal(Float.NaN),
      Literal(math.log(-2)),
      Literal(Double.MaxValue))

    val nullOnly = Seq(Literal("x"),
      Literal.create(null, DoubleType),
      Literal.create(null, DecimalType.USER_DEFAULT),
      Literal(Float.MaxValue),
      Literal(false))

    checkEvaluation(AtLeastNNonNulls(2, mix), true, EmptyRow)
    checkEvaluation(AtLeastNNonNulls(3, mix), false, EmptyRow)
    checkEvaluation(AtLeastNNonNulls(3, nanOnly), true, EmptyRow)
    checkEvaluation(AtLeastNNonNulls(4, nanOnly), false, EmptyRow)
    checkEvaluation(AtLeastNNonNulls(3, nullOnly), true, EmptyRow)
    checkEvaluation(AtLeastNNonNulls(4, nullOnly), false, EmptyRow)
  }
}
