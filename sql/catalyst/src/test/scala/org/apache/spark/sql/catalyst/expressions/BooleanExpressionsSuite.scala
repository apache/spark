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
import org.apache.spark.sql.catalyst.expressions.BooleanTest._
import org.apache.spark.sql.types._

class BooleanExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("istrue and isnottrue") {
    checkEvaluation(BooleanTest(Literal.create(false, BooleanType), TRUE), false)
    checkEvaluation(Not(BooleanTest(Literal.create(false, BooleanType), TRUE)), true)
    checkEvaluation(BooleanTest(Literal.create(true, BooleanType), TRUE), true)
    checkEvaluation(Not(BooleanTest(Literal.create(true, BooleanType), TRUE)), false)
  }

  test("isfalse and isnotfalse") {
    checkEvaluation(BooleanTest(Literal.create(false, BooleanType), FALSE), true)
    checkEvaluation(Not(BooleanTest(Literal.create(false, BooleanType), FALSE)), false)
    checkEvaluation(BooleanTest(Literal.create(true, BooleanType), FALSE), false)
    checkEvaluation(Not(BooleanTest(Literal.create(true, BooleanType), FALSE)), true)
  }

  test("isunknown and isnotunknown") {
    checkEvaluation(BooleanTest(Literal.create(1.toByte, ByteType), UNKNOWN), true)
    checkEvaluation(BooleanTest(Literal.create(1.toShort, ShortType), UNKNOWN), true)
    checkEvaluation(BooleanTest(Literal.create(1, IntegerType), UNKNOWN), true)
    checkEvaluation(BooleanTest(Literal.create(1L, LongType), UNKNOWN), true)
    checkEvaluation(BooleanTest(Literal.create(1.0F, FloatType), UNKNOWN), true)
    checkEvaluation(BooleanTest(Literal.create(1.0, DoubleType), UNKNOWN), true)
    checkEvaluation(
      BooleanTest(Literal.create(Decimal(1.5), DecimalType(2, 1)), UNKNOWN), true)
    checkEvaluation(
      BooleanTest(Literal.create(new java.sql.Date(10), DateType), UNKNOWN), true)
    checkEvaluation(
      BooleanTest(Literal.create(new java.sql.Timestamp(10), TimestampType), UNKNOWN), true)
    checkEvaluation(BooleanTest(Literal.create("abc", StringType), UNKNOWN), true)
    checkEvaluation(BooleanTest(Literal.create(false, BooleanType), UNKNOWN), false)
    checkEvaluation(BooleanTest(Literal.create(true, BooleanType), UNKNOWN), false)

    checkEvaluation(Not(BooleanTest(Literal.create(1.toByte, ByteType), UNKNOWN)), false)
    checkEvaluation(Not(BooleanTest(Literal.create(1.toShort, ShortType), UNKNOWN)), false)
    checkEvaluation(Not(BooleanTest(Literal.create(1, IntegerType), UNKNOWN)), false)
    checkEvaluation(Not(BooleanTest(Literal.create(1L, LongType), UNKNOWN)), false)
    checkEvaluation(Not(BooleanTest(Literal.create(1.0F, FloatType), UNKNOWN)), false)
    checkEvaluation(Not(BooleanTest(Literal.create(1.0, DoubleType), UNKNOWN)), false)
    checkEvaluation(Not(
      BooleanTest(Literal.create(Decimal(1.5), DecimalType(2, 1)), UNKNOWN)), false)
    checkEvaluation(Not(
      BooleanTest(Literal.create(new java.sql.Date(10), DateType), UNKNOWN)), false)
    checkEvaluation(Not(BooleanTest(
      Literal.create(new java.sql.Timestamp(10), TimestampType), UNKNOWN)), false)
    checkEvaluation(Not(BooleanTest(Literal.create("abc", StringType), UNKNOWN)), false)
    checkEvaluation(Not(BooleanTest(Literal.create(false, BooleanType), UNKNOWN)), true)
    checkEvaluation(Not(BooleanTest(Literal.create(true, BooleanType), UNKNOWN)), true)
  }

}

