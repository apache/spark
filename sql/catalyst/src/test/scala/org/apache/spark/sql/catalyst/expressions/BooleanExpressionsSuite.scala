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
    checkEvaluation(BooleanTest(Literal.create(false, BooleanType), Option(TRUE)), false)
    checkEvaluation(Not(BooleanTest(Literal.create(false, BooleanType), Option(TRUE))), true)
    checkEvaluation(BooleanTest(Literal.create(true, BooleanType), Option(TRUE)), true)
    checkEvaluation(Not(BooleanTest(Literal.create(true, BooleanType), Option(TRUE))), false)
  }

  test("isfalse and isnotfalse") {
    checkEvaluation(BooleanTest(Literal.create(false, BooleanType), Option(FALSE)), true)
    checkEvaluation(Not(BooleanTest(Literal.create(false, BooleanType), Option(FALSE))), false)
    checkEvaluation(BooleanTest(Literal.create(true, BooleanType), Option(FALSE)), false)
    checkEvaluation(Not(BooleanTest(Literal.create(true, BooleanType), Option(FALSE))), true)
  }

  test("isunknown and isnotunknown") {
	  checkEvaluation(BooleanTest(Literal.create(1.toByte, ByteType), Option(UNKNOWN)), true)
	  checkEvaluation(BooleanTest(Literal.create(1.toShort, ByteType), Option(UNKNOWN)), true)
    checkEvaluation(BooleanTest(Literal.create(1, IntegerType), Option(UNKNOWN)), true)
    checkEvaluation(BooleanTest(Literal.create(1L, LongType), Option(UNKNOWN)), true)
    checkEvaluation(BooleanTest(Literal.create(1.0F, FloatType), Option(UNKNOWN)), true)
    checkEvaluation(BooleanTest(Literal.create(1.0, DoubleType), Option(UNKNOWN)), true)
    checkEvaluation(
      BooleanTest(Literal.create(Decimal(1.5), DecimalType(2, 1)), Option(UNKNOWN)), true)
    checkEvaluation(
      BooleanTest(Literal.create(new java.sql.Date(10), DateType), Option(UNKNOWN)), true)
    checkEvaluation(
      BooleanTest(Literal.create(new java.sql.Timestamp(10), TimestampType), Option(UNKNOWN)), true)
    checkEvaluation(BooleanTest(Literal.create("abc", StringType), Option(UNKNOWN)), true)
    checkEvaluation(BooleanTest(Literal.create(false, BooleanType), Option(UNKNOWN)), false)
    checkEvaluation(BooleanTest(Literal.create(true, BooleanType), Option(UNKNOWN)), false)

    checkEvaluation(Not(BooleanTest(Literal.create(1.toByte, ByteType), Option(UNKNOWN))), false)
	  checkEvaluation(Not(BooleanTest(Literal.create(1.toShort, ByteType), Option(UNKNOWN))), false)
    checkEvaluation(Not(BooleanTest(Literal.create(1, IntegerType), Option(UNKNOWN))), false)
    checkEvaluation(Not(BooleanTest(Literal.create(1L, LongType), Option(UNKNOWN))), false)
    checkEvaluation(Not(BooleanTest(Literal.create(1.0F, FloatType), Option(UNKNOWN))), false)
    checkEvaluation(Not(BooleanTest(Literal.create(1.0, DoubleType), Option(UNKNOWN))), false)
    checkEvaluation(Not(
      BooleanTest(Literal.create(Decimal(1.5), DecimalType(2, 1)), Option(UNKNOWN))), false)
    checkEvaluation(Not(
      BooleanTest(Literal.create(new java.sql.Date(10), DateType), Option(UNKNOWN))), false)
    checkEvaluation(Not(
      BooleanTest(Literal.create(new java.sql.Timestamp(10), TimestampType), Option(UNKNOWN))), false)
    checkEvaluation(Not(BooleanTest(Literal.create("abc", StringType), Option(UNKNOWN))), false)
    checkEvaluation(Not(BooleanTest(Literal.create(false, BooleanType), Option(UNKNOWN))), true)
    checkEvaluation(Not(BooleanTest(Literal.create(true, BooleanType), Option(UNKNOWN))), true)
  }

}

