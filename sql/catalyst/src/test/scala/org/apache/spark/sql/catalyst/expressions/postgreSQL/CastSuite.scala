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
package org.apache.spark.sql.catalyst.expressions.postgreSQL

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, Literal}

class CastSuite extends SparkFunSuite with ExpressionEvalHelper {
  private def checkPostgreCastToBoolean(v: Any, expected: Any): Unit = {
    checkEvaluation(PostgreCastToBoolean(Literal(v), None), expected)
  }

  test("cast string to boolean") {
    checkPostgreCastToBoolean("true", true)
    checkPostgreCastToBoolean("tru", true)
    checkPostgreCastToBoolean("tr", true)
    checkPostgreCastToBoolean("t", true)
    checkPostgreCastToBoolean("tRUe", true)
    checkPostgreCastToBoolean("    tRue   ", true)
    checkPostgreCastToBoolean("    tRu   ", true)
    checkPostgreCastToBoolean("yes", true)
    checkPostgreCastToBoolean("ye", true)
    checkPostgreCastToBoolean("y", true)
    checkPostgreCastToBoolean("1", true)
    checkPostgreCastToBoolean("on", true)

    checkPostgreCastToBoolean("false", false)
    checkPostgreCastToBoolean("fals", false)
    checkPostgreCastToBoolean("fal", false)
    checkPostgreCastToBoolean("fa", false)
    checkPostgreCastToBoolean("f", false)
    checkPostgreCastToBoolean("    fAlse    ", false)
    checkPostgreCastToBoolean("    fAls    ", false)
    checkPostgreCastToBoolean("    FAlsE    ", false)
    checkPostgreCastToBoolean("no", false)
    checkPostgreCastToBoolean("n", false)
    checkPostgreCastToBoolean("0", false)
    checkPostgreCastToBoolean("off", false)
    checkPostgreCastToBoolean("of", false)

    intercept[IllegalArgumentException](PostgreCastToBoolean(Literal("o"), None).eval())
    intercept[IllegalArgumentException](PostgreCastToBoolean(Literal("abc"), None).eval())
    intercept[IllegalArgumentException](PostgreCastToBoolean(Literal(""), None).eval())
  }

  test("unsupported data types to cast to boolean") {
    assert(PostgreCastToBoolean(Literal(new Timestamp(1)), None).checkInputDataTypes().isFailure)
    assert(PostgreCastToBoolean(Literal(new Date(1)), None).checkInputDataTypes().isFailure)
    assert(PostgreCastToBoolean(Literal(1.toLong), None).checkInputDataTypes().isFailure)
    assert(PostgreCastToBoolean(Literal(1.toShort), None).checkInputDataTypes().isFailure)
    assert(PostgreCastToBoolean(Literal(1.toByte), None).checkInputDataTypes().isFailure)
    assert(PostgreCastToBoolean(Literal(BigDecimal(1.0)), None).checkInputDataTypes().isFailure)
    assert(PostgreCastToBoolean(Literal(1.toDouble), None).checkInputDataTypes().isFailure)
    assert(PostgreCastToBoolean(Literal(1.toFloat), None).checkInputDataTypes().isFailure)
  }
}
