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
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, Literal}

class CastSuite extends SparkFunSuite with ExpressionEvalHelper {
  private def checkPostgreCastStringToBoolean(v: Any, expected: Any): Unit = {
    checkEvaluation(PostgreCastStringToBoolean(Literal(v)), expected)
  }

  test("cast string to boolean") {
    checkPostgreCastStringToBoolean("true", true)
    checkPostgreCastStringToBoolean("tru", true)
    checkPostgreCastStringToBoolean("tr", true)
    checkPostgreCastStringToBoolean("t", true)
    checkPostgreCastStringToBoolean("tRUe", true)
    checkPostgreCastStringToBoolean("    tRue   ", true)
    checkPostgreCastStringToBoolean("    tRu   ", true)
    checkPostgreCastStringToBoolean("yes", true)
    checkPostgreCastStringToBoolean("ye", true)
    checkPostgreCastStringToBoolean("y", true)
    checkPostgreCastStringToBoolean("1", true)
    checkPostgreCastStringToBoolean("on", true)

    checkPostgreCastStringToBoolean("false", false)
    checkPostgreCastStringToBoolean("fals", false)
    checkPostgreCastStringToBoolean("fal", false)
    checkPostgreCastStringToBoolean("fa", false)
    checkPostgreCastStringToBoolean("f", false)
    checkPostgreCastStringToBoolean("    fAlse    ", false)
    checkPostgreCastStringToBoolean("    fAls    ", false)
    checkPostgreCastStringToBoolean("    FAlsE    ", false)
    checkPostgreCastStringToBoolean("no", false)
    checkPostgreCastStringToBoolean("n", false)
    checkPostgreCastStringToBoolean("0", false)
    checkPostgreCastStringToBoolean("off", false)
    checkPostgreCastStringToBoolean("of", false)

    checkPostgreCastStringToBoolean("o", null)
    checkPostgreCastStringToBoolean("abc", null)
    checkPostgreCastStringToBoolean("", null)
  }

  test("Unsupported data types to cast to integer") {
    assert(PostgreCastToInteger(Literal(new Timestamp(1)), None).checkInputDataTypes().isFailure)
    assert(PostgreCastToInteger(Literal(new Date(1)), None).checkInputDataTypes().isFailure)
    assert(PostgreCastToInteger(Literal(1.toByte), None).checkInputDataTypes().isFailure)
  }
}
