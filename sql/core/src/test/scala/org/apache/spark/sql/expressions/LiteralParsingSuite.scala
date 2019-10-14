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

package org.apache.spark.sql.expressions

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{DoubleLiteral, FloatLiteral, Literal}
import org.apache.spark.sql.test.SharedSparkSession

class LiteralParsingSuite extends QueryTest with SharedSparkSession {
  def testRoundtrip(testName: String, literal: Literal): Unit = {
    test(testName) {
      val roundTrippedValue = sql(s"SELECT ${literal.sql}").collect().head.get(0)
      literal match {
        case FloatLiteral(f) if f.isNaN =>
          assert(roundTrippedValue.isInstanceOf[Float])
          assert(roundTrippedValue.asInstanceOf[Float].isNaN)
        case DoubleLiteral(d) if d.isNaN =>
          assert(roundTrippedValue.isInstanceOf[Double])
          assert(roundTrippedValue.asInstanceOf[Double].isNaN)
        case _ =>
          assert(Literal(roundTrippedValue) === literal)
      }
    }
  }

  testRoundtrip("Timestamp", Literal(Timestamp.from(
    LocalDateTime.of(2019, 3, 21, 0, 2, 3, 456789123)
      .atZone(ZoneOffset.UTC)
      .toInstant)))

  testRoundtrip("FloatNan", Literal(Float.NaN))
  testRoundtrip("FloatLiteralNegativeInfinity", Literal(Float.NegativeInfinity))
  testRoundtrip("FloatLiteralMin", Literal(Float.MinValue))
  testRoundtrip("FloatLiteralSubnormal", Literal(Float.MinPositiveValue))
  testRoundtrip("FloatLiteralMax", Literal(Float.MaxValue))
  testRoundtrip("FloatLiteralPositiveInfinity", Literal(Float.PositiveInfinity))

  testRoundtrip("DoubleNan", Literal(Double.NaN))
  testRoundtrip("DoubleLiteralNegativeInfinity", Literal(Double.NegativeInfinity))
  testRoundtrip("DoubleLiteralMin", Literal(Double.MinValue))
  testRoundtrip("DoubleLiteralSubnormal", Literal(Double.MinPositiveValue))
  testRoundtrip("DoubleLiteralMax", Literal(Double.MaxValue))
  testRoundtrip("DoubleLiteralPositiveInfinity", Literal(Double.PositiveInfinity))
}
