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
import org.apache.spark.unsafe.types.UTF8String

class BinaryComparisonSuite extends SparkFunSuite {

  test("EqualTo with String and Integer") {
    val expr = EqualTo(Literal(UTF8String.fromString("123")),
                       Literal(UTF8String.fromString("123")))
    assert(expr.eval(null) === true)
  }

  test("EqualNullSafe with String and Integer") {
    val expr = EqualNullSafe(Literal(UTF8String.fromString("123")),
                             Literal(UTF8String.fromString("123")))
    assert(expr.eval(null) === true)
  }

  test("LessThan with String and Integer") {
    val expr = LessThan(Literal(UTF8String.fromString("123")),
                        Literal(UTF8String.fromString("124")))
    assert(expr.eval(null) === true)
  }

  test("LessThanOrEqual with String and Integer") {
    val expr = LessThanOrEqual(Literal(UTF8String.fromString("123")),
                               Literal(UTF8String.fromString("123")))
    assert(expr.eval(null) === true)
  }

  test("GreaterThan with String and Integer") {
    val expr = GreaterThan(Literal(UTF8String.fromString("124")),
                           Literal(UTF8String.fromString("123")))
    assert(expr.eval(null) === true)
  }

  test("GreaterThanOrEqual with String and Integer") {
    val expr = GreaterThanOrEqual(Literal(UTF8String.fromString("123")),
                                  Literal(UTF8String.fromString("123")))
    assert(expr.eval(null) === true)
  }

  test("In with mixed types") {
    val expr = In(Literal(UTF8String.fromString("123")),
                  Seq(Literal(UTF8String.fromString("123")),
                      Literal(UTF8String.fromString("456"))))
    assert(expr.eval(null) === true)
  }

  test("InSet with mixed types") {
    val expr = InSet(Literal(UTF8String.fromString("123")),
                     Set(UTF8String.fromString("123"),
                         UTF8String.fromString("456")))
    assert(expr.eval(null) === true)
  }

}
