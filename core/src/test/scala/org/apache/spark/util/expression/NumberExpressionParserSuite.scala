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
package org.apache.spark.util.expression

import org.scalatest.FunSuite

class NumberExpressionParserSuite extends FunSuite {
  val parser = Parsers.NumberParser

  def testParser(in: String, expectedResult:Double): Unit = {
    val parseResult: Option[Double] = parser.parse(in)
    assert(parseResult == Some(expectedResult))
  }

  test("simple addition") {
    testParser(in = "4+4", expectedResult = 8)
  }

  test("simple subtraction") {
    testParser(in = "5-5", expectedResult = 0)
  }

  test("simple division") {
    testParser(in = "1/2", expectedResult = 0.5)
  }

  test("simple multiplication") {
    testParser(in = "1*2", expectedResult = 2)
  }

  test("multiplication and division") {
    testParser(in = "1*2/4", expectedResult = 0.5)
  }

  test("precedance of multiplication") {
    testParser(in = "1 + 2 * 3", expectedResult = 7.0)
  }

  test("precedance of division") {
    testParser(in = "1 + 3 / 3", expectedResult = 2.0)
  }

  test("bracketsPrecedance") {
    testParser(in = "(1 + 2) * 3", expectedResult = 9.0)
  }

  test("precedance2") {
    testParser(in = "1 * 2 + 3", expectedResult = 5.0)
  }

  test("subtracting two negative numbers") {
    testParser(in = "-1 - -1", expectedResult = 0.0)
  }

  test("multiplying two negative numbers") {
    testParser(in = "-2 * -2", expectedResult = 4.0)
  }

  test("multiplying one negative and one positive number") {
    testParser(in = "-2 * 2", expectedResult = -4.0)
  }

  test("divide by zero") {
    testParser(in = "1 / 0", expectedResult = Double.PositiveInfinity)
  }

  test("multiplay by zero") {
    testParser(in = "1 * 0", expectedResult = 0.0)
  }

  test("numCores") {
    val numCores = Runtime.getRuntime.availableProcessors() * 1.0
    testParser(in="JVMnumCores", expectedResult=numCores)
  }

  test("numCores fractional") {
    val numCores = Runtime.getRuntime.availableProcessors() * 1.0
    testParser(in="JVMnumCores/2", expectedResult=numCores/2)
  }
}
