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

class TimeExpressionParserSuite extends FunSuite {
  val parser = Parsers.TimeAsMSParser

  val secParser = Parsers.TimeAsSecParser

  val secMs = 1000.0
  val minMs = 60 * secMs
  val hourMs = 60 * minMs
  val dayMs = 24 * hourMs
  val weekMS = 7 * dayMs


  def testParser(in: String, expectedResult: Double): Unit = {
    val parseResult = parser.parse(in)
    assert(parseResult == Some(expectedResult))
  }

  def testSecParser(in: String, expectedResult: Double): Unit = {
    val parseResult = secParser.parse(in)
    assert(parseResult == Some(expectedResult))
  }

  test("ms") {
    testParser(in = "$10ms", expectedResult = 10)
  }

  test("seconds") {
    testParser(in = "$10 seconds", expectedResult = 10 * secMs)
  }

  test("minutes") {
    testParser(in = "$1 * 0.5 m", expectedResult = 30 * secMs)
  }

  test("hours") {
    testParser(in = "$5 hours", expectedResult = 5 * hourMs)
  }

  test("days") {
    testParser(in = "$5 day", expectedResult = 5 * dayMs)
  }

  test("weeks") {
    testParser(in = "$5 weeks", expectedResult = 5 * weekMS)
  }

  test("addition") {
    testParser(in = "$5 weeks + 3 days", expectedResult = 5 * weekMS + 3 * dayMs)
  }

  test("subtraction") {
    testParser(in = "$5 weeks - 3 days", expectedResult = 5 * weekMS - 3 * dayMs)
  }

  test("division") {
    testParser(in = "$5 weeks / 5", expectedResult = 1 * weekMS )
  }

  test("multiplication") {
    testParser(in = "$5 * 5 weeks", expectedResult = 25 * weekMS )
  }

  test("standalone ") {
    testParser(in = "$5 + 5 ms", expectedResult = 10 )
  }

  test("SecondsParser Seconds ") {
    testSecParser(in = "$5 + 5 Sec", expectedResult = 10 )
  }
}
