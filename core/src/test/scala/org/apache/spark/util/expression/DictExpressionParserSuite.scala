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

class DictExpressionParserSuite  extends FunSuite {
  private val sparkConfig: String = "spark.config,numbers.2"
  private val sparkConfigCommonPrefix: String = "spark.config.numbers.2 two"

  private val SymbolWithRegexChars: String = "spark.*.test"

  val config = Map[String,Long]("one"->1, sparkConfig-> 2, SymbolWithRegexChars-> 3,
    sparkConfigCommonPrefix->4, "1 KB"->5)

  val parser = Parsers.ByteDictParser(config)

  def testParser(in: String, expectedResult: Double): Unit = {
    val parseResult = parser.parse(in)
    assert(parseResult == Some(expectedResult))
  }

  test("simple expansion") {
    testParser(in = "one", 1)
  }

  test("config style expansion") {
    testParser(in = s"$sparkConfig + 1", 3)
  }

  test("test for regexp conflicts") {
    testParser(in = s"$SymbolWithRegexChars * 2", 6)
  }

  test("test for earlier symbols match before later ones do") {
    testParser(in = s"$sparkConfigCommonPrefix", 4)
  }

  test("test for precedence of byte units against dict entries") {
    testParser(in = "1 KB", 1000)
  }

}
