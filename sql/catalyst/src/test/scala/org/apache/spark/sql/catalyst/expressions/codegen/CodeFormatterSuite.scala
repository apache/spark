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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite


class CodeFormatterSuite extends SparkFunSuite {

  def testCase(name: String)(input: String)(expected: String): Unit = {
    test(name) {
      assert(CodeFormatter.format(input).trim === expected.trim)
    }
  }

  testCase("basic example") {
    """
      |class A {
      |blahblah;
      |}
    """.stripMargin
  }{
    """
      |class A {
      |  blahblah;
      |}
    """.stripMargin
  }

  testCase("nested example") {
    """
      |class A {
      | if (c) {
      |duh;
      |}
      |}
    """.stripMargin
  } {
    """
      |class A {
      |  if (c) {
      |    duh;
      |  }
      |}
    """.stripMargin
  }

  testCase("single line") {
    """
      |class A {
      | if (c) {duh;}
      |}
    """.stripMargin
  }{
    """
      |class A {
      |  if (c) {duh;}
      |}
    """.stripMargin
  }

  testCase("if else on the same line") {
    """
      |class A {
      | if (c) {duh;} else {boo;}
      |}
    """.stripMargin
  }{
    """
      |class A {
      |  if (c) {duh;} else {boo;}
      |}
    """.stripMargin
  }

  testCase("function calls") {
    """
      |foo(
      |a,
      |b,
      |c)
    """.stripMargin
  }{
    """
      |foo(
      |  a,
      |  b,
      |  c)
    """.stripMargin
  }
}
