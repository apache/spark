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
import org.apache.spark.sql.catalyst.util._

class CodeFormatterSuite extends SparkFunSuite {

  def testCase(name: String)(input: String,
      comment: Map[String, String] = Map.empty, maxLines: Int = -1)(expected: String): Unit = {
    test(name) {
      val sourceCode = new CodeAndComment(input.trim, comment)
      if (CodeFormatter.format(sourceCode, maxLines).trim !== expected.trim) {
        fail(
          s"""
             |== FAIL: Formatted code doesn't match ===
             |${sideBySide(CodeFormatter.format(sourceCode, maxLines).trim,
                 expected.trim).mkString("\n")}
           """.stripMargin)
      }
    }
  }

  test("removing overlapping comments") {
    val code = new CodeAndComment(
      """/*project_c4*/
        |/*project_c3*/
        |/*project_c2*/
      """.stripMargin,
      Map(
        "project_c4" -> "// (((input[0, bigint, false] + 1) + 2) + 3))",
        "project_c3" -> "// ((input[0, bigint, false] + 1) + 2)",
        "project_c2" -> "// (input[0, bigint, false] + 1)"
      ))

    val reducedCode = CodeFormatter.stripOverlappingComments(code)
    assert(reducedCode.body === "/*project_c4*/")
  }

  test("removing extra new lines and comments") {
    val code =
      """
        |/*
        |  * multi
        |  * line
        |  * comments
        |  */
        |
        |public function() {
        |/*comment*/
        |  /*comment_with_space*/
        |code_body
        |//comment
        |code_body
        |  //comment_with_space
        |
        |code_body
        |}
      """.stripMargin

    val reducedCode = CodeFormatter.stripExtraNewLinesAndComments(code)
    assert(reducedCode ===
      """
        |public function() {
        |code_body
        |code_body
        |code_body
        |}
      """.stripMargin)
  }

  testCase("basic example") {
    """
      |class A {
      |blahblah;
      |}
    """.stripMargin
  }{
    """
      |/* 001 */ class A {
      |/* 002 */   blahblah;
      |/* 003 */ }
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
      |/* 001 */ class A {
      |/* 002 */   if (c) {
      |/* 003 */     duh;
      |/* 004 */   }
      |/* 005 */ }
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
      |/* 001 */ class A {
      |/* 002 */   if (c) {duh;}
      |/* 003 */ }
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
      |/* 001 */ class A {
      |/* 002 */   if (c) {duh;} else {boo;}
      |/* 003 */ }
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
      |/* 001 */ foo(
      |/* 002 */   a,
      |/* 003 */   b,
      |/* 004 */   c)
    """.stripMargin
  }

  testCase("function calls with maxLines=0") (
    """
      |foo(
      |a,
      |b,
      |c)
    """.stripMargin,
    maxLines = 0
  ) {
    """
      |/* 001 */ [truncated to 0 lines (total lines is 4)]
    """.stripMargin
  }

  testCase("function calls with maxLines=2") (
    """
      |foo(
      |a,
      |b,
      |c)
    """.stripMargin,
    maxLines = 2
  ) {
    """
      |/* 001 */ foo(
      |/* 002 */   a,
      |/* 003 */   [truncated to 2 lines (total lines is 4)]
    """.stripMargin
  }

  testCase("single line comments") {
    """
      |// This is a comment about class A { { { ( (
      |class A {
      |class body;
      |}
    """.stripMargin
  }{
    """
      |/* 001 */ // This is a comment about class A { { { ( (
      |/* 002 */ class A {
      |/* 003 */   class body;
      |/* 004 */ }
    """.stripMargin
  }

  testCase("single line comments /* */ ") {
    """
      |/** This is a comment about class A { { { ( ( */
      |class A {
      |class body;
      |}
    """.stripMargin
  }{
    """
      |/* 001 */ /** This is a comment about class A { { { ( ( */
      |/* 002 */ class A {
      |/* 003 */   class body;
      |/* 004 */ }
    """.stripMargin
  }

  testCase("multi-line comments") {
    """
      |    /* This is a comment about
      |class A {
      |class body; ...*/
      |class A {
      |class body;
      |}
    """.stripMargin
  }{
    """
      |/* 001 */ /* This is a comment about
      |/* 002 */ class A {
      |/* 003 */   class body; ...*/
      |/* 004 */ class A {
      |/* 005 */   class body;
      |/* 006 */ }
    """.stripMargin
  }

  testCase("reduce empty lines") {
    CodeFormatter.stripExtraNewLines(
      """
        |class A {
        |
        |
        | /*
        |  * multi
        |  * line
        |  * comment
        |  */
        |
        | class body;
        |
        |
        | if (c) {duh;}
        | else {boo;}
        |}
      """.stripMargin.trim)
  }{
    """
      |/* 001 */ class A {
      |/* 002 */   /*
      |/* 003 */    * multi
      |/* 004 */    * line
      |/* 005 */    * comment
      |/* 006 */    */
      |/* 007 */   class body;
      |/* 008 */
      |/* 009 */   if (c) {duh;}
      |/* 010 */   else {boo;}
      |/* 011 */ }
    """.stripMargin
  }

  testCase("comment place holder")(
    """
      |/*c1*/
      |class A
      |/*c2*/
      |class B
      |/*c1*//*c2*/
    """.stripMargin, Map("c1" -> "/*abc*/", "c2" -> "/*xyz*/")
  ) {
    """
      |/* 001 */ /*abc*/
      |/* 002 */ class A
      |/* 003 */ /*xyz*/
      |/* 004 */ class B
      |/* 005 */ /*abc*//*xyz*/
    """.stripMargin
  }
}
