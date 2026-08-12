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

import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

class ParseCommandSuite extends SparkFunSuite with ExpressionEvalHelper {

  private def evalJson(sql: String): JValue = {
    val result = ParseCommand(Literal(sql)).eval().asInstanceOf[UTF8String].toString
    parse(result)
  }

  test("parse_command returns JSON for a valid SELECT") {
    val j = evalJson("SELECT 1 AS a")
    assert(j \ "parse_success" === JBool(true))
    assert(j \ "statement_identifier" === JString("SELECT"))
    assert(j \ "statement_code" === JInt(21))
  }

  test("parse_command returns null for null input") {
    checkEvaluation(ParseCommand(Literal.create(null, StringType)), null)
  }

  test("parse_command does not throw on syntax error") {
    val j = evalJson("NOT A STATEMENT !!!")
    assert(j \ "parse_success" === JBool(false))
    assert(j \ "error" \ "errorClass" === JString("PARSE_SYNTAX_ERROR"))
    assert((j \ "error" \ "messageTemplate") != JNothing)
  }

  test("parse_command works with CodegenFallback path") {
    val expr = ParseCommand(Literal("INSERT INTO t SELECT 1"))
    assert(expr.isInstanceOf[CodegenFallback])
    val j = evalJson("INSERT INTO t SELECT 1")
    assert(j \ "statement_identifier" === JString("INSERT"))
  }
}
