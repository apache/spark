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
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.LiteralToSqlConverter
import org.apache.spark.sql.types._

/**
 * Test suite for the new parameter substitution architecture.
 * Tests the unified parameter handling components.
 */
class ParameterSubstitutionSuite extends SparkFunSuite {

  test("ParameterHandler - basic named parameter substitution") {
    val context = NamedParameterContext(Map("param1" -> Literal(42)))
    val result = ParameterHandler.substituteParameters("SELECT :param1", context)
    assert(result === "SELECT 42")
  }

  test("ParameterHandler - basic positional parameter substitution") {
    val context = PositionalParameterContext(Seq(Literal(42)))
    val result = ParameterHandler.substituteParameters("SELECT ?", context)
    assert(result === "SELECT 42")
  }

  test("ParameterHandler - auto rule detection") {
    val context = NamedParameterContext(Map("param1" -> Literal(42)))

    // Regular statement
    val result1 = ParameterHandler.substituteParameters("SELECT :param1", context)
    assert(result1 === "SELECT 42")

    // SQL scripting block
    val result2 = ParameterHandler.substituteParameters("BEGIN SELECT :param1; END", context)
    assert(result2.contains("SELECT 42"))
  }

  test("ParameterHandler - mixed parameter detection during substitution") {

    // Test that named parameters work
    val result1 = ParameterHandler.substituteParameters("SELECT :param1",
      NamedParameterContext(Map("param1" -> Literal("value"))))
    assert(result1.contains("'value'"))

    // Test that positional parameters work
    val result2 = ParameterHandler.substituteParameters("SELECT ?",
      PositionalParameterContext(List(Literal(42))))
    assert(result2.contains("42"))

    // Test that no parameters works
    val result3 = ParameterHandler.substituteParameters("SELECT 1",
      NamedParameterContext(Map.empty))
    assert(result3 == "SELECT 1")
  }

  test("ParameterHandler - named parameters direct") {

    val result = ParameterHandler.substituteNamedParameters("SELECT :param1",
      Map("param1" -> Literal(42)))
    assert(result === "SELECT 42")
  }

  test("ParameterHandler - positional parameters direct") {

    val result = ParameterHandler.substitutePositionalParameters("SELECT ?", Seq(Literal(42)))
    assert(result === "SELECT 42")
  }

  test("ParameterHandler - empty parameters") {

    val result1 = ParameterHandler.substituteNamedParameters("SELECT 1", Map.empty)
    assert(result1 === "SELECT 1")

    val result2 = ParameterHandler.substitutePositionalParameters("SELECT 1", Seq.empty)
    assert(result2 === "SELECT 1")
  }

  test("LiteralToSqlConverter - basic literals") {
    assert(LiteralToSqlConverter.convert(Literal(42)) === "42")
    assert(LiteralToSqlConverter.convert(Literal("hello")) === "'hello'")
    assert(LiteralToSqlConverter.convert(Literal(true)) === "true")
    assert(LiteralToSqlConverter.convert(Literal(null, StringType)) === "CAST(NULL AS STRING)")
  }

  test("LiteralToSqlConverter - string escaping") {
    assert(LiteralToSqlConverter.convert(Literal("it's")) === "'it\\'s'")
    assert(LiteralToSqlConverter.convert(Literal("'quoted'")) === "'\\'quoted\\''")
  }

  test("LiteralToSqlConverter - array literals") {
    val arrayData = Array(1, 2, 3)
    val arrayLiteral = Literal.create(arrayData, ArrayType(IntegerType))
    val result = LiteralToSqlConverter.convert(arrayLiteral)
    assert(result === "ARRAY(1, 2, 3)")
  }

  test("LiteralToSqlConverter - map literals") {
    val mapData = Map("key1" -> "value1", "key2" -> "value2")
    val mapLiteral = Literal.create(mapData, MapType(StringType, StringType))
    val result = LiteralToSqlConverter.convert(mapLiteral)
    assert(result.startsWith("MAP("))
    assert(result.contains("'key1', 'value1'"))
    assert(result.contains("'key2', 'value2'"))
  }

  test("LiteralToSqlConverter - handles complex data types") {
    // Test with more complex data types to ensure comprehensive support

    // Test with nested array
    val nestedArrayData = Array(Array(1, 2), Array(3, 4))
    val nestedArrayLit = Literal.create(nestedArrayData, ArrayType(ArrayType(IntegerType)))
    val nestedResult = LiteralToSqlConverter.convert(nestedArrayLit)
    assert(nestedResult.startsWith("ARRAY("))
    assert(nestedResult.contains("ARRAY(1, 2)"))
    assert(nestedResult.contains("ARRAY(3, 4)"))

    // Test with null values
    val nullLit = Literal.create(null, StringType)
    assert(LiteralToSqlConverter.convert(nullLit) === "CAST(NULL AS STRING)")
  }

  test("LiteralToSqlConverter - handles foldable expressions") {
    import org.apache.spark.sql.catalyst.expressions.Add

    // Test with a foldable expression (should evaluate and convert)
    val addExpr = Add(Literal(1), Literal(2))
    val result = LiteralToSqlConverter.convert(addExpr)
    assert(result === "3")

  }

  test("Integration - complex parameter substitution") {

    val arrayData = Array(1, 2, 3)
    val mapData = Map("status" -> "active")

    val context = NamedParameterContext(Map(
      "id" -> Literal(42),
      "name" -> Literal("John"),
      "ids" -> Literal.create(arrayData, ArrayType(IntegerType)),
      "metadata" -> Literal.create(mapData, MapType(StringType, StringType))
    ))

    val sql = "SELECT :id, :name WHERE id IN (SELECT explode(:ids)) AND meta = :metadata"
    val result = ParameterHandler.substituteParameters(sql, context)

    assert(result.contains("42"))
    assert(result.contains("'John'"))
    assert(result.contains("ARRAY(1, 2, 3)"))
    assert(result.contains("MAP("))
    assert(result.contains("'status', 'active'"))
  }

  test("Error handling - mixed parameters validation") {

    // Mixed parameters should be detected during substitution itself
    intercept[Exception] {
      ParameterHandler.substituteParameters("SELECT :named, ?",
        NamedParameterContext(Map("named" -> Literal("value"))))
    }
  }

  test("Large parameter set") {

    val largeParamMap = (1 to 100).map(i => s"param$i" -> Literal(i)).toMap
    val context = NamedParameterContext(largeParamMap)
    val paramRefs = (1 to 100).map(i => s":param$i").mkString(", ")
    val sql = s"SELECT $paramRefs"

    val result = ParameterHandler.substituteParameters(sql, context)

    assert(result.contains("1, 2, 3"))
    assert(result.contains("98, 99, 100"))
  }
}
