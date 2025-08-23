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
import org.apache.spark.sql.catalyst.util.ExpressionToSqlConverter
import org.apache.spark.sql.types._

/**
 * Test suite for the new parameter substitution architecture.
 * Tests the unified parameter handling components.
 */
class ParameterSubstitutionSuite extends SparkFunSuite {

  test("ParameterHandler - basic named parameter substitution") {
    val handler = new ParameterHandler()
    val context = NamedParameterContext(Map("param1" -> Literal(42)))
    val result = handler.substituteParameters("SELECT :param1", context)
    assert(result === "SELECT 42")
  }

  test("ParameterHandler - basic positional parameter substitution") {
    val handler = new ParameterHandler()
    val context = PositionalParameterContext(Seq(Literal(42)))
    val result = handler.substituteParameters("SELECT ?", context)
    assert(result === "SELECT 42")
  }

  test("ParameterHandler - auto rule detection") {
    val handler = new ParameterHandler()
    val context = NamedParameterContext(Map("param1" -> Literal(42)))

    // Regular statement
    val result1 = handler.substituteParametersWithAutoRule("SELECT :param1", context)
    assert(result1 === "SELECT 42")

    // SQL scripting block
    val result2 = handler.substituteParametersWithAutoRule("BEGIN SELECT :param1; END", context)
    assert(result2.contains("SELECT 42"))
  }

  test("ParameterHandler - parameter detection") {
    val handler = new ParameterHandler()

    val (hasPos1, hasNamed1) = handler.detectParameters("SELECT :param1")
    assert(!hasPos1 && hasNamed1)

    val (hasPos2, hasNamed2) = handler.detectParameters("SELECT ?")
    assert(hasPos2 && !hasNamed2)

    val (hasPos3, hasNamed3) = handler.detectParameters("SELECT 1")
    assert(!hasPos3 && !hasNamed3)
  }

  test("ParameterSubstitutionStrategy - named strategy") {
    val strategy = ParameterSubstitutionStrategy.named(Map("param1" -> Literal(42)))
    val substitutor = new SubstituteParamsParser()

    val result = strategy.substitute("SELECT :param1", SubstitutionRule.Statement, substitutor)
    assert(result === "SELECT 42")

    assert(strategy.canHandle("SELECT :param1", SubstitutionRule.Statement, substitutor))
    assert(!strategy.canHandle("SELECT ?", SubstitutionRule.Statement, substitutor))
  }

  test("ParameterSubstitutionStrategy - positional strategy") {
    val strategy = ParameterSubstitutionStrategy.positional(Seq(Literal(42)))
    val substitutor = new SubstituteParamsParser()

    val result = strategy.substitute("SELECT ?", SubstitutionRule.Statement, substitutor)
    assert(result === "SELECT 42")

    assert(strategy.canHandle("SELECT ?", SubstitutionRule.Statement, substitutor))
    assert(!strategy.canHandle("SELECT :param1", SubstitutionRule.Statement, substitutor))
  }

  test("ParameterSubstitutionStrategy - no parameter strategy") {
    val strategy = NoParameterStrategy
    val substitutor = new SubstituteParamsParser()

    val result = strategy.substitute("SELECT 1", SubstitutionRule.Statement, substitutor)
    assert(result === "SELECT 1")

    assert(strategy.canHandle("SELECT 1", SubstitutionRule.Statement, substitutor))
  }

  test("ExpressionToSqlConverter - basic literals") {
    assert(ExpressionToSqlConverter.convert(Literal(42)) === "42")
    assert(ExpressionToSqlConverter.convert(Literal("hello")) === "'hello'")
    assert(ExpressionToSqlConverter.convert(Literal(true)) === "TRUE")
    assert(ExpressionToSqlConverter.convert(Literal(null, StringType)) === "NULL")
  }

  test("ExpressionToSqlConverter - string escaping") {
    assert(ExpressionToSqlConverter.convert(Literal("it's")) === "'it''s'")
    assert(ExpressionToSqlConverter.convert(Literal("'quoted'")) === "'''quoted'''")
  }

  test("ExpressionToSqlConverter - array literals") {
    val arrayData = Array(1, 2, 3)
    val arrayLiteral = Literal.create(arrayData, ArrayType(IntegerType))
    val result = ExpressionToSqlConverter.convert(arrayLiteral)
    assert(result === "ARRAY(1, 2, 3)")
  }

  test("ExpressionToSqlConverter - map literals") {
    val mapData = Map("key1" -> "value1", "key2" -> "value2")
    val mapLiteral = Literal.create(mapData, MapType(StringType, StringType))
    val result = ExpressionToSqlConverter.convert(mapLiteral)
    assert(result.startsWith("MAP("))
    assert(result.contains("'key1', 'value1'"))
    assert(result.contains("'key2', 'value2'"))
  }

  test("Integration - complex parameter substitution") {
    val handler = new ParameterHandler()

    val arrayData = Array(1, 2, 3)
    val mapData = Map("status" -> "active")

    val context = NamedParameterContext(Map(
      "id" -> Literal(42),
      "name" -> Literal("John"),
      "ids" -> Literal.create(arrayData, ArrayType(IntegerType)),
      "metadata" -> Literal.create(mapData, MapType(StringType, StringType))
    ))

    val sql = "SELECT :id, :name WHERE id IN (SELECT explode(:ids)) AND meta = :metadata"
    val result = handler.substituteParameters(sql, context)

    assert(result.contains("42"))
    assert(result.contains("'John'"))
    assert(result.contains("ARRAY(1, 2, 3)"))
    assert(result.contains("MAP("))
    assert(result.contains("'status', 'active'"))
  }

  test("Error handling - mixed parameters validation") {
    val handler = new ParameterHandler()

    intercept[Exception] {
      handler.validateParameterConsistency("SELECT :named, ?")
    }
  }

  test("Performance - large parameter set") {
    val handler = new ParameterHandler()

    val largeParamMap = (1 to 100).map(i => s"param$i" -> Literal(i)).toMap
    val context = NamedParameterContext(largeParamMap)
    val paramRefs = (1 to 100).map(i => s":param$i").mkString(", ")
    val sql = s"SELECT $paramRefs"

    val startTime = System.currentTimeMillis()
    val result = handler.substituteParameters(sql, context)
    val endTime = System.currentTimeMillis()

    assert(endTime - startTime < 2000, s"Took ${endTime - startTime}ms")
    assert(result.contains("1, 2, 3"))
    assert(result.contains("98, 99, 100"))
  }
}
