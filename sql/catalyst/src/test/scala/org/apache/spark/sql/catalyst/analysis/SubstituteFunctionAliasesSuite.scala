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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.types._

/** Unit tests for [[SubstituteFunctionAliases]]. */
class SubstituteFunctionAliasesSuite extends PlanTest {

  private def ruleTest(initial: Expression, transformed: Expression): Unit = {
    ruleTest(SubstituteFunctionAliases, initial, transformed)
  }

  private def func(name: String, arg: Any, isDistinct: Boolean = false): UnresolvedFunction = {
    UnresolvedFunction(name, Literal(arg) :: Nil, isDistinct)
  }

  test("boolean") {
    ruleTest(func("boolean", 10), Cast(Literal(10), BooleanType))
  }

  test("tinyint") {
    ruleTest(func("tinyint", 10), Cast(Literal(10), ByteType))
  }

  test("smallint") {
    ruleTest(func("smallint", 10), Cast(Literal(10), ShortType))
  }

  test("int") {
    ruleTest(func("int", 10), Cast(Literal(10), IntegerType))
  }

  test("bigint") {
    ruleTest(func("bigint", 10), Cast(Literal(10), LongType))
  }

  test("float") {
    ruleTest(func("float", 10), Cast(Literal(10), FloatType))
  }

  test("double") {
    ruleTest(func("double", 10), Cast(Literal(10), DoubleType))
  }

  test("decimal") {
    ruleTest(func("decimal", 10), Cast(Literal(10), DecimalType.USER_DEFAULT))
  }

  test("binary") {
    ruleTest(func("binary", 10), Cast(Literal(10), BinaryType))
  }

  test("string") {
    ruleTest(func("string", 10), Cast(Literal(10), StringType))
  }

  test("function is not an alias for cast if it has a database defined") {
    val f = UnresolvedFunction(
      FunctionIdentifier("int", database = Option("db")), Literal(10) :: Nil, isDistinct = false)
    ruleTest(f, f)
  }

  test("function is not an alias for cast if it has zero input arg") {
    val f = UnresolvedFunction("int", Nil, isDistinct = false)
    ruleTest(f, f)
  }

  test("function is not an alias for cast if it has more than one input args") {
    val f = UnresolvedFunction("int", Literal(10) :: Literal(11) :: Nil, isDistinct = false)
    ruleTest(f, f)
  }

  test("function is not an alias for cast if it is distinct (aggregate function)") {
    val f = UnresolvedFunction("int", Literal(10) :: Nil, isDistinct = true)
    ruleTest(f, f)
  }
}
