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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * A test suite that makes sure code generation handles expression internally states correctly.
 */
class CodegenExpressionCachingSuite extends SparkFunSuite {

  test("GenerateUnsafeProjection should initialize expressions") {
    // Use an Add to wrap two of them together in case we only initialize the top level expressions.
    val expr = And(NondeterministicExpression(), NondeterministicExpression())
    val instance = UnsafeProjection.create(Seq(expr))
    instance.initialize(0)
    assert(instance.apply(null).getBoolean(0) === false)
  }

  test("GenerateMutableProjection should initialize expressions") {
    val expr = And(NondeterministicExpression(), NondeterministicExpression())
    val instance = GenerateMutableProjection.generate(Seq(expr))
    instance.initialize(0)
    assert(instance.apply(null).getBoolean(0) === false)
  }

  test("GeneratePredicate should initialize expressions") {
    val expr = And(NondeterministicExpression(), NondeterministicExpression())
    val instance = GeneratePredicate.generate(expr)
    instance.initialize(0)
    assert(instance.eval(null) === false)
  }

  test("GenerateUnsafeProjection should not share expression instances") {
    val expr1 = MutableExpression()
    val instance1 = UnsafeProjection.create(Seq(expr1))
    assert(instance1.apply(null).getBoolean(0) === false)

    val expr2 = MutableExpression()
    expr2.mutableState = true
    val instance2 = UnsafeProjection.create(Seq(expr2))
    assert(instance1.apply(null).getBoolean(0) === false)
    assert(instance2.apply(null).getBoolean(0))
  }

  test("GenerateMutableProjection should not share expression instances") {
    val expr1 = MutableExpression()
    val instance1 = GenerateMutableProjection.generate(Seq(expr1))
    assert(instance1.apply(null).getBoolean(0) === false)

    val expr2 = MutableExpression()
    expr2.mutableState = true
    val instance2 = GenerateMutableProjection.generate(Seq(expr2))
    assert(instance1.apply(null).getBoolean(0) === false)
    assert(instance2.apply(null).getBoolean(0))
  }

  test("GeneratePredicate should not share expression instances") {
    val expr1 = MutableExpression()
    val instance1 = GeneratePredicate.generate(expr1)
    assert(instance1.eval(null) === false)

    val expr2 = MutableExpression()
    expr2.mutableState = true
    val instance2 = GeneratePredicate.generate(expr2)
    assert(instance1.eval(null) === false)
    assert(instance2.eval(null))
  }

}

/**
 * An expression that's non-deterministic and doesn't support codegen.
 */
case class NondeterministicExpression()
  extends LeafExpression with Nondeterministic with CodegenFallback {
  override protected def initializeInternal(partitionIndex: Int): Unit = {}
  override protected def evalInternal(input: InternalRow): Any = false
  override def nullable: Boolean = false
  override def dataType: DataType = BooleanType
}


/**
 * An expression with mutable state so we can change it freely in our test suite.
 */
case class MutableExpression() extends LeafExpression with CodegenFallback {
  var mutableState: Boolean = false
  override def eval(input: InternalRow): Any = mutableState

  override def nullable: Boolean = false
  override def dataType: DataType = BooleanType
}
