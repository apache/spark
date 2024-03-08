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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.LongType

class SubExprEvaluationRuntimeSuite extends SparkFunSuite {

  test("Evaluate ExpressionProxy should create cached result") {
    val runtime = new SubExprEvaluationRuntime(1)
    val proxy = ExpressionProxy(Literal(1), 0, runtime)
    assert(runtime.cache.size() == 0)
    proxy.eval()
    assert(runtime.cache.size() == 1)
    assert(runtime.cache.get(proxy) == ResultProxy(1))
  }

  test("SubExprEvaluationRuntime cannot exceed configured max entries") {
    val runtime = new SubExprEvaluationRuntime(2)
    assert(runtime.cache.size() == 0)

    val proxy1 = ExpressionProxy(Literal(1), 0, runtime)
    proxy1.eval()
    assert(runtime.cache.size() == 1)
    assert(runtime.cache.get(proxy1) == ResultProxy(1))

    val proxy2 = ExpressionProxy(Literal(2), 1, runtime)
    proxy2.eval()
    assert(runtime.cache.size() == 2)
    assert(runtime.cache.get(proxy2) == ResultProxy(2))

    val proxy3 = ExpressionProxy(Literal(3), 2, runtime)
    proxy3.eval()
    assert(runtime.cache.size() == 2)
    assert(runtime.cache.get(proxy3) == ResultProxy(3))
  }

  test("setInput should empty cached result") {
    val runtime = new SubExprEvaluationRuntime(2)
    val proxy1 = ExpressionProxy(Literal(1), 0, runtime)
    assert(runtime.cache.size() == 0)
    proxy1.eval()
    assert(runtime.cache.size() == 1)
    assert(runtime.cache.get(proxy1) == ResultProxy(1))

    val proxy2 = ExpressionProxy(Literal(2), 1, runtime)
    proxy2.eval()
    assert(runtime.cache.size() == 2)
    assert(runtime.cache.get(proxy2) == ResultProxy(2))

    runtime.setInput()
    assert(runtime.cache.size() == 0)
  }

  test("Wrap ExpressionProxy on subexpressions") {
    val runtime = new SubExprEvaluationRuntime(1)

    val one = Literal(1)
    val two = Literal(2)
    val mul = Multiply(one, two)
    val mul2 = Multiply(mul, mul)
    val sqrt = Sqrt(mul2)
    val sum = Add(mul2, sqrt)

    //  ( (one * two) * (one * two) ) + sqrt( (one * two) * (one * two) )
    val proxyExpressions = runtime.proxyExpressions(Seq(sum))
    val proxys = proxyExpressions.flatMap(_.collect {
      case p: ExpressionProxy => p
    })
    // ( (one * two) * (one * two) )
    assert(proxys.size == 2)
    assert(proxys.forall(_.child == mul2))
  }

  test("ExpressionProxy won't be on non deterministic") {
    val runtime = new SubExprEvaluationRuntime(1)

    val sum = Add(Rand(0), Rand(0))
    val proxys = runtime.proxyExpressions(Seq(sum, sum)).flatMap(_.collect {
      case p: ExpressionProxy => p
    })
    assert(proxys.isEmpty)
  }

  test("SubExprEvaluationRuntime should wrap semantically equal exprs") {
    val runtime = new SubExprEvaluationRuntime(1)

    val one = Literal(1)
    val two = Literal(2)
    def mul: (Literal, Literal) => Expression =
      (left: Literal, right: Literal) => Multiply(left, right)

    val mul2_1 = Multiply(mul(one, two), mul(one, two))
    val mul2_2 = Multiply(mul(one, two), mul(one, two))

    val sqrt = Sqrt(mul2_1)
    val sum = Add(mul2_2, sqrt)
    val proxyExpressions = runtime.proxyExpressions(Seq(sum))
    val proxys = proxyExpressions.flatMap(_.collect {
      case p: ExpressionProxy => p
    })
    // ( (one * two) * (one * two) )
    assert(proxys.size == 2)
    assert(proxys.forall(_.child.semanticEquals(mul2_1)))
  }

  test("SPARK-41991: CheckOverflowInTableInsert with ExpressionProxy child") {
    val runtime = new SubExprEvaluationRuntime(1)
    val proxy = ExpressionProxy(Cast(Literal.apply(1), LongType), 0, runtime)
    val checkOverflow = CheckOverflowInTableInsert(Cast(Literal.apply(1), LongType), "col")
      .withNewChildrenInternal(IndexedSeq(proxy))
    assert(runtime.cache.size() == 0)
    checkOverflow.eval()
    assert(runtime.cache.size() == 1)
    assert(runtime.cache.get(proxy) == ResultProxy(1L))
  }
}
