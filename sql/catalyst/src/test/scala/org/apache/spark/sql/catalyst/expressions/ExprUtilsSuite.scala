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
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}

class ExprUtilsSuite extends SparkFunSuite {

  private val a = AttributeReference("a", LongType)()

  test("canEvaluateUnconditionally: whitelisted total expressions") {
    assert(ExprUtils.canEvaluateUnconditionally(a))
    assert(ExprUtils.canEvaluateUnconditionally(Literal(1L)))
    assert(ExprUtils.canEvaluateUnconditionally(
      And(LessThan(a, Literal(5L)), IsNotNull(a))))
    assert(ExprUtils.canEvaluateUnconditionally(
      GetStructField(
        AttributeReference("s", StructType(StructField("f1", IntegerType) :: Nil))(), 0)))
    assert(ExprUtils.canEvaluateUnconditionally(Coalesce(Seq(a, Literal(0L)))))
    assert(ExprUtils.canEvaluateUnconditionally(In(a, Seq(Literal(1L), Literal(2L)))))
  }

  test("canEvaluateUnconditionally: expressions that can throw are excluded") {
    // Arithmetic can throw (overflow, div-by-zero in ANSI mode) even though it does not
    // override `throwable` and inherits false from its non-throwing children, which is
    // why the whitelist is used instead of the throwable flag.
    val arithmetic = LessThan(Add(a, Literal(1L)), Literal(5L))
    assert(!arithmetic.throwable)
    assert(!ExprUtils.canEvaluateUnconditionally(arithmetic))
    assert(!ExprUtils.canEvaluateUnconditionally(EqualTo(Remainder(a, Literal(3L)), Literal(0L))))
    assert(!ExprUtils.canEvaluateUnconditionally(Cast(a, StringType)))
    // GetArrayItem/ElementAt can throw on invalid ordinals in ANSI mode.
    val arr = AttributeReference("arr", ArrayType(IntegerType))()
    assert(!ExprUtils.canEvaluateUnconditionally(GetArrayItem(arr, Literal(0))))
    // A whitelisted predicate over a non-whitelisted child is still excluded.
    assert(!ExprUtils.canEvaluateUnconditionally(IsNotNull(Add(a, Literal(1L)))))
  }

  test("canEvaluateUnconditionally: non-deterministic expressions are excluded") {
    assert(!ExprUtils.canEvaluateUnconditionally(LessThan(Rand(Literal(0L)), Literal(0.5))))
  }
}
