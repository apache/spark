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
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.types.{IntegerType, LongType}

class RandomSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("random") {
    checkEvaluation(Rand(30), 0.2762195585886885)
    checkEvaluation(Randn(30), -1.0451987154313813)

    checkEvaluation(new Rand(Literal.create(null, LongType)), 0.7604953758285915)
    checkEvaluation(new Randn(Literal.create(null, IntegerType)), 1.6034991609278433)
  }

  test("SPARK-9127 codegen with long seed") {
    checkEvaluation(Rand(5419823303878592871L), 0.7145363364564755)
    checkEvaluation(Randn(5419823303878592871L), 0.7816815274533012)
  }

  test("SPARK-31594: Do not display the seed of rand/randn with no argument in output schema") {
    assert(Rand(Literal(1L), true).sql === "rand()")
    assert(Randn(Literal(1L), true).sql === "randn()")
    assert(Rand(Literal(1L), false).sql === "rand(1L)")
    assert(Randn(Literal(1L), false).sql === "randn(1L)")
  }

  test("SPARK-49505: Test the RANDSTR and UNIFORM SQL functions without codegen") {
    // Note that we use a seed of zero in these tests to keep the results deterministic.
    def testRandStr(first: Any, result: Any): Unit = {
      checkEvaluationWithoutCodegen(
        RandStr(Literal(first), Literal(0)), CatalystTypeConverters.convertToCatalyst(result))
    }
    testRandStr(1, "c")
    testRandStr(5, "ceV0P")
    testRandStr(10, "ceV0PXaR2I")
    testRandStr(10L, "ceV0PXaR2I")

    def testUniform(first: Any, second: Any, result: Any): Unit = {
      checkEvaluationWithoutCodegen(
        Uniform(Literal(first), Literal(second), Literal(0)).replacement,
        CatalystTypeConverters.convertToCatalyst(result))
    }
    testUniform(0, 1, 0)
    testUniform(0, 10, 7)
    testUniform(0L, 10L, 7L)
    testUniform(10.0F, 20.0F, 17.604954F)
    testUniform(10L, 20.0F, 17.604954F)
  }
}
