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
import org.apache.spark.sql.internal.SQLConf
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

    def testUniform(first: Any, second: Any, result: Any): Unit = {
      checkEvaluationWithoutCodegen(
        Uniform(Literal(first), Literal(second), Literal(0)).replacement,
        CatalystTypeConverters.convertToCatalyst(result))
    }
    testUniform(0, 1, 1)
    testUniform(0, 10, 8)
    testUniform(0L, 10L, 8L)
    testUniform(10.0F, 20.0F, 17.604954F)
    testUniform(10L, 20.0F, 17.604954F)
  }

  /** Evaluates a nondeterministic expression in partition 0 without code generation. */
  private def evalInPartitionZero(e: Expression): Any = {
    val copied = e.freshCopyIfContainsStatefulExpression()
    copied.foreach {
      case n: Nondeterministic => n.initialize(0)
      case _ =>
    }
    copied.eval()
  }

  test("UniformLongOffset agrees with and without codegen") {
    Seq(0L, 3L, (1L << 53) + 1, Long.MaxValue, Long.MinValue, -3L).foreach { n =>
      val offset = UniformLongOffset(n, Literal(0L))
      val expected = evalInPartitionZero(offset)
      checkEvaluation(offset, expected)
      val value = expected.asInstanceOf[Long]
      if (n != 0L) {
        assert(java.lang.Long.compareUnsigned(value, n) < 0, s"$value is not below $n")
      }
    }
  }

  test("Uniform with BIGINT bounds stays in range at the BIGINT limits") {
    Seq(
      (Long.MinValue, Long.MaxValue),
      (Long.MinValue, -1L),
      (0L, Long.MaxValue),
      (Long.MaxValue - 1, Long.MaxValue),
      (Long.MinValue, Long.MinValue + 1)).foreach { case (lo, hi) =>
      Seq(0L, 1L, 42L).foreach { seed =>
        val replacement = Uniform(Literal(lo), Literal(hi), Literal(seed)).replacement
        val expected = evalInPartitionZero(replacement)
        checkEvaluation(replacement, expected)
        val value = expected.asInstanceOf[Long]
        assert(lo <= value && value <= hi, s"$value is not in [$lo, $hi]")
      }
    }
  }

  test("Uniform keeps the legacy flag captured when it was built") {
    val legacy = withSQLConf(SQLConf.LEGACY_UNIFORM_INTEGRAL_TRUNCATE.key -> "true") {
      Uniform(Literal(-1), Literal(1), Literal(0))
    }
    val closed = Uniform(Literal(-1), Literal(1), Literal(0))
    assert(legacy.legacyIntegralTruncate)
    assert(!closed.legacyIntegralTruncate)
    Seq(legacy.withNewSeed(1), legacy.withShiftedSeed(1),
        legacy.withTimeZone("UTC"), legacy.freshCopyIfContainsStatefulExpression()).foreach {
      case copied: Uniform => assert(copied.legacyIntegralTruncate)
      case other => fail(s"Expected Uniform but got ${other.getClass.getName}")
    }
    assert(!legacy.semanticEquals(closed))
  }

  test("SPARK-58208: Uniform preserves its time zone when copied") {
    val uniform = Uniform(
      Literal(10), Literal(20), Literal(0), hideSeed = false, timeZoneId = Some("UTC"))
    assert(uniform.resolved)

    val copied = uniform.freshCopyIfContainsStatefulExpression().asInstanceOf[Uniform]
    assert(copied ne uniform)
    assert(copied.timeZoneId == uniform.timeZoneId)
    assert(copied.resolved)

    Seq(uniform.withNewSeed(1), uniform.withShiftedSeed(1)).foreach {
      case copied: Uniform =>
        assert(copied.timeZoneId == uniform.timeZoneId)
        assert(copied.resolved)
      case other => fail(s"Expected Uniform but got ${other.getClass.getName}")
    }
  }
}
