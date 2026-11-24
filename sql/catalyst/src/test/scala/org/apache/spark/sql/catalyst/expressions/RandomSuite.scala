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

  /**
   * Draws `rows` values of `e` in partition 0, in interpreted and in generated code, checks that
   * both give the same values, and returns them.
   */
  private def drawRows(e: Expression, rows: Int): Seq[Any] = {
    val results = Seq(CodegenObjectFactoryMode.NO_CODEGEN, CodegenObjectFactoryMode.CODEGEN_ONLY)
      .map { mode =>
        withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> mode.toString) {
          val projection = MutableProjection.create(Seq(e))
          projection.initialize(0)
          Seq.fill(rows)(projection(EmptyRow).get(0, e.dataType))
        }
      }
    assert(results.head === results.last, "interpreted and generated values differ")
    results.head
  }

  private def integralDraw(lo: Long, hi: Long, seed: Long = 0L): UniformIntegralDraw =
    UniformIntegralDraw(Literal(lo), Literal(hi), Literal(seed))

  test("UniformIntegralDraw stays in range, in either order and at the BIGINT limits") {
    Seq(
      (0L, 9L),
      (9L, 0L),
      (5L, 5L),
      (-(1L << 40), 1L << 40),
      (Long.MinValue, Long.MaxValue),
      (Long.MinValue, -1L),
      (0L, Long.MaxValue),
      (Long.MaxValue - 1, Long.MaxValue),
      (Long.MinValue, Long.MinValue + 1)).foreach { case (a, b) =>
      val (lo, hi) = (math.min(a, b), math.max(a, b))
      drawRows(integralDraw(a, b), 1000).foreach { case value: Long =>
        assert(lo <= value && value <= hi, s"$value is not in [$lo, $hi]")
      }
    }
  }

  test("UniformIntegralDraw rejects the words that would bias the 64-bit draw") {
    // A range of 3 * 2^62 + 1 values rejects about a quarter of all words. Without the rejection,
    // the lowest 2^62 values would each be twice as likely and would take about half the results.
    val values = drawRows(integralDraw(Long.MinValue, 1L << 62), 10000)
    val share = values.count(_.asInstanceOf[Long] < Long.MinValue + (1L << 62)) / 10000.0
    assert(share > 0.31 && share < 0.36, s"share $share is not about 1/3")
  }

  test("UniformIntegralDraw takes the 64-bit draw above 2^32 values") {
    // In a range of 3 * 2^51 values, one 53-bit DOUBLE draw makes one integer in three twice as
    // likely as its neighbors.
    val values = drawRows(integralDraw(0L, 3L * (1L << 51) - 1), 10000)
    val shares = values.groupBy(v => Math.floorMod(v.asInstanceOf[Long], 3L))
      .map { case (k, vs) => k -> vs.size / 10000.0 }
    assert(shares.size == 3 && shares.values.forall(s => s > 0.31 && s < 0.36), s"$shares")
  }

  test("UniformIntegralDraw renews and shifts its seed") {
    val draw = integralDraw(0L, 1000000L)
    val base = drawRows(draw, 20)
    assert(drawRows(draw.withNewSeed(7L), 20) === drawRows(integralDraw(0L, 1000000L, 7L), 20))
    assert(drawRows(draw.withShiftedSeed(5L), 20) === drawRows(integralDraw(0L, 1000000L, 5L), 20))
    assert(drawRows(draw.withShiftedSeed(5L), 20) !== base)
  }

  test("UniformIntegralDraw returns NULL for a NULL bound") {
    Seq(
      UniformIntegralDraw(Literal.create(null, LongType), Literal(1L), Literal(0L)),
      UniformIntegralDraw(Literal(1L), Literal.create(null, LongType), Literal(0L))).foreach { e =>
      assert(drawRows(e, 3) === Seq(null, null, null))
    }
  }

  test("Uniform does not evaluate its bounds when it builds the replacement") {
    // The optimizer gives current-time and special datetime bounds their query values after the
    // replacement is built, and folds stateful bounds on a fresh copy.
    val now = UnixMicros(CurrentTimestamp())
    val mapSize = Size(CreateMap(Seq(Literal(1), Literal(2), Literal(3), Literal(4))))
    Seq(
      Uniform(now, now, Literal(0)) -> classOf[CurrentTimestamp],
      Uniform(Literal(0), mapSize, Literal(0)) -> classOf[CreateMap]).foreach {
      case (uniform, boundClass) =>
        assert(uniform.replacement.exists(boundClass.isInstance), uniform.replacement)
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
    assert(legacy.canonicalized != closed.canonicalized)
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
