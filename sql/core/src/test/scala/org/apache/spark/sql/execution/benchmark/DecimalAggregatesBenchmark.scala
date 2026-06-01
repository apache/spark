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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal

/**
 * Benchmark for the DecimalAggregates widened-cast peel optimizer rule
 * (both SUM and AVG arms).
 *
 * Each case is a three-way comparison on the same `DECIMAL(p, s)` input:
 *   1. `native`              -- query writes `SUM(x)` / `AVG(x)` directly,
 *                               hitting the existing fast path (rule on).
 *   2. `widened, peel off`   -- query writes `SUM(CAST(x AS DECIMAL(p', s)))`
 *                               with `DecimalAggregates` excluded; the cast
 *                               defeats the existing fast path, so the
 *                               baseline `CheckOverflow` path runs.
 *   3. `widened, peel on`    -- same widened query with the rule enabled;
 *                               the new peel arm strips the cast and
 *                               restores the fast path.
 *
 * Reviewer story:
 *   - `native` vs `widened, peel off` -- shows the widening cast really
 *     evicts user-visible work onto the slow path (motivation).
 *   - `widened, peel off` vs `widened, peel on` -- shows the peel rule
 *     recovers the lost performance (rule benefit).
 *   - `widened, peel on` vs `native` -- shows the peel makes the cast
 *     effectively free (rule correctness echo of the numerical-equivalence
 *     PBT in `DataFrameAggregateSuite`).
 *
 * Sections:
 *   A -- Aggregate SUM widened-cast sweep (`p + 10 <= MAX_LONG_DIGITS`,
 *        any `pPrime > p` up to 38).
 *   B -- Aggregate AVG widened-cast sweep (`pPrime + 4 <= MAX_DOUBLE_DIGITS`
 *        so the rule fires only inside the existing AVG Double-regime
 *        envelope; wider casts stay on the Decimal-exact path).
 *   C -- Aggregate MIN widened-cast sweep (no regime guard: the MIN arm
 *        peels for any `pPrime >= p` same-scale widening; expected
 *        BigInteger-domain outer (`pPrime > MAX_LONG_DIGITS = 18`) is the
 *        main saving path per design).
 *   D -- Aggregate MAX widened-cast sweep (mirrors C).
 *
 * NOTE on Window arm: the optimizer does not extend widened-Cast peel to
 * the Window arm (see DecimalAggregates rule comment) because the analyzer
 * hoists the Cast into a child Project, so a Window microbenchmark would
 * not exercise this rule. A Window benchmark belongs with a future
 * plan-layer rewrite, not here.
 *
 * Case design:
 *   - Section A pairs a `p+1` boundary widening with a `p+10`-class wider
 *     cast (A2 mirrors the TPC-DS q18 inner-widened-decimal shape), so
 *     reviewers see whether widening magnitude matters.
 *   - Section B pairs a `p+1` boundary widening with the `pPrime <= 11`
 *     upper bound, the widest cast the AVG arm will accept under the
 *     semantics-preserving guard.
 *
 * Args: aN (Section A/B row count), iters, apl
 * (`spark.sql.decimalOperations.allowPrecisionLoss`; default true).
 * Defaults committed for GHA: aN=10000000, iters=5, apl=true.
 *
 * To run this benchmark locally (pre-GHA smoke):
 * {{{
 *   build/sbt "sql/Test/runMain \
 *     org.apache.spark.sql.execution.benchmark.DecimalAggregatesBenchmark \
 *     10000000 5"
 * }}}
 *
 * To regenerate committed baselines (via `benchmark.yml` GHA workflow):
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *     "sql/Test/runMain org.apache.spark.sql.execution.benchmark.DecimalAggregatesBenchmark"
 * }}}
 *
 * Committed results:
 *   `sql/core/benchmarks/DecimalAggregatesBenchmark-results.txt` (JDK 17).
 *   `sql/core/benchmarks/DecimalAggregatesBenchmark-jdk21-results.txt`.
 *   `sql/core/benchmarks/DecimalAggregatesBenchmark-jdk25-results.txt`.
 */
object DecimalAggregatesBenchmark extends SqlBasedBenchmark {

  /**
   * Aggregate SUM cases: (label, p, s, widened p').
   *
   * All `p + 10 <= 18` so the *native* `SUM(x)` query hits the existing
   * SUM Long fast path -- providing a meaningful baseline for the
   * peel-on leg. Coverage: `p+1` boundary widening (A1, A3) plus a
   * `p+10`-class wider cast representative of production shapes (A2,
   * A4; A2 mirrors the TPC-DS q18 inner-widened-decimal shape).
   */
  private val SumAggCases: Seq[(String, Int, Int, Int)] = Seq(
    ("A1 p=7 s=2 p'=8", 7, 2, 8),    // p+1 boundary
    ("A2 p=7 s=2 p'=17", 7, 2, 17),  // p+10, canonical TPC-DS q18 shape
    ("A3 p=5 s=0 p'=6", 5, 0, 6),    // p+1 boundary, zero scale
    ("A4 p=5 s=0 p'=15", 5, 0, 15)   // p+10, zero scale
  )

  /**
   * Aggregate AVG cases: (label, p, s, widened p').
   *
   * All `pPrime + 4 <= MAX_DOUBLE_DIGITS = 15`, i.e. `pPrime <= 11` -- the
   * AVG peel arm only fires inside the existing Double-regime envelope, so
   * the un-rewritten Decimal-exact path is preserved for wider casts (see
   * SPARK-56983).
   */
  private val AvgAggCases: Seq[(String, Int, Int, Int)] = Seq(
    ("B1 p=7 s=2 p'=8", 7, 2, 8),    // p+1 boundary
    ("B2 p=7 s=2 p'=11", 7, 2, 11),  // pPrime upper bound
    ("B3 p=5 s=0 p'=6", 5, 0, 6),    // p+1 boundary, zero scale
    ("B4 p=5 s=0 p'=11", 5, 0, 11)   // pPrime upper bound, zero scale
  )

  /**
   * Aggregate MIN cases: (label, p, s, widened p').
   *
   * MIN/MAX widened-cast peel has NO regime guard -- it peels for any
   * `pPrime >= p` same-scale widening (`Optimizer.scala WidenedDecimalChild`).
   * The main saving path is `pPrime > MAX_LONG_DIGITS = 18`, where the
   * unrewritten plan would create a BigInteger-domain outer Decimal for
   * every row, while the rewritten plan compares the inner Long-domain
   * values and casts only the single aggregate result.
   *
   * Coverage:
   *   - C1: inner Long, outer Long  -- weakest saving (sibling-compatible
   *         baseline; the row-cast still goes through `changePrecision`
   *         but stays in Long).
   *   - C2: inner Long, outer BigInteger -- the main saving regime.
   *   - C3: inner at Long boundary (p=18), outer BigInteger -- isolates
   *         the outer-domain cost.
   *   - C4: inner Long, outer at MAX_PRECISION=38 -- deepest BigInteger.
   */
  private val MinAggCases: Seq[(String, Int, Int, Int)] = Seq(
    ("C1 p=10 s=2 p'=18", 10, 2, 18), // inner Long, outer Long (boundary)
    ("C2 p=10 s=2 p'=28", 10, 2, 28), // inner Long, outer BigInteger (main saving)
    ("C3 p=18 s=2 p'=28", 18, 2, 28), // inner Long max, outer BigInteger
    ("C4 p=10 s=2 p'=38", 10, 2, 38)  // inner Long, outer MAX_PRECISION
  )

  /** Aggregate MAX cases: mirror C above. */
  private val MaxAggCases: Seq[(String, Int, Int, Int)] = Seq(
    ("D1 p=10 s=2 p'=18", 10, 2, 18),
    ("D2 p=10 s=2 p'=28", 10, 2, 28),
    ("D3 p=18 s=2 p'=28", 18, 2, 28),
    ("D4 p=10 s=2 p'=38", 10, 2, 38)
  )

  /** Clamp generator to `10^(p-s) - 1` so rand() * bound fits `DECIMAL(p, s)`. */
  private def unscaledBound(p: Int, s: Int): Long = {
    require(p - s >= 0, s"p=$p s=$s p-s must be non-negative")
    math.pow(10.0, (p - s).toDouble).toLong - 1L
  }

  private def setupAggTable(spark: org.apache.spark.sql.SparkSession,
      n: Long, p: Int, s: Int): Unit = {
    val bound = unscaledBound(p, s)
    spark.range(n)
      .selectExpr(s"cast(rand(42) * $bound as decimal($p, $s)) as x")
      .coalesce(1)
      .createOrReplaceTempView("t")
  }

  private val ExcludedRulesKey: String = SQLConf.OPTIMIZER_EXCLUDED_RULES.key
  private val DecimalAggregatesRule: String =
    "org.apache.spark.sql.catalyst.optimizer.DecimalAggregates"

  /**
   * Run a single three-way comparison: native (no cast, rule on),
   * widened with rule off (baseline slow path), widened with rule on
   * (peel restores fast path). `apl` is held identical across all three
   * legs so any delta is attributable to (a) the widening cast and
   * (b) the peel rule respectively.
   */
  private def runThreeWay(label: String, n: Long, nativeSql: String,
      widenedSql: String, iters: Int, apl: String): Unit = {
    val bench = new Benchmark(label, n, output = output)
    bench.addCase("native (no cast, rule on)", numIters = iters) { _ =>
      withSQLConf(SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> apl) {
        spark.sql(nativeSql).noop()
      }
    }
    bench.addCase("widened cast, peel off", numIters = iters) { _ =>
      withSQLConf(
          ExcludedRulesKey -> DecimalAggregatesRule,
          SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> apl) {
        spark.sql(widenedSql).noop()
      }
    }
    bench.addCase("widened cast, peel on", numIters = iters) { _ =>
      withSQLConf(SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> apl) {
        spark.sql(widenedSql).noop()
      }
    }
    bench.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val aN: Long = if (mainArgs.length > 0) mainArgs(0).toLong else 10L * 1000L * 1000L
    val iters: Int = if (mainArgs.length > 1) mainArgs(1).toInt else 5
    val apl: String = if (mainArgs.length > 2) mainArgs(2) else "true"

    require(Decimal.MAX_LONG_DIGITS == 18,
      s"Decimal.MAX_LONG_DIGITS drift: expected 18 got ${Decimal.MAX_LONG_DIGITS}")

    // Section A -- Aggregate SUM widened-cast.
    runBenchmark("DecimalAggregates SUM widened-cast peel (Aggregate)") {
      SumAggCases.foreach { case (label, p, s, pPrime) =>
        require(pPrime > p, s"$label: p'=$pPrime must exceed p=$p")
        require(p + 10 <= 18,
          s"$label: p=$p violates SUM Long fast path guard p+10<=MAX_LONG_DIGITS=18; " +
            s"native baseline would not be meaningful")
        setupAggTable(spark, aN, p, s)
        runThreeWay(label, aN,
          nativeSql = "select sum(x) from t",
          widenedSql = s"select sum(cast(x as decimal($pPrime, $s))) from t",
          iters, apl)
      }
    }

    // Section B -- Aggregate AVG widened-cast.
    runBenchmark("DecimalAggregates AVG widened-cast peel (Aggregate)") {
      AvgAggCases.foreach { case (label, p, s, pPrime) =>
        require(pPrime > p, s"$label: p'=$pPrime must exceed p=$p")
        require(pPrime + 4 <= 15,
          s"$label: p'=$pPrime violates AVG fast-path guard " +
            s"pPrime+4<=MAX_DOUBLE_DIGITS=15; rule would not fire")
        setupAggTable(spark, aN, p, s)
        runThreeWay(label, aN,
          nativeSql = "select avg(x) from t",
          widenedSql = s"select avg(cast(x as decimal($pPrime, $s))) from t",
          iters, apl)
      }
    }

    // Section C -- Aggregate MIN widened-cast.
    runBenchmark("DecimalAggregates MIN widened-cast peel (Aggregate)") {
      MinAggCases.foreach { case (label, p, s, pPrime) =>
        require(pPrime >= p, s"$label: p'=$pPrime must be >= p=$p (widening)")
        require(pPrime <= 38, s"$label: p'=$pPrime exceeds MAX_PRECISION=38")
        setupAggTable(spark, aN, p, s)
        runThreeWay(label, aN,
          nativeSql = "select min(x) from t",
          widenedSql = s"select min(cast(x as decimal($pPrime, $s))) from t",
          iters, apl)
      }
    }

    // Section D -- Aggregate MAX widened-cast.
    runBenchmark("DecimalAggregates MAX widened-cast peel (Aggregate)") {
      MaxAggCases.foreach { case (label, p, s, pPrime) =>
        require(pPrime >= p, s"$label: p'=$pPrime must be >= p=$p (widening)")
        require(pPrime <= 38, s"$label: p'=$pPrime exceeds MAX_PRECISION=38")
        setupAggTable(spark, aN, p, s)
        runThreeWay(label, aN,
          nativeSql = "select max(x) from t",
          widenedSql = s"select max(cast(x as decimal($pPrime, $s))) from t",
          iters, apl)
      }
    }
  }
}
