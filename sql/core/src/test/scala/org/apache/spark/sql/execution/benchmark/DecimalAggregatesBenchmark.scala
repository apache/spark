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
 *   A -- Aggregate SUM widened-cast sweep over (p, s, p') cases.
 *   B -- Aggregate AVG widened-cast sweep (p <= 7 per
 *        AVG_PEEL_MAX_INNER_PRECISION).
 *
 * NOTE on Window arm: the optimizer does not extend widened-Cast peel to
 * the Window arm (see DecimalAggregates rule comment) because the analyzer
 * hoists the Cast into a child Project, so a Window microbenchmark would
 * not exercise this rule. A Window benchmark belongs with a future
 * plan-layer rewrite, not here.
 *
 * Case design (`p+1` boundary vs `p+10`-class wider) deliberately includes
 * both the minimum widening (one extra digit, e.g. `dec(7,2) -> dec(8,2)`)
 * and a production-canonical wider one (e.g. `dec(7,2) -> dec(17,2)` is the
 * inner-widened-decimal shape in TPC-DS q18) so reviewers see whether
 * widening magnitude matters and whether the canonical shape behaves like
 * the boundary one.
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
   * All `p <= 7` per the conservative `AVG_PEEL_MAX_INNER_PRECISION = 7`
   * guard (see design doc 0001 rev 7 S7.1 -- strict-subset narrowing so
   * SPARK-37024 Double-regime exposure is NOT amplified by this rule).
   * Same `p+1` / `p+10` split as Section A. B2 mirrors the canonical
   * TPC-DS q18 AVG shape.
   */
  private val AvgAggCases: Seq[(String, Int, Int, Int)] = Seq(
    ("B1 p=7 s=2 p'=8", 7, 2, 8),    // p+1 boundary
    ("B2 p=7 s=2 p'=12", 7, 2, 12),  // canonical TPC-DS q18 AVG shape
    ("B3 p=5 s=0 p'=6", 5, 0, 6),    // p+1 boundary, zero scale
    ("B4 p=5 s=0 p'=15", 5, 0, 15)   // p+10, zero scale
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
        require(p <= 7,
          s"$label: p=$p violates conservative AVG_PEEL_MAX_INNER_PRECISION=7 guard")
        setupAggTable(spark, aN, p, s)
        runThreeWay(label, aN,
          nativeSql = "select avg(x) from t",
          widenedSql = s"select avg(cast(x as decimal($pPrime, $s))) from t",
          iters, apl)
      }
    }
  }
}
