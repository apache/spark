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

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config.ConfigBindingPolicy
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.SQLFunction
import org.apache.spark.sql.catalyst.plans.logical.View
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

/**
 * Measures view and SQL function body conf construction with retained-config memoization on/off.
 * Each reported row is one body conf build. Workload sizes exclude four fixed control settings.
 * Retained entries are split between SESSION-bound and catalog keys; the rest are split between
 * PERSISTED-bound and unknown keys. Config registration and fixture setup are outside timing.
 *
 * The flag-off baseline still uses ModificationCountingMap. Measuring the cost of replacing the
 * settings map requires running the same builder workload on the parent revision as well.
 *
 * To run:
 * {{{
 *   build/sbt "catalyst/Test/runMain <this class>"
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain <this class>"
 * }}}
 * Optional arguments select benchmark names containing every argument, e.g. `view warm`.
 */
object RetainedResolutionConfigsBenchmark extends BenchmarkBase {
  private val memoizeKey = "spark.sql.analyzer.memoizeRetainedResolutionConfigs.enabled"

  // Publish the whole result so the JVM cannot discard the destination conf or its entries.
  @volatile private var result: SQLConf = _

  private case class Scenario(name: String, mutationInterval: Int = 0, cold: Boolean = false)

  private val scenarios = Seq(
    Scenario("warm"),
    Scenario("mutate every 16 builds", mutationInterval = 16),
    Scenario("mutate every build", mutationInterval = 1),
    Scenario("cold", cold = true))

  private class Fixture(val size: Int, val retainedPercent: Int) {
    private val prefix = s"spark.sql.retainedConfigsBenchmark.s$size.r$retainedPercent"
    private val retainedCount = size * retainedPercent / 100
    val changedKey: String = s"$prefix.session.0"
    private val persistedKey = s"$prefix.persisted.$retainedCount"

    private def register(key: String, policy: ConfigBindingPolicy.Value): String = {
      SQLConf.buildConf(key).internal().withBindingPolicy(policy)
        .stringConf.createWithDefault("initial").key
    }

    private val settings = (0 until size).map { i =>
      val key = if (i < retainedCount / 2) {
        register(s"$prefix.session.$i", ConfigBindingPolicy.SESSION)
      } else if (i < retainedCount) {
        s"spark.sql.catalog.retained_configs_benchmark.option$i"
      } else if (i < retainedCount + (size - retainedCount) / 2) {
        register(s"$prefix.persisted.$i", ConfigBindingPolicy.PERSISTED)
      } else {
        s"$prefix.unknown.$i"
      }
      key -> "initial"
    }

    private val captured = Map(
      changedKey -> "captured session value",
      persistedKey -> "captured persisted value",
      s"$prefix.captured" -> "captured unknown value",
      SQLConf.ANSI_ENABLED.key -> "true")

    private val function = SQLFunction(
      name = FunctionIdentifier("retained_configs_benchmark"),
      inputParam = None,
      returnType = Left(IntegerType),
      exprText = Some("1"),
      queryText = None,
      comment = None,
      collation = None,
      deterministic = Some(true),
      containsSQL = Some(false),
      isTableFunc = false,
      properties = captured.map { case (key, value) => s"sqlConfig.$key" -> value })

    val builders: Seq[(String, SQLConf => SQLConf)] = Seq(
      "view" -> { (source: SQLConf) =>
        SQLConf.withExistingConf(source) {
          View.effectiveSQLConf(captured, isTempView = false, createSparkVersion = "4.4.0")
        }
      },
      "function" -> { (source: SQLConf) =>
        SQLConf.withExistingConf(source) {
          Analyzer.buildSQLFunctionConf(
            function, applySessionOverrides = true, alwaysSetAnsiValue = false)
        }
      })

    def newConf(memoize: Boolean): SQLConf = {
      val source = new SQLConf
      (settings ++ Seq(
        memoizeKey -> memoize.toString,
        SQLConf.ASSUME_ANSI_FALSE_IF_NOT_PERSISTED.key -> "true",
        SQLConf.APPLY_SESSION_CONF_OVERRIDES_TO_FUNCTION_RESOLUTION.key -> "true",
        SQLConf.USE_CURRENT_SQL_CONFIGS_FOR_VIEW.key -> "false")).foreach { case (key, value) =>
        source.setConfString(key, value)
      }
      source
    }

    def validate(build: SQLConf => SQLConf): Unit = {
      val disabled = newConf(memoize = false)
      val enabled = newConf(memoize = true)
      Seq("initial", "changed", "initial").foreach { value =>
        disabled.setConfString(changedKey, value)
        enabled.setConfString(changedKey, value)
        val expected = build(disabled).getAllConfs - memoizeKey
        assert(expected(changedKey) == value)
        assert(expected(persistedKey) == captured(persistedKey))
        // Check both the first build after a change and subsequent cache hits.
        assert(build(enabled).getAllConfs - memoizeKey == expected)
        assert(build(enabled).getAllConfs - memoizeKey == expected)
      }
    }
  }

  private def benchmark(
      name: String,
      fixture: Fixture,
      build: SQLConf => SQLConf,
      scenario: Scenario): Unit = {
    // Bound the cold case's source pool so setup does not retain thousands of large confs.
    val builds = if (scenario.cold) 256 else 4096
    val benchmark = new Benchmark(name, builds, minNumIters = 5, output = output)
    val mutationValues = Array("changed", "initial")
    Seq(false, true).foreach { memoize =>
      val source = fixture.newConf(memoize)
      if (!scenario.cold) {
        result = build(source)
      }
      val label = if (memoize) "memo enabled" else "memo disabled"
      benchmark.addTimerCase(label) { timer =>
        // Recreate cold sources for every iteration, including JIT warm-up iterations.
        val sources = if (scenario.cold) {
          Array.fill(builds)(fixture.newConf(memoize))
        } else {
          Array(source)
        }
        var i = 0
        timer.startTiming()
        while (i < builds) {
          val active = if (scenario.cold) sources(i) else sources(0)
          if (scenario.mutationInterval > 0 && i % scenario.mutationInterval == 0) {
            active.setConfString(
              fixture.changedKey, mutationValues((i / scenario.mutationInterval) % 2))
          }
          result = build(active)
          i += 1
        }
        timer.stopTiming()
        assert(result != null)
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    var selected = 0
    for ((size, retainedPercent) <- Seq((32, 25), (128, 25), (512, 25), (128, 75))) {
      val fixture = new Fixture(size, retainedPercent)
      for ((builderName, build) <- fixture.builders) {
        fixture.validate(build)
        for (scenario <- scenarios) {
          val name = s"$builderName: $size settings, $retainedPercent% retained, ${scenario.name}"
          if (mainArgs.forall(name.contains)) {
            selected += 1
            runBenchmark(name) {
              benchmark(name, fixture, build, scenario)
            }
          }
        }
      }
    }
    require(selected > 0, s"No benchmark names matched: ${mainArgs.mkString(" ")}")
  }
}
