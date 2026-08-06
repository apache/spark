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

package org.apache.spark.sql.execution.exchange

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{SparkSession, TPCDSSchema}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/**
 * WIP runner (SPARK-57399 local-repartition v2) with two modes:
 *
 *   - `verify`: runs each TPC-DS query twice in ONE session -- pipelined rule off (regular
 *     shuffle baseline) then on -- and compares full result sets. Uses local[128] purely to
 *     clear the gang slot check by oversubscribing the cores; timing meaningless.
 *   - `bench`: same off/on structure but TIMED (1 warm-up + best of 5), on local[N =
 *     physical cores] so the pipelined group does not oversubscribe and the comparison is
 *     fair. A query whose whole-group demand exceeds N is REJECTED by gang admission and
 *     reported as such -- itself an honest datapoint about the slot ceiling.
 *
 * Usage (data: dsdgen .dat files; parquet is built on first run):
 * {{{
 *   build/sbt "sql/Test/runMain org.apache.spark.sql.execution.exchange.TPCDSPipelinedRunner
 *     <verify|bench> <datDir> <parquetDir> q3 q42 q55 ..."
 * }}}
 */
object TPCDSPipelinedRunner extends TPCDSSchema with AdaptiveSparkPlanHelper {

  private def buildParquet(spark: SparkSession, datDir: String, parquetDir: String): Unit = {
    tableColumns.foreach { case (table, schema) =>
      val out = s"$parquetDir/$table"
      if (!Files.exists(Paths.get(out))) {
        val dat = s"$datDir/$table.dat"
        require(Files.exists(Paths.get(dat)), s"missing $dat")
        // dsdgen lines end with a trailing '|': parse with one extra dummy column so the
        // row width matches, then drop it.
        spark.read
          .option("delimiter", "|")
          .schema(schema.replace("\n", " ") + ", `_dsdgen_trailing` STRING")
          .csv(dat)
          .drop("_dsdgen_trailing")
          .write.mode("overwrite").parquet(out)
        // scalastyle:off println
        println(s"[tpcds] built parquet for $table")
        // scalastyle:on println
      }
    }
  }

  private def loadQuery(name: String): String = {
    val in = getClass.getResourceAsStream(s"/tpcds/$name.sql")
    require(in != null, s"query resource /tpcds/$name.sql not found")
    try new String(in.readAllBytes(), StandardCharsets.UTF_8) finally in.close()
  }

  private def exchangeSummary(plan: SparkPlan): (Int, Int, Boolean, String) = {
    val exchanges = plan.collect { case s: ShuffleExchangeExec => s }
    val reused = plan.exists(_.isInstanceOf[ReusedExchangeExec])
    val kinds = exchanges.map(_.outputPartitioning.getClass.getSimpleName
      .stripSuffix("$")).mkString(",")
    (exchanges.length, exchanges.count(_.pipelined), reused, kinds)
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 4, "args: <verify|bench> <datDir> <parquetDir> <query> [query...]")
    val mode = args(0)
    require(mode == "verify" || mode == "bench" || mode == "benchwide",
      s"unknown mode $mode")
    val (datDir, parquetDir, queries) = (args(1), args(2), args.drop(3).toSeq)
    // bench: gang fits the physical cores (squeezed scans, no oversubscription).
    // benchwide: near-natural scan parallelism; local[2*cores] admits the gang and the
    // pipelined group oversubscribes the cores instead (the regular baseline is unaffected:
    // its per-stage width still fits the physical cores). Which cost is smaller --
    // squeezed scans or thread contention -- is an empirical question; run both.
    val master = mode match {
      case "bench" => s"local[${Runtime.getRuntime.availableProcessors()}]"
      case "benchwide" => s"local[${2 * Runtime.getRuntime.availableProcessors()}]"
      case _ => "local[128]"
    }

    val spark = SparkSession.builder()
      .master(master)
      .appName("tpcds-pipelined-correctness")
      .config("spark.shuffle.manager.incremental",
        "org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.speculation", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.memory", "6g")
      // TPCDSSchema declares CHAR/VARCHAR columns; treat them as STRING like the TPCDS
      // suites do.
      .config("spark.sql.legacy.charVarcharAsString", "true")
      // Fewer, larger scan partitions keep the gang's whole-group slot demand low (the big
      // SF=1 fact scans otherwise contribute ~64 producer partitions each). In bench mode
      // the scan must be squeezed further: FilePartition planning otherwise targets
      // defaultParallelism (16) leaf partitions, and 16 (scan) + 4 (shuffle) already
      // exceeds the 16 honest slots -- every query would be rejected. ~32m over the 190MB
      // store_sales gives ~6 scan partitions, so a 3-stage group is 6 + 4 + 4 = 14 <= 16.
      // BOTH modes in the session share these settings, so the comparison stays fair --
      // but note the whole benchmark therefore runs at reduced scan parallelism, which is
      // itself the honest cost of the slot ceiling on a single box. (64m over the ~24
      // small store_sales files packs to ~5 scan partitions once per-file open cost is
      // counted; 32m still left 13, and 13 + 4 = 17 > 16 rejected everything.)
      .config("spark.sql.files.maxPartitionBytes", mode match {
        case "bench" => "64m"      // ~5 store_sales scan partitions: gang fits 16 slots
        case "benchwide" => "32m"  // ~13 scan partitions: near the box's natural 16
        case _ => "512m"
      })
      .config("spark.sql.files.minPartitionNum", "1")
      .getOrCreate()
    try {
      buildParquet(spark, datDir, parquetDir)
      tableColumns.keys.foreach { table =>
        spark.read.parquet(s"$parquetDir/$table").createOrReplaceTempView(table)
      }

      if (mode == "verify") {
        queries.foreach(q => verifyOne(spark, q))
      } else {
        // scalastyle:off println
        println(s"[tpcds] bench on $master, 1 warm-up + best of 5 per mode")
        // scalastyle:on println
        queries.foreach(q => benchOne(spark, q))
      }
    } finally {
      spark.stop()
    }
  }

  private def verifyOne(spark: SparkSession, q: String): Unit = {
    val sql = loadQuery(q)
    // scalastyle:off println
    try {
      spark.conf.set("spark.sql.pipelinedShuffle.enabled", "false")
      val baseline = spark.sql(sql).collect()

      spark.conf.set("spark.sql.pipelinedShuffle.enabled", "true")
      val pipeDf = spark.sql(sql)
      val (nEx, nPipe, reused, kinds) = exchangeSummary(pipeDf.queryExecution.executedPlan)
      val result = pipeDf.collect()

      val same = baseline.map(_.toString).sorted.sameElements(
        result.map(_.toString).sorted)
      val status = if (same) "OK " else "MISMATCH"
      println(s"[tpcds] $q: $status rows=${result.length} " +
        s"exchanges=$nEx pipelined=$nPipe reusedExchange=$reused kinds=$kinds")
      if (!same) {
        println(s"[tpcds] $q baseline=${baseline.length} rows, pipelined=${result.length}")
      }
    } catch {
      case e: Exception =>
        println(s"[tpcds] $q: FAILED ${e.getClass.getSimpleName}: " +
          s"${Option(e.getMessage).getOrElse("").linesIterator.take(2).mkString(" / ")}")
    }
    // scalastyle:on println
  }

  private def bestMs(spark: SparkSession, sql: String): Long = {
    spark.sql(sql).collect() // warm up
    var best = Long.MaxValue
    var i = 0
    while (i < 5) {
      val t0 = System.nanoTime()
      spark.sql(sql).collect()
      best = math.min(best, (System.nanoTime() - t0) / 1000000L)
      i += 1
    }
    best
  }

  private def benchOne(spark: SparkSession, q: String): Unit = {
    val sql = loadQuery(q)
    // scalastyle:off println
    try {
      spark.conf.set("spark.sql.pipelinedShuffle.enabled", "false")
      val regular = bestMs(spark, sql)

      spark.conf.set("spark.sql.pipelinedShuffle.enabled", "true")
      val (_, nPipe, _, kinds) = exchangeSummary(
        spark.sql(sql).queryExecution.executedPlan)
      if (nPipe == 0) {
        println(f"[tpcds] $q%-5s regular=${regular}%5dms  pipelined=  skip (rule left plan " +
          s"regular, kinds=$kinds)")
      } else {
        val pipe = bestMs(spark, sql)
        val speedup = regular.toDouble / pipe
        println(f"[tpcds] $q%-5s regular=${regular}%5dms  pipelined=${pipe}%5dms  " +
          f"speedup=${speedup}%.2fx  nPipelined=$nPipe kinds=$kinds")
      }
    } catch {
      case e: Exception =>
        println(s"[tpcds] $q: FAILED ${e.getClass.getSimpleName}: " +
          s"${Option(e.getMessage).getOrElse("").linesIterator.take(2).mkString(" / ")}")
    }
    // scalastyle:on println
  }
}
