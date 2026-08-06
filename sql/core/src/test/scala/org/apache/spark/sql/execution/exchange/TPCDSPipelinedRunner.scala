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
 * WIP correctness runner (SPARK-57399 local-repartition v2): runs selected TPC-DS queries
 * twice in ONE session -- pipelined rule off (regular shuffle baseline) then on -- and
 * compares full result sets. Correctness only: local[64] clears the gang slot check by
 * oversubscribing 16 cores, which makes timing meaningless by design.
 *
 * Usage (data: dsdgen .dat files; parquet is built on first run):
 * {{{
 *   build/sbt "sql/Test/runMain org.apache.spark.sql.execution.exchange.TPCDSPipelinedRunner
 *     <datDir> <parquetDir> q3 q42 q55 ..."
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
    require(args.length >= 3, "args: <datDir> <parquetDir> <query> [query...]")
    val (datDir, parquetDir, queries) = (args(0), args(1), args.drop(2).toSeq)

    val spark = SparkSession.builder()
      .master("local[128]")
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
      // SF=1 fact scans otherwise contribute ~64 producer partitions each).
      .config("spark.sql.files.maxPartitionBytes", "512m")
      .getOrCreate()
    try {
      buildParquet(spark, datDir, parquetDir)
      tableColumns.keys.foreach { table =>
        spark.read.parquet(s"$parquetDir/$table").createOrReplaceTempView(table)
      }

      queries.foreach { q =>
        val sql = loadQuery(q)
        // scalastyle:off println
        try {
          spark.conf.set("spark.sql.pipelinedShuffle.enabled", "false")
          val baselineDf = spark.sql(sql)
          val baseline = baselineDf.collect()

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
    } finally {
      spark.stop()
    }
  }
}
