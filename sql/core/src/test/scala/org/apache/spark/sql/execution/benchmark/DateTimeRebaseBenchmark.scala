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

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset}

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.util.DateTimeConstants.SECONDS_PER_DAY
import org.apache.spark.sql.internal.SQLConf

/**
 * Synthetic benchmark for rebasing of date and timestamp in read/write.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/DateTimeRebaseBenchmark-results.txt".
 * }}}
 */
object DateTimeRebaseBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  private def genTs(cardinality: Int, start: LocalDateTime, end: LocalDateTime): DataFrame = {
    val startSec = start.toEpochSecond(ZoneOffset.UTC)
    val endSec = end.toEpochSecond(ZoneOffset.UTC)
    spark.range(0, cardinality, 1, 1)
      .select((($"id" % (endSec - startSec)) + startSec).as("seconds"))
      .select($"seconds".cast("timestamp").as("ts"))
  }

  private def genTsAfter1582(cardinality: Int): DataFrame = {
    val start = LocalDateTime.of(1582, 10, 15, 0, 0, 0)
    val end = LocalDateTime.of(3000, 1, 1, 0, 0, 0)
    genTs(cardinality, start, end)
  }

  private def genTsBefore1582(cardinality: Int): DataFrame = {
    val start = LocalDateTime.of(10, 1, 1, 0, 0, 0)
    val end = LocalDateTime.of(1580, 1, 1, 0, 0, 0)
    genTs(cardinality, start, end)
  }

  private def genDate(cardinality: Int, start: LocalDate, end: LocalDate): DataFrame = {
    val startSec = LocalDateTime.of(start, LocalTime.MIDNIGHT).toEpochSecond(ZoneOffset.UTC)
    val endSec = LocalDateTime.of(end, LocalTime.MIDNIGHT).toEpochSecond(ZoneOffset.UTC)
    spark.range(0, cardinality * SECONDS_PER_DAY, SECONDS_PER_DAY, 1)
      .select((($"id" % (endSec - startSec)) + startSec).as("seconds"))
      .select($"seconds".cast("timestamp").as("ts"))
      .select($"ts".cast("date").as("date"))
  }

  private def genDateAfter1582(cardinality: Int): DataFrame = {
    val start = LocalDate.of(1582, 10, 15)
    val end = LocalDate.of(3000, 1, 1)
    genDate(cardinality, start, end)
  }

  private def genDateBefore1582(cardinality: Int): DataFrame = {
    val start = LocalDate.of(10, 1, 1)
    val end = LocalDate.of(1580, 1, 1)
    genDate(cardinality, start, end)
  }

  private def genDF(cardinality: Int, dateTime: String, after1582: Boolean): DataFrame = {
    (dateTime, after1582) match {
      case ("date", true) => genDateAfter1582(cardinality)
      case ("date", false) => genDateBefore1582(cardinality)
      case ("timestamp", true) => genTsAfter1582(cardinality)
      case ("timestamp", false) => genTsBefore1582(cardinality)
      case _ => throw new IllegalArgumentException(
        s"cardinality = $cardinality dateTime = $dateTime after1582 = $after1582")
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    withTempPath { path =>
      runBenchmark("Rebasing dates/timestamps in Parquet datasource") {
        val rowsNum = 100000000
        Seq("date", "timestamp").foreach { dateTime =>
          val benchmark = new Benchmark(s"Save ${dateTime}s to parquet", rowsNum, output = output)
          benchmark.addCase("after 1582, noop", 1) { _ =>
            genDF(rowsNum, dateTime, after1582 = true).noop()
          }
          benchmark.addCase("before 1582, noop", 1) { _ =>
            genDF(rowsNum, dateTime, after1582 = false).noop()
          }

          def save(after1582: Boolean, rebase: Boolean): Unit = {
            val period = if (after1582) "after" else "before"
            val rebaseFlag = if (rebase) "on" else "off"
            val caseName = s"$period 1582, rebase $rebaseFlag"
            benchmark.addCase(caseName, 1) { _ =>
              withSQLConf(SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> rebase.toString) {
                val df = genDF(rowsNum, dateTime, after1582)
                val pathToWrite = path.getAbsolutePath + s"/${dateTime}_${period}_1582_$rebaseFlag"
                df.write
                  .mode("overwrite")
                  .format("parquet")
                  .save(pathToWrite)
              }
            }
          }

          Seq(true, false).foreach { after1582 =>
            Seq(false, true).foreach { rebase =>
              save(after1582, rebase)
            }
          }
          benchmark.run()

          val benchmark2 = new Benchmark(
            s"Load ${dateTime}s from parquet", rowsNum, output = output)

          def load(after1582: Boolean, vec: Boolean, rebase: Boolean): Unit = {
            val period = if (after1582) "after" else "before"
            val rebaseFlag = if (rebase) "on" else "off"
            val vecFlag = if (vec) "on" else "off"
            val caseName = s"$period 1582, vec $vecFlag, rebase $rebaseFlag"
            benchmark2.addCase(caseName, 3) { _ =>
              withSQLConf(
                SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vec.toString,
                SQLConf.LEGACY_PARQUET_REBASE_DATETIME.key -> rebase.toString) {
                val pathToRead = path.getAbsolutePath + s"/${dateTime}_${period}_1582_$rebaseFlag"
                spark.read.format("parquet").load(pathToRead).noop()
              }
            }
          }

          Seq(true, false).foreach { after1582 =>
            Seq(false, true).foreach { vec =>
              Seq(false, true).foreach { rebase =>
                load(after1582, vec, rebase)
              }
            }
          }

          benchmark2.run()
        }
      }
    }
  }
}
