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

import java.io.File
import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset}

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.util.DateTimeConstants.SECONDS_PER_DAY
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{withDefaultTimeZone, LA}
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

  private def benchmarkInputs(benchmark: Benchmark, rowsNum: Int, dateTime: String): Unit = {
    benchmark.addCase("after 1582, noop", 1) { _ =>
      genDF(rowsNum, dateTime, after1582 = true).noop()
    }
    benchmark.addCase("before 1582, noop", 1) { _ =>
      genDF(rowsNum, dateTime, after1582 = false).noop()
    }
  }

  private def flagToStr(flag: Boolean): String = {
    if (flag) "on" else "off"
  }

  private def caseName(
      after1582: Boolean,
      rebase: Option[Boolean] = None,
      vec: Option[Boolean] = None): String = {
    val period = if (after1582) "after" else "before"
    val vecFlag = vec.map(flagToStr).map(flag => s", vec $flag").getOrElse("")
    val rebaseFlag = rebase.map(flagToStr).map(flag => s", rebase $flag").getOrElse("")
    s"$period 1582$vecFlag$rebaseFlag"
  }

  private def getPath(
      basePath: File,
      dateTime: String,
      after1582: Boolean,
      rebase: Option[Boolean] = None): String = {
    val period = if (after1582) "after" else "before"
    val rebaseFlag = rebase.map(flagToStr).map(flag => s"_$flag").getOrElse("")
    basePath.getAbsolutePath + s"/${dateTime}_${period}_1582$rebaseFlag"
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val rowsNum = 100000000

    withDefaultTimeZone(LA) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> LA.getId) {
        withTempPath { path =>
          runBenchmark("Rebasing dates/timestamps in Parquet datasource") {
            Seq("date", "timestamp").foreach { dateTime =>
              val benchmark = new Benchmark(
                s"Save ${dateTime}s to parquet",
                rowsNum,
                output = output)
              benchmarkInputs(benchmark, rowsNum, dateTime)
              Seq(true, false).foreach { after1582 =>
                Seq(false, true).foreach { rebase =>
                  benchmark.addCase(caseName(after1582, Some(rebase)), 1) { _ =>
                    withSQLConf(
                      SQLConf.LEGACY_PARQUET_REBASE_DATETIME_IN_WRITE.key -> rebase.toString) {
                      genDF(rowsNum, dateTime, after1582)
                        .write
                        .mode("overwrite")
                        .format("parquet")
                        .save(getPath(path, dateTime, after1582, Some(rebase)))
                    }
                  }
                }
              }
              benchmark.run()

              val benchmark2 = new Benchmark(
                s"Load ${dateTime}s from parquet", rowsNum, output = output)
              Seq(true, false).foreach { after1582 =>
                Seq(false, true).foreach { vec =>
                  Seq(false, true).foreach { rebase =>
                    benchmark2.addCase(caseName(after1582, Some(rebase), Some(vec)), 3) { _ =>
                      withSQLConf(
                        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vec.toString,
                        SQLConf.LEGACY_PARQUET_REBASE_DATETIME_IN_READ.key -> rebase.toString) {
                        spark.read
                          .format("parquet")
                          .load(getPath(path, dateTime, after1582, Some(rebase)))
                          .noop()
                      }
                    }
                  }
                }
              }
              benchmark2.run()
            }
          }
        }

        withTempPath { path =>
          runBenchmark("Rebasing dates/timestamps in ORC datasource") {
            Seq("date", "timestamp").foreach { dateTime =>
              val benchmark = new Benchmark(s"Save ${dateTime}s to ORC", rowsNum, output = output)
              benchmarkInputs(benchmark, rowsNum, dateTime)
              Seq(true, false).foreach { after1582 =>
                benchmark.addCase(caseName(after1582), 1) { _ =>
                  genDF(rowsNum, dateTime, after1582)
                    .write
                    .mode("overwrite")
                    .format("orc")
                    .save(getPath(path, dateTime, after1582))
                }
              }
              benchmark.run()

              val benchmark2 = new Benchmark(
                s"Load ${dateTime}s from ORC",
                rowsNum,
                output = output)
              Seq(true, false).foreach { after1582 =>
                Seq(false, true).foreach { vec =>
                  benchmark2.addCase(caseName(after1582, vec = Some(vec)), 3) { _ =>
                    withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> vec.toString) {
                      spark
                        .read
                        .format("orc")
                        .load(getPath(path, dateTime, after1582))
                        .noop()
                    }
                  }
                }
              }
              benchmark2.run()
            }
          }
        }
      }
    }
  }
}
