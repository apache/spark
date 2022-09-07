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
import org.apache.spark.sql.internal.SQLConf.{LegacyBehaviorPolicy, ParquetOutputTimestampType}

object DateTime extends Enumeration {
  type DateTime = Value
  val DATE, TIMESTAMP, TIMESTAMP_INT96, TIMESTAMP_MICROS, TIMESTAMP_MILLIS = Value
}

/**
 * Synthetic benchmark for rebasing of date and timestamp in read/write.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/DateTimeRebaseBenchmark-results.txt".
 * }}}
 */
object DateTimeRebaseBenchmark extends SqlBasedBenchmark {
  import spark.implicits._
  import DateTime._

  private def genTs(cardinality: Int, start: LocalDateTime, end: LocalDateTime): DataFrame = {
    val startSec = start.toEpochSecond(ZoneOffset.UTC)
    val endSec = end.toEpochSecond(ZoneOffset.UTC)
    spark.range(0, cardinality, 1, 1)
      .select((($"id" % (endSec - startSec)) + startSec).as("seconds"))
      .select($"seconds".cast("timestamp").as("ts"))
  }

  private def genTsAfter1900(cardinality: Int): DataFrame = {
    val start = LocalDateTime.of(1900, 1, 31, 0, 0, 0)
    val end = LocalDateTime.of(3000, 1, 1, 0, 0, 0)
    genTs(cardinality, start, end)
  }

  private def genTsBefore1900(cardinality: Int): DataFrame = {
    val start = LocalDateTime.of(10, 1, 1, 0, 0, 0)
    val end = LocalDateTime.of(1900, 1, 1, 0, 0, 0)
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
    val start = LocalDate.of(1582, 10, 31)
    val end = LocalDate.of(3000, 1, 1)
    genDate(cardinality, start, end)
  }

  private def genDateBefore1582(cardinality: Int): DataFrame = {
    val start = LocalDate.of(10, 1, 1)
    val end = LocalDate.of(1580, 10, 1)
    genDate(cardinality, start, end)
  }

  private def genDF(cardinality: Int, dateTime: DateTime, modernDates: Boolean): DataFrame = {
    dateTime match {
      case DATE =>
        if (modernDates) genDateAfter1582(cardinality) else genDateBefore1582(cardinality)
      case TIMESTAMP | TIMESTAMP_INT96 | TIMESTAMP_MICROS | TIMESTAMP_MILLIS =>
        if (modernDates) genTsAfter1900(cardinality) else genTsBefore1900(cardinality)
      case _ => throw new IllegalArgumentException(
        s"cardinality = $cardinality dateTime = $dateTime modernDates = $modernDates")
    }
  }

  private def benchmarkInputs(benchmark: Benchmark, rowsNum: Int, dateTime: DateTime): Unit = {
    val year = if (dateTime == DATE) 1582 else 1900
    benchmark.addCase(s"after $year, noop", 1) { _ =>
      genDF(rowsNum, dateTime, modernDates = true).noop()
    }
    benchmark.addCase(s"before $year, noop", 1) { _ =>
      genDF(rowsNum, dateTime, modernDates = false).noop()
    }
  }

  private def flagToStr(flag: Boolean): String = {
    if (flag) "on" else "off"
  }

  private def caseName(
      modernDates: Boolean,
      dateTime: DateTime,
      mode: Option[LegacyBehaviorPolicy.Value] = None,
      vec: Option[Boolean] = None): String = {
    val period = if (modernDates) "after" else "before"
    val year = if (dateTime == DATE) 1582 else 1900
    val vecFlag = vec.map(flagToStr).map(flag => s", vec $flag").getOrElse("")
    val rebaseFlag = mode.map(_.toString).map(m => s", rebase $m").getOrElse("")
    s"$period $year$vecFlag$rebaseFlag"
  }

  private def getPath(
      basePath: File,
      dateTime: DateTime,
      modernDates: Boolean,
      mode: Option[LegacyBehaviorPolicy.Value] = None): String = {
    val period = if (modernDates) "after" else "before"
    val year = if (dateTime == DATE) 1582 else 1900
    val rebaseFlag = mode.map(_.toString).map(m => s"_$m").getOrElse("")
    basePath.getAbsolutePath + s"/${dateTime}_${period}_$year$rebaseFlag"
  }

  private def getOutputType(dateTime: DateTime): String = dateTime match {
    case TIMESTAMP_INT96 => ParquetOutputTimestampType.INT96.toString
    case TIMESTAMP_MILLIS => ParquetOutputTimestampType.TIMESTAMP_MILLIS.toString
    case _ => ParquetOutputTimestampType.TIMESTAMP_MICROS.toString
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val rowsNum = 100000000

    withDefaultTimeZone(LA) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> LA.getId) {
        withTempPath { path =>
          runBenchmark("Rebasing dates/timestamps in Parquet datasource") {
            Seq(
              DATE, TIMESTAMP_INT96, TIMESTAMP_MICROS, TIMESTAMP_MILLIS
            ).foreach { dateTime =>
              val benchmark = new Benchmark(
                s"Save $dateTime to parquet",
                rowsNum,
                output = output)
              benchmarkInputs(benchmark, rowsNum, dateTime)
              Seq(true, false).foreach { modernDates =>
                LegacyBehaviorPolicy.values
                  .filterNot(v => !modernDates && v == LegacyBehaviorPolicy.EXCEPTION)
                  .foreach { mode =>
                  benchmark.addCase(caseName(modernDates, dateTime, Some(mode)), 1) { _ =>
                    withSQLConf(
                      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> getOutputType(dateTime),
                      SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> mode.toString,
                      SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> mode.toString) {
                      genDF(rowsNum, dateTime, modernDates)
                        .write
                        .mode("overwrite")
                        .format("parquet")
                        .save(getPath(path, dateTime, modernDates, Some(mode)))
                    }
                  }
                }
              }
              benchmark.run()

              val benchmark2 = new Benchmark(
                s"Load $dateTime from parquet", rowsNum, output = output)
              Seq(true, false).foreach { modernDates =>
                Seq(false, true).foreach { vec =>
                  LegacyBehaviorPolicy.values
                    .filterNot(v => !modernDates && v == LegacyBehaviorPolicy.EXCEPTION)
                    .foreach { mode =>
                    val name = caseName(modernDates, dateTime, Some(mode), Some(vec))
                    benchmark2.addCase(name, 3) { _ =>
                      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vec.toString) {
                        spark.read
                          .format("parquet")
                          .load(getPath(path, dateTime, modernDates, Some(mode)))
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
            Seq(DATE, TIMESTAMP).foreach { dateTime =>
              val benchmark = new Benchmark(s"Save $dateTime to ORC", rowsNum, output = output)
              benchmarkInputs(benchmark, rowsNum, dateTime)
              Seq(true, false).foreach { modernDates =>
                benchmark.addCase(caseName(modernDates, dateTime), 1) { _ =>
                  genDF(rowsNum, dateTime, modernDates)
                    .write
                    .mode("overwrite")
                    .format("orc")
                    .save(getPath(path, dateTime, modernDates))
                }
              }
              benchmark.run()

              val benchmark2 = new Benchmark(
                s"Load $dateTime from ORC",
                rowsNum,
                output = output)
              Seq(true, false).foreach { modernDates =>
                Seq(false, true).foreach { vec =>
                  benchmark2.addCase(caseName(modernDates, dateTime, vec = Some(vec)), 3) { _ =>
                    withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> vec.toString) {
                      spark
                        .read
                        .format("orc")
                        .load(getPath(path, dateTime, modernDates))
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
