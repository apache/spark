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

package org.apache.spark.sql.hive.orc

import java.io.File

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Benchmark to measure ORC read performance.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <catalyst test jar>,<core test jar>,<spark sql test jar> <spark-hive test jar>
 *   2. build/sbt "hive/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "hive/test:runMain <this class>"
 *      Results will be written to "benchmarks/OrcReadBenchmark-results.txt".
 * }}}
 *
 * This is in `sql/hive` module in order to compare `sql/core` and `sql/hive` ORC data sources.
 */
// scalastyle:off line.size.limit
object OrcReadBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
    conf.set("orc.compression", "snappy")

    val sparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("OrcReadBenchmark")
      .config(conf)
      .getOrCreate()

    // Set default configs. Individual cases will change them if necessary.
    sparkSession.conf.set(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key, "true")

    sparkSession
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  private val NATIVE_ORC_FORMAT = classOf[org.apache.spark.sql.execution.datasources.orc.OrcFileFormat].getCanonicalName
  private val HIVE_ORC_FORMAT = classOf[org.apache.spark.sql.hive.orc.OrcFileFormat].getCanonicalName

  private def prepareTable(dir: File, df: DataFrame, partition: Option[String] = None): Unit = {
    val dirORC = dir.getCanonicalPath

    if (partition.isDefined) {
      df.write.partitionBy(partition.get).orc(dirORC)
    } else {
      df.write.orc(dirORC)
    }

    spark.read.format(NATIVE_ORC_FORMAT).load(dirORC).createOrReplaceTempView("nativeOrcTable")
    spark.read.format(HIVE_ORC_FORMAT).load(dirORC).createOrReplaceTempView("hiveOrcTable")
  }

  def numericScanBenchmark(values: Int, dataType: DataType): Unit = {
    val benchmark = new Benchmark(s"SQL Single ${dataType.sql} Column Scan", values, output = output)

    withTempPath { dir =>
      withTempTable("t1", "nativeOrcTable", "hiveOrcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql(s"SELECT CAST(value as ${dataType.sql}) id FROM t1"))

        benchmark.addCase("Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(id) FROM nativeOrcTable").noop()
          }
        }

        benchmark.addCase("Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(id) FROM nativeOrcTable").noop()
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(id) FROM hiveOrcTable").noop()
        }

        benchmark.run()
      }
    }
  }

  def intStringScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Int and String Scan", values, output = output)

    withTempPath { dir =>
      withTempTable("t1", "nativeOrcTable", "hiveOrcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(
          dir,
          spark.sql("SELECT CAST(value AS INT) AS c1, CAST(value as STRING) AS c2 FROM t1"))

        benchmark.addCase("Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(c1), sum(length(c2)) FROM nativeOrcTable").noop()
          }
        }

        benchmark.addCase("Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(c1), sum(length(c2)) FROM nativeOrcTable").noop()
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(c1), sum(length(c2)) FROM hiveOrcTable").noop()
        }

        benchmark.run()
      }
    }
  }

  def partitionTableScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Partitioned Table", values, output = output)

    withTempPath { dir =>
      withTempTable("t1", "nativeOrcTable", "hiveOrcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql("SELECT value % 2 AS p, value AS id FROM t1"), Some("p"))

        benchmark.addCase("Data column - Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(id) FROM nativeOrcTable").noop()
          }
        }

        benchmark.addCase("Data column - Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(id) FROM nativeOrcTable").noop()
        }

        benchmark.addCase("Data column - Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(id) FROM hiveOrcTable").noop()
        }

        benchmark.addCase("Partition column - Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(p) FROM nativeOrcTable").noop()
          }
        }

        benchmark.addCase("Partition column - Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(p) FROM nativeOrcTable").noop()
        }

        benchmark.addCase("Partition column - Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(p) FROM hiveOrcTable").noop()
        }

        benchmark.addCase("Both columns - Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(p), sum(id) FROM nativeOrcTable").noop()
          }
        }

        benchmark.addCase("Both columns - Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(p), sum(id) FROM nativeOrcTable").noop()
        }

        benchmark.addCase("Both columns - Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(p), sum(id) FROM hiveOrcTable").noop()
        }

        benchmark.run()
      }
    }
  }

  def repeatedStringScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Repeated String", values, output = output)

    withTempPath { dir =>
      withTempTable("t1", "nativeOrcTable", "hiveOrcTable") {
        spark.range(values).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql("SELECT CAST((id % 200) + 10000 as STRING) AS c1 FROM t1"))

        benchmark.addCase("Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(length(c1)) FROM nativeOrcTable").noop()
          }
        }

        benchmark.addCase("Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(length(c1)) FROM nativeOrcTable").noop()
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(length(c1)) FROM hiveOrcTable").noop()
        }

        benchmark.run()
      }
    }
  }

  def stringWithNullsScanBenchmark(values: Int, fractionOfNulls: Double): Unit = {
    withTempPath { dir =>
      withTempTable("t1", "nativeOrcTable", "hiveOrcTable") {
        spark.range(values).createOrReplaceTempView("t1")

        prepareTable(
          dir,
          spark.sql(
            s"SELECT IF(RAND(1) < $fractionOfNulls, NULL, CAST(id as STRING)) AS c1, " +
            s"IF(RAND(2) < $fractionOfNulls, NULL, CAST(id as STRING)) AS c2 FROM t1"))

        val percentageOfNulls = fractionOfNulls * 100
        val benchmark =
          new Benchmark(s"String with Nulls Scan ($percentageOfNulls%)", values, output = output)

        benchmark.addCase("Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT SUM(LENGTH(c2)) FROM nativeOrcTable " +
              "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").noop()
          }
        }

        benchmark.addCase("Native ORC Vectorized") { _ =>
          spark.sql("SELECT SUM(LENGTH(c2)) FROM nativeOrcTable " +
            "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").noop()
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT SUM(LENGTH(c2)) FROM hiveOrcTable " +
            "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").noop()
        }

        benchmark.run()
      }
    }
  }

  def columnsBenchmark(values: Int, width: Int): Unit = {
    val benchmark = new Benchmark(s"Single Column Scan from $width columns", values, output = output)

    withTempPath { dir =>
      withTempTable("t1", "nativeOrcTable", "hiveOrcTable") {
        import spark.implicits._
        val middle = width / 2
        val selectExpr = (1 to width).map(i => s"value as c$i")
        spark.range(values).map(_ => Random.nextLong).toDF()
          .selectExpr(selectExpr: _*).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql("SELECT * FROM t1"))

        benchmark.addCase("Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql(s"SELECT sum(c$middle) FROM nativeOrcTable").noop()
          }
        }

        benchmark.addCase("Native ORC Vectorized") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM nativeOrcTable").noop()
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM hiveOrcTable").noop()
        }

        benchmark.run()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("SQL Single Numeric Column Scan") {
      Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType).foreach { dataType =>
        numericScanBenchmark(1024 * 1024 * 15, dataType)
      }
    }
    runBenchmark("Int and String Scan") {
      intStringScanBenchmark(1024 * 1024 * 10)
    }
    runBenchmark("Partitioned Table Scan") {
      partitionTableScanBenchmark(1024 * 1024 * 15)
    }
    runBenchmark("Repeated String Scan") {
      repeatedStringScanBenchmark(1024 * 1024 * 10)
    }
    runBenchmark("String with Nulls Scan") {
      for (fractionOfNulls <- List(0.0, 0.50, 0.95)) {
        stringWithNullsScanBenchmark(1024 * 1024 * 10, fractionOfNulls)
      }
    }
    runBenchmark("Single Column Scan From Wide Columns") {
      columnsBenchmark(1024 * 1024 * 1, 100)
      columnsBenchmark(1024 * 1024 * 1, 200)
      columnsBenchmark(1024 * 1024 * 1, 300)
    }
  }
}
// scalastyle:on line.size.limit
