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

import scala.util.{Random, Try}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.{Benchmark, Utils}


/**
 * Benchmark to measure ORC read performance.
 *
 * This is in `sql/hive` module in order to compare `sql/core` and `sql/hive` ORC data sources.
 */
// scalastyle:off line.size.limit
object OrcReadBenchmark {
  val conf = new SparkConf()
  conf.set("orc.compression", "snappy")

  private val spark = SparkSession.builder()
    .master("local[1]")
    .appName("OrcReadBenchmark")
    .config(conf)
    .getOrCreate()

  // Set default configs. Individual cases will change them if necessary.
  spark.conf.set(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key, "true")

  def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(spark.conf.get(key)).toOption)
    (keys, values).zipped.foreach(spark.conf.set)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
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
    val sqlBenchmark = new Benchmark(s"SQL Single ${dataType.sql} Column Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "nativeOrcTable", "hiveOrcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql(s"SELECT CAST(value as ${dataType.sql}) id FROM t1"))

        sqlBenchmark.addCase("Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
          }
        }

        sqlBenchmark.addCase("Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
        }

        sqlBenchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(id) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.2
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        SQL Single TINYINT Column Scan:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1202 / 1265         13.1          76.4       1.0X
        Native ORC Vectorized                          162 /  172         97.1          10.3       7.4X
        Hive built-in ORC                             1410 / 1428         11.2          89.6       0.9X

        SQL Single SMALLINT Column Scan:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1266 / 1286         12.4          80.5       1.0X
        Native ORC Vectorized                          165 /  174         95.2          10.5       7.7X
        Hive built-in ORC                             1701 / 1704          9.2         108.2       0.7X

        SQL Single INT Column Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1307 / 1307         12.0          83.1       1.0X
        Native ORC Vectorized                          232 /  248         67.9          14.7       5.6X
        Hive built-in ORC                             1793 / 1793          8.8         114.0       0.7X

        SQL Single BIGINT Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1360 / 1372         11.6          86.5       1.0X
        Native ORC Vectorized                          293 /  303         53.8          18.6       4.7X
        Hive built-in ORC                             1913 / 1933          8.2         121.6       0.7X

        SQL Single FLOAT Column Scan:            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1389 / 1488         11.3          88.3       1.0X
        Native ORC Vectorized                          340 /  346         46.2          21.6       4.1X
        Hive built-in ORC                             1976 / 1997          8.0         125.6       0.7X

        SQL Single DOUBLE Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1461 / 1465         10.8          92.9       1.0X
        Native ORC Vectorized                          395 /  406         39.8          25.1       3.7X
        Hive built-in ORC                             2127 / 2146          7.4         135.2       0.7X
        */
        sqlBenchmark.run()
      }
    }
  }

  def intStringScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Int and String Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "nativeOrcTable", "hiveOrcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(
          dir,
          spark.sql("SELECT CAST(value AS INT) AS c1, CAST(value as STRING) AS c2 FROM t1"))

        benchmark.addCase("Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(c1), sum(length(c2)) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(c1), sum(length(c2)) FROM nativeOrcTable").collect()
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(c1), sum(length(c2)) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.2
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        Int and String Scan:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 2843 / 2935          3.7         271.1       1.0X
        Native ORC Vectorized                         1349 / 1359          7.8         128.6       2.1X
        Hive built-in ORC                             3862 / 3881          2.7         368.3       0.7X
        */
        benchmark.run()
      }
    }
  }

  def partitionTableScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Partitioned Table", values)

    withTempPath { dir =>
      withTempTable("t1", "nativeOrcTable", "hiveOrcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql("SELECT value % 2 AS p, value AS id FROM t1"), Some("p"))

        benchmark.addCase("Read data column - Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Read data column - Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
        }

        benchmark.addCase("Read data column - Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(id) FROM hiveOrcTable").collect()
        }

        benchmark.addCase("Read partition column - Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(p) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Read partition column - Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(p) FROM nativeOrcTable").collect()
        }

        benchmark.addCase("Read partition column - Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(p) FROM hiveOrcTable").collect()
        }

        benchmark.addCase("Read both columns - Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(p), sum(id) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Read both columns - Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(p), sum(id) FROM nativeOrcTable").collect()
        }

        benchmark.addCase("Read both columns - Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(p), sum(id) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.2
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        Partitioned Table:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Read data column - Native ORC MR               1545 / 1568         10.2          98.2       1.0X
        Read data column - Native ORC Vectorized        300 /  304         52.4          19.1       5.1X
        Read data column - Hive built-in ORC           2097 / 2117          7.5         133.3       0.7X
        Read partition column - Native ORC MR          1023 / 1026         15.4          65.1       1.5X
        Read partition column - Native ORC Vectorized    54 /   56        292.3           3.4      28.7X
        Read partition column - Hive built-in ORC      1285 / 1289         12.2          81.7       1.2X
        Read both columns - Native ORC MR              1564 / 1565         10.1          99.4       1.0X
        Read both columns - Native ORC Vectorized       336 /  340         46.8          21.4       4.6X
        Read both columns - Hive built-in ORC          2100 / 2123          7.5         133.5       0.7X
        */
        benchmark.run()
      }
    }
  }

  def repeatedStringScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Repeated String", values)

    withTempPath { dir =>
      withTempTable("t1", "nativeOrcTable", "hiveOrcTable") {
        spark.range(values).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql("SELECT CAST((id % 200) + 10000 as STRING) AS c1 FROM t1"))

        benchmark.addCase("Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(length(c1)) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(length(c1)) FROM nativeOrcTable").collect()
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(length(c1)) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.2
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        Repeated String:                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1338 / 1340          7.8         127.6       1.0X
        Native ORC Vectorized                          342 /  350         30.6          32.6       3.9X
        Hive built-in ORC                             2036 / 2117          5.2         194.2       0.7X
        */
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

        val benchmark = new Benchmark(s"String with Nulls Scan ($fractionOfNulls%)", values)

        benchmark.addCase("Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT SUM(LENGTH(c2)) FROM nativeOrcTable " +
              "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").collect()
          }
        }

        benchmark.addCase("Native ORC Vectorized") { _ =>
          spark.sql("SELECT SUM(LENGTH(c2)) FROM nativeOrcTable " +
            "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").collect()
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT SUM(LENGTH(c2)) FROM hiveOrcTable " +
            "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.2
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        String with Nulls Scan (0.0%):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 2510 / 2527          4.2         239.3       1.0X
        Native ORC Vectorized                         1012 / 1012         10.4          96.5       2.5X
        Hive built-in ORC                             3967 / 3984          2.6         378.3       0.6X

        String with Nulls Scan (0.5%):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 2374 / 2388          4.4         226.4       1.0X
        Native ORC Vectorized                         1269 / 1275          8.3         121.0       1.9X
        Hive built-in ORC                             2994 / 2998          3.5         285.5       0.8X

        String with Nulls Scan (0.95%):          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1285 / 1312          8.2         122.6       1.0X
        Native ORC Vectorized                          500 /  508         21.0          47.7       2.6X
        Hive built-in ORC                             1630 / 1638          6.4         155.5       0.8X
        */
        benchmark.run()
      }
    }
  }

  def columnsBenchmark(values: Int, width: Int): Unit = {
    val sqlBenchmark = new Benchmark(s"SQL Single Column Scan from $width columns", values)

    withTempPath { dir =>
      withTempTable("t1", "nativeOrcTable", "hiveOrcTable") {
        import spark.implicits._
        val middle = width / 2
        val selectExpr = (1 to width).map(i => s"value as c$i")
        spark.range(values).map(_ => Random.nextLong).toDF()
          .selectExpr(selectExpr: _*).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql("SELECT * FROM t1"))

        sqlBenchmark.addCase("Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql(s"SELECT sum(c$middle) FROM nativeOrcTable").collect()
          }
        }

        sqlBenchmark.addCase("Native ORC Vectorized") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM nativeOrcTable").collect()
        }

        sqlBenchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.2
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        SQL Single Column Scan from 100 columns: Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1107 / 1111          0.9        1055.4       1.0X
        Native ORC Vectorized                           93 /  102         11.3          88.8      11.9X
        Hive built-in ORC                              377 /  389          2.8         359.6       2.9X

        SQL Single Column Scan from 200 columns: Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 2258 / 2270          0.5        2153.8       1.0X
        Native ORC Vectorized                          160 /  168          6.6         152.6      14.1X
        Hive built-in ORC                              591 /  597          1.8         563.7       3.8X

        SQL Single Column Scan from 300 columns: Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 3364 / 3391          0.3        3208.3       1.0X
        Native ORC Vectorized                          273 /  284          3.8         260.1      12.3X
        Hive built-in ORC                              831 /  842          1.3         792.8       4.0X
        */
        sqlBenchmark.run()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType).foreach { dataType =>
      numericScanBenchmark(1024 * 1024 * 15, dataType)
    }
    intStringScanBenchmark(1024 * 1024 * 10)
    partitionTableScanBenchmark(1024 * 1024 * 15)
    repeatedStringScanBenchmark(1024 * 1024 * 10)
    for (fractionOfNulls <- List(0.0, 0.50, 0.95)) {
      stringWithNullsScanBenchmark(1024 * 1024 * 10, fractionOfNulls)
    }
    columnsBenchmark(1024 * 1024 * 1, 100)
    columnsBenchmark(1024 * 1024 * 1, 200)
    columnsBenchmark(1024 * 1024 * 1, 300)
  }
}
// scalastyle:on line.size.limit
