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

  private val NATIVE_ORC_FORMAT = "org.apache.spark.sql.execution.datasources.orc.OrcFileFormat"
  private val HIVE_ORC_FORMAT = "org.apache.spark.sql.hive.orc.OrcFileFormat"

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
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        SQL Single TINYINT Column Scan:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1263 / 1296         12.5          80.3       1.0X
        Native ORC Vectorized                          159 /  166         98.6          10.1       7.9X
        Hive built-in ORC                             1513 / 1525         10.4          96.2       0.8X

        SQL Single SMALLINT Column Scan:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1196 / 1232         13.1          76.1       1.0X
        Native ORC Vectorized                          163 /  168         96.7          10.3       7.4X
        Hive built-in ORC                             1625 / 1640          9.7         103.3       0.7X

        SQL Single INT Column Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1292 / 1378         12.2          82.2       1.0X
        Native ORC Vectorized                          228 /  236         68.9          14.5       5.7X
        Hive built-in ORC                             1829 / 1835          8.6         116.3       0.7X

        SQL Single BIGINT Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1372 / 1398         11.5          87.2       1.0X
        Native ORC Vectorized                          286 /  300         55.1          18.2       4.8X
        Hive built-in ORC                             1911 / 1913          8.2         121.5       0.7X

        SQL Single FLOAT Column Scan:            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1387 / 1415         11.3          88.2       1.0X
        Native ORC Vectorized                          326 /  329         48.2          20.7       4.3X
        Hive built-in ORC                             2004 / 2013          7.8         127.4       0.7X

        SQL Single DOUBLE Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1478 / 1524         10.6          94.0       1.0X
        Native ORC Vectorized                          412 /  416         38.2          26.2       3.6X
        Hive built-in ORC                             2070 / 2106          7.6         131.6       0.7X
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
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        Int and String Scan:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 2597 / 2671          4.0         247.7       1.0X
        Native ORC Vectorized                         1307 / 1315          8.0         124.7       2.0X
        Hive built-in ORC                             3867 / 3878          2.7         368.8       0.7X
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
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        Partitioned Table:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------------
        Read data column - Native ORC MR              1560 / 1562         10.1          99.2       1.0X
        Read data column - Native ORC Vectorized       294 /  301         53.4          18.7       5.3X
        Read data column - Hive built-in ORC          2101 / 2128          7.5         133.6       0.7X
        Read partition column - Native ORC MR         1080 / 1087         14.6          68.7       1.4X
        Read partition column - Native ORC Vectorized   54 /   58        289.5           3.5      28.7X
        Read partition column - Hive built-in ORC     1315 / 1316         12.0          83.6       1.2X
        Read both columns - Native ORC MR             1581 / 1591          9.9         100.5       1.0X
        Read both columns - Native ORC Vectorized      329 /  339         47.8          20.9       4.7X
        Read both columns - Hive built-in ORC         2124 / 2158          7.4         135.0       0.7X
        */
        benchmark.run()
      }
    }
  }

  def stringDictionaryScanBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("String Dictionary", values)

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
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        String Dictionary:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1307 / 1309          8.0         124.6       1.0X
        Native ORC Vectorized                          327 /  336         32.1          31.2       4.0X
        Hive built-in ORC                             2009 / 2072          5.2         191.6       0.7X
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

        val benchmark = new Benchmark("String with Nulls Scan", values)

        benchmark.addCase(s"Native ORC MR ($fractionOfNulls%)") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT SUM(LENGTH(c2)) FROM nativeOrcTable " +
              "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").collect()
          }
        }

        benchmark.addCase(s"Native ORC Vectorized ($fractionOfNulls%)") { _ =>
          spark.sql("SELECT SUM(LENGTH(c2)) FROM nativeOrcTable " +
            "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").collect()
        }

        benchmark.addCase(s"Hive built-in ORC ($fractionOfNulls%)") { _ =>
          spark.sql("SELECT SUM(LENGTH(c2)) FROM hiveOrcTable " +
            "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR (0.0%)                          2534 / 2535          4.1         241.7       1.0X
        Native ORC Vectorized (0.0%)                   961 /  963         10.9          91.6       2.6X
        Hive built-in ORC (0.0%)                      3980 / 4005          2.6         379.5       0.6X

        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR (0.5%)                          2417 / 2422          4.3         230.5       1.0X
        Native ORC Vectorized (0.5%)                  1314 / 1315          8.0         125.3       1.8X
        Hive built-in ORC (0.5%)                      2940 / 2953          3.6         280.4       0.8X

        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR (0.95%)                         1306 / 1318          8.0         124.5       1.0X
        Native ORC Vectorized (0.95%)                  521 /  539         20.1          49.7       2.5X
        Hive built-in ORC (0.95%)                     1636 / 1641          6.4         156.0       0.8X
        */
        benchmark.run()
      }
    }
  }

  def columnsBenchmark(values: Int, width: Int): Unit = {
    val sqlBenchmark = new Benchmark(s"SQL Single Column Scan from wide table ($width columns)", values)

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
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        SQL Single Column Scan from wide table (100 columns): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1119 / 1126          0.9        1067.4       1.0X
        Native ORC Vectorized                           93 /   99         11.3          88.3      12.1X
        Hive built-in ORC                              386 /  389          2.7         368.3       2.9X

        SQL Single Column Scan from wide table (200 columns): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 2250 / 2271          0.5        2145.6       1.0X
        Native ORC Vectorized                          156 /  164          6.7         149.1      14.4X
        Hive built-in ORC                              589 /  601          1.8         561.7       3.8X

        SQL Single Column Scan from wide table (300 columns): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 3340 / 3360          0.3        3185.5       1.0X
        Native ORC Vectorized                          273 /  283          3.8         260.8      12.2X
        Hive built-in ORC                              851 /  861          1.2         811.4       3.9X
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
    stringDictionaryScanBenchmark(1024 * 1024 * 10)
    for (fractionOfNulls <- List(0.0, 0.50, 0.95)) {
      stringWithNullsScanBenchmark(1024 * 1024 * 10, fractionOfNulls)
    }
    columnsBenchmark(1024 * 1024 * 1, 100)
    columnsBenchmark(1024 * 1024 * 1, 200)
    columnsBenchmark(1024 * 1024 * 1, 300)
  }
}
// scalastyle:on line.size.limit
