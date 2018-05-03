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
    val benchmark = new Benchmark(s"SQL Single ${dataType.sql} Column Scan", values)

    withTempPath { dir =>
      withTempTable("t1", "nativeOrcTable", "hiveOrcTable") {
        import spark.implicits._
        spark.range(values).map(_ => Random.nextLong).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql(s"SELECT CAST(value as ${dataType.sql}) id FROM t1"))

        benchmark.addCase("Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
        }

        benchmark.addCase("Native ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(id) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

        SQL Single TINYINT Column Scan:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1135 / 1171         13.9          72.2       1.0X
        Native ORC Vectorized                          152 /  163        103.4           9.7       7.5X
        Native ORC Vectorized with copy                149 /  162        105.4           9.5       7.6X
        Hive built-in ORC                             1380 / 1384         11.4          87.7       0.8X

        SQL Single SMALLINT Column Scan:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1182 / 1244         13.3          75.2       1.0X
        Native ORC Vectorized                          145 /  156        108.7           9.2       8.2X
        Native ORC Vectorized with copy                148 /  158        106.4           9.4       8.0X
        Hive built-in ORC                             1591 / 1636          9.9         101.2       0.7X

        SQL Single INT Column Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1271 / 1271         12.4          80.8       1.0X
        Native ORC Vectorized                          206 /  212         76.3          13.1       6.2X
        Native ORC Vectorized with copy                200 /  213         78.8          12.7       6.4X
        Hive built-in ORC                             1776 / 1787          8.9         112.9       0.7X

        SQL Single BIGINT Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1344 / 1355         11.7          85.4       1.0X
        Native ORC Vectorized                          258 /  268         61.0          16.4       5.2X
        Native ORC Vectorized with copy                252 /  257         62.4          16.0       5.3X
        Hive built-in ORC                             1818 / 1823          8.7         115.6       0.7X

        SQL Single FLOAT Column Scan:            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1333 / 1352         11.8          84.8       1.0X
        Native ORC Vectorized                          310 /  324         50.7          19.7       4.3X
        Native ORC Vectorized with copy                312 /  320         50.4          19.9       4.3X
        Hive built-in ORC                             1904 / 1918          8.3         121.0       0.7X

        SQL Single DOUBLE Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1408 / 1585         11.2          89.5       1.0X
        Native ORC Vectorized                          359 /  368         43.8          22.8       3.9X
        Native ORC Vectorized with copy                364 /  371         43.2          23.2       3.9X
        Hive built-in ORC                             1881 / 1954          8.4         119.6       0.7X
        */
        benchmark.run()
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

        benchmark.addCase("Native ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("SELECT sum(c1), sum(length(c2)) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(c1), sum(length(c2)) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

        Int and String Scan:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 2566 / 2592          4.1         244.7       1.0X
        Native ORC Vectorized                         1098 / 1113          9.6         104.7       2.3X
        Native ORC Vectorized with copy               1527 / 1593          6.9         145.6       1.7X
        Hive built-in ORC                             3561 / 3705          2.9         339.6       0.7X
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

        benchmark.addCase("Data column - Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Data column - Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
        }

        benchmark.addCase("Data column - Native ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Data column - Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(id) FROM hiveOrcTable").collect()
        }

        benchmark.addCase("Partition column - Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(p) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Partition column - Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(p) FROM nativeOrcTable").collect()
        }

        benchmark.addCase("Partition column - Native ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("SELECT sum(p) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Partition column - Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(p) FROM hiveOrcTable").collect()
        }

        benchmark.addCase("Both columns - Native ORC MR") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(p), sum(id) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Both columns - Native ORC Vectorized") { _ =>
          spark.sql("SELECT sum(p), sum(id) FROM nativeOrcTable").collect()
        }

        benchmark.addCase("Both column - Native ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("SELECT sum(p), sum(id) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Both columns - Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(p), sum(id) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

        Partitioned Table:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Data only - Native ORC MR                      1447 / 1457         10.9          92.0       1.0X
        Data only - Native ORC Vectorized               256 /  266         61.4          16.3       5.6X
        Data only - Native ORC Vectorized with copy     263 /  273         59.8          16.7       5.5X
        Data only - Hive built-in ORC                  1960 / 1988          8.0         124.6       0.7X
        Partition only - Native ORC MR                 1039 / 1043         15.1          66.0       1.4X
        Partition only - Native ORC Vectorized           48 /   53        326.6           3.1      30.1X
        Partition only - Native ORC Vectorized with copy 48 /   53        328.4           3.0      30.2X
        Partition only - Hive built-in ORC             1234 / 1242         12.7          78.4       1.2X
        Both columns - Native ORC MR                   1465 / 1475         10.7          93.1       1.0X
        Both columns - Native ORC Vectorized            292 /  301         53.9          18.6       5.0X
        Both column - Native ORC Vectorized with copy   348 /  354         45.1          22.2       4.2X
        Both columns - Hive built-in ORC               2051 / 2060          7.7         130.4       0.7X
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

        benchmark.addCase("Native ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("SELECT sum(length(c1)) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(length(c1)) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

        Repeated String:                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1271 / 1278          8.3         121.2       1.0X
        Native ORC Vectorized                          200 /  212         52.4          19.1       6.4X
        Native ORC Vectorized with copy                342 /  347         30.7          32.6       3.7X
        Hive built-in ORC                             1874 / 2105          5.6         178.7       0.7X
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

        benchmark.addCase("Native ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql("SELECT SUM(LENGTH(c2)) FROM nativeOrcTable " +
              "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").collect()
          }
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT SUM(LENGTH(c2)) FROM hiveOrcTable " +
            "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

        String with Nulls Scan (0.0%):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 2394 / 2886          4.4         228.3       1.0X
        Native ORC Vectorized                          699 /  729         15.0          66.7       3.4X
        Native ORC Vectorized with copy                959 / 1025         10.9          91.5       2.5X
        Hive built-in ORC                             3899 / 3901          2.7         371.9       0.6X

        String with Nulls Scan (0.5%):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 2234 / 2255          4.7         213.1       1.0X
        Native ORC Vectorized                          854 /  869         12.3          81.4       2.6X
        Native ORC Vectorized with copy               1099 / 1128          9.5         104.8       2.0X
        Hive built-in ORC                             2767 / 2793          3.8         263.9       0.8X

        String with Nulls Scan (0.95%):          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1166 / 1202          9.0         111.2       1.0X
        Native ORC Vectorized                          338 /  345         31.1          32.2       3.5X
        Native ORC Vectorized with copy                418 /  428         25.1          39.9       2.8X
        Hive built-in ORC                             1730 / 1761          6.1         164.9       0.7X
        */
        benchmark.run()
      }
    }
  }

  def columnsBenchmark(values: Int, width: Int): Unit = {
    val benchmark = new Benchmark(s"Single Column Scan from $width columns", values)

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
            spark.sql(s"SELECT sum(c$middle) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Native ORC Vectorized") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM nativeOrcTable").collect()
        }

        benchmark.addCase("Native ORC Vectorized with copy") { _ =>
          withSQLConf(SQLConf.ORC_COPY_BATCH_TO_SPARK.key -> "true") {
            spark.sql(s"SELECT sum(c$middle) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

        Single Column Scan from 100 columns:     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1050 / 1053          1.0        1001.1       1.0X
        Native ORC Vectorized                           95 /  101         11.0          90.9      11.0X
        Native ORC Vectorized with copy                 95 /  102         11.0          90.9      11.0X
        Hive built-in ORC                              348 /  358          3.0         331.8       3.0X

        Single Column Scan from 200 columns:     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 2099 / 2108          0.5        2002.1       1.0X
        Native ORC Vectorized                          179 /  187          5.8         171.1      11.7X
        Native ORC Vectorized with copy                176 /  188          6.0         167.6      11.9X
        Hive built-in ORC                              562 /  581          1.9         535.9       3.7X

        Single Column Scan from 300 columns:     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 3221 / 3246          0.3        3071.4       1.0X
        Native ORC Vectorized                          312 /  322          3.4         298.0      10.3X
        Native ORC Vectorized with copy                306 /  320          3.4         291.6      10.5X
        Hive built-in ORC                              815 /  824          1.3         777.3       4.0X
        */
        benchmark.run()
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
