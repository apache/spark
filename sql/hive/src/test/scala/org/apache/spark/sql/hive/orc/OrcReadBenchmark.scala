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
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
          }
        }

        sqlBenchmark.addCase("Native ORC Vectorized (Java)") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "true") {
            spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
          }
        }

        sqlBenchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(id) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        SQL Single TINYINT Column Scan:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1161 / 1168         13.5          73.8       1.0X
        Native ORC Vectorized                          163 /  171         96.3          10.4       7.1X
        Native ORC Vectorized (Java)                   155 /  163        101.6           9.8       7.5X
        Hive built-in ORC                             1427 / 1427         11.0          90.7       0.8X

        SQL Single SMALLINT Column Scan:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1261 / 1321         12.5          80.2       1.0X
        Native ORC Vectorized                          160 /  167         98.2          10.2       7.9X
        Native ORC Vectorized (Java)                   160 /  167         98.4          10.2       7.9X
        Hive built-in ORC                             1655 / 1687          9.5         105.2       0.8X

        SQL Single INT Column Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1369 / 1449         11.5          87.1       1.0X
        Native ORC Vectorized                          263 /  277         59.8          16.7       5.2X
        Native ORC Vectorized (Java)                   225 /  237         70.0          14.3       6.1X
        Hive built-in ORC                             1867 / 1899          8.4         118.7       0.7X

        SQL Single BIGINT Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1472 / 1474         10.7          93.6       1.0X
        Native ORC Vectorized                          289 /  300         54.5          18.4       5.1X
        Native ORC Vectorized (Java)                   286 /  294         54.9          18.2       5.1X
        Hive built-in ORC                             1917 / 1934          8.2         121.9       0.8X

        SQL Single FLOAT Column Scan:            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1484 / 1484         10.6          94.3       1.0X
        Native ORC Vectorized                          365 /  370         43.1          23.2       4.1X
        Native ORC Vectorized (Java)                   326 /  335         48.2          20.7       4.5X
        Hive built-in ORC                             1978 / 2049          8.0         125.8       0.8X

        SQL Single DOUBLE Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1550 / 1554         10.1          98.6       1.0X
        Native ORC Vectorized                          396 /  405         39.7          25.2       3.9X
        Native ORC Vectorized (Java)                   394 /  402         39.9          25.1       3.9X
        Hive built-in ORC                             2072 / 2084          7.6         131.8       0.7X
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
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(c1), sum(length(c2)) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Native ORC Vectorized (Java)") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "true") {
            spark.sql("SELECT sum(c1), sum(length(c2)) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(c1), sum(length(c2)) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        Int and String Scan:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 2636 / 2734          4.0         251.4       1.0X
        Native ORC Vectorized                         1267 / 1267          8.3         120.9       2.1X
        Native ORC Vectorized (Java)                  1182 / 1183          8.9         112.7       2.2X
        Hive built-in ORC                             3724 / 3764          2.8         355.2       0.7X
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
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Read data column - Native ORC Vectorized (Java)") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "true") {
            spark.sql("SELECT sum(id) FROM nativeOrcTable").collect()
          }
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
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(p) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Read partition column - Native ORC Vectorized (Java)") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "true") {
            spark.sql("SELECT sum(p) FROM nativeOrcTable").collect()
          }
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
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(p), sum(id) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Read both columns - Native ORC Vectorized (Java)") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "true") {
            spark.sql("SELECT sum(p), sum(id) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Read both columns - Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(p), sum(id) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        Partitioned Table:                            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------------
        Read data column - Native ORC MR                    1587 / 1592          9.9         100.9       1.0X
        Read data column - Native ORC Vectorized             290 /  309         54.3          18.4       5.5X
        Read data column - Native ORC Vectorized (Java)      293 /  297         53.7          18.6       5.4X
        Read data column - Hive built-in ORC                2204 / 2214          7.1         140.1       0.7X
        Read partition column - Native ORC MR               1078 / 1097         14.6          68.5       1.5X
        Read partition column - Native ORC Vectorized         53 /   56        294.0           3.4      29.7X
        Read partition column - Native ORC Vectorized (Java)  52 /   55        300.7           3.3      30.4X
        Read partition column - Hive built-in ORC           1279 / 1287         12.3          81.3       1.2X
        Read both columns - Native ORC MR                   1665 / 1674          9.4         105.9       1.0X
        Read both columns - Native ORC Vectorized            327 /  333         48.0          20.8       4.8X
        Read both columns - Native ORC Vectorized (Java)     327 /  332         48.2          20.8       4.9X
        Read both columns - Hive built-in ORC               2157 / 2169          7.3         137.1       0.7X
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
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "false") {
            spark.sql("SELECT sum(length(c1)) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Native ORC Vectorized (Java)") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "true") {
            spark.sql("SELECT sum(length(c1)) FROM nativeOrcTable").collect()
          }
        }

        benchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql("SELECT sum(length(c1)) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        String Dictionary:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1355 / 1355          7.7         129.2       1.0X
        Native ORC Vectorized                          262 /  270         40.0          25.0       5.2X
        Native ORC Vectorized (Java)                   223 /  227         46.9          21.3       6.1X
        Hive built-in ORC                             2017 / 2027          5.2         192.4       0.7X
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
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "false") {
            spark.sql("SELECT SUM(LENGTH(c2)) FROM nativeOrcTable " +
              "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").collect()
          }
        }

        benchmark.addCase(s"Native ORC Vectorized ($fractionOfNulls%) (Java)") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "true") {
            spark.sql("SELECT SUM(LENGTH(c2)) FROM nativeOrcTable " +
              "WHERE c1 IS NOT NULL AND c2 IS NOT NULL").collect()
          }
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
        Native ORC MR (0.0%)                          2575 / 2618          4.1         245.5       1.0X
        Native ORC Vectorized (0.0%)                   841 /  852         12.5          80.2       3.1X
        Native ORC Vectorized (0.0%) (Java)            757 /  760         13.9          72.2       3.4X
        Hive built-in ORC (0.0%)                      4149 / 4162          2.5         395.7       0.6X

        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR (0.5%)                          2446 / 2460          4.3         233.3       1.0X
        Native ORC Vectorized (0.5%)                  1081 / 1084          9.7         103.1       2.3X
        Native ORC Vectorized (0.5%) (Java)           1066 / 1069          9.8         101.6       2.3X
        Hive built-in ORC (0.5%)                      2928 / 2938          3.6         279.2       0.8X

        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR (0.95%)                         1287 / 1331          8.2         122.7       1.0X
        Native ORC Vectorized (0.95%)                  404 /  407         26.0          38.5       3.2X
        Native ORC Vectorized (0.95%) (Java)           405 /  409         25.9          38.6       3.2X
        Hive built-in ORC (0.95%)                     1612 / 1644          6.5         153.7       0.8X
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
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "false") {
            spark.sql(s"SELECT sum(c$middle) FROM nativeOrcTable").collect()
          }
        }

        sqlBenchmark.addCase("Native ORC Vectorized (Java)") { _ =>
          withSQLConf(SQLConf.ORC_VECTORIZED_JAVA_READER_ENABLED.key -> "true") {
            spark.sql(s"SELECT sum(c$middle) FROM nativeOrcTable").collect()
          }
        }

        sqlBenchmark.addCase("Hive built-in ORC") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM hiveOrcTable").collect()
        }

        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.1
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        SQL Single Column Scan from wide table (100 columns): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1116 / 1117          0.9        1064.7       1.0X
        Native ORC Vectorized                           92 /   98         11.3          88.2      12.1X
        Native ORC Vectorized (Java)                    90 /   96         11.6          86.0      12.4X
        Hive built-in ORC                              376 /  386          2.8         358.6       3.0X

        SQL Single Column Scan from wide table (200 columns): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 2268 / 2283          0.5        2162.6       1.0X
        Native ORC Vectorized                          158 /  165          6.6         150.5      14.4X
        Native ORC Vectorized (Java)                   155 /  167          6.8         147.8      14.6X
        Hive built-in ORC                              597 /  601          1.8         569.0       3.8X

        SQL Single Column Scan from wide table (300 columns): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 3377 / 3387          0.3        3220.7       1.0X
        Native ORC Vectorized                          274 /  282          3.8         261.7      12.3X
        Native ORC Vectorized (Java)                   269 /  277          3.9         256.7      12.5X
        Hive built-in ORC                              845 /  858          1.2         805.8       4.0X
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
