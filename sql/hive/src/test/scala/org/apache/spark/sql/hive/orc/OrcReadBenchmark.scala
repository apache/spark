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
        Native ORC MR                                 1198 / 1236         13.1          76.2       1.0X
        Native ORC Vectorized                          163 /  170         96.4          10.4       7.3X
        Native ORC Vectorized (Java)                   156 /  161        101.1           9.9       7.7X
        Hive built-in ORC                             1508 / 1511         10.4          95.8       0.8X

        SQL Single SMALLINT Column Scan:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1361 / 1391         11.6          86.5       1.0X
        Native ORC Vectorized                          197 /  204         80.0          12.5       6.9X
        Native ORC Vectorized (Java)                   198 /  203         79.5          12.6       6.9X
        Hive built-in ORC                             1670 / 1680          9.4         106.2       0.8X

        SQL Single INT Column Scan:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1643 / 1829          9.6         104.5       1.0X
        Native ORC Vectorized                          232 /  245         67.9          14.7       7.1X
        Native ORC Vectorized (Java)                   225 /  232         69.9          14.3       7.3X
        Hive built-in ORC                             1978 / 1988          8.0         125.8       0.8X

        SQL Single BIGINT Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1504 / 1522         10.5          95.7       1.0X
        Native ORC Vectorized                          288 /  298         54.6          18.3       5.2X
        Native ORC Vectorized (Java)                   281 /  294         55.9          17.9       5.3X
        Hive built-in ORC                             1913 / 1925          8.2         121.6       0.8X

        SQL Single FLOAT Column Scan:            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1452 / 1498         10.8          92.3       1.0X
        Native ORC Vectorized                          324 /  329         48.5          20.6       4.5X
        Native ORC Vectorized (Java)                   323 /  329         48.7          20.5       4.5X
        Hive built-in ORC                             1945 / 2013          8.1         123.7       0.7X

        SQL Single DOUBLE Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 1548 / 1551         10.2          98.4       1.0X
        Native ORC Vectorized                          394 /  399         39.9          25.0       3.9X
        Native ORC Vectorized (Java)                   396 /  406         39.8          25.1       3.9X
        Hive built-in ORC                             2079 / 2098          7.6         132.2       0.7X
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
        Native ORC MR                                 2859 / 2860          3.7         272.6       1.0X
        Native ORC Vectorized                         1223 / 1227          8.6         116.6       2.3X
        Native ORC Vectorized (Java)                  1337 / 1339          7.8         127.5       2.1X
        Hive built-in ORC                             3838 / 3893          2.7         366.0       0.7X
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
        Read data column - Native ORC MR                    1519 / 1546         10.4          96.6       1.0X
        Read data column - Native ORC Vectorized             293 /  301         53.7          18.6       5.2X
        Read data column - Native ORC Vectorized (Java)      295 /  300         53.3          18.8       5.1X
        Read data column - Hive built-in ORC                2120 / 2163          7.4         134.8       0.7X
        Read partition column - Native ORC MR               1077 / 1084         14.6          68.5       1.4X
        Read partition column - Native ORC Vectorized         51 /   55        305.6           3.3      29.5X
        Read partition column - Native ORC Vectorized (Java)  52 /   55        301.2           3.3      29.1X
        Read partition column - Hive built-in ORC           1299 / 1302         12.1          82.6       1.2X
        Read both columns - Native ORC MR                   1699 / 1720          9.3         108.0       0.9X
        Read both columns - Native ORC Vectorized            327 /  333         48.1          20.8       4.6X
        Read both columns - Native ORC Vectorized (Java)     330 /  335         47.6          21.0       4.6X
        Read both columns - Hive built-in ORC               2149 / 2152          7.3         136.6       0.7X
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
        Native ORC MR                                 1403 / 1407          7.5         133.8       1.0X
        Native ORC Vectorized                          349 /  360         30.1          33.2       4.0X
        Native ORC Vectorized (Java)                   360 /  366         29.1          34.3       3.9X
        Hive built-in ORC                             1950 / 1974          5.4         185.9       0.7X
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
        Native ORC MR (0.0%)                          2548 / 2586          4.1         243.0       1.0X
        Native ORC Vectorized (0.0%)                   965 /  972         10.9          92.0       2.6X
        Native ORC Vectorized (0.0%) (Java)            942 /  953         11.1          89.9       2.7X
        Hive built-in ORC (0.0%)                      3899 / 3930          2.7         371.8       0.7X

        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR (0.5%)                          2431 / 2435          4.3         231.9       1.0X
        Native ORC Vectorized (0.5%)                  1220 / 1225          8.6         116.3       2.0X
        Native ORC Vectorized (0.5%) (Java)           1257 / 1261          8.3         119.9       1.9X
        Hive built-in ORC (0.5%)                      2944 / 2956          3.6         280.8       0.8X

        String with Nulls Scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR (0.95%)                         1281 / 1288          8.2         122.2       1.0X
        Native ORC Vectorized (0.95%)                  430 /  434         24.4          41.0       3.0X
        Native ORC Vectorized (0.95%) (Java)           490 /  498         21.4          46.7       2.6X
        Hive built-in ORC (0.95%)                     1641 / 1661          6.4         156.5       0.8X
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
        Native ORC MR                                 1116 / 1124          0.9        1064.1       1.0X
        Native ORC Vectorized                           91 /   99         11.5          87.0      12.2X
        Native ORC Vectorized (Java)                    91 /   98         11.5          87.0      12.2X
        Hive built-in ORC                              383 /  389          2.7         364.9       2.9X

        SQL Single Column Scan from wide table (200 columns): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 2277 / 2283          0.5        2171.2       1.0X
        Native ORC Vectorized                          158 /  166          6.6         150.5      14.4X
        Native ORC Vectorized (Java)                   157 /  165          6.7         149.4      14.5X
        Hive built-in ORC                              588 /  599          1.8         560.8       3.9X

        SQL Single Column Scan from wide table (300 columns): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Native ORC MR                                 3370 / 3382          0.3        3214.1       1.0X
        Native ORC Vectorized                          265 /  279          4.0         252.8      12.7X
        Native ORC Vectorized (Java)                   266 /  277          3.9         253.7      12.7X
        Hive built-in ORC                              845 /  856          1.2         806.0       4.0X
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
