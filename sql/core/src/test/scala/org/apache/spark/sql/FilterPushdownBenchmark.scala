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

package org.apache.spark.sql

import java.io.File

import scala.util.{Random, Try}

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{Benchmark, Utils}


/**
 * Benchmark to measure read performance with Filter pushdown.
 */
// scalastyle:off line.size.limit
object FilterPushdownBenchmark {
  val conf = new SparkConf()
  conf.set("orc.compression", "snappy")
  conf.set("spark.sql.parquet.compression.codec", "snappy")

  private val spark = SparkSession.builder()
    .master("local[1]")
    .appName("FilterPushdownBenchmark")
    .config(conf)
    .getOrCreate()

  // Set default configs. Individual cases will change them if necessary.
  spark.conf.set(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key, "true")
  spark.conf.set(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key, "true")

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

  private def prepareTable(dir: File, df: DataFrame): Unit = {
    val dirORC = dir.getCanonicalPath + "/orc"
    val dirParquet = dir.getCanonicalPath + "/parquet"

    df.write.mode("overwrite").orc(dirORC)
    df.write.mode("overwrite").parquet(dirParquet)

    spark.read.orc(dirORC).createOrReplaceTempView("orcTable")
    spark.read.parquet(dirParquet).createOrReplaceTempView("parquetTable")
  }

  def filterPushDownBenchmark(values: Int, width: Int, expr: String): Unit = {
    val benchmark = new Benchmark(s"Filter Pushdown ($expr)", values)

    withTempPath { dir =>
      withTempTable("t1", "orcTable", "patquetTable") {
        import spark.implicits._
        val selectExpr = (1 to width).map(i => s"CAST(value AS STRING) c$i")
        val df = spark.range(values).map(_ => Random.nextLong).selectExpr(selectExpr: _*)
          .withColumn("id", monotonically_increasing_id())

        df.createOrReplaceTempView("t1")
        prepareTable(dir, spark.sql("SELECT * FROM t1"))

        Seq(false, true).foreach { value =>
          benchmark.addCase(s"Parquet Vectorized ${if (value) s"(Pushdown)" else ""}") { _ =>
            withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> s"$value") {
              spark.sql(s"SELECT * FROM parquetTable WHERE $expr").collect()
            }
          }
        }

        Seq(false, true).foreach { value =>
          benchmark.addCase(s"Native ORC Vectorized ${if (value) s"(Pushdown)" else ""}") { _ =>
            withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> s"$value") {
              spark.sql(s"SELECT * FROM orcTable WHERE $expr").collect()
            }
          }
        }

        // Positive cases: Select one or no rows
        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.2
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        Filter Pushdown (id = 0):                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Parquet Vectorized                            2267 / 2287          0.5        2162.0       1.0X
        Parquet Vectorized (Pushdown)                  735 /  803          1.4         701.1       3.1X
        Native ORC Vectorized                         1708 / 1718          0.6        1629.1       1.3X
        Native ORC Vectorized (Pushdown)                83 /   88         12.7          79.0      27.4X

        Filter Pushdown (id == 0):               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Parquet Vectorized                            2005 / 2123          0.5        1911.7       1.0X
        Parquet Vectorized (Pushdown)                  701 /  773          1.5         668.1       2.9X
        Native ORC Vectorized                         1618 / 1632          0.6        1543.3       1.2X
        Native ORC Vectorized (Pushdown)                77 /   80         13.6          73.6      26.0X

        Filter Pushdown (id <= 0):               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Parquet Vectorized                            2085 / 2165          0.5        1988.0       1.0X
        Parquet Vectorized (Pushdown)                  704 /  769          1.5         671.1       3.0X
        Native ORC Vectorized                         1637 / 1638          0.6        1561.1       1.3X
        Native ORC Vectorized (Pushdown)                76 /   79         13.8          72.4      27.4X

        Filter Pushdown (id < 1):                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Parquet Vectorized                            2069 / 2133          0.5        1972.7       1.0X
        Parquet Vectorized (Pushdown)                  705 /  764          1.5         672.7       2.9X
        Native ORC Vectorized                         1637 / 1651          0.6        1561.3       1.3X
        Native ORC Vectorized (Pushdown)                75 /   77         14.0          71.4      27.6X

        Filter Pushdown (id IS NULL):            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Parquet Vectorized                            2081 / 2123          0.5        1984.4       1.0X
        Parquet Vectorized (Pushdown)                   36 /   37         29.3          34.1      58.1X
        Native ORC Vectorized                         1616 / 1645          0.6        1540.7       1.3X
        Native ORC Vectorized (Pushdown)                41 /   43         25.7          39.0      50.9X
        */

        // Negative cases: Select all rows which means the predicate is always true.
        /*
        Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.2
        Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

        Filter Pushdown (id > -1):               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Parquet Vectorized                            8346 / 8516          0.1        7959.8       1.0X
        Parquet Vectorized (Pushdown)                 8611 / 8630          0.1        8212.4       1.0X
        Native ORC Vectorized                         7700 / 7940          0.1        7343.2       1.1X
        Native ORC Vectorized (Pushdown)              7572 / 7635          0.1        7221.5       1.1X

        Filter Pushdown (id != -1):              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Parquet Vectorized                            8088 / 8297          0.1        7713.2       1.0X
        Parquet Vectorized (Pushdown)                 7110 / 8674          0.1        6780.8       1.1X
        Native ORC Vectorized                         7430 / 7567          0.1        7086.0       1.1X
        Native ORC Vectorized (Pushdown)              7739 / 7832          0.1        7380.9       1.0X

        Filter Pushdown (id IS NOT NULL):        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
        ------------------------------------------------------------------------------------------------
        Parquet Vectorized                            7927 / 8284          0.1        7560.3       1.0X
        Parquet Vectorized (Pushdown)                 7329 / 7332          0.1        6989.6       1.1X
        Native ORC Vectorized                         7928 / 7971          0.1        7560.5       1.0X
        Native ORC Vectorized (Pushdown)              7392 / 7502          0.1        7049.9       1.1X
        */
        benchmark.run()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    // Positive cases: Select one or no rows
    Seq("id = 0", "id == 0", "id <= 0", "id < 1", "id IS NULL").foreach { expr =>
      filterPushDownBenchmark(1024 * 1024 * 1, 20, expr)
    }

    // Negative cases: Select all rows which means the predicate is always true.
    Seq("id > -1", "id != -1", "id IS NOT NULL").foreach { expr =>
      filterPushDownBenchmark(1024 * 1024 * 1, 20, expr)
    }
  }
}
// scalastyle:on line.size.limit
