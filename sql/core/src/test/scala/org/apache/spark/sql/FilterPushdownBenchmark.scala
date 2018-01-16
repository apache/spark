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
object FilterPushdownBenchmark {
  val conf = new SparkConf()
  conf.set("orc.compression", "snappy")
  conf.set("spark.sql.parquet.compression.codec", "snappy")

  private val spark = SparkSession.builder()
    .master("local[1]")
    .appName("FilterPushdownBenchmark")
    .config(conf)
    .getOrCreate()

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

  private def prepareTable(dir: File, numRows: Int, width: Int): Unit = {
    import spark.implicits._
    val selectExpr = (1 to width).map(i => s"CAST(value AS STRING) c$i")
    val df = spark.range(numRows).map(_ => Random.nextLong).selectExpr(selectExpr: _*)
      .withColumn("id", monotonically_increasing_id())

    val dirORC = dir.getCanonicalPath + "/orc"
    val dirParquet = dir.getCanonicalPath + "/parquet"

    df.write.mode("overwrite").orc(dirORC)
    df.write.mode("overwrite").parquet(dirParquet)

    spark.read.orc(dirORC).createOrReplaceTempView("orcTable")
    spark.read.parquet(dirParquet).createOrReplaceTempView("parquetTable")
  }

  def filterPushDownBenchmark(values: Int, title: String, expr: String): Unit = {
    val benchmark = new Benchmark(title, values, minNumIters = 5)

    Seq(false, true).foreach { pushDownEnabled =>
      val name = s"Parquet Vectorized ${if (pushDownEnabled) s"(Pushdown)" else ""}"
      benchmark.addCase(name) { _ =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> s"$pushDownEnabled") {
          spark.sql(s"SELECT * FROM parquetTable WHERE $expr").collect()
        }
      }
    }

    Seq(false, true).foreach { pushDownEnabled =>
      val name = s"Native ORC Vectorized ${if (pushDownEnabled) s"(Pushdown)" else ""}"
      benchmark.addCase(name) { _ =>
        withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> s"$pushDownEnabled") {
          spark.sql(s"SELECT * FROM orcTable WHERE $expr").collect()
        }
      }
    }

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.2
    Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

    Select 0 row (id IS NULL):              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            2091 / 2258          0.5        1993.9       1.0X
    Parquet Vectorized (Pushdown)                   41 /   44         25.6          39.0      51.1X
    Native ORC Vectorized                         1625 / 1648          0.6        1549.6       1.3X
    Native ORC Vectorized (Pushdown)                45 /   47         23.5          42.5      46.9X

    Select 0 row (524288 < id < 524288):    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            2202 / 2294          0.5        2099.7       1.0X
    Parquet Vectorized (Pushdown)                  734 /  844          1.4         699.9       3.0X
    Native ORC Vectorized                         1632 / 1659          0.6        1556.0       1.3X
    Native ORC Vectorized (Pushdown)                94 /   98         11.2          89.6      23.4X

    Select 1 row (id = 524288):             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            2113 / 2160          0.5        2015.3       1.0X
    Parquet Vectorized (Pushdown)                  711 /  790          1.5         677.7       3.0X
    Native ORC Vectorized                         1612 / 1657          0.7        1537.2       1.3X
    Native ORC Vectorized (Pushdown)                92 /   95         11.4          87.7      23.0X

    Select 1 row (id <=> 524288):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            2105 / 2149          0.5        2007.9       1.0X
    Parquet Vectorized (Pushdown)                  712 /  794          1.5         679.2       3.0X
    Native ORC Vectorized                         1619 / 1655          0.6        1543.7       1.3X
    Native ORC Vectorized (Pushdown)                90 /   93         11.6          85.9      23.4X

    Select 1 row (524288 <= id <= 524288):  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            2081 / 2120          0.5        1984.8       1.0X
    Parquet Vectorized (Pushdown)                  700 /  793          1.5         667.5       3.0X
    Native ORC Vectorized                         1618 / 1653          0.6        1542.7       1.3X
    Native ORC Vectorized (Pushdown)                91 /   94         11.5          86.6      22.9X

    Select 1 row (524287 < id < 524289):    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            2094 / 2127          0.5        1997.3       1.0X
    Parquet Vectorized (Pushdown)                  714 /  792          1.5         680.8       2.9X
    Native ORC Vectorized                         1621 / 1644          0.6        1546.3       1.3X
    Native ORC Vectorized (Pushdown)                90 /   94         11.6          86.1      23.2X

    Select 10% rows (id < 104857):          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            2498 / 2591          0.4        2381.9       1.0X
    Parquet Vectorized (Pushdown)                 1047 / 1082          1.0         998.2       2.4X
    Native ORC Vectorized                         1986 / 2119          0.5        1893.8       1.3X
    Native ORC Vectorized (Pushdown)               552 /  582          1.9         526.1       4.5X

    Select 50% rows (id < 524288):          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            4321 / 5021          0.2        4121.3       1.0X
    Parquet Vectorized (Pushdown)                 3967 / 4183          0.3        3783.6       1.1X
    Native ORC Vectorized                         4107 / 4565          0.3        3916.9       1.1X
    Native ORC Vectorized (Pushdown)              2983 / 3861          0.4        2844.5       1.4X

    Select 90% rows (id < 943718):          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            6815 / 7287          0.2        6499.0       1.0X
    Parquet Vectorized (Pushdown)                 6891 / 7220          0.2        6571.5       1.0X
    Native ORC Vectorized                         7337 / 7565          0.1        6997.1       0.9X
    Native ORC Vectorized (Pushdown)              7274 / 7523          0.1        6936.6       0.9X

    Select all rows (id IS NOT NULL):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7321 / 7380          0.1        6981.5       1.0X
    Parquet Vectorized (Pushdown)                 7352 / 7398          0.1        7011.2       1.0X
    Native ORC Vectorized                         7386 / 7660          0.1        7043.9       1.0X
    Native ORC Vectorized (Pushdown)              7629 / 7705          0.1        7275.9       1.0X

    Select all rows (id > -1):              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7125 / 7384          0.1        6795.2       1.0X
    Parquet Vectorized (Pushdown)                 7334 / 7390          0.1        6994.3       1.0X
    Native ORC Vectorized                         7517 / 7642          0.1        7168.7       0.9X
    Native ORC Vectorized (Pushdown)              7323 / 7601          0.1        6983.7       1.0X

    Select all rows (id != -1):             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7281 / 7850          0.1        6944.0       1.0X
    Parquet Vectorized (Pushdown)                 7311 / 7939          0.1        6972.7       1.0X
    Native ORC Vectorized                         7530 / 7748          0.1        7181.4       1.0X
    Native ORC Vectorized (Pushdown)              7309 / 7667          0.1        6970.2       1.0X
    */
    benchmark.run()
  }

  def main(args: Array[String]): Unit = {
    val numRows = 1024 * 1024
    val width = 20
    val mid = numRows / 2

    withTempPath { dir =>
      withTempTable("orcTable", "patquetTable") {
        prepareTable(dir, numRows, width)

        Seq("id IS NULL", s"$mid < id AND id < $mid").foreach { expr =>
          val title = s"Select 0 row ($expr)".replace("id AND id", "id")
          filterPushDownBenchmark(numRows, title, expr)
        }

        Seq(
          s"id = $mid",
          s"id <=> $mid",
          s"$mid <= id AND id <= $mid",
          s"${mid - 1} < id AND id < ${mid + 1}"
        ).foreach { expr =>
          val title = s"Select 1 row ($expr)".replace("id AND id", "id")
          filterPushDownBenchmark(numRows, title, expr)
        }

        Seq(10, 50, 90).foreach { percent =>
          filterPushDownBenchmark(numRows,
            s"Select $percent% rows (id < ${numRows * percent / 100})",
            s"id < ${numRows * percent / 100}")
        }

        Seq("id IS NOT NULL", "id > -1", "id != -1").foreach { expr =>
          filterPushDownBenchmark(numRows, s"Select all rows ($expr)", expr)
        }
      }
    }
  }
}
