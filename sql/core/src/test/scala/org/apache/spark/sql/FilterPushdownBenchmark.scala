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

  def filterPushDownBenchmark(
      values: Int,
      title: String,
      whereExpr: String,
      selectExpr: String = "*"): Unit = {
    val benchmark = new Benchmark(title, values, minNumIters = 5)

    Seq(false, true).foreach { pushDownEnabled =>
      val name = s"Parquet Vectorized ${if (pushDownEnabled) s"(Pushdown)" else ""}"
      benchmark.addCase(name) { _ =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> s"$pushDownEnabled") {
          spark.sql(s"SELECT $selectExpr FROM parquetTable WHERE $whereExpr").collect()
        }
      }
    }

    Seq(false, true).foreach { pushDownEnabled =>
      val name = s"Native ORC Vectorized ${if (pushDownEnabled) s"(Pushdown)" else ""}"
      benchmark.addCase(name) { _ =>
        withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> s"$pushDownEnabled") {
          spark.sql(s"SELECT $selectExpr FROM orcTable WHERE $whereExpr").collect()
        }
      }
    }

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_152-b16 on Mac OS X 10.13.2
    Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

    Select 0 row (id IS NULL):              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7906 / 7955          2.0         502.6       1.0X
    Parquet Vectorized (Pushdown)                   56 /   60        281.1           3.6     141.3X
    Native ORC Vectorized                         5655 / 5700          2.8         359.5       1.4X
    Native ORC Vectorized (Pushdown)                68 /   71        233.0           4.3     117.1X

    Select 0 row (7864320 < id < 7864320):  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7891 / 7922          2.0         501.7       1.0X
    Parquet Vectorized (Pushdown)                  746 /  769         21.1          47.5      10.6X
    Native ORC Vectorized                         5645 / 5686          2.8         358.9       1.4X
    Native ORC Vectorized (Pushdown)                82 /   84        192.9           5.2      96.8X

    Select 1 row (id = 7864320):            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7963 / 8069          2.0         506.3       1.0X
    Parquet Vectorized (Pushdown)                  752 /  778         20.9          47.8      10.6X
    Native ORC Vectorized                         5726 / 5789          2.7         364.1       1.4X
    Native ORC Vectorized (Pushdown)                78 /   81        201.4           5.0     102.0X

    Select 1 row (id <=> 7864320):          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7983 / 8015          2.0         507.5       1.0X
    Parquet Vectorized (Pushdown)                  753 /  774         20.9          47.9      10.6X
    Native ORC Vectorized                         5772 / 5814          2.7         367.0       1.4X
    Native ORC Vectorized (Pushdown)                76 /   78        207.3           4.8     105.2X

    Select 1 row (7864320 <= id <= 7864320):Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7929 / 7999          2.0         504.1       1.0X
    Parquet Vectorized (Pushdown)                  747 /  770         21.1          47.5      10.6X
    Native ORC Vectorized                         5756 / 5810          2.7         366.0       1.4X
    Native ORC Vectorized (Pushdown)                76 /   79        206.4           4.8     104.0X

    Select 1 row (7864319 < id < 7864321):  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7968 / 8027          2.0         506.6       1.0X
    Parquet Vectorized (Pushdown)                  750 /  771         21.0          47.7      10.6X
    Native ORC Vectorized                         5776 / 5811          2.7         367.2       1.4X
    Native ORC Vectorized (Pushdown)                75 /   78        208.5           4.8     105.6X

    Select 10% rows (id < 1572864):         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            8156 / 8257          1.9         518.5       1.0X
    Parquet Vectorized (Pushdown)                 1620 / 1684          9.7         103.0       5.0X
    Native ORC Vectorized                         5951 / 5990          2.6         378.3       1.4X
    Native ORC Vectorized (Pushdown)               803 /  810         19.6          51.0      10.2X

    Select 50% rows (id < 7864320):         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            8690 / 8717          1.8         552.5       1.0X
    Parquet Vectorized (Pushdown)                 5067 / 5099          3.1         322.2       1.7X
    Native ORC Vectorized                         6530 / 6552          2.4         415.1       1.3X
    Native ORC Vectorized (Pushdown)              3630 / 3670          4.3         230.8       2.4X

    Select 90% rows (id < 14155776):        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            9241 / 9293          1.7         587.5       1.0X
    Parquet Vectorized (Pushdown)                 8474 / 8505          1.9         538.8       1.1X
    Native ORC Vectorized                         7080 / 7107          2.2         450.1       1.3X
    Native ORC Vectorized (Pushdown)              6507 / 6552          2.4         413.7       1.4X

    Select all rows (id IS NOT NULL):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            9317 / 9366          1.7         592.4       1.0X
    Parquet Vectorized (Pushdown)                 9316 / 9367          1.7         592.3       1.0X
    Native ORC Vectorized                         7148 / 7210          2.2         454.5       1.3X
    Native ORC Vectorized (Pushdown)              7092 / 7152          2.2         450.9       1.3X

    Select all rows (id > -1):              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            9307 / 9353          1.7         591.7       1.0X
    Parquet Vectorized (Pushdown)                 9303 / 9340          1.7         591.5       1.0X
    Native ORC Vectorized                         7192 / 7249          2.2         457.2       1.3X
    Native ORC Vectorized (Pushdown)              7182 / 7216          2.2         456.6       1.3X

    Select all rows (id != -1):             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            9145 / 9328          1.7         581.4       1.0X
    Parquet Vectorized (Pushdown)                 9320 / 9368          1.7         592.5       1.0X
    Native ORC Vectorized                         7202 / 7230          2.2         457.9       1.3X
    Native ORC Vectorized (Pushdown)              7170 / 7206          2.2         455.9       1.3X
    */
    benchmark.run()
  }

  def main(args: Array[String]): Unit = {
    val numRows = 1024 * 1024 * 15
    val width = 5
    val mid = numRows / 2

    withTempPath { dir =>
      withTempTable("orcTable", "patquetTable") {
        prepareTable(dir, numRows, width)

        Seq("id IS NULL", s"$mid < id AND id < $mid").foreach { whereExpr =>
          val title = s"Select 0 row ($whereExpr)".replace("id AND id", "id")
          filterPushDownBenchmark(numRows, title, whereExpr)
        }

        Seq(
          s"id = $mid",
          s"id <=> $mid",
          s"$mid <= id AND id <= $mid",
          s"${mid - 1} < id AND id < ${mid + 1}"
        ).foreach { whereExpr =>
          val title = s"Select 1 row ($whereExpr)".replace("id AND id", "id")
          filterPushDownBenchmark(numRows, title, whereExpr)
        }

        val selectExpr = (1 to width).map(i => s"LENGTH(c$i)").mkString("SUM(", "+", ")")

        Seq(10, 50, 90).foreach { percent =>
          filterPushDownBenchmark(
            numRows,
            s"Select $percent% rows (id < ${numRows * percent / 100})",
            s"id < ${numRows * percent / 100}",
            selectExpr
          )
        }

        Seq("id IS NOT NULL", "id > -1", "id != -1").foreach { whereExpr =>
          filterPushDownBenchmark(
            numRows,
            s"Select all rows ($whereExpr)",
            whereExpr,
            selectExpr)
        }
      }
    }
  }
}
