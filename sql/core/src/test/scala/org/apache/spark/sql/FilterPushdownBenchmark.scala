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
    Parquet Vectorized                            7882 / 7957          2.0         501.1       1.0X
    Parquet Vectorized (Pushdown)                   55 /   60        285.2           3.5     142.9X
    Native ORC Vectorized                         5592 / 5627          2.8         355.5       1.4X
    Native ORC Vectorized (Pushdown)                66 /   70        237.2           4.2     118.9X

    Select 0 row (7864320 < id < 7864320):  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7884 / 7909          2.0         501.2       1.0X
    Parquet Vectorized (Pushdown)                  739 /  752         21.3          47.0      10.7X
    Native ORC Vectorized                         5614 / 5646          2.8         356.9       1.4X
    Native ORC Vectorized (Pushdown)                81 /   83        195.2           5.1      97.8X

    Select 1 row (id = 7864320):            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7905 / 8027          2.0         502.6       1.0X
    Parquet Vectorized (Pushdown)                  740 /  766         21.2          47.1      10.7X
    Native ORC Vectorized                         5684 / 5738          2.8         361.4       1.4X
    Native ORC Vectorized (Pushdown)                78 /   81        202.4           4.9     101.7X

    Select 1 row (id <=> 7864320):          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7928 / 7993          2.0         504.1       1.0X
    Parquet Vectorized (Pushdown)                  747 /  772         21.0          47.5      10.6X
    Native ORC Vectorized                         5728 / 5753          2.7         364.2       1.4X
    Native ORC Vectorized (Pushdown)                76 /   78        207.9           4.8     104.8X

    Select 1 row (7864320 <= id <= 7864320):Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7939 / 8021          2.0         504.8       1.0X
    Parquet Vectorized (Pushdown)                  746 /  770         21.1          47.4      10.6X
    Native ORC Vectorized                         5690 / 5734          2.8         361.7       1.4X
    Native ORC Vectorized (Pushdown)                76 /   79        206.7           4.8     104.3X

    Select 1 row (7864319 < id < 7864321):  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            7972 / 8019          2.0         506.9       1.0X
    Parquet Vectorized (Pushdown)                  742 /  764         21.2          47.2      10.7X
    Native ORC Vectorized                         5704 / 5743          2.8         362.6       1.4X
    Native ORC Vectorized (Pushdown)                76 /   78        207.9           4.8     105.4X

    Select 10% rows (id < 1572864):         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                            8733 / 8808          1.8         555.2       1.0X
    Parquet Vectorized (Pushdown)                 2213 / 2267          7.1         140.7       3.9X
    Native ORC Vectorized                         6420 / 6463          2.4         408.2       1.4X
    Native ORC Vectorized (Pushdown)              1313 / 1331         12.0          83.5       6.7X

    Select 50% rows (id < 7864320):         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                          11518 / 11591          1.4         732.3       1.0X
    Parquet Vectorized (Pushdown)                 7962 / 7991          2.0         506.2       1.4X
    Native ORC Vectorized                         8927 / 8985          1.8         567.6       1.3X
    Native ORC Vectorized (Pushdown)              6102 / 6160          2.6         387.9       1.9X

    Select 90% rows (id < 14155776):        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                          14255 / 14389          1.1         906.3       1.0X
    Parquet Vectorized (Pushdown)               13564 / 13594          1.2         862.4       1.1X
    Native ORC Vectorized                       11442 / 11608          1.4         727.5       1.2X
    Native ORC Vectorized (Pushdown)            10991 / 11029          1.4         698.8       1.3X

    Select all rows (id IS NOT NULL):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                          14917 / 14938          1.1         948.4       1.0X
    Parquet Vectorized (Pushdown)               14910 / 14964          1.1         948.0       1.0X
    Native ORC Vectorized                       11986 / 12069          1.3         762.0       1.2X
    Native ORC Vectorized (Pushdown)            12037 / 12123          1.3         765.3       1.2X

    Select all rows (id > -1):              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                          14951 / 14976          1.1         950.6       1.0X
    Parquet Vectorized (Pushdown)               14934 / 15016          1.1         949.5       1.0X
    Native ORC Vectorized                       12000 / 12156          1.3         763.0       1.2X
    Native ORC Vectorized (Pushdown)            12079 / 12113          1.3         767.9       1.2X

    Select all rows (id != -1):             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -----------------------------------------------------------------------------------------------
    Parquet Vectorized                          14930 / 14972          1.1         949.3       1.0X
    Parquet Vectorized (Pushdown)               15015 / 15047          1.0         954.6       1.0X
    Native ORC Vectorized                       12090 / 12259          1.3         768.7       1.2X
    Native ORC Vectorized (Pushdown)            12021 / 12096          1.3         764.2       1.2X
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

        val selectExpr = (1 to width).map(i => s"MAX(c$i)").mkString("", ",", ", MAX(id)")

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
