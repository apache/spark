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
    .setMaster("local[1]")
    .setAppName("FilterPushdownBenchmark")
    .set("spark.driver.memory", "3g")
    .set("spark.executor.memory", "3g")
    .set("orc.compression", "snappy")
    .set("spark.sql.parquet.compression.codec", "snappy")

  private val spark = SparkSession.builder().config(conf).getOrCreate()

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

  private def prepareTable(
      dir: File, numRows: Int, width: Int, useStringForValue: Boolean): Unit = {
    import spark.implicits._
    val selectExpr = (1 to width).map(i => s"CAST(value AS STRING) c$i")
    val valueCol = if (useStringForValue) {
      monotonically_increasing_id().cast("string")
    } else {
      monotonically_increasing_id()
    }
    val df = spark.range(numRows).map(_ => Random.nextLong).selectExpr(selectExpr: _*)
      .withColumn("value", valueCol)
      .sort("value")

    saveAsOrcTable(df, dir.getCanonicalPath + "/orc")
    saveAsParquetTable(df, dir.getCanonicalPath + "/parquet")
  }

  private def prepareStringDictTable(
      dir: File, numRows: Int, numDistinctValues: Int, width: Int): Unit = {
    val selectExpr = (0 to width).map {
      case 0 => s"CAST(id % $numDistinctValues AS STRING) AS value"
      case i => s"CAST(rand() AS STRING) c$i"
    }
    val df = spark.range(numRows).selectExpr(selectExpr: _*).sort("value")

    saveAsOrcTable(df, dir.getCanonicalPath + "/orc")
    saveAsParquetTable(df, dir.getCanonicalPath + "/parquet")
  }

  private def saveAsOrcTable(df: DataFrame, dir: String): Unit = {
    df.write.mode("overwrite").orc(dir)
    spark.read.orc(dir).createOrReplaceTempView("orcTable")
  }

  private def saveAsParquetTable(df: DataFrame, dir: String): Unit = {
    df.write.mode("overwrite").parquet(dir)
    spark.read.parquet(dir).createOrReplaceTempView("parquetTable")
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
    Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
    Select 0 string row (value IS NULL):     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8452 / 8504          1.9         537.3       1.0X
    Parquet Vectorized (Pushdown)                  274 /  281         57.3          17.4      30.8X
    Native ORC Vectorized                         8167 / 8185          1.9         519.3       1.0X
    Native ORC Vectorized (Pushdown)               365 /  379         43.1          23.2      23.1X


    Select 0 string row
    ('7864320' < value < '7864320'):         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8532 / 8564          1.8         542.4       1.0X
    Parquet Vectorized (Pushdown)                  366 /  386         43.0          23.3      23.3X
    Native ORC Vectorized                         8289 / 8300          1.9         527.0       1.0X
    Native ORC Vectorized (Pushdown)               378 /  385         41.6          24.0      22.6X


    Select 1 string row (value = '7864320'): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8547 / 8564          1.8         543.4       1.0X
    Parquet Vectorized (Pushdown)                  351 /  356         44.9          22.3      24.4X
    Native ORC Vectorized                         8310 / 8323          1.9         528.3       1.0X
    Native ORC Vectorized (Pushdown)               370 /  375         42.5          23.5      23.1X


    Select 1 string row
    (value <=> '7864320'):                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8537 / 8563          1.8         542.8       1.0X
    Parquet Vectorized (Pushdown)                  310 /  319         50.7          19.7      27.5X
    Native ORC Vectorized                         8316 / 8335          1.9         528.7       1.0X
    Native ORC Vectorized (Pushdown)               364 /  367         43.2          23.1      23.5X


    Select 1 string row
    ('7864320' <= value <= '7864320'):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8594 / 8607          1.8         546.4       1.0X
    Parquet Vectorized (Pushdown)                  370 /  374         42.5          23.5      23.2X
    Native ORC Vectorized                         8350 / 8358          1.9         530.9       1.0X
    Native ORC Vectorized (Pushdown)               371 /  374         42.4          23.6      23.2X


    Select all string rows
    (value IS NOT NULL):                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          19601 / 19625          0.8        1246.2       1.0X
    Parquet Vectorized (Pushdown)               19698 / 19703          0.8        1252.3       1.0X
    Native ORC Vectorized                       19435 / 19470          0.8        1235.6       1.0X
    Native ORC Vectorized (Pushdown)            19568 / 19590          0.8        1244.1       1.0X


    Select 0 int row (value IS NULL):        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7815 / 7824          2.0         496.9       1.0X
    Parquet Vectorized (Pushdown)                  245 /  251         64.2          15.6      31.9X
    Native ORC Vectorized                         7436 / 7460          2.1         472.8       1.1X
    Native ORC Vectorized (Pushdown)               344 /  351         45.7          21.9      22.7X


    Select 0 int row
    (7864320 < value < 7864320):             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7792 / 7807          2.0         495.4       1.0X
    Parquet Vectorized (Pushdown)                  349 /  353         45.1          22.2      22.3X
    Native ORC Vectorized                         7451 / 7465          2.1         473.7       1.0X
    Native ORC Vectorized (Pushdown)               365 /  368         43.0          23.2      21.3X


    Select 1 int row (value = 7864320):      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7836 / 7872          2.0         498.2       1.0X
    Parquet Vectorized (Pushdown)                  322 /  327         48.8          20.5      24.3X
    Native ORC Vectorized                         7533 / 7540          2.1         478.9       1.0X
    Native ORC Vectorized (Pushdown)               358 /  363         43.9          22.8      21.9X


    Select 1 int row (value <=> 7864320):    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7855 / 7870          2.0         499.4       1.0X
    Parquet Vectorized (Pushdown)                  286 /  297         54.9          18.2      27.4X
    Native ORC Vectorized                         7511 / 7557          2.1         477.5       1.0X
    Native ORC Vectorized (Pushdown)               358 /  361         43.9          22.8      21.9X


    Select 1 int row
    (7864320 <= value <= 7864320):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7851 / 7870          2.0         499.2       1.0X
    Parquet Vectorized (Pushdown)                  345 /  347         45.6          21.9      22.8X
    Native ORC Vectorized                         7543 / 7554          2.1         479.6       1.0X
    Native ORC Vectorized (Pushdown)               364 /  374         43.2          23.1      21.6X


    Select 1 int row
    (7864319 < value < 7864321):             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7837 / 7840          2.0         498.2       1.0X
    Parquet Vectorized (Pushdown)                  338 /  339         46.6          21.5      23.2X
    Native ORC Vectorized                         7524 / 7541          2.1         478.3       1.0X
    Native ORC Vectorized (Pushdown)               361 /  364         43.6          22.9      21.7X


    Select 10% int rows (value < 1572864):   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8864 / 8900          1.8         563.5       1.0X
    Parquet Vectorized (Pushdown)                 2088 / 2095          7.5         132.7       4.2X
    Native ORC Vectorized                         8562 / 8579          1.8         544.3       1.0X
    Native ORC Vectorized (Pushdown)              2127 / 2131          7.4         135.2       4.2X


    Select 50% int rows (value < 7864320):   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          12671 / 12684          1.2         805.6       1.0X
    Parquet Vectorized (Pushdown)                 9032 / 9041          1.7         574.2       1.4X
    Native ORC Vectorized                       12388 / 12411          1.3         787.6       1.0X
    Native ORC Vectorized (Pushdown)              8873 / 8884          1.8         564.1       1.4X


    Select 90% int rows (value < 14155776):  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          16481 / 16495          1.0        1047.8       1.0X
    Parquet Vectorized (Pushdown)               15906 / 15919          1.0        1011.3       1.0X
    Native ORC Vectorized                       16224 / 16254          1.0        1031.5       1.0X
    Native ORC Vectorized (Pushdown)            15632 / 15661          1.0         993.9       1.1X


    Select all int rows (value IS NOT NULL): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          17341 / 17354          0.9        1102.5       1.0X
    Parquet Vectorized (Pushdown)               17463 / 17481          0.9        1110.2       1.0X
    Native ORC Vectorized                       17073 / 17089          0.9        1085.4       1.0X
    Native ORC Vectorized (Pushdown)            17194 / 17232          0.9        1093.2       1.0X


    Select all int rows (value > -1):        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          17452 / 17467          0.9        1109.6       1.0X
    Parquet Vectorized (Pushdown)               17613 / 17630          0.9        1119.8       1.0X
    Native ORC Vectorized                       17259 / 17271          0.9        1097.3       1.0X
    Native ORC Vectorized (Pushdown)            17385 / 17429          0.9        1105.3       1.0X


    Select all int rows (value != -1):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          17363 / 17372          0.9        1103.9       1.0X
    Parquet Vectorized (Pushdown)               17526 / 17535          0.9        1114.2       1.0X
    Native ORC Vectorized                       17052 / 17089          0.9        1084.2       1.0X
    Native ORC Vectorized (Pushdown)            17209 / 17229          0.9        1094.1       1.0X


    Select 0 distinct string row
    (value IS NULL):                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7697 / 7751          2.0         489.4       1.0X
    Parquet Vectorized (Pushdown)                  264 /  284         59.5          16.8      29.1X
    Native ORC Vectorized                         6942 / 6970          2.3         441.4       1.1X
    Native ORC Vectorized (Pushdown)               372 /  381         42.3          23.7      20.7X


    Select 0 distinct string row
    ('100' < value < '100'):                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7983 / 8018          2.0         507.5       1.0X
    Parquet Vectorized (Pushdown)                  334 /  337         47.0          21.3      23.9X
    Native ORC Vectorized                         7307 / 7313          2.2         464.5       1.1X
    Native ORC Vectorized (Pushdown)               363 /  371         43.3          23.1      22.0X


    Select 1 distinct string row
    (value = '100'):                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7882 / 7915          2.0         501.1       1.0X
    Parquet Vectorized (Pushdown)                  504 /  522         31.2          32.1      15.6X
    Native ORC Vectorized                         7143 / 7155          2.2         454.1       1.1X
    Native ORC Vectorized (Pushdown)               555 /  573         28.4          35.3      14.2X


    Select 1 distinct string row
    (value <=> '100'):                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7898 / 7912          2.0         502.1       1.0X
    Parquet Vectorized (Pushdown)                  470 /  481         33.5          29.9      16.8X
    Native ORC Vectorized                         7135 / 7149          2.2         453.6       1.1X
    Native ORC Vectorized (Pushdown)               552 /  557         28.5          35.1      14.3X


    Select 1 distinct string row
    ('100' <= value <= '100'):               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8189 / 8213          1.9         520.7       1.0X
    Parquet Vectorized (Pushdown)                  527 /  534         29.9          33.5      15.5X
    Native ORC Vectorized                         7477 / 7498          2.1         475.3       1.1X
    Native ORC Vectorized (Pushdown)               558 /  566         28.2          35.5      14.7X


    Select all distinct string rows
    (value IS NOT NULL):                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          19462 / 19476          0.8        1237.4       1.0X
    Parquet Vectorized (Pushdown)               19570 / 19582          0.8        1244.2       1.0X
    Native ORC Vectorized                       18577 / 18604          0.8        1181.1       1.0X
    Native ORC Vectorized (Pushdown)            18701 / 18742          0.8        1189.0       1.0X
    */
    benchmark.run()
  }

  private def runIntBenchmark(numRows: Int, width: Int, mid: Int): Unit = {
    Seq("value IS NULL", s"$mid < value AND value < $mid").foreach { whereExpr =>
      val title = s"Select 0 int row ($whereExpr)".replace("value AND value", "value")
      filterPushDownBenchmark(numRows, title, whereExpr)
    }

    Seq(
      s"value = $mid",
      s"value <=> $mid",
      s"$mid <= value AND value <= $mid",
      s"${mid - 1} < value AND value < ${mid + 1}"
    ).foreach { whereExpr =>
      val title = s"Select 1 int row ($whereExpr)".replace("value AND value", "value")
      filterPushDownBenchmark(numRows, title, whereExpr)
    }

    val selectExpr = (1 to width).map(i => s"MAX(c$i)").mkString("", ",", ", MAX(value)")

    Seq(10, 50, 90).foreach { percent =>
      filterPushDownBenchmark(
        numRows,
        s"Select $percent% int rows (value < ${numRows * percent / 100})",
        s"value < ${numRows * percent / 100}",
        selectExpr
      )
    }

    Seq("value IS NOT NULL", "value > -1", "value != -1").foreach { whereExpr =>
      filterPushDownBenchmark(
        numRows,
        s"Select all int rows ($whereExpr)",
        whereExpr,
        selectExpr)
    }
  }

  private def runStringBenchmark(
      numRows: Int, width: Int, searchValue: Int, colType: String): Unit = {
    Seq("value IS NULL", s"'$searchValue' < value AND value < '$searchValue'")
        .foreach { whereExpr =>
      val title = s"Select 0 $colType row ($whereExpr)".replace("value AND value", "value")
      filterPushDownBenchmark(numRows, title, whereExpr)
    }

    Seq(
      s"value = '$searchValue'",
      s"value <=> '$searchValue'",
      s"'$searchValue' <= value AND value <= '$searchValue'"
    ).foreach { whereExpr =>
      val title = s"Select 1 $colType row ($whereExpr)".replace("value AND value", "value")
      filterPushDownBenchmark(numRows, title, whereExpr)
    }

    val selectExpr = (1 to width).map(i => s"MAX(c$i)").mkString("", ",", ", MAX(value)")

    Seq("value IS NOT NULL").foreach { whereExpr =>
      filterPushDownBenchmark(
        numRows,
        s"Select all $colType rows ($whereExpr)",
        whereExpr,
        selectExpr)
    }
  }

  def main(args: Array[String]): Unit = {
    val numRows = 1024 * 1024 * 15
    val width = 5

    // Pushdown for many distinct value case
    withTempPath { dir =>
      val mid = numRows / 2

      withTempTable("orcTable", "patquetTable") {
        Seq(true, false).foreach { useStringForValue =>
          prepareTable(dir, numRows, width, useStringForValue)
          if (useStringForValue) {
            runStringBenchmark(numRows, width, mid, "string")
          } else {
            runIntBenchmark(numRows, width, mid)
          }
        }
      }
    }

    // Pushdown for few distinct value case (use dictionary encoding)
    withTempPath { dir =>
      val numDistinctValues = 200
      val mid = numDistinctValues / 2

      withTempTable("orcTable", "patquetTable") {
        prepareStringDictTable(dir, numRows, numDistinctValues, width)
        runStringBenchmark(numRows, width, mid, "distinct string")
      }
    }
  }
}
