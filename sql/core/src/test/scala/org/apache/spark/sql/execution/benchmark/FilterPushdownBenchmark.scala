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

package org.apache.spark.sql.execution.benchmark

import java.io.File

import scala.util.{Random, Try}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{Benchmark, Utils}


/**
 * Benchmark to measure read performance with Filter pushdown.
 * To run this:
 *  spark-submit --class <this class> <spark sql test jar>
 */
object FilterPushdownBenchmark {
  val conf = new SparkConf()
    .setAppName("FilterPushdownBenchmark")
    .setIfMissing("spark.master", "local[1]")
    .setIfMissing("spark.driver.memory", "3g")
    .setIfMissing("spark.executor.memory", "3g")
    .setIfMissing("orc.compression", "snappy")
    .setIfMissing("spark.sql.parquet.compression.codec", "snappy")

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
    // To always turn on dictionary encoding, we set 1.0 at the threshold (the default is 0.8)
    df.write.mode("overwrite").option("orc.dictionary.key.threshold", 1.0).orc(dir)
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
    OpenJDK 64-Bit Server VM 1.8.0_171-b10 on Linux 4.14.33-51.37.amzn1.x86_64
    Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
    Select 0 string row (value IS NULL):     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3354 / 3431          4.7         213.2       1.0X
    Parquet Vectorized (Pushdown)                  131 /  147        119.9           8.3      25.6X
    Native ORC Vectorized                         5244 / 5288          3.0         333.4       0.6X
    Native ORC Vectorized (Pushdown)               147 /  168        107.1           9.3      22.8X


    Select 0 string row
    ('7864320' < value < '7864320'): Best/Avg Time(ms)            Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3475 / 3564          4.5         221.0       1.0X
    Parquet Vectorized (Pushdown)                  153 /  171        102.7           9.7      22.7X
    Native ORC Vectorized                         5478 / 5675          2.9         348.3       0.6X
    Native ORC Vectorized (Pushdown)               173 /  180         90.9          11.0      20.1X


    Select 1 string row (value = '7864320'): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3273 / 3373          4.8         208.1       1.0X
    Parquet Vectorized (Pushdown)                  132 /  145        118.9           8.4      24.8X
    Native ORC Vectorized                         5546 / 5610          2.8         352.6       0.6X
    Native ORC Vectorized (Pushdown)               169 /  177         93.2          10.7      19.4X


    Select 1 string row
    (value <=> '7864320'):                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3358 / 3414          4.7         213.5       1.0X
    Parquet Vectorized (Pushdown)                  123 /  134        128.2           7.8      27.4X
    Native ORC Vectorized                         5453 / 5652          2.9         346.7       0.6X
    Native ORC Vectorized (Pushdown)               170 /  179         92.4          10.8      19.7X


    Select 1 string row
    ('7864320' <= value <= '7864320'):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3604 / 3652          4.4         229.1       1.0X
    Parquet Vectorized (Pushdown)                  147 /  157        106.8           9.4      24.5X
    Native ORC Vectorized                         5795 / 5935          2.7         368.5       0.6X
    Native ORC Vectorized (Pushdown)               180 /  189         87.6          11.4      20.1X


    Select all string rows
    (value IS NOT NULL):                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8780 / 9055          1.8         558.2       1.0X
    Parquet Vectorized (Pushdown)                 9010 / 9228          1.7         572.9       1.0X
    Native ORC Vectorized                       12970 / 13162          1.2         824.6       0.7X
    Native ORC Vectorized (Pushdown)            12785 / 13283          1.2         812.8       0.7X


    Select 0 int row (value IS NULL):        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2740 / 2880          5.7         174.2       1.0X
    Parquet Vectorized (Pushdown)                   88 /  103        178.5           5.6      31.1X
    Native ORC Vectorized                         4786 / 4870          3.3         304.3       0.6X
    Native ORC Vectorized (Pushdown)               125 /  137        126.2           7.9      22.0X


    Select 0 int row
    (7864320 < value < 7864320):             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2852 / 2896          5.5         181.3       1.0X
    Parquet Vectorized (Pushdown)                  140 /  148        112.3           8.9      20.4X
    Native ORC Vectorized                         4743 / 4931          3.3         301.5       0.6X
    Native ORC Vectorized (Pushdown)               155 /  168        101.3           9.9      18.4X

    Select 1 int row (value = 7864320):      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2859 / 2896          5.5         181.8       1.0X
    Parquet Vectorized (Pushdown)                  136 /  142        115.7           8.6      21.0X
    Native ORC Vectorized                         4942 / 5069          3.2         314.2       0.6X
    Native ORC Vectorized (Pushdown)               155 /  164        101.2           9.9      18.4X


    Select 1 int row (value <=> 7864320):    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2863 / 2914          5.5         182.0       1.0X
    Parquet Vectorized (Pushdown)                  125 /  133        125.9           7.9      22.9X
    Native ORC Vectorized                         4885 / 4980          3.2         310.6       0.6X
    Native ORC Vectorized (Pushdown)               146 /  158        107.6           9.3      19.6X


    Select 1 int row
    (7864320 <= value <= 7864320):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2863 / 2926          5.5         182.0       1.0X
    Parquet Vectorized (Pushdown)                  135 /  145        116.1           8.6      21.1X
    Native ORC Vectorized                         4924 / 5154          3.2         313.1       0.6X
    Native ORC Vectorized (Pushdown)               156 /  166        101.1           9.9      18.4X


    Select 1 int row
    (7864319 < value < 7864321):             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2856 / 2886          5.5         181.6       1.0X
    Parquet Vectorized (Pushdown)                  131 /  143        120.4           8.3      21.9X
    Native ORC Vectorized                         5167 / 5258          3.0         328.5       0.6X
    Native ORC Vectorized (Pushdown)               152 /  158        103.8           9.6      18.9X


    Select 10% int rows (value < 1572864):   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3439 / 3493          4.6         218.6       1.0X
    Parquet Vectorized (Pushdown)                  931 /  952         16.9          59.2       3.7X
    Native ORC Vectorized                         5653 / 5734          2.8         359.4       0.6X
    Native ORC Vectorized (Pushdown)              1405 / 1416         11.2          89.3       2.4X


    Select 50% int rows (value < 7864320):   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3545 / 3626          4.4         225.4       1.0X
    Parquet Vectorized (Pushdown)                 1068 / 1153         14.7          67.9       3.3X
    Native ORC Vectorized                         5578 / 5829          2.8         354.6       0.6X
    Native ORC Vectorized (Pushdown)              1502 / 1550         10.5          95.5       2.4X


    Select 90% int rows (value < 14155776):  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3567 / 3608          4.4         226.8       1.0X
    Parquet Vectorized (Pushdown)                 1072 / 1152         14.7          68.1       3.3X
    Native ORC Vectorized                         5713 / 5845          2.8         363.2       0.6X
    Native ORC Vectorized (Pushdown)              1571 / 1686         10.0          99.9       2.3X


    Select all int rows (value IS NOT NULL): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8099 / 8360          1.9         514.9       1.0X
    Parquet Vectorized (Pushdown)                 8257 / 8552          1.9         525.0       1.0X
    Native ORC Vectorized                       12113 / 12390          1.3         770.1       0.7X
    Native ORC Vectorized (Pushdown)            12485 / 12595          1.3         793.8       0.6X


    Select all int rows (value > -1):        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8201 / 8348          1.9         521.4       1.0X
    Parquet Vectorized (Pushdown)                 8410 / 8561          1.9         534.7       1.0X
    Native ORC Vectorized                       12408 / 12545          1.3         788.9       0.7X
    Native ORC Vectorized (Pushdown)            12403 / 12489          1.3         788.6       0.7X


    Select all int rows (value != -1):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8133 / 8526          1.9         517.1       1.0X
    Parquet Vectorized (Pushdown)                 8301 / 8401          1.9         527.8       1.0X
    Native ORC Vectorized                       12411 / 12809          1.3         789.1       0.7X
    Native ORC Vectorized (Pushdown)            12316 / 12605          1.3         783.0       0.7X


    Select 0 distinct string row
    (value IS NULL):                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2800 / 2886          5.6         178.0       1.0X
    Parquet Vectorized (Pushdown)                   84 /   99        187.0           5.3      33.3X
    Native ORC Vectorized                         4593 / 4709          3.4         292.0       0.6X
    Native ORC Vectorized (Pushdown)               122 /  129        128.5           7.8      22.9X


    Select 0 distinct string row
    ('100' < value < '100'):                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2870 / 2954          5.5         182.5       1.0X
    Parquet Vectorized (Pushdown)                  109 /  130        144.4           6.9      26.4X
    Native ORC Vectorized                         4794 / 5000          3.3         304.8       0.6X
    Native ORC Vectorized (Pushdown)               128 /  135        122.5           8.2      22.3X


    Select 1 distinct string row
    (value = '100'):                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3008 / 3095          5.2         191.3       1.0X
    Parquet Vectorized (Pushdown)                  328 /  348         47.9          20.9       9.2X
    Native ORC Vectorized                         4997 / 5101          3.1         317.7       0.6X
    Native ORC Vectorized (Pushdown)               406 /  416         38.7          25.8       7.4X


    Select 1 distinct string row
    (value <=> '100'):                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3048 / 3150          5.2         193.8       1.0X
    Parquet Vectorized (Pushdown)                  326 /  341         48.3          20.7       9.4X
    Native ORC Vectorized                         4979 / 5008          3.2         316.5       0.6X
    Native ORC Vectorized (Pushdown)               396 /  401         39.7          25.2       7.7X


    Select 1 distinct string row
    ('100' <= value <= '100'):               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3123 / 3213          5.0         198.5       1.0X
    Parquet Vectorized (Pushdown)                  337 /  352         46.7          21.4       9.3X
    Native ORC Vectorized                         5211 / 5247          3.0         331.3       0.6X
    Native ORC Vectorized (Pushdown)               402 /  412         39.1          25.6       7.8X


    Select all distinct string rows
    (value IS NOT NULL):                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7995 / 8324          2.0         508.3       1.0X
    Parquet Vectorized (Pushdown)                 8094 / 8333          1.9         514.6       1.0X
    Native ORC Vectorized                       11501 / 11872          1.4         731.2       0.7X
    Native ORC Vectorized (Pushdown)            11436 / 11904          1.4         727.1       0.7X
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
