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
    OpenJDK 64-Bit Server VM 1.8.0_171-b10 on Linux 4.14.26-46.32.amzn1.x86_64
    Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
    Select 0 string row (value IS NULL):     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2961 / 3123          5.3         188.3       1.0X
    Parquet Vectorized (Pushdown)                 3057 / 3121          5.1         194.4       1.0X
    Native ORC Vectorized                         4083 / 4139          3.9         259.6       0.7X
    Native ORC Vectorized (Pushdown)               123 /  140        127.9           7.8      24.1X


    Select 0 string row
    ('7864320' < value < '7864320'):         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3153 / 3235          5.0         200.4       1.0X
    Parquet Vectorized (Pushdown)                 3161 / 3252          5.0         201.0       1.0X
    Native ORC Vectorized                         4254 / 4337          3.7         270.5       0.7X
    Native ORC Vectorized (Pushdown)               138 /  163        114.1           8.8      22.9X


    Select 1 string row
    (value = '7864320'):                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3140 / 3192          5.0         199.7       1.0X
    Parquet Vectorized (Pushdown)                 3136 / 3173          5.0         199.4       1.0X
    Native ORC Vectorized                         4266 / 4348          3.7         271.2       0.7X
    Native ORC Vectorized (Pushdown)               139 /  147        113.5           8.8      22.7X


    Select 1 string row
    (value <=> '7864320'):                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3103 / 3178          5.1         197.3       1.0X
    Parquet Vectorized (Pushdown)                 3131 / 3179          5.0         199.1       1.0X
    Native ORC Vectorized                         4316 / 4342          3.6         274.4       0.7X
    Native ORC Vectorized (Pushdown)               132 /  143        118.9           8.4      23.5X


    Select 1 string row
    ('7864320' <= value <= '7864320'):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3287 / 3311          4.8         209.0       1.0X
    Parquet Vectorized (Pushdown)                 3301 / 3326          4.8         209.9       1.0X
    Native ORC Vectorized                         4432 / 4475          3.5         281.8       0.7X
    Native ORC Vectorized (Pushdown)               140 /  146        112.4           8.9      23.5X


    Select all string rows
    (value IS NOT NULL):                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8005 / 8230          2.0         509.0       1.0X
    Parquet Vectorized (Pushdown)                 8046 / 8112          2.0         511.6       1.0X
    Native ORC Vectorized                       10443 / 10627          1.5         664.0       0.8X
    Native ORC Vectorized (Pushdown)            10695 / 10847          1.5         680.0       0.7X


    Select 0 int row (value IS NULL):        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2797 / 2859          5.6         177.8       1.0X
    Parquet Vectorized (Pushdown)                   66 /   80        238.5           4.2      42.4X
    Native ORC Vectorized                         3837 / 3936          4.1         244.0       0.7X
    Native ORC Vectorized (Pushdown)               113 /  117        139.6           7.2      24.8X


    Select 0 int row
    (7864320 < value < 7864320):             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2754 / 2815          5.7         175.1       1.0X
    Parquet Vectorized (Pushdown)                   91 /  101        173.7           5.8      30.4X
    Native ORC Vectorized                         3858 / 3964          4.1         245.3       0.7X
    Native ORC Vectorized (Pushdown)               134 /  141        117.4           8.5      20.5X


    Select 1 int row (value = 7864320):      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2735 / 2780          5.8         173.9       1.0X
    Parquet Vectorized (Pushdown)                   93 /   98        169.8           5.9      29.5X
    Native ORC Vectorized                         3859 / 3951          4.1         245.3       0.7X
    Native ORC Vectorized (Pushdown)               133 /  142        118.1           8.5      20.5X


    Select 1 int row (value <=> 7864320):    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2709 / 2806          5.8         172.2       1.0X
    Parquet Vectorized (Pushdown)                   87 /   94        180.8           5.5      31.1X
    Native ORC Vectorized                         3787 / 3904          4.2         240.8       0.7X
    Native ORC Vectorized (Pushdown)               128 /  138        122.8           8.1      21.1X


    Select 1 int row
    (7864320 <= value <= 7864320):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2691 / 2829          5.8         171.1       1.0X
    Parquet Vectorized (Pushdown)                   91 /   98        172.9           5.8      29.6X
    Native ORC Vectorized                         3915 / 4071          4.0         248.9       0.7X
    Native ORC Vectorized (Pushdown)               131 /  137        120.2           8.3      20.6X


    Select 1 int row
    (7864319 < value < 7864321):             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2861 / 2880          5.5         181.9       1.0X
    Parquet Vectorized (Pushdown)                   90 /   96        174.7           5.7      31.8X
    Native ORC Vectorized                         3937 / 4029          4.0         250.3       0.7X
    Native ORC Vectorized (Pushdown)               132 /  140        118.7           8.4      21.6X


    Select 10% int rows (value < 1572864):   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3211 / 3361          4.9         204.2       1.0X
    Parquet Vectorized (Pushdown)                  784 /  849         20.1          49.8       4.1X
    Native ORC Vectorized                         4420 / 4462          3.6         281.0       0.7X
    Native ORC Vectorized (Pushdown)              1112 / 1172         14.1          70.7       2.9X


    Select 50% int rows (value < 7864320):   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3329 / 3378          4.7         211.7       1.0X
    Parquet Vectorized (Pushdown)                  962 / 1036         16.3          61.2       3.5X
    Native ORC Vectorized                         4595 / 4640          3.4         292.2       0.7X
    Native ORC Vectorized (Pushdown)              1291 / 1362         12.2          82.1       2.6X


    Select 90% int rows (value < 14155776):  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            3355 / 3403          4.7         213.3       1.0X
    Parquet Vectorized (Pushdown)                  976 / 1037         16.1          62.1       3.4X
    Native ORC Vectorized                         4594 / 4685          3.4         292.1       0.7X
    Native ORC Vectorized (Pushdown)              1220 / 1335         12.9          77.6       2.8X


    Select all int rows (value IS NOT NULL): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7300 / 7426          2.2         464.1       1.0X
    Parquet Vectorized (Pushdown)                 7195 / 7381          2.2         457.4       1.0X
    Native ORC Vectorized                         9677 / 9989          1.6         615.2       0.8X
    Native ORC Vectorized (Pushdown)              9610 / 9830          1.6         611.0       0.8X


    Select all int rows (value > -1):        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7317 / 7452          2.1         465.2       1.0X
    Parquet Vectorized (Pushdown)                 7229 / 7537          2.2         459.6       1.0X
    Native ORC Vectorized                         9553 / 9744          1.6         607.4       0.8X
    Native ORC Vectorized (Pushdown)              9780 / 9996          1.6         621.8       0.7X


    Select all int rows (value != -1):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7378 / 7508          2.1         469.1       1.0X
    Parquet Vectorized (Pushdown)                 7343 / 7512          2.1         466.8       1.0X
    Native ORC Vectorized                         9611 / 9842          1.6         611.1       0.8X
    Native ORC Vectorized (Pushdown)              9564 / 9894          1.6         608.1       0.8X


    Select 0 distinct string row
    (value IS NULL):                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2622 / 2681          6.0         166.7       1.0X
    Parquet Vectorized (Pushdown)                 2637 / 2676          6.0         167.6       1.0X
    Native ORC Vectorized                         3638 / 3702          4.3         231.3       0.7X
    Native ORC Vectorized (Pushdown)               102 /  109        153.6           6.5      25.6X


    Select 0 distinct string row
    ('100' < value < '100'):                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2712 / 2753          5.8         172.4       1.0X
    Parquet Vectorized (Pushdown)                 2637 / 2713          6.0         167.7       1.0X
    Native ORC Vectorized                         3853 / 3930          4.1         244.9       0.7X
    Native ORC Vectorized (Pushdown)               106 /  119        147.9           6.8      25.5X


    Select 1 distinct string row
    (value = '100'):                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2747 / 2878          5.7         174.7       1.0X
    Parquet Vectorized (Pushdown)                 2758 / 2821          5.7         175.3       1.0X
    Native ORC Vectorized                         3807 / 4026          4.1         242.0       0.7X
    Native ORC Vectorized (Pushdown)               316 /  322         49.8          20.1       8.7X


    Select 1 distinct string row
    (value <=> '100'):                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2753 / 2800          5.7         175.0       1.0X
    Parquet Vectorized (Pushdown)                 2747 / 2805          5.7         174.6       1.0X
    Native ORC Vectorized                         3902 / 3967          4.0         248.1       0.7X
    Native ORC Vectorized (Pushdown)               316 /  331         49.8          20.1       8.7X


    Select 1 distinct string row
    ('100' <= value <= '100'):               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            2906 / 2917          5.4         184.8       1.0X
    Parquet Vectorized (Pushdown)                 2819 / 2886          5.6         179.2       1.0X
    Native ORC Vectorized                         4025 / 4102          3.9         255.9       0.7X
    Native ORC Vectorized (Pushdown)               325 /  341         48.4          20.7       8.9X


    Select all distinct string rows
    (value IS NOT NULL):                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            7582 / 7942          2.1         482.0       1.0X
    Parquet Vectorized (Pushdown)                 7450 / 7916          2.1         473.6       1.0X
    Native ORC Vectorized                         9497 / 9784          1.7         603.8       0.8X
    Native ORC Vectorized (Pushdown)              9423 / 9646          1.7         599.1       0.8X
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
