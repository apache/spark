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
    // Since `spark.master` always exists, overrides this value
    .set("spark.master", "local[1]")
    .setIfMissing("spark.driver.memory", "3g")
    .setIfMissing("spark.executor.memory", "3g")
    .setIfMissing("spark.ui.enabled", "false")
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
    Intel(R) Xeon(R) CPU E5-2670 v2 @ 2.50GHz
    Select 0 string row (value IS NULL):     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            9201 / 9300          1.7         585.0       1.0X
    Parquet Vectorized (Pushdown)                   89 /  105        176.3           5.7     103.1X
    Native ORC Vectorized                         8886 / 8898          1.8         564.9       1.0X
    Native ORC Vectorized (Pushdown)               110 /  128        143.4           7.0      83.9X


    Select 0 string row
    ('7864320' < value < '7864320'):         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            9336 / 9357          1.7         593.6       1.0X
    Parquet Vectorized (Pushdown)                  927 /  937         17.0          58.9      10.1X
    Native ORC Vectorized                         9026 / 9041          1.7         573.9       1.0X
    Native ORC Vectorized (Pushdown)               257 /  272         61.1          16.4      36.3X


    Select 1 string row (value = '7864320'): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            9209 / 9223          1.7         585.5       1.0X
    Parquet Vectorized (Pushdown)                  908 /  925         17.3          57.7      10.1X
    Native ORC Vectorized                         8878 / 8904          1.8         564.4       1.0X
    Native ORC Vectorized (Pushdown)               248 /  261         63.4          15.8      37.1X


    Select 1 string row
    (value <=> '7864320'):                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            9194 / 9216          1.7         584.5       1.0X
    Parquet Vectorized (Pushdown)                  899 /  908         17.5          57.2      10.2X
    Native ORC Vectorized                         8934 / 8962          1.8         568.0       1.0X
    Native ORC Vectorized (Pushdown)               249 /  254         63.3          15.8      37.0X


    Select 1 string row
    ('7864320' <= value <= '7864320'):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            9332 / 9351          1.7         593.3       1.0X
    Parquet Vectorized (Pushdown)                  915 /  934         17.2          58.2      10.2X
    Native ORC Vectorized                         9049 / 9057          1.7         575.3       1.0X
    Native ORC Vectorized (Pushdown)               248 /  258         63.5          15.8      37.7X


    Select all string rows
    (value IS NOT NULL):                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          20478 / 20497          0.8        1301.9       1.0X
    Parquet Vectorized (Pushdown)               20461 / 20550          0.8        1300.9       1.0X
    Native ORC Vectorized                       27464 / 27482          0.6        1746.1       0.7X
    Native ORC Vectorized (Pushdown)            27454 / 27488          0.6        1745.5       0.7X


    Select 0 int row (value IS NULL):        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8489 / 8519          1.9         539.7       1.0X
    Parquet Vectorized (Pushdown)                   64 /   69        246.1           4.1     132.8X
    Native ORC Vectorized                         8064 / 8099          2.0         512.7       1.1X
    Native ORC Vectorized (Pushdown)                88 /   94        178.6           5.6      96.4X


    Select 0 int row
    (7864320 < value < 7864320):             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8494 / 8514          1.9         540.0       1.0X
    Parquet Vectorized (Pushdown)                  835 /  840         18.8          53.1      10.2X
    Native ORC Vectorized                         8090 / 8106          1.9         514.4       1.0X
    Native ORC Vectorized (Pushdown)               249 /  257         63.2          15.8      34.1X


    Select 1 int row (value = 7864320):      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8552 / 8560          1.8         543.7       1.0X
    Parquet Vectorized (Pushdown)                  837 /  841         18.8          53.2      10.2X
    Native ORC Vectorized                         8178 / 8188          1.9         519.9       1.0X
    Native ORC Vectorized (Pushdown)               249 /  258         63.2          15.8      34.4X


    Select 1 int row (value <=> 7864320):    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8562 / 8580          1.8         544.3       1.0X
    Parquet Vectorized (Pushdown)                  833 /  836         18.9          53.0      10.3X
    Native ORC Vectorized                         8164 / 8185          1.9         519.0       1.0X
    Native ORC Vectorized (Pushdown)               245 /  254         64.3          15.6      35.0X


    Select 1 int row
    (7864320 <= value <= 7864320):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8540 / 8555          1.8         542.9       1.0X
    Parquet Vectorized (Pushdown)                  837 /  839         18.8          53.2      10.2X
    Native ORC Vectorized                         8182 / 8231          1.9         520.2       1.0X
    Native ORC Vectorized (Pushdown)               250 /  259         62.9          15.9      34.1X


    Select 1 int row
    (7864319 < value < 7864321):             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8535 / 8555          1.8         542.6       1.0X
    Parquet Vectorized (Pushdown)                  835 /  841         18.8          53.1      10.2X
    Native ORC Vectorized                         8159 / 8179          1.9         518.8       1.0X
    Native ORC Vectorized (Pushdown)               244 /  250         64.5          15.5      35.0X


    Select 10% int rows (value < 1572864):   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            9609 / 9634          1.6         610.9       1.0X
    Parquet Vectorized (Pushdown)                 2663 / 2672          5.9         169.3       3.6X
    Native ORC Vectorized                         9824 / 9850          1.6         624.6       1.0X
    Native ORC Vectorized (Pushdown)              2717 / 2722          5.8         172.7       3.5X


    Select 50% int rows (value < 7864320):   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          13592 / 13613          1.2         864.2       1.0X
    Parquet Vectorized (Pushdown)                 9720 / 9738          1.6         618.0       1.4X
    Native ORC Vectorized                       16366 / 16397          1.0        1040.5       0.8X
    Native ORC Vectorized (Pushdown)            12437 / 12459          1.3         790.7       1.1X


    Select 90% int rows (value < 14155776):  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          17580 / 17617          0.9        1117.7       1.0X
    Parquet Vectorized (Pushdown)               16803 / 16827          0.9        1068.3       1.0X
    Native ORC Vectorized                       24169 / 24187          0.7        1536.6       0.7X
    Native ORC Vectorized (Pushdown)            22147 / 22341          0.7        1408.1       0.8X


    Select all int rows (value IS NOT NULL): Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          18461 / 18491          0.9        1173.7       1.0X
    Parquet Vectorized (Pushdown)               18466 / 18530          0.9        1174.1       1.0X
    Native ORC Vectorized                       24231 / 24270          0.6        1540.6       0.8X
    Native ORC Vectorized (Pushdown)            24207 / 24304          0.6        1539.0       0.8X


    Select all int rows (value > -1):        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          18414 / 18453          0.9        1170.7       1.0X
    Parquet Vectorized (Pushdown)               18435 / 18464          0.9        1172.1       1.0X
    Native ORC Vectorized                       24430 / 24454          0.6        1553.2       0.8X
    Native ORC Vectorized (Pushdown)            24410 / 24465          0.6        1552.0       0.8X


    Select all int rows (value != -1):       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          18446 / 18457          0.9        1172.8       1.0X
    Parquet Vectorized (Pushdown)               18428 / 18440          0.9        1171.6       1.0X
    Native ORC Vectorized                       24414 / 24450          0.6        1552.2       0.8X
    Native ORC Vectorized (Pushdown)            24385 / 24472          0.6        1550.4       0.8X


    Select 0 distinct string row
    (value IS NULL):                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8322 / 8352          1.9         529.1       1.0X
    Parquet Vectorized (Pushdown)                   53 /   57        296.3           3.4     156.7X
    Native ORC Vectorized                         7903 / 7953          2.0         502.4       1.1X
    Native ORC Vectorized (Pushdown)                80 /   82        197.2           5.1     104.3X


    Select 0 distinct string row
    ('100' < value < '100'):                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8712 / 8743          1.8         553.9       1.0X
    Parquet Vectorized (Pushdown)                  995 / 1030         15.8          63.3       8.8X
    Native ORC Vectorized                         8345 / 8362          1.9         530.6       1.0X
    Native ORC Vectorized (Pushdown)                84 /   87        187.6           5.3     103.9X


    Select 1 distinct string row
    (value = '100'):                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8574 / 8610          1.8         545.1       1.0X
    Parquet Vectorized (Pushdown)                 1127 / 1135         14.0          71.6       7.6X
    Native ORC Vectorized                         8163 / 8181          1.9         519.0       1.1X
    Native ORC Vectorized (Pushdown)               426 /  433         36.9          27.1      20.1X


    Select 1 distinct string row
    (value <=> '100'):                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8549 / 8568          1.8         543.5       1.0X
    Parquet Vectorized (Pushdown)                 1124 / 1131         14.0          71.4       7.6X
    Native ORC Vectorized                         8163 / 8210          1.9         519.0       1.0X
    Native ORC Vectorized (Pushdown)               426 /  436         36.9          27.1      20.1X


    Select 1 distinct string row
    ('100' <= value <= '100'):               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                            8889 / 8896          1.8         565.2       1.0X
    Parquet Vectorized (Pushdown)                 1161 / 1168         13.6          73.8       7.7X
    Native ORC Vectorized                         8519 / 8554          1.8         541.6       1.0X
    Native ORC Vectorized (Pushdown)               430 /  437         36.6          27.3      20.7X


    Select all distinct string rows
    (value IS NOT NULL):                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Parquet Vectorized                          20433 / 20533          0.8        1299.1       1.0X
    Parquet Vectorized (Pushdown)               20433 / 20456          0.8        1299.1       1.0X
    Native ORC Vectorized                       25435 / 25513          0.6        1617.1       0.8X
    Native ORC Vectorized (Pushdown)            25435 / 25507          0.6        1617.1       0.8X
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
