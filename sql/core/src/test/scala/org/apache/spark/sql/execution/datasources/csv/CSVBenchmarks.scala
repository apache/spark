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
package org.apache.spark.sql.execution.datasources.csv

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.util.{Benchmark, Utils}

/**
 * Benchmark to measure CSV read/write performance.
 * To run this:
 *  spark-submit --class <this class> --jars <spark sql test jar>
 */
object CSVBenchmarks {
  val conf = new SparkConf()

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("benchmark-csv-datasource")
    .config(conf)
    .getOrCreate()
  import spark.implicits._

  def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  def quotedValuesBenchmark(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark(s"Parsing quoted values", rowsNum)

    withTempPath { path =>
      val str = (0 until 10000).map(i => s""""$i"""").mkString(",")

      spark.range(rowsNum)
        .map(_ => str)
        .write.option("header", true)
        .csv(path.getAbsolutePath)

      val schema = new StructType().add("value", StringType)
      val ds = spark.read.option("header", true).schema(schema).csv(path.getAbsolutePath)

      benchmark.addCase(s"One quoted string", numIters) { _ =>
        ds.filter((_: Row) => true).count()
      }

      /*
      Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz

      Parsing quoted values:               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      --------------------------------------------------------------------------------------------
      One quoted string                       30273 / 30549          0.0      605451.2       1.0X
      */
      benchmark.run()
    }
  }

  def multiColumnsBenchmark(rowsNum: Int): Unit = {
    val colsNum = 1000
    val benchmark = new Benchmark(s"Wide rows with $colsNum columns", rowsNum)

    withTempPath { path =>
      val fields = Seq.tabulate(colsNum)(i => StructField(s"col$i", IntegerType))
      val schema = StructType(fields)
      val values = (0 until colsNum).map(i => i.toString).mkString(",")
      val columnNames = schema.fieldNames

      spark.range(rowsNum)
        .select(Seq.tabulate(colsNum)(i => lit(i).as(s"col$i")): _*)
        .write.option("header", true)
        .csv(path.getAbsolutePath)

      val ds = spark.read.schema(schema).csv(path.getAbsolutePath)

      benchmark.addCase(s"Select $colsNum columns", 3) { _ =>
        ds.select("*").filter((row: Row) => true).count()
      }
      val cols100 = columnNames.take(100).map(Column(_))
      benchmark.addCase(s"Select 100 columns", 3) { _ =>
        ds.select(cols100: _*).filter((row: Row) => true).count()
      }
      benchmark.addCase(s"Select one column", 3) { _ =>
        ds.select($"col1").filter((row: Row) => true).count()
      }
      benchmark.addCase(s"count()", 3) { _ =>
        ds.count()
      }

      /*
      Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz

      Wide rows with 1000 columns:         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      --------------------------------------------------------------------------------------------
      Select 1000 columns                     81091 / 81692          0.0       81090.7       1.0X
      Select 100 columns                      30003 / 34448          0.0       30003.0       2.7X
      Select one column                       24792 / 24855          0.0       24792.0       3.3X
      count()                                 24344 / 24642          0.0       24343.8       3.3X
      */
      benchmark.run()
    }
  }

  def countBenchmark(rowsNum: Int): Unit = {
    val colsNum = 10
    val benchmark = new Benchmark(s"Count a dataset with $colsNum columns", rowsNum)

    withTempPath { path =>
      val fields = Seq.tabulate(colsNum)(i => StructField(s"col$i", IntegerType))
      val schema = StructType(fields)

      spark.range(rowsNum)
        .select(Seq.tabulate(colsNum)(i => lit(i).as(s"col$i")): _*)
        .write
        .csv(path.getAbsolutePath)

      val ds = spark.read.schema(schema).csv(path.getAbsolutePath)

      benchmark.addCase(s"Select $colsNum columns + count()", 3) { _ =>
        ds.select("*").filter((_: Row) => true).count()
      }
      benchmark.addCase(s"Select 1 column + count()", 3) { _ =>
        ds.select($"col1").filter((_: Row) => true).count()
      }
      benchmark.addCase(s"count()", 3) { _ =>
        ds.count()
      }

      /*
      Intel(R) Core(TM) i7-7700HQ CPU @ 2.80GHz

      Count a dataset with 10 columns:      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      ---------------------------------------------------------------------------------------------
      Select 10 columns + count()              12598 / 12740          0.8        1259.8       1.0X
      Select 1 column + count()                  7960 / 8175          1.3         796.0       1.6X
      count()                                    2332 / 2386          4.3         233.2       5.4X
      */
      benchmark.run()
    }
  }

  def main(args: Array[String]): Unit = {
    quotedValuesBenchmark(rowsNum = 50 * 1000, numIters = 3)
    multiColumnsBenchmark(rowsNum = 1000 * 1000)
    countBenchmark(10 * 1000 * 1000)
  }
}
