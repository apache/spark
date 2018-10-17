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
package org.apache.spark.sql.execution.datasources.json

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._

/**
 * The benchmarks aims to measure performance of JSON parsing when encoding is set and isn't.
 * To run this:
 *  spark-submit --class <this class> --jars <spark sql test jar>
 */
object JSONBenchmarks extends SQLHelper {
  val conf = new SparkConf()

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("benchmark-json-datasource")
    .config(conf)
    .getOrCreate()
  import spark.implicits._

  def schemaInferring(rowsNum: Int): Unit = {
    val benchmark = new Benchmark("JSON schema inferring", rowsNum)

    withTempPath { path =>
      // scalastyle:off println
      benchmark.out.println("Preparing data for benchmarking ...")
      // scalastyle:on println

      spark.sparkContext.range(0, rowsNum, 1)
        .map(_ => "a")
        .toDF("fieldA")
        .write
        .option("encoding", "UTF-8")
        .json(path.getAbsolutePath)

      benchmark.addCase("No encoding", 3) { _ =>
        spark.read.json(path.getAbsolutePath)
      }

      benchmark.addCase("UTF-8 is set", 3) { _ =>
        spark.read
          .option("encoding", "UTF-8")
          .json(path.getAbsolutePath)
      }

      /*
      Java HotSpot(TM) 64-Bit Server VM 1.8.0_172-b11 on Mac OS X 10.13.5
      Intel(R) Core(TM) i7-7820HQ CPU @ 2.90GHz

      JSON schema inferring:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      ---------------------------------------------------------------------------------------------
      No encoding                              45908 / 46480          2.2         459.1       1.0X
      UTF-8 is set                             68469 / 69762          1.5         684.7       0.7X
      */
      benchmark.run()
    }
  }

  def perlineParsing(rowsNum: Int): Unit = {
    val benchmark = new Benchmark("JSON per-line parsing", rowsNum)

    withTempPath { path =>
      // scalastyle:off println
      benchmark.out.println("Preparing data for benchmarking ...")
      // scalastyle:on println

      spark.sparkContext.range(0, rowsNum, 1)
        .map(_ => "a")
        .toDF("fieldA")
        .write.json(path.getAbsolutePath)
      val schema = new StructType().add("fieldA", StringType)

      benchmark.addCase("No encoding", 3) { _ =>
        spark.read
          .schema(schema)
          .json(path.getAbsolutePath)
          .count()
      }

      benchmark.addCase("UTF-8 is set", 3) { _ =>
        spark.read
          .option("encoding", "UTF-8")
          .schema(schema)
          .json(path.getAbsolutePath)
          .count()
      }

      /*
      Java HotSpot(TM) 64-Bit Server VM 1.8.0_172-b11 on Mac OS X 10.13.5
      Intel(R) Core(TM) i7-7820HQ CPU @ 2.90GHz

      JSON per-line parsing:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      ---------------------------------------------------------------------------------------------
      No encoding                               9982 / 10237         10.0          99.8       1.0X
      UTF-8 is set                             16373 / 16806          6.1         163.7       0.6X
      */
      benchmark.run()
    }
  }

  def perlineParsingOfWideColumn(rowsNum: Int): Unit = {
    val benchmark = new Benchmark("JSON parsing of wide lines", rowsNum)

    withTempPath { path =>
      // scalastyle:off println
      benchmark.out.println("Preparing data for benchmarking ...")
      // scalastyle:on println

      spark.sparkContext.range(0, rowsNum, 1)
        .map { i =>
          val s = "abcdef0123456789ABCDEF" * 20
          s"""{"a":"$s","b": $i,"c":"$s","d":$i,"e":"$s","f":$i,"x":"$s","y":$i,"z":"$s"}"""
         }
        .toDF().write.text(path.getAbsolutePath)
      val schema = new StructType()
        .add("a", StringType).add("b", LongType)
        .add("c", StringType).add("d", LongType)
        .add("e", StringType).add("f", LongType)
        .add("x", StringType).add("y", LongType)
        .add("z", StringType)

      benchmark.addCase("No encoding", 3) { _ =>
        spark.read
          .schema(schema)
          .json(path.getAbsolutePath)
          .count()
      }

      benchmark.addCase("UTF-8 is set", 3) { _ =>
        spark.read
          .option("encoding", "UTF-8")
          .schema(schema)
          .json(path.getAbsolutePath)
          .count()
      }

      /*
      Java HotSpot(TM) 64-Bit Server VM 1.8.0_172-b11 on Mac OS X 10.13.5
      Intel(R) Core(TM) i7-7820HQ CPU @ 2.90GHz

      JSON parsing of wide lines:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      ---------------------------------------------------------------------------------------------
      No encoding                              26038 / 26386          0.4        2603.8       1.0X
      UTF-8 is set                             28343 / 28557          0.4        2834.3       0.9X
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
      val columnNames = schema.fieldNames

      spark.range(rowsNum)
        .select(Seq.tabulate(colsNum)(i => lit(i).as(s"col$i")): _*)
        .write
        .json(path.getAbsolutePath)

      val ds = spark.read.schema(schema).json(path.getAbsolutePath)

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
      Select 10 columns + count()               9961 / 10006          1.0         996.1       1.0X
      Select 1 column + count()                  8355 / 8470          1.2         835.5       1.2X
      count()                                    2104 / 2156          4.8         210.4       4.7X
      */
      benchmark.run()
    }
  }

  def main(args: Array[String]): Unit = {
    schemaInferring(100 * 1000 * 1000)
    perlineParsing(100 * 1000 * 1000)
    perlineParsingOfWideColumn(10 * 1000 * 1000)
    countBenchmark(10 * 1000 * 1000)
  }
}
