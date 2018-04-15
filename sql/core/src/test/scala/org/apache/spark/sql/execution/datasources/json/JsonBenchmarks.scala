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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.{Benchmark, Utils}

/**
 * Benchmark to measure JSON read/write performance.
 * To run this:
 *  spark-submit --class <this class> --jars <spark sql test jar>
 */
object JSONBenchmarks {
  val conf = new SparkConf()

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("benchmark-json-datasource")
    .config(conf)
    .getOrCreate()
  import spark.implicits._

  def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }


  def schemaInferring(rowsNum: Int): Unit = {
    val benchmark = new Benchmark("JSON schema inferring", rowsNum)

    withTempPath { path =>
      // scalastyle:off
      benchmark.out.println("Preparing data for benchmarking ...")
      // scalastyle:on

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
      Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz

      JSON schema inferring:               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      --------------------------------------------------------------------------------------------
      No encoding                             41193 / 41484          2.4         411.9       1.0X
      UTF-8 is set                          109807 / 110674          0.9        1098.1       0.4X
      */
      benchmark.run()
    }
  }

  def perlineParsing(rowsNum: Int): Unit = {
    val benchmark = new Benchmark("JSON per-line parsing", rowsNum)

    withTempPath { path =>
      // scalastyle:off
      benchmark.out.println("Preparing data for benchmarking ...")
      // scalastyle:on

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
      Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz

      JSON per-line parsing:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      ------------------------------------------------------------------------------------------------
      No encoding                                 28953 / 29274          3.5         289.5       1.0X
      UTF-8 is set                                96040 / 96888          1.0         960.4       0.3X
      */
      benchmark.run()
    }
  }

  def main(args: Array[String]): Unit = {
    schemaInferring(100 * 1000 * 1000)
    perlineParsing(100 * 1000 * 1000)
  }
}