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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{Benchmark, Utils}

/**
 * The benchmarks aims to measure performance of
 * [SPARK-24900][SQL]speed up sort when the dataset is small
 */
object SmallDataSortBenchmark {

  val conf = new SparkConf()

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("speed up sort when the dataset is small")
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  def run(rowsNum: Int): Unit = {
    val factor = 1000
    val key = rowsNum / 2
    val benchmark = new Benchmark("small data sort", rowsNum * factor)
    withTempPath { path =>
      // scalastyle:off println
      benchmark.out.println("Preparing data for benchmarking ...")
      // scalastyle:on println

      val list = (0 to factor).toList

      spark.sparkContext.range(0, rowsNum, 1)
        .flatMap(num => {
          list.map(x => (num, x))
        })
        .toDF("key", "value")
        .write
        .option("encoding", "UTF-8")
        .json(path.getAbsolutePath)

      val dataset = spark.read.json(path.getAbsolutePath)

      dataset.createOrReplaceTempView("src")

      benchmark.addCase("with optimization", 10) { _ =>
        // 334 * 3 > 1000, the optimization works
        spark.conf.set("spark.sql.shuffle.partitions", dataset.rdd.getNumPartitions)
        spark.conf.set("spark.sql.execution.rangeExchange.sampleSizePerPartition", "334")
        val result = spark.
          sql(s"select * from src where key = $key order by value").collectAsList().size()

      }

      benchmark.addCase("without optimization", 10) { _ =>
        // 333 * 3 < 1000, the optimization doesn't work
        spark.conf.set("spark.sql.shuffle.partitions", dataset.rdd.getNumPartitions)
        spark.conf.set("spark.sql.execution.rangeExchange.sampleSizePerPartition", "333")
        val result = spark.
          sql(s"select * from src where key = $key order by value").collectAsList().size()

      }

      /*
       * Java HotSpot(TM) 64-Bit Server VM 1.8.0_91-b14 on Mac OS X 10.13.6
       * Intel(R) Core(TM) i5-5257U CPU @ 2.70GHz
       *
       * small data sort:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
       * ----------------------------------------------------------------------------------
       * with optimization             54077 / 57989          1.8         540.8       1.0X
       * without optimization        111780 / 115001          0.9        1117.8       0.5X
       */
      benchmark.run()
    }
  }

  def main(args: Array[String]): Unit = {
    run(100 * 1000)
  }
}
