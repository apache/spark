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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession

/**
 * Benchmark to measure insert into table with dynamic partition columns.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to
 *      "benchmarks/InsertTableWithDynamicPartitionsBenchmark-results.txt".
 * }}}
 */
object InsertTableWithDynamicPartitionsBenchmark extends DataSourceWriteBenchmark {

  override def getSparkSession: SparkSession = {
    SparkSession.builder().master("local[4]").getOrCreate()
  }

  def prepareTable(): Unit = {
    spark.sql("create table " +
      "tableOnePartitionColumn(i bigint, part bigint) " +
      "using parquet partitioned by (part)")
    spark.sql("create table " +
      "tableTwoPartitionColumn(i bigint, part1 bigint, part2 bigint) " +
      "using parquet partitioned by (part1, part2)")
    spark.sql("create table " +
      "tableThreePartitionColumn(i bigint, part1 bigint, part2 bigint, part3 bigint) " +
      "using parquet partitioned by (part1, part2, part3)")
  }

  def writeOnePartitionColumnTable(
      numberRows: Int, partitionNumberSeeds: Seq[Int], benchmark: Benchmark): Unit = {
    partitionNumberSeeds.foreach { partitionNumber =>
      benchmark.addCase(s"insert table with $numberRows rows, " +
        s"one partition column, $partitionNumber partitions") { _ =>
        spark.sql("insert overwrite table " +
          "tableOnePartitionColumn partition(part) " +
          s"select id, " +
          s"id % $partitionNumber as part " +
          s"from range($numberRows)")
      }
    }
  }

  def writeTwoPartitionColumnTable(
      numberRows: Int, partitionNumberSeeds: Seq[Int], benchmark: Benchmark): Unit = {
    partitionNumberSeeds.foreach { partitionNumber =>
      benchmark.addCase(s"insert table with $numberRows rows, " +
        s"two partition columns, $partitionNumber partitions") { _ =>
        spark.sql("insert overwrite table " +
          "tableTwoPartitionColumn partition(part1, part2) " +
          s"select id, " +
          s"id % $partitionNumber as part1, " +
          s"id % $partitionNumber as part2 " +
          s"from range($numberRows)")
      }
    }
  }

  def writeThreePartitionColumnTable(
      numberRows: Int, partitionNumberSeeds: Seq[Int], benchmark: Benchmark): Unit = {
    partitionNumberSeeds.foreach { partitionNumber =>
      benchmark.addCase(s"insert table with $numberRows rows, " +
        s"three partition columns, $partitionNumber partitions") { _ =>
        spark.sql("insert overwrite table " +
          "tableThreePartitionColumn partition(part1, part2, part3) " +
          s"select id, " +
          s"id % $partitionNumber as part1, " +
          s"id % $partitionNumber as part2, " +
          s"id % $partitionNumber as part3 " +
          s"from range($numberRows)")
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val tableOnePartitionColumn = "tableOnePartitionColumn"
    val tableTwoPartitionColumn = "tableTwoPartitionColumn"
    val tableThreePartitionColumn = "tableThreePartitionColumn"
    val numberRows = 10000
    val partitionNumberSeeds = Seq(10, 50, 100, 200, 500)
    withTable(tableOnePartitionColumn, tableTwoPartitionColumn, tableThreePartitionColumn) {
      prepareTable()
      val benchmark = new Benchmark(s"dynamic insert table benchmark", numberRows, output = output)
      writeOnePartitionColumnTable(numberRows, partitionNumberSeeds, benchmark)
      writeTwoPartitionColumnTable(numberRows, partitionNumberSeeds, benchmark)
      writeThreePartitionColumnTable(numberRows, partitionNumberSeeds, benchmark)
      benchmark.run()
    }
  }
}
