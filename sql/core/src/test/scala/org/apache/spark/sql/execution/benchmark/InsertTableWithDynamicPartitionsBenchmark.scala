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

  def prepareSourceTableAndGetTotalRowsCount(numberRows: Long,
      part1Step: Int, part2Step: Int, part3Step: Int): Long = {
    val dataFrame = spark.range(0, numberRows, 1, 4)
    val dataFrame1 = spark.range(0, numberRows, part1Step, 4)
    val dataFrame2 = spark.range(0, numberRows, part2Step, 4)
    val dataFrame3 = spark.range(0, numberRows, part3Step, 4)

    val data = dataFrame.join(dataFrame1).join(dataFrame2).join(dataFrame3)
      .toDF("id", "part1", "part2", "part3")

    data.createOrReplaceTempView("tmpTable")

    spark.sql("create table " +
      "sourceTable(id bigint, part1 bigint, part2 bigint, part3 bigint) " +
      "using parquet")

    spark.sql("insert overwrite table sourceTable " +
      s"select id, " +
      s"part1, " +
      s"part2, " +
      s"part3 " +
      s"from tmpTable")

    spark.catalog.dropTempView("tmpTable")
    data.count()
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

  def writeOnePartitionColumnTable(partitionNumber: Long, benchmark: Benchmark): Unit = {
    benchmark.addCase(s"one partition column, $partitionNumber partitions") { _ =>
      spark.sql("insert overwrite table " +
        "tableOnePartitionColumn partition(part) " +
        s"select id, " +
        s"part1 as part " +
        s"from sourceTable")
    }
  }

  def writeTwoPartitionColumnTable(partitionNumber: Long, benchmark: Benchmark): Unit = {
    benchmark.addCase(s"two partition columns, $partitionNumber partitions") { _ =>
      spark.sql("insert overwrite table " +
        "tableTwoPartitionColumn partition(part1, part2) " +
        s"select id, " +
        s"part1, " +
        s"part2 " +
        s"from sourceTable")
    }
  }

  def writeThreePartitionColumnTable(partitionNumber: Long, benchmark: Benchmark): Unit = {
    benchmark.addCase(s"three partition columns, $partitionNumber partitions") { _ =>
      spark.sql("insert overwrite table " +
        "tableThreePartitionColumn partition(part1, part2, part3) " +
        s"select id, " +
        s"part1, " +
        s"part2, " +
        s"part3 " +
        s"from sourceTable")
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val sourceTable = "sourceTable"
    val tableOnePartitionColumn = "tableOnePartitionColumn"
    val tableTwoPartitionColumn = "tableTwoPartitionColumn"
    val tableThreePartitionColumn = "tableThreePartitionColumn"
    val numberRows = 100L
    val part1Step = 1
    val part2Step = 20
    val part3Step = 25
    val part1Number = numberRows / part1Step
    val part2Number = numberRows / part2Step *  part1Number
    val part3Number = numberRows / part3Step *  part2Number
    withTable(sourceTable, tableOnePartitionColumn, tableTwoPartitionColumn,
      tableThreePartitionColumn) {
      val totalRows =
        prepareSourceTableAndGetTotalRowsCount(numberRows, part1Step, part2Step, part3Step)
      prepareTable()
      val benchmark =
        new Benchmark(s"dynamic insert table benchmark, totalRows = $totalRows",
          totalRows, output = output)
      writeOnePartitionColumnTable(part1Number, benchmark)
      writeTwoPartitionColumnTable(part2Number, benchmark)
      writeThreePartitionColumnTable(part3Number, benchmark)
      benchmark.run()
    }
  }
}
