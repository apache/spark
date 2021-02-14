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

  def prepareSourceTableAndGetTotalRows(numberRows: Long, sourceTable: String,
      part1Step: Int, part2Step: Int, part3Step: Int): Long = {
    val dataFrame = spark.range(0, numberRows, 1, 4)
    val dataFrame1 = spark.range(0, numberRows, part1Step, 4)
    val dataFrame2 = spark.range(0, numberRows, part2Step, 4)
    val dataFrame3 = spark.range(0, numberRows, part3Step, 4)

    val data = dataFrame.join(dataFrame1).join(dataFrame2).join(dataFrame3)
      .toDF("id", "part1", "part2", "part3")
    data.write.saveAsTable(sourceTable)
    data.count()
  }

  def writeOnePartitionColumnTable(tableName: String,
      partitionNumber: Long, benchmark: Benchmark): Unit = {
    spark.sql(s"create table $tableName(i bigint, part bigint) " +
      "using parquet partitioned by (part)")
    benchmark.addCase(s"one partition column, $partitionNumber partitions") { _ =>
      spark.sql(s"insert overwrite table $tableName partition(part) " +
        "select id, part1 as part from sourceTable")
    }
  }

  def writeTwoPartitionColumnTable(tableName: String,
      partitionNumber: Long, benchmark: Benchmark): Unit = {
    spark.sql(s"create table $tableName(i bigint, part1 bigint, part2 bigint) " +
      "using parquet partitioned by (part1, part2)")
    benchmark.addCase(s"two partition columns, $partitionNumber partitions") { _ =>
      spark.sql(s"insert overwrite table $tableName partition(part1, part2) " +
        "select id, part1, part2 from sourceTable")
    }
  }

  def writeThreePartitionColumnTable(tableName: String,
      partitionNumber: Long, benchmark: Benchmark): Unit = {
    spark.sql(s"create table $tableName(i bigint, part1 bigint, part2 bigint, part3 bigint) " +
      "using parquet partitioned by (part1, part2, part3)")
    benchmark.addCase(s"three partition columns, $partitionNumber partitions") { _ =>
      spark.sql(s"insert overwrite table $tableName partition(part1, part2, part3) " +
        "select id, part1, part2, part3 from sourceTable")
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val sourceTable = "sourceTable"
    val onePartColTable = "onePartColTable"
    val twoPartColTable = "twoPartColTable"
    val threePartColTable = "threePartColTable"
    val numberRows = 100L
    val part1Step = 1
    val part2Step = 20
    val part3Step = 25
    val part1Number = numberRows / part1Step
    val part2Number = numberRows / part2Step *  part1Number
    val part3Number = numberRows / part3Step *  part2Number

    withTable(sourceTable, onePartColTable, twoPartColTable, threePartColTable) {
      val totalRows =
        prepareSourceTableAndGetTotalRows(numberRows, sourceTable, part1Step, part2Step, part3Step)
      val benchmark =
        new Benchmark(s"dynamic insert table benchmark, totalRows = $totalRows",
          totalRows, output = output)
      writeOnePartitionColumnTable(onePartColTable, part1Number, benchmark)
      writeTwoPartitionColumnTable(twoPartColTable, part2Number, benchmark)
      writeThreePartitionColumnTable(threePartColTable, part3Number, benchmark)
      benchmark.run()
    }
  }
}
