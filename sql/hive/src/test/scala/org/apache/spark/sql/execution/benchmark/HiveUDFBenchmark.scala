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

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFReplicateRows

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.hive.test.TestHive

/**
 * Benchmark to measure hive table write performance.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *     bin/spark-submit --class org.apache.spark.sql.execution.benchmark.HiveUDFBenchmark
 *        --jars <spark catalyst test jar>,<spark core test jar>,<spark sql test jar>
 *        <spark hive test jar>
 *   2. build/sbt "hive/Test/runMain org.apache.spark.sql.execution.benchmark.HiveUDFBenchmark"
 *   3. generate result:
 *   SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "hive/Test/runMain <this class>"
 *      Results will be written to "benchmarks/HiveUDFBenchmark-results.txt".
 * }}}
 */
object HiveUDFBenchmark extends SqlBasedBenchmark {
  override def getSparkSession: SparkSession = TestHive.sparkSession

  val numRows = 1024 * 1024
  val sql: String => DataFrame = spark.sql _
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmark = new Benchmark(s"Hive UDTF benchmark", numRows, output = output)
    sql(s"CREATE TEMPORARY FUNCTION HiveUDFBenchmark AS " +
      s"'${classOf[GenericUDTFReplicateRows].getName}'")
    benchmark.addCase("Hive UDTF dup 2", 5) { _ =>
      spark.range(numRows).selectExpr("HiveUDFBenchmark(2L, id)").collect()
    }

    benchmark.addCase("Hive UDTF dup 4", 5) { N =>
      spark.range(numRows).selectExpr("HiveUDFBenchmark(4L, id)").collect()
    }

    benchmark.run()
  }
}
