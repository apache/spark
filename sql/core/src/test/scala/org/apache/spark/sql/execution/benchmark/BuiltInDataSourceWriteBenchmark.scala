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

import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure built-in data sources write performance.
 * To run this benchmark:
 * {{{
 *   By default it measures 4 data source format: Parquet, ORC, JSON, CSV.
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/BuiltInDataSourceWriteBenchmark-results.txt".
 *
 *   To measure specified formats, run it with arguments.
 *   1. without sbt:
 *        bin/spark-submit --class <this class> --jars <spark core test jar>,
 *        <spark catalyst test jar> <spark sql test jar> format1 [format2] [...]
 *   2. build/sbt "sql/test:runMain <this class> format1 [format2] [...]"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt
 *        "sql/test:runMain <this class> format1 [format2] [...]"
 *      Results will be written to "benchmarks/BuiltInDataSourceWriteBenchmark-results.txt".
 * }}}
 *
 */
object BuiltInDataSourceWriteBenchmark extends DataSourceWriteBenchmark {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val formats: Seq[String] = if (mainArgs.isEmpty) {
      Seq("Parquet", "ORC", "JSON", "CSV")
    } else {
      mainArgs
    }

    spark.conf.set(SQLConf.PARQUET_COMPRESSION.key, "snappy")
    spark.conf.set(SQLConf.ORC_COMPRESSION.key, "snappy")

    formats.foreach { format =>
      runBenchmark(s"$format writer benchmark") {
        runDataSourceBenchmark(format)
      }
    }
  }
}
