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
import org.apache.spark.sql.internal.SQLConf

/**
 * Synthetic benchmark for nested schema pruning performance for Parquet datasource.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/ParquetVectorizedBytePackerDecoderBenchmark-results.txt".
 * }}}
 */
object ParquetVectorizedBytePackerDecoderBenchmark extends SqlBasedBenchmark {
    import spark.implicits._

    spark.conf.set(SQLConf.PARQUET_VECTOR512_READ_ENABLED.key, true)

    val dataSourceName: String = "parquet"
    val benchmarkName: String = "Vector Decode Benchmark For Parquet"

    private val N = 1000000
    private val numIters = 10

    private val df = spark
      .range(N)
      .map(x => (x % 128, x % 64, x % 4))
      .toDF("col1", "col2", "col3")

    private def addCase(benchmark: Benchmark, name: String,
                        vector512ReadEnabled: String, sql: String): Unit = {
        benchmark.addCase(name) { _ =>
            withSQLConf((SQLConf.PARQUET_VECTOR512_READ_ENABLED.key -> vector512ReadEnabled)) {
                spark.sql(sql).noop()
            }
        }
    }

    protected def selectBenchmark(numRows: Int, numIters: Int): Unit = {
        withTempPath { dir =>
            val path = dir.getCanonicalPath

            df.write.format(dataSourceName).save(path)
            spark.read.format(dataSourceName).load(path).createOrReplaceTempView(s"t1")
            val benchmark = new Benchmark(s"Selection", numRows, numIters, output = output)
            addCase(benchmark, "Selection after optimization", "true", "select * from t1")
            addCase(benchmark, "Selection before optimization", "false", "select * from t1")

            benchmark.run()
        }
    }

    override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
        runBenchmark(benchmarkName) {
            selectBenchmark(N, numIters)
        }
    }
}
