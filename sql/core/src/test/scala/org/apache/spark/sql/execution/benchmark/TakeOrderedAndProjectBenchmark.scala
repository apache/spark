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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * TakeOrderedAndProject benchmark.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/TakeOrderedAndProjectBenchmark-results.txt".
 * }}}
 */
object TakeOrderedAndProjectBenchmark extends SqlBasedBenchmark {

  private def takeOrderedAndProjectWithSMJ(): Unit = {
    val row = 10 * 1000

    val df1 = spark.range(0, row, 1, 2).selectExpr("id % 3 as c1")
    val df2 = spark.range(0, row, 1, 2).selectExpr("id % 3 as c2")

    val benchmark = new Benchmark("TakeOrderedAndProject with SMJ", row, output = output)

    benchmark.addCase("TakeOrderedAndProject with SMJ for doExecute", 3) { _ =>
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.SHUFFLE_PARTITIONS.key -> "5",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        df1.join(df2, col("c1") === col("c2"))
          .orderBy(col("c1"))
          .limit(100)
          .noop()
      }
    }

    benchmark.addCase("TakeOrderedAndProject with SMJ for executeCollect", 3) { _ =>
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.SHUFFLE_PARTITIONS.key -> "5",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        df1.join(df2, col("c1") === col("c2"))
          .orderBy(col("c1"))
          .limit(100)
          .collect()
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("TakeOrderedAndProject") {
      takeOrderedAndProjectWithSMJ()
    }
  }
}
