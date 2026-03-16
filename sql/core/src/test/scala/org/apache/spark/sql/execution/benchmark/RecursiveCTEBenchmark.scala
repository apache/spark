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
 * Benchmark for measure writing and reading char/varchar values with implicit length check
 * and padding.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/RecursiveCTEBenchmark-results.txt".
 * }}}
 */
object RecursiveCTEBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("Recursive CTE with only LocalRelation") {
      spark.sql("CREATE TABLE tb(a INT)")
      spark.sql("INSERT INTO tb VALUES 1")
      for (i <- 10 to 100 by 10) {
        val benchmark =
          new Benchmark(s"First $i integers", i, output = output)
        benchmark.addCase(s"First $i integers using VALUES", 3) { _ =>
          spark.sql(
            s"""WITH RECURSIVE t(n) AS (
               |  VALUES(1)
               |  UNION ALL
               |  SELECT n+1 FROM t WHERE n < $i)
               |SELECT * FROM t""".stripMargin).count()
        }
        benchmark.addCase(s"First $i integers using SELECT", 3) { _ =>
          spark.sql(
            s"""WITH RECURSIVE t(n) AS (
               |  SELECT 1
               |  UNION ALL
               |  SELECT n+1 FROM t WHERE n < $i)
               |SELECT * FROM t""".stripMargin).count()
        }
        benchmark.addCase(s"First $i integers using SELECT and LIMIT", 3) { _ =>
          spark.sql(
            s"""WITH RECURSIVE t(n) AS (
               |  SELECT 1
               |  UNION ALL
               |  SELECT n+1 FROM t)
               |SELECT * FROM t  LIMIT $i""".stripMargin).count()
        }
        benchmark.addCase(s"First $i integers referencing external table in anchor", 3) { _ =>
          spark.sql(
            s"""WITH RECURSIVE t(n) AS (
               |  SELECT * FROM tb
               |  UNION ALL
               |  SELECT n+1 FROM t WHERE n < $i)
               |SELECT * FROM t""".stripMargin).count()
        }
        benchmark.run()
      }
      spark.sql("DROP TABLE tb")
    }
  }
}
