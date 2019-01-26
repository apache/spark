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

package org.apache.spark.sql

import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.execution.datasources.json.JSONBenchmark.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

/**
 * Synthetic benchmark for date and timestamp functions.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/test:runMain <this class>"
 *      Results will be written to "benchmarks/DateTimeBenchmark-results.txt".
 * }}}
 */
object DateTimeBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  def doBenchmark(cardinality: Int, expr: String): Unit = {
    spark.range(cardinality)
      .selectExpr(expr)
      .write.format("noop").save()
  }

  def castToTimestamp(cardinality: Int): Unit = {
    codegenBenchmark("cast to timestamp", cardinality) {
      doBenchmark(cardinality, "cast(id as timestamp)")
    }
  }

  def getYear(cardinality: Int): Unit = {
    codegenBenchmark("year from timestamp", cardinality) {
      doBenchmark(cardinality, "year(cast(id as timestamp))")
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val cardinality = 100000000
    runBenchmark("Extract components") {
      castToTimestamp(cardinality)
      getYear(cardinality)
    }
  }
}
