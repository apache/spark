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
package org.apache.spark.sql.execution.datasources.json

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType

/**
 * Benchmarks sharing repeated simple get_json_object parsing.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain <this class>"
 *   2. Generate the result file:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to
 *      "sql/core/benchmarks/SharedJsonParseBenchmark-results.txt" on JDK 17.
 * }}}
 */
object SharedJsonParseBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Benchmark for sharing repeated get_json_object parsing") {
      val rows = 200000
      val fieldCount = 32
      val fieldValue = concat(lit("value-"), $"id".cast(StringType), lit("-" + "x" * 64))
      val data = spark.range(0, rows, 1, 4)
        .select(to_json(struct(Seq.tabulate(fieldCount) { index =>
          fieldValue.as(s"field_$index")
        }: _*)).as("json"))
        .cache()
      data.count()

      Seq(2, 4, 8, 16).foreach { selectedFieldCount =>
        val pathBenchmark = new Benchmark(
          s"get_json_object extracting $selectedFieldCount of $fieldCount fields",
          rows,
          output = output)

        def extractPaths(sharedParsing: Boolean): Unit = {
          withSQLConf(
              SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> "true",
              SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED.key -> sharedParsing.toString) {
            data.select(Seq.tabulate(selectedFieldCount) { index =>
              get_json_object($"json", s"$$.field_$index")
            }: _*).noop()
          }
        }

        pathBenchmark.addCase("shared parsing off", 3) { _ =>
          extractPaths(sharedParsing = false)
        }
        pathBenchmark.addCase("shared parsing on", 3) { _ =>
          extractPaths(sharedParsing = true)
        }
        pathBenchmark.run()
      }

      data.unpersist()

      val nestedData = spark.range(0, rows, 1, 4)
        .select(to_json(struct(struct(Seq.tabulate(fieldCount) { index =>
          fieldValue.as(s"field_$index")
        }: _*).as("payload"))).as("json"))
        .cache()
      nestedData.count()

      Seq(2, 4, 8, 16).foreach { selectedFieldCount =>
        val pathBenchmark = new Benchmark(
          s"get_json_object extracting $selectedFieldCount of $fieldCount nested fields",
          rows,
          output = output)

        def extractPaths(sharedParsing: Boolean): Unit = {
          withSQLConf(
              SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> "true",
              SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED.key -> sharedParsing.toString) {
            nestedData.select(Seq.tabulate(selectedFieldCount) { index =>
              get_json_object($"json", s"$$.payload.field_$index")
            }: _*).noop()
          }
        }

        pathBenchmark.addCase("shared parsing off", 3) { _ =>
          extractPaths(sharedParsing = false)
        }
        pathBenchmark.addCase("shared parsing on", 3) { _ =>
          extractPaths(sharedParsing = true)
        }
        pathBenchmark.run()
      }

      nestedData.unpersist()

      val payload = struct(Seq.tabulate(fieldCount) { index =>
        fieldValue.as(s"field_$index")
      }: _*)
      val mixedObjectArrayData = spark.range(0, rows, 1, 4)
        .select(when(($"id" % 2) === 0, to_json(payload))
          .otherwise(to_json(array(payload)))
          .as("json"))
        .cache()
      mixedObjectArrayData.count()

      Seq(2, 4, 8, 16).foreach { selectedFieldCount =>
        val pathBenchmark = new Benchmark(
          s"get_json_object coalescing $selectedFieldCount object/array field paths",
          rows,
          output = output)

        def extractPaths(sharedParsing: Boolean): Unit = {
          withSQLConf(
              SQLConf.JSON_EXPRESSION_OPTIMIZATION.key -> "true",
              SQLConf.GET_JSON_OBJECT_SHARED_PARSING_ENABLED.key -> sharedParsing.toString) {
            mixedObjectArrayData.select(Seq.tabulate(selectedFieldCount) { index =>
              coalesce(
                get_json_object($"json", s"$$.field_$index"),
                get_json_object($"json", s"$$[0].field_$index"))
            }: _*).noop()
          }
        }

        pathBenchmark.addCase("shared parsing off", 3) { _ =>
          extractPaths(sharedParsing = false)
        }
        pathBenchmark.addCase("shared parsing on", 3) { _ =>
          extractPaths(sharedParsing = true)
        }
        pathBenchmark.run()
      }

      mixedObjectArrayData.unpersist()
    }
  }
}
