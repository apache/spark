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

/**
 * Benchmark to measure higher-order function performance with codegen on/off.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to
 *        "benchmarks/HigherOrderFunctionBenchmark-results.txt".
 * }}}
 */
object HigherOrderFunctionBenchmark extends SqlBasedBenchmark {

  private val numRows = 1 << 20  // ~1M rows
  private val arraySize = 10

  private def prepareArrayData(): Unit = {
    spark.range(numRows).selectExpr(
      "id",
      s"array_repeat(cast(id as int), $arraySize) as int_arr",
      s"array_repeat(cast(id as string), $arraySize) as str_arr",
      s"array_repeat(named_struct('a', cast(id as int), 'b', cast(id as string)), " +
        s"$arraySize) as struct_arr",
      s"transform(array_repeat(cast(id as int), $arraySize), " +
        s"(x, i) -> if(i % 3 = 0, null, x)) as nullable_arr"
    ).createOrReplaceTempView("t")
  }

  def transformPrimitiveArray(): Unit = {
    runBenchmark("transform on primitive (int) array") {
      codegenBenchmark("transform int array", numRows) {
        spark.sql(
          "select transform(int_arr, x -> x + 1) from t"
        ).noop()
      }
    }
  }

  def transformWithIndex(): Unit = {
    runBenchmark("transform with index variable") {
      codegenBenchmark("transform with index", numRows) {
        spark.sql(
          "select transform(int_arr, (x, i) -> x + i) from t"
        ).noop()
      }
    }
  }

  def transformStringArray(): Unit = {
    runBenchmark("transform on string array") {
      codegenBenchmark("transform string array", numRows) {
        spark.sql(
          "select transform(str_arr, x -> upper(x)) from t"
        ).noop()
      }
    }
  }

  def transformStructArray(): Unit = {
    runBenchmark("transform on struct array") {
      codegenBenchmark("transform struct array", numRows) {
        spark.sql(
          "select transform(struct_arr, x -> named_struct('a', x.a + 1, 'b', x.b)) from t"
        ).noop()
      }
    }
  }

  def transformNullableArray(): Unit = {
    runBenchmark("transform on nullable element array") {
      codegenBenchmark("transform nullable array", numRows) {
        spark.sql(
          "select transform(nullable_arr, x -> x + 1) from t"
        ).noop()
      }
    }
  }

  def nestedTransform(): Unit = {
    runBenchmark("nested transform") {
      codegenBenchmark("nested transform", numRows) {
        spark.sql(
          "select transform(transform(int_arr, x -> x + 1), y -> y * 2) from t"
        ).noop()
      }
    }
  }

  def transformWithCodegenFallbackBody(): Unit = {
    runBenchmark("transform with CodegenFallback body (filter)") {
      val arrExpr = s"array_repeat(array_repeat(cast(id as int), 5), $arraySize)"
      spark.range(numRows).selectExpr("id", s"$arrExpr as nested_arr")
        .createOrReplaceTempView("t_nested")

      codegenBenchmark("transform + filter (mixed codegen/fallback)", numRows) {
        spark.sql(
          "select transform(nested_arr, x -> filter(x, y -> y > 0)) from t_nested"
        ).noop()
      }
    }
  }

  def filterVsTransform(): Unit = {
    runBenchmark("filter (CodegenFallback) vs transform (codegen)") {
      codegenBenchmark("filter array (CodegenFallback)", numRows) {
        spark.sql(
          "select filter(int_arr, x -> x > 0) from t"
        ).noop()
      }

      codegenBenchmark("transform array (codegen)", numRows) {
        spark.sql(
          "select transform(int_arr, x -> x + 1) from t"
        ).noop()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    prepareArrayData()
    transformPrimitiveArray()
    transformWithIndex()
    transformStringArray()
    transformStructArray()
    transformNullableArray()
    nestedTransform()
    transformWithCodegenFallbackBody()
    filterVsTransform()
  }
}
