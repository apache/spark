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
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * Synthetic benchmark for Scala User Defined Functions.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/UDFBenchmark-results.txt".
 * }}}
 */
object UDFBenchmark extends SqlBasedBenchmark {

  private def doRunBenchmarkWithMixedTypes(udf: UserDefinedFunction, cardinality: Int): Unit = {
    val idCol = col("id")
    val nullableIntCol = when(
      idCol % 2 === 0, idCol.cast(IntegerType)).otherwise(Literal(null, IntegerType))
    val stringCol = idCol.cast(StringType)
    spark.range(cardinality)
      .select(udf(idCol, nullableIntCol, stringCol))
      .noop()
  }

  private def doRunBenchmarkWithPrimitiveTypes(
      udf: UserDefinedFunction, cardinality: Int): Unit = {
    val idCol = col("id")
    val nullableIntCol = when(
      idCol % 2 === 0, idCol.cast(IntegerType)).otherwise(Literal(null, IntegerType))
    spark.range(cardinality)
      .select(udf(idCol, nullableIntCol))
      .noop()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val cardinality = 100000
    runBenchmark("UDF with mixed input types") {
      codegenBenchmark("long/nullable int/string to string", cardinality) {
        val sampleUDF = udf {(a: Long, b: java.lang.Integer, c: String) =>
          s"$a,$b,$c"
        }
        doRunBenchmarkWithMixedTypes(sampleUDF, cardinality)
      }

      codegenBenchmark("long/nullable int/string to option", cardinality) {
        val sampleUDF = udf {(_: Long, b: java.lang.Integer, _: String) =>
          Option(b)
        }
        doRunBenchmarkWithMixedTypes(sampleUDF, cardinality)
      }

      codegenBenchmark("long/nullable int/string to primitive", cardinality) {
        val sampleUDF = udf {(a: Long, b: java.lang.Integer, _: String) =>
          Option(b).map(_.longValue()).getOrElse(a)
        }
        doRunBenchmarkWithMixedTypes(sampleUDF, cardinality)
      }
    }

    runBenchmark("UDF with primitive types") {
      codegenBenchmark("long/nullable int to string", cardinality) {
        val sampleUDF = udf {(a: Long, b: java.lang.Integer) =>
          s"$a,$b"
        }
        doRunBenchmarkWithPrimitiveTypes(sampleUDF, cardinality)
      }

      codegenBenchmark("long/nullable int to option", cardinality) {
        val sampleUDF = udf {(_: Long, b: java.lang.Integer) =>
          Option(b)
        }
        doRunBenchmarkWithPrimitiveTypes(sampleUDF, cardinality)
      }

      codegenBenchmark("long/nullable int to primitive", cardinality) {
        val sampleUDF = udf {(a: Long, b: java.lang.Integer) =>
          Option(b).map(_.longValue()).getOrElse(a)
        }
        doRunBenchmarkWithPrimitiveTypes(sampleUDF, cardinality)
      }

      val benchmark = new Benchmark("UDF identity overhead", cardinality, output = output)

      benchmark.addCase(s"Baseline", numIters = 5) { _ =>
        spark.range(cardinality)
          .select(col("id"), col("id") * 2, col("id") * 3)
          .noop()
      }

      val identityUDF = udf { x: Long => x }
      benchmark.addCase(s"With identity UDF", numIters = 5) { _ =>
        spark.range(cardinality)
          .select(
            identityUDF(col("id")),
            identityUDF(col("id") * 2),
            identityUDF(col("id") * 3))
          .noop()
      }

      benchmark.run()
    }
  }
}
