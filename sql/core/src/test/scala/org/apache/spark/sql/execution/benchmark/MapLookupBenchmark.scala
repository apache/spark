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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Benchmark to measure performance of map lookup operations.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.MapLookupBenchmark"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt \
 *        "sql/Test/runMain org.apache.spark.sql.execution.benchmark.MapLookupBenchmark"
 *      Results will be written to "benchmarks/MapLookupBenchmark-results.txt".
 * }}}
 */
object MapLookupBenchmark extends SqlBasedBenchmark {
  private val NUMBER_OF_ITER = 10

  override def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[1]")
      .appName("MapLookupBenchmark")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memory", "8g")
      .getOrCreate()
  }

  private def run(
      mapSize: Int,
      hitRate: Double,
      keyType: DataType): Unit = {
    val numRows = 10000

    val benchmark = new Benchmark(
      s"MapLookup (size=$mapSize, hit=$hitRate, type=$keyType)",
      numRows,
      NUMBER_OF_ITER,
      output = output)

    import spark.implicits._

    // Create a DataFrame with a single column 'm' containing the map,
    // and 'k' containing the key to lookup.
    // Use `typedLit` to create the map literal directly.
    val keys = (0 until mapSize).toArray
    val map = keys.zip(keys.map(_.toString)).toMap
    val mapCol = typedLit(map)

    // Generate lookup keys
    val lookupKeys = (0 until numRows).map { i =>
      if (i < numRows * hitRate) keys(i % mapSize) else -1
    }

    val lookupDf = lookupKeys.toDF("key").select(mapCol.as("m"), $"key")

    val expr = col("m").getItem(col("key"))
    val elementAtExpr = element_at(col("m"), col("key"))

    benchmark.addCase("GetMapValue interpreted - Linear Lookup") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> Int.MaxValue.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(expr).write.format("noop").mode("overwrite").save()
      }
    }

    benchmark.addCase("GetMapValue interpreted - Hash Lookup") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> 0.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(expr).write.format("noop").mode("overwrite").save()
      }
    }

    benchmark.addCase("GetMapValue codegen - Linear Lookup") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> Int.MaxValue.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(expr).write.format("noop").mode("overwrite").save()
      }
    }

    benchmark.addCase("GetMapValue codegen - Hash Lookup") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> 0.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(expr).write.format("noop").mode("overwrite").save()
      }
    }

    benchmark.addCase("ElementAt interpreted - Linear Lookup") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> Int.MaxValue.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(elementAtExpr).write.format("noop").mode("overwrite").save()
      }
    }

    benchmark.addCase("ElementAt interpreted - Hash Lookup") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> 0.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(elementAtExpr).write.format("noop").mode("overwrite").save()
      }
    }

    benchmark.addCase("ElementAt codegen - Linear Lookup") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> Int.MaxValue.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(elementAtExpr).write.format("noop").mode("overwrite").save()
      }
    }

    benchmark.addCase("ElementAt codegen - Hash Lookup") { _ =>
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY",
        SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> 0.toString,
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
        lookupDf.select(elementAtExpr).write.format("noop").mode("overwrite").save()
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val sizes = Seq(1, 10, 100, 1000, 10000, 100000, 1000000)
    for (size <- sizes) {
      run(size, 1.0, IntegerType)
      run(size, 0.5, IntegerType)
      run(size, 0.0, IntegerType)
    }
  }
}
