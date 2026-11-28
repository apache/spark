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

package org.apache.spark.ml.util

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType

/**
 * Benchmark for [[DatasetUtils.checkNonNanVectors]].
 *
 * It compares the current implementation (compiled UDF for dense vectors, `exists` for sparse
 * vectors) with the Catalyst `exists`-only implementation (kept here as
 * `checkNonNanVectorsExists`), and with a baseline that does no validation at all, for dense
 * vectors of different sizes and for sparse vectors.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar> <spark mllib test jar> [numRows]
 *   2. build/sbt "mllib/Test/runMain <this class> [numRows]"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "mllib/Test/runMain <this class>"
 *      Results will be written to "benchmarks/CheckNonNanVectorsBenchmark-results.txt".
 * }}}
 */
object CheckNonNanVectorsBenchmark extends BenchmarkBase {

  // Equivalent of checkNonNanVectors before it was changed to use a compiled UDF.
  private def checkNonNanVectorsExists(vectorCol: Column): Column = {
    when(vectorCol.isNull, raise_error(lit("Vectors MUST NOT be Null")))
      .when(exists(unwrap_udt(vectorCol).getField("values"),
        v => v.isNaN || v === Double.NegativeInfinity || v === Double.PositiveInfinity),
        raise_error(concat(lit("Vector values MUST NOT be NaN or Infinity, but got "),
          vectorCol.cast(StringType))))
      .otherwise(vectorCol)
  }

  private def newVectors(size: Int, numActives: Int, count: Int): Array[Vector] = {
    val rng = new java.util.Random(42)
    Array.fill(count) {
      if (numActives == size) {
        Vectors.dense(Array.fill(size)(rng.nextDouble()))
      } else {
        val indices = rng.ints(0, size).distinct().limit(numActives).sorted().toArray
        Vectors.sparse(size, indices, Array.fill(numActives)(rng.nextDouble()))
      }
    }
  }

  private def run(
      spark: SparkSession,
      name: String,
      numRows: Int,
      size: Int,
      numActives: Int): Unit = {
    import spark.implicits._

    // Reuse a small pool of vectors so that memory use does not depend on `numRows`.
    val pool = newVectors(size, numActives, 64)
    val df: DataFrame = spark.range(numRows).map(i => Tuple1(pool((i % pool.length).toInt)))
      .toDF("features")

    def noop(c: DataFrame => Column): () => Unit = { () =>
      df.select(c(df)).write.format("noop").mode(Overwrite).save()
    }

    val benchmark = new Benchmark(name, numRows, output = output)
    val none = noop(_ => col("features"))
    val old = noop(_ => checkNonNanVectorsExists(col("features")))
    val udf = noop(_ => DatasetUtils.checkNonNanVectors("features"))
    benchmark.addCase("no validation (baseline)") { _ => none() }
    benchmark.addCase("exists (before)") { _ => old() }
    benchmark.addCase("dense UDF + sparse exists (after)") { _ => udf() }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numRows = if (mainArgs.nonEmpty) mainArgs(0).toInt else 1000000

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName(this.getClass.getCanonicalName)
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 1)
      .config("spark.ui.enabled", false)
      .getOrCreate()

    try {
      runBenchmark("checkNonNanVectors") {
        run(spark, "dense vectors, size 256", numRows, 256, 256)
        run(spark, "dense vectors, size 477", numRows, 477, 477)
        run(spark, "sparse vectors, size 10000, 477 non-zeros", numRows, 10000, 477)
      }
    } finally {
      spark.stop()
    }
  }
}
