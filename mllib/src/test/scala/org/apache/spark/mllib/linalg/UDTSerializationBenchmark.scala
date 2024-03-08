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

package org.apache.spark.mllib.linalg

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

/**
 * Serialization benchmark for VectorUDT.
 * To run this benchmark:
 * {{{
 * 1. without sbt:
 *    bin/spark-submit --class <this class>
 *      --jars <spark core test jar> <spark mllib test jar>
 * 2. build/sbt "mllib/Test/runMain <this class>"
 * 3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "mllib/Test/runMain <this class>"
 *    Results will be written to "benchmarks/UDTSerializationBenchmark-results.txt".
 * }}}
 */
object UDTSerializationBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("VectorUDT de/serialization") {
      val iters = 1e2.toInt
      val numRows = 1e3.toInt

      val encoder = ExpressionEncoder[Vector]().resolveAndBind()
      val toRow = encoder.createSerializer()
      val fromRow = encoder.createDeserializer()

      val vectors = (1 to numRows).map { i =>
        Vectors.dense(Array.fill(1e5.toInt)(1.0 * i))
      }.toArray
      val rows = vectors.map(toRow)

      val benchmark = new Benchmark("VectorUDT de/serialization", numRows, iters, output = output)

      benchmark.addCase("serialize") { _ =>
        var sum = 0
        var i = 0
        while (i < numRows) {
          sum += toRow(vectors(i)).numFields
          i += 1
        }
      }

      benchmark.addCase("deserialize") { _ =>
        var sum = 0
        var i = 0
        while (i < numRows) {
          sum += fromRow(rows(i)).numActives
          i += 1
        }
      }

      benchmark.run()
    }
  }
}
