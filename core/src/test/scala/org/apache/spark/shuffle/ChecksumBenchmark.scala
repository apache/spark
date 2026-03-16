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

package org.apache.spark.shuffle

import java.util.zip.{Adler32, CRC32, CRC32C}

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for Checksum Algorithms used by shuffle.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/ChecksumBenchmark-results.txt".
 * }}}
 */
object ChecksumBenchmark extends BenchmarkBase {

  val N = 1024

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Benchmark Checksum Algorithms") {
      val data: Array[Byte] = (1 until 32 * 1024 * 1024).map(_.toByte).toArray
      val benchmark = new Benchmark("Checksum Algorithms", N, 3, output = output)
      benchmark.addCase(s"Adler32") { _ =>
        (1 to N).foreach(_ => new Adler32().update(data))
      }
      benchmark.addCase("CRC32") { _ =>
        (1 to N).foreach(_ => new CRC32().update(data))
      }
      benchmark.addCase(s"CRC32C") { _ =>
        (1 to N).foreach(_ => new CRC32C().update(data))
      }
      benchmark.run()
    }
  }
}
