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

package org.apache.spark.io

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
import java.nio.file.{Files, Paths}

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Benchmark for LZ4 codec performance.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/LZ4TPCDSDataBenchmark-results.txt".
 * }}}
 */
object LZ4TPCDSDataBenchmark extends BenchmarkBase {

  val N = 4

  // the size of TPCDS catalog_sales.dat (SF1) is about 283M
  val data = Files.readAllBytes(Paths.get(sys.env("SPARK_TPCDS_DATA_TEXT"), "catalog_sales.dat"))

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val name = "Benchmark LZ4CompressionCodec"
    runBenchmark(name) {
      val benchmark1 = new Benchmark(name, N, output = output)
      compressionBenchmark(benchmark1, N)
      benchmark1.run()

      val benchmark2 = new Benchmark(name, N, output = output)
      decompressionBenchmark(benchmark2, N)
      benchmark2.run()
    }
  }

  private def compressionBenchmark(benchmark: Benchmark, N: Int): Unit = {
    val conf = new SparkConf(false)
    benchmark.addCase(s"Compression $N times") { _ =>
      (1 until N).foreach { _ =>
        val os = new LZ4CompressionCodec(conf)
          .compressedOutputStream(OutputStream.nullOutputStream())
        os.write(data)
        os.close()
      }
    }
  }

  private def decompressionBenchmark(benchmark: Benchmark, N: Int): Unit = {
    val conf = new SparkConf(false)
    val outputStream = new ByteArrayOutputStream()
    val out = new LZ4CompressionCodec(conf).compressedOutputStream(outputStream)
    out.write(data)
    out.close()
    val bytes = outputStream.toByteArray

    benchmark.addCase(s"Decompression $N times") { _ =>
      (1 until N).foreach { _ =>
        val bais = new ByteArrayInputStream(bytes)
        val is = new LZ4CompressionCodec(conf).compressedInputStream(bais)
        is.readAllBytes()
        is.close()
      }
    }
  }
}
