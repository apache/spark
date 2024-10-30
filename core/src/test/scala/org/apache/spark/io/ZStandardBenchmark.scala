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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream}

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config.{IO_COMPRESSION_ZSTD_BUFFERPOOL_ENABLED, IO_COMPRESSION_ZSTD_BUFFERSIZE, IO_COMPRESSION_ZSTD_LEVEL, IO_COMPRESSION_ZSTD_WORKERS}


/**
 * Benchmark for ZStandard codec performance.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark core test jar>
 *   2. build/sbt "core/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/Test/runMain <this class>"
 *      Results will be written to "benchmarks/ZStandardBenchmark-results.txt".
 * }}}
 */
object ZStandardBenchmark extends BenchmarkBase {

  val N = 10000
  val numInteger = IO_COMPRESSION_ZSTD_BUFFERSIZE.defaultValue.get.toInt / 4

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val name = "Benchmark ZStandardCompressionCodec"
    runBenchmark(name) {
      val benchmark1 = new Benchmark(name, N, output = output)
      compressionBenchmark(benchmark1, N)
      benchmark1.run()

      val benchmark2 = new Benchmark(name, N, output = output)
      decompressionBenchmark(benchmark2, N)
      benchmark2.run()
      parallelCompressionBenchmark()
    }
  }

  private def compressionBenchmark(benchmark: Benchmark, N: Int): Unit = {
    Seq(false, true).foreach { enablePool =>
      Seq(1, 2, 3).foreach { level =>
        val conf = new SparkConf(false)
          .set(IO_COMPRESSION_ZSTD_BUFFERPOOL_ENABLED, enablePool)
          .set(IO_COMPRESSION_ZSTD_LEVEL, level)
        val condition = if (enablePool) "with" else "without"
        benchmark.addCase(s"Compression $N times at level $level $condition buffer pool") { _ =>
          (1 until N).foreach { _ =>
            val os = new ZStdCompressionCodec(conf)
              .compressedOutputStream(new ByteArrayOutputStream())
            for (i <- 1 until numInteger) {
              os.write(i)
            }
            os.close()
          }
        }
      }
    }
  }

  private def decompressionBenchmark(benchmark: Benchmark, N: Int): Unit = {
    Seq(false, true).foreach { enablePool =>
      Seq(1, 2, 3).foreach { level =>
        val conf = new SparkConf(false)
          .set(IO_COMPRESSION_ZSTD_BUFFERPOOL_ENABLED, enablePool)
          .set(IO_COMPRESSION_ZSTD_LEVEL, level)
        val outputStream = new ByteArrayOutputStream()
        val out = new ZStdCompressionCodec(conf).compressedOutputStream(outputStream)
        for (i <- 1 until numInteger) {
          out.write(i)
        }
        out.close()
        val bytes = outputStream.toByteArray

        val condition = if (enablePool) "with" else "without"
        benchmark.addCase(s"Decompression $N times from level $level $condition buffer pool") { _ =>
          (1 until N).foreach { _ =>
            val bais = new ByteArrayInputStream(bytes)
            val is = new ZStdCompressionCodec(conf).compressedInputStream(bais)
            for (i <- 1 until numInteger) {
              is.read()
            }
            is.close()
          }
        }
      }
    }
  }

  private def parallelCompressionBenchmark(): Unit = {
    val numberOfLargeObjectToWrite = 128
    val data: Array[Byte] = (1 until 256 * 1024 * 1024).map(_.toByte).toArray

    Seq(3, 9).foreach { level =>
      val benchmark = new Benchmark(
        s"Parallel Compression at level $level", numberOfLargeObjectToWrite, output = output)
      Seq(0, 1, 2, 4, 8, 16).foreach { workers =>
        val conf = new SparkConf(false)
          .set(IO_COMPRESSION_ZSTD_LEVEL, level)
          .set(IO_COMPRESSION_ZSTD_WORKERS, workers)
        benchmark.addCase(s"Parallel Compression with $workers workers") { _ =>
          val baos = new ByteArrayOutputStream()
          val zcos = new ZStdCompressionCodec(conf).compressedOutputStream(baos)
          val oos = new ObjectOutputStream(zcos)
          1 to numberOfLargeObjectToWrite foreach { _ =>
            oos.writeObject(data)
          }
          oos.close()
        }
      }
      benchmark.run()
    }
  }
}
