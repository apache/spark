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

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.lang.management.ManagementFactory

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.config.IO_COMPRESSION_LZF_PARALLEL

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
object LZFBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Benchmark LZFCompressionCodec") {
      compressSmallObjects()
      compressLargeObjects()
    }
  }

  private def compressSmallObjects(): Unit = {
    val N = 256_000_000
    val benchmark = new Benchmark("Compress small objects", N, output = output)
    Seq(true, false).foreach { parallel =>
      val conf = new SparkConf(false).set(IO_COMPRESSION_LZF_PARALLEL, parallel)
      val condition = if (parallel) "in parallel" else "single-threaded"
      benchmark.addCase(s"Compression $N int values $condition") { _ =>
        val os = new LZFCompressionCodec(conf).compressedOutputStream(new ByteArrayOutputStream())
        for (i <- 1 until N) {
          os.write(i)
        }
        os.close()
      }
    }
    benchmark.run()
  }

  private def compressLargeObjects(): Unit = {
    val N = 1024
    val data: Array[Byte] = (1 until 128 * 1024 * 1024).map(_.toByte).toArray
    val benchmark = new Benchmark(s"Compress large objects", N, output = output)

    // com.ning.compress.lzf.parallel.PLZFOutputStream.getNThreads
    def getNThreads: Int = {
      var nThreads = Runtime.getRuntime.availableProcessors
      val jmx = ManagementFactory.getOperatingSystemMXBean
      if (jmx != null)  {
        val loadAverage = jmx.getSystemLoadAverage.toInt
        if (nThreads > 1 && loadAverage >= 1)  nThreads = Math.max(1, nThreads - loadAverage)
      }
      nThreads
    }
    Seq(true, false).foreach { parallel =>
      val conf = new SparkConf(false).set(IO_COMPRESSION_LZF_PARALLEL, parallel)
      val condition = if (parallel) s"in $getNThreads threads" else "single-threaded"
      benchmark.addCase(s"Compression $N array values $condition") { _ =>
        val baos = new ByteArrayOutputStream()
        val zcos = new LZFCompressionCodec(conf).compressedOutputStream(baos)
        val oos = new ObjectOutputStream(zcos)
        1 to N foreach { _ =>
          oos.writeObject(data)
        }
        oos.close()
      }
    }
    benchmark.run()
  }
}
