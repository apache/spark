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

/**
 * Benchmark for measuring perf of different Base64 implementations
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/Base64Benchmark-results.txt".
 * }}}
 */
object Base64Benchmark extends SqlBasedBenchmark {
  import spark.implicits._
  private val N = 20L * 1000 * 1000

  private def doEncode(len: Int, f: Array[Byte] => Array[Byte]): Unit = {
    spark.range(N).map(_ => "Spark" * len).foreach { s =>
      f(s.getBytes)
      ()
    }
  }

  private def doDecode(len: Int, f: Array[Byte] => Array[Byte]): Unit = {
    spark.range(N).map(_ => "Spark" * len).map { s =>
      // using the same encode func
      java.util.Base64.getMimeEncoder.encode(s.getBytes)
    }.foreach { s =>
      f(s)
      ()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    Seq(1, 3, 5, 7).map { len =>
      val benchmark = new Benchmark(s"encode for $len", N, output = output)
      benchmark.addCase("java", 3) { _ =>
        doEncode(len, x => java.util.Base64.getMimeEncoder().encode(x))
      }
      benchmark.addCase(s"apache", 3) { _ =>
        doEncode(len, org.apache.commons.codec.binary.Base64.encodeBase64)
      }
      benchmark
    }.foreach(_.run())

    Seq(1, 3, 5, 7).map { len =>
      val benchmark = new Benchmark(s"decode for $len", N, output = output)
      benchmark.addCase("java", 3) { _ =>
        doDecode(len, x => java.util.Base64.getMimeDecoder.decode(x))
      }
      benchmark.addCase(s"apache", 3) { _ =>
        doDecode(len, org.apache.commons.codec.binary.Base64.decodeBase64)
      }
      benchmark
    }.foreach(_.run())
  }
}
