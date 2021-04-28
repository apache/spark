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

package org.apache.spark.ml.linalg

import dev.ludovic.netlib.blas.NetlibF2jBLAS
import scala.concurrent.duration._

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Serialization benchmark for BLAS.
 * To run this benchmark:
 * {{{
 * 1. without sbt: bin/spark-submit --class <this class> <spark mllib test jar>
 * 2. build/sbt "mllib-local/test:runMain <this class>"
 * 3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "mllib/test:runMain <this class>"
 *    Results will be written to "benchmarks/BLASBenchmark-results.txt".
 * }}}
 */
object BLASBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val iters = 1e2.toInt
    val rnd = new scala.util.Random(0)

    val f2jBLAS = NetlibF2jBLAS.getInstance
    val javaBLAS = BLAS.javaBLAS
    val nativeBLAS = BLAS.nativeBLAS

    // scalastyle:off println
    println("f2jBLAS    = " + f2jBLAS.getClass.getName)
    println("javaBLAS   = " + javaBLAS.getClass.getName)
    println("nativeBLAS = " + nativeBLAS.getClass.getName)
    // scalastyle:on println

    runBenchmark("daxpy") {
      val n = 1e8.toInt
      val alpha = rnd.nextDouble
      val x = Array.fill(n) { rnd.nextDouble }
      val y = Array.fill(n) { rnd.nextDouble }

      val benchmark = new Benchmark("daxpy", n, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.daxpy(n, alpha, x, 1, y.clone, 1)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.daxpy(n, alpha, x, 1, y.clone, 1)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.daxpy(n, alpha, x, 1, y.clone, 1)
        }
      }

      benchmark.run()
    }

    runBenchmark("saxpy") {
      val n = 1e8.toInt
      val alpha = rnd.nextFloat
      val x = Array.fill(n) { rnd.nextFloat }
      val y = Array.fill(n) { rnd.nextFloat }

      val benchmark = new Benchmark("saxpy", n, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.saxpy(n, alpha, x, 1, y.clone, 1)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.saxpy(n, alpha, x, 1, y.clone, 1)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.saxpy(n, alpha, x, 1, y.clone, 1)
        }
      }

      benchmark.run()
    }

    runBenchmark("ddot") {
      val n = 1e8.toInt
      val x = Array.fill(n) { rnd.nextDouble }
      val y = Array.fill(n) { rnd.nextDouble }

      val benchmark = new Benchmark("ddot", n, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.ddot(n, x, 1, y, 1)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.ddot(n, x, 1, y, 1)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.ddot(n, x, 1, y, 1)
        }
      }

      benchmark.run()
    }

    runBenchmark("sdot") {
      val n = 1e8.toInt
      val x = Array.fill(n) { rnd.nextFloat }
      val y = Array.fill(n) { rnd.nextFloat }

      val benchmark = new Benchmark("sdot", n, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.sdot(n, x, 1, y, 1)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.sdot(n, x, 1, y, 1)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.sdot(n, x, 1, y, 1)
        }
      }

      benchmark.run()
    }

    runBenchmark("dscal") {
      val n = 1e8.toInt
      val alpha = rnd.nextDouble
      val x = Array.fill(n) { rnd.nextDouble }

      val benchmark = new Benchmark("dscal", n, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.dscal(n, alpha, x.clone, 1)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.dscal(n, alpha, x.clone, 1)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.dscal(n, alpha, x.clone, 1)
        }
      }

      benchmark.run()
    }

    runBenchmark("sscal") {
      val n = 1e8.toInt
      val alpha = rnd.nextFloat
      val x = Array.fill(n) { rnd.nextFloat }

      val benchmark = new Benchmark("sscal", n, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.sscal(n, alpha, x.clone, 1)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.sscal(n, alpha, x.clone, 1)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.sscal(n, alpha, x.clone, 1)
        }
      }

      benchmark.run()
    }

    runBenchmark("dspmv[U]") {
      val n = 1e3.toInt
      val alpha = rnd.nextDouble
      val a = Array.fill(n * (n + 1) / 2) { rnd.nextDouble }
      val x = Array.fill(n) { rnd.nextDouble }
      val beta = rnd.nextDouble
      val y = Array.fill(n) { rnd.nextDouble }

      val benchmark = new Benchmark("dspmv[U]", n * (n + 1) / 2, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.dspmv("U", n, alpha, a, x, 1, beta, y.clone, 1)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.dspmv("U", n, alpha, a, x, 1, beta, y.clone, 1)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.dspmv("U", n, alpha, a, x, 1, beta, y.clone, 1)
        }
      }

      benchmark.run()
    }

    runBenchmark("dspr[U]") {
      val n = 1e3.toInt
      val alpha = rnd.nextDouble
      val x = Array.fill(n) { rnd.nextDouble }
      val a = Array.fill(n * (n + 1) / 2) { rnd.nextDouble }

      val benchmark = new Benchmark("dspr[U]", n * (n + 1) / 2, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.dspr("U", n, alpha, x, 1, a.clone)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.dspr("U", n, alpha, x, 1, a.clone)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.dspr("U", n, alpha, x, 1, a.clone)
        }
      }

      benchmark.run()
    }

    runBenchmark("dsyr[U]") {
      val n = 1e3.toInt
      val alpha = rnd.nextDouble
      val x = Array.fill(n) { rnd.nextDouble }
      val a = Array.fill(n * n) { rnd.nextDouble }

      val benchmark = new Benchmark("dsyr[U]", n * (n + 1) / 2, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.dsyr("U", n, alpha, x, 1, a.clone, n)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.dsyr("U", n, alpha, x, 1, a.clone, n)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.dsyr("U", n, alpha, x, 1, a.clone, n)
        }
      }

      benchmark.run()
    }

    runBenchmark("dgemv[N]") {
      val m = 1e3.toInt
      val n = 1e3.toInt
      val alpha = rnd.nextDouble
      val a = Array.fill(m * n) { rnd.nextDouble }
      val lda = m
      val x = Array.fill(n) { rnd.nextDouble }
      val beta = rnd.nextDouble
      val y = Array.fill(m) { rnd.nextDouble }

      val benchmark = new Benchmark("dgemv[N]", m * n, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.dgemv("N", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.dgemv("N", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.dgemv("N", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
        }
      }

      benchmark.run()
    }

    runBenchmark("dgemv[T]") {
      val m = 1e3.toInt
      val n = 1e3.toInt
      val alpha = rnd.nextDouble
      val a = Array.fill(m * n) { rnd.nextDouble }
      val lda = m
      val x = Array.fill(m) { rnd.nextDouble }
      val beta = rnd.nextDouble
      val y = Array.fill(n) { rnd.nextDouble }

      val benchmark = new Benchmark("dgemv[T]", m * n, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.dgemv("T", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.dgemv("T", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.dgemv("T", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
        }
      }

      benchmark.run()
    }

    runBenchmark("sgemv[N]") {
      val m = 1e3.toInt
      val n = 1e3.toInt
      val alpha = rnd.nextFloat
      val a = Array.fill(m * n) { rnd.nextFloat }
      val lda = m
      val x = Array.fill(n) { rnd.nextFloat }
      val beta = rnd.nextFloat
      val y = Array.fill(m) { rnd.nextFloat }

      val benchmark = new Benchmark("sgemv[N]", m * n, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.sgemv("N", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.sgemv("N", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.sgemv("N", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
        }
      }

      benchmark.run()
    }

    runBenchmark("sgemv[T]") {
      val m = 1e3.toInt
      val n = 1e3.toInt
      val alpha = rnd.nextFloat
      val a = Array.fill(m * n) { rnd.nextFloat }
      val lda = m
      val x = Array.fill(m) { rnd.nextFloat }
      val beta = rnd.nextFloat
      val y = Array.fill(n) { rnd.nextFloat }

      val benchmark = new Benchmark("sgemv[T]", m * n, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.sgemv("T", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.sgemv("T", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.sgemv("T", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
        }
      }

      benchmark.run()
    }

    runBenchmark("dgemm[N,N]") {
      val m = 1e3.toInt
      val n = 1e3.toInt
      val k = 1e3.toInt
      val alpha = rnd.nextDouble
      val a = Array.fill(m * k) { rnd.nextDouble }
      val lda = m
      val b = Array.fill(k * n) { rnd.nextDouble }
      val ldb = k
      val beta = rnd.nextDouble
      val c = Array.fill(m * n) { rnd.nextDouble }
      var ldc = m

      val benchmark = new Benchmark("dgemm[N,N]", m * n * k, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.dgemm("N", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.dgemm("N", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.dgemm("N", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
        }
      }

      benchmark.run()
    }

    runBenchmark("dgemm[N,T]") {
      val m = 1e3.toInt
      val n = 1e3.toInt
      val k = 1e3.toInt
      val alpha = rnd.nextDouble
      val a = Array.fill(m * k) { rnd.nextDouble }
      val lda = m
      val b = Array.fill(k * n) { rnd.nextDouble }
      val ldb = n
      val beta = rnd.nextDouble
      val c = Array.fill(m * n) { rnd.nextDouble }
      var ldc = m

      val benchmark = new Benchmark("dgemm[N,T]", m * n * k, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.dgemm("N", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.dgemm("N", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.dgemm("N", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
        }
      }

      benchmark.run()
    }

    runBenchmark("dgemm[T,N]") {
      val m = 1e3.toInt
      val n = 1e3.toInt
      val k = 1e3.toInt
      val alpha = rnd.nextDouble
      val a = Array.fill(m * k) { rnd.nextDouble }
      val lda = k
      val b = Array.fill(k * n) { rnd.nextDouble }
      val ldb = k
      val beta = rnd.nextDouble
      val c = Array.fill(m * n) { rnd.nextDouble }
      var ldc = m

      val benchmark = new Benchmark("dgemm[T,N]", m * n * k, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.dgemm("T", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.dgemm("T", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.dgemm("T", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
        }
      }

      benchmark.run()
    }

    runBenchmark("dgemm[T,T]") {
      val m = 1e3.toInt
      val n = 1e3.toInt
      val k = 1e3.toInt
      val alpha = rnd.nextDouble
      val a = Array.fill(m * k) { rnd.nextDouble }
      val lda = k
      val b = Array.fill(k * n) { rnd.nextDouble }
      val ldb = n
      val beta = rnd.nextDouble
      val c = Array.fill(m * n) { rnd.nextDouble }
      var ldc = m

      val benchmark = new Benchmark("dgemm[T,T]", m * n * k, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.dgemm("T", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.dgemm("T", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.dgemm("T", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
        }
      }

      benchmark.run()
    }

    runBenchmark("sgemm[N,N]") {
      val m = 1e3.toInt
      val n = 1e3.toInt
      val k = 1e3.toInt
      val alpha = rnd.nextFloat
      val a = Array.fill(m * k) { rnd.nextFloat }
      val lda = m
      val b = Array.fill(k * n) { rnd.nextFloat }
      val ldb = k
      val beta = rnd.nextFloat
      val c = Array.fill(m * n) { rnd.nextFloat }
      var ldc = m

      val benchmark = new Benchmark("sgemm[N,N]", m * n * k, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.sgemm("N", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.sgemm("N", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.sgemm("N", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
        }
      }

      benchmark.run()
    }

    runBenchmark("sgemm[N,T]") {
      val m = 1e3.toInt
      val n = 1e3.toInt
      val k = 1e3.toInt
      val alpha = rnd.nextFloat
      val a = Array.fill(m * k) { rnd.nextFloat }
      val lda = m
      val b = Array.fill(k * n) { rnd.nextFloat }
      val ldb = n
      val beta = rnd.nextFloat
      val c = Array.fill(m * n) { rnd.nextFloat }
      var ldc = m

      val benchmark = new Benchmark("sgemm[N,T]", m * n * k, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.sgemm("N", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.sgemm("N", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.sgemm("N", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
        }
      }

      benchmark.run()
    }

    runBenchmark("sgemm[T,N]") {
      val m = 1e3.toInt
      val n = 1e3.toInt
      val k = 1e3.toInt
      val alpha = rnd.nextFloat
      val a = Array.fill(m * k) { rnd.nextFloat }
      val lda = k
      val b = Array.fill(k * n) { rnd.nextFloat }
      val ldb = k
      val beta = rnd.nextFloat
      val c = Array.fill(m * n) { rnd.nextFloat }
      var ldc = m

      val benchmark = new Benchmark("sgemm[T,N]", m * n * k, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.sgemm("T", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.sgemm("T", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.sgemm("T", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
        }
      }

      benchmark.run()
    }

    runBenchmark("sgemm[T,T]") {
      val m = 1e3.toInt
      val n = 1e3.toInt
      val k = 1e3.toInt
      val alpha = rnd.nextFloat
      val a = Array.fill(m * k) { rnd.nextFloat }
      val lda = k
      val b = Array.fill(k * n) { rnd.nextFloat }
      val ldb = n
      val beta = rnd.nextFloat
      val c = Array.fill(m * n) { rnd.nextFloat }
      var ldc = m

      val benchmark = new Benchmark("sgemm[T,T]", m * n * k, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        f2jBLAS.sgemm("T", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      benchmark.addCase("java") { _ =>
        javaBLAS.sgemm("T", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          nativeBLAS.sgemm("T", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
        }
      }

      benchmark.run()
    }
  }
}
