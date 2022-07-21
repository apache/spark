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

import dev.ludovic.netlib.{BLAS => NetlibBLAS}
import dev.ludovic.netlib.blas.F2jBLAS
import scala.concurrent.duration._

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}

/**
 * Serialization benchmark for BLAS.
 * To run this benchmark:
 * {{{
 * 1. without sbt: bin/spark-submit --class <this class> <spark mllib test jar>
 * 2. build/sbt "mllib-local/Test/runMain <this class>"
 * 3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "mllib/Test/runMain <this class>"
 *    Results will be written to "benchmarks/BLASBenchmark-results.txt".
 * }}}
 */
object BLASBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val iters = 1e2.toInt
    val rnd = new scala.util.Random(0)

    val f2jBLAS = F2jBLAS.getInstance
    val javaBLAS = BLAS.javaBLAS
    val nativeBLAS = BLAS.nativeBLAS

    // scalastyle:off println
    println("f2jBLAS    = " + f2jBLAS.getClass.getName)
    println("javaBLAS   = " + javaBLAS.getClass.getName)
    println("nativeBLAS = " + nativeBLAS.getClass.getName)
    // scalastyle:on println

    def runBLASBenchmark(name: String, n: Int)(bench: NetlibBLAS => Unit): Unit = {
      val benchmark = new Benchmark(name, n, iters,
                                    warmupTime = 30.seconds,
                                    minTime = 30.seconds,
                                    output = output)

      benchmark.addCase("f2j") { _ =>
        bench(f2jBLAS)
      }

      benchmark.addCase("java") { _ =>
        bench(javaBLAS)
      }

      if (nativeBLAS != javaBLAS) {
        benchmark.addCase("native") { _ =>
          bench(nativeBLAS)
        }
      }

      benchmark.run()
    }

    runBenchmark("daxpy") {
      val n = 1e8.toInt
      val alpha = rnd.nextDouble
      val x = Array.fill(n) { rnd.nextDouble }
      val y = Array.fill(n) { rnd.nextDouble }

      runBLASBenchmark("daxpy", n) { impl =>
        impl.daxpy(n, alpha, x, 1, y.clone, 1)
      }
    }

    runBenchmark("saxpy") {
      val n = 1e8.toInt
      val alpha = rnd.nextFloat
      val x = Array.fill(n) { rnd.nextFloat }
      val y = Array.fill(n) { rnd.nextFloat }

      runBLASBenchmark("saxpy", n) { impl =>
        impl.saxpy(n, alpha, x, 1, y.clone, 1)
      }
    }

    runBenchmark("dcopy") {
      val n = 1e8.toInt
      val x = Array.fill(n) { rnd.nextDouble }
      val y = Array.fill(n) { 0.0 }

      runBLASBenchmark("dcopy", n) { impl =>
        impl.dcopy(n, x, 1, y.clone, 1)
      }
    }

    runBenchmark("scopy") {
      val n = 1e8.toInt
      val x = Array.fill(n) { rnd.nextFloat }
      val y = Array.fill(n) { 0.0f }

      runBLASBenchmark("scopy", n) { impl =>
        impl.scopy(n, x, 1, y.clone, 1)
      }
    }

    runBenchmark("ddot") {
      val n = 1e8.toInt
      val x = Array.fill(n) { rnd.nextDouble }
      val y = Array.fill(n) { rnd.nextDouble }

      runBLASBenchmark("ddot", n) { impl =>
        impl.ddot(n, x, 1, y, 1)
      }
    }

    runBenchmark("sdot") {
      val n = 1e8.toInt
      val x = Array.fill(n) { rnd.nextFloat }
      val y = Array.fill(n) { rnd.nextFloat }

      runBLASBenchmark("sdot", n) { impl =>
        impl.sdot(n, x, 1, y, 1)
      }
    }

    runBenchmark("dnrm2") {
      val n = 1e8.toInt
      val x = Array.fill(n) { rnd.nextDouble }

      runBLASBenchmark("dnrm2", n) { impl =>
        impl.dnrm2(n, x, 1)
      }
    }

    runBenchmark("snrm2") {
      val n = 1e8.toInt
      val x = Array.fill(n) { rnd.nextFloat }

      runBLASBenchmark("snrm2", n) { impl =>
        impl.snrm2(n, x, 1)
      }
    }

    runBenchmark("dscal") {
      val n = 1e8.toInt
      val alpha = rnd.nextDouble
      val x = Array.fill(n) { rnd.nextDouble }

      runBLASBenchmark("dscal", n) { impl =>
        impl.dscal(n, alpha, x.clone, 1)
      }
    }

    runBenchmark("sscal") {
      val n = 1e8.toInt
      val alpha = rnd.nextFloat
      val x = Array.fill(n) { rnd.nextFloat }

      runBLASBenchmark("sscal", n) { impl =>
        impl.sscal(n, alpha, x.clone, 1)
      }
    }

    runBenchmark("dgemv[N]") {
      val m = 1e4.toInt
      val n = 1e4.toInt
      val alpha = rnd.nextDouble
      val a = Array.fill(m * n) { rnd.nextDouble }
      val lda = m
      val x = Array.fill(n) { rnd.nextDouble }
      val beta = rnd.nextDouble
      val y = Array.fill(m) { rnd.nextDouble }

      runBLASBenchmark("dgemv[N]", m * n) { impl =>
        impl.dgemv("N", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
      }
    }

    runBenchmark("dgemv[T]") {
      val m = 1e4.toInt
      val n = 1e4.toInt
      val alpha = rnd.nextDouble
      val a = Array.fill(m * n) { rnd.nextDouble }
      val lda = m
      val x = Array.fill(m) { rnd.nextDouble }
      val beta = rnd.nextDouble
      val y = Array.fill(n) { rnd.nextDouble }

      runBLASBenchmark("dgemv[T]", m * n) { impl =>
        impl.dgemv("T", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
      }
    }

    runBenchmark("sgemv[N]") {
      val m = 1e4.toInt
      val n = 1e4.toInt
      val alpha = rnd.nextFloat
      val a = Array.fill(m * n) { rnd.nextFloat }
      val lda = m
      val x = Array.fill(n) { rnd.nextFloat }
      val beta = rnd.nextFloat
      val y = Array.fill(m) { rnd.nextFloat }

      runBLASBenchmark("sgemv[N]", m * n) { impl =>
        impl.sgemv("N", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
      }
    }

    runBenchmark("sgemv[T]") {
      val m = 1e4.toInt
      val n = 1e4.toInt
      val alpha = rnd.nextFloat
      val a = Array.fill(m * n) { rnd.nextFloat }
      val lda = m
      val x = Array.fill(m) { rnd.nextFloat }
      val beta = rnd.nextFloat
      val y = Array.fill(n) { rnd.nextFloat }

      runBLASBenchmark("sgemv[T]", m * n) { impl =>
        impl.sgemv("T", m, n, alpha, a, lda, x, 1, beta, y.clone, 1)
      }
    }

    runBenchmark("dger") {
      val m = 1e4.toInt
      val n = 1e4.toInt
      val alpha = rnd.nextDouble
      val a = Array.fill(m * n) { rnd.nextDouble }
      val lda = m
      val x = Array.fill(n) { rnd.nextDouble }
      val beta = rnd.nextDouble
      val y = Array.fill(m) { rnd.nextDouble }

      runBLASBenchmark("dger", m * n) { impl =>
        impl.dger(m, n, alpha, x, 1, y, 1, a.clone(), m)
      }
    }

    runBenchmark("dspmv[U]") {
      val n = 1e4.toInt
      val alpha = rnd.nextDouble
      val a = Array.fill(n * (n + 1) / 2) { rnd.nextDouble }
      val x = Array.fill(n) { rnd.nextDouble }
      val beta = rnd.nextDouble
      val y = Array.fill(n) { rnd.nextDouble }

      runBLASBenchmark("dspmv[U]", n * (n + 1) / 2) { impl =>
        impl.dspmv("U", n, alpha, a, x, 1, beta, y.clone, 1)
      }
    }

    runBenchmark("dspr[U]") {
      val n = 1e4.toInt
      val alpha = rnd.nextDouble
      val x = Array.fill(n) { rnd.nextDouble }
      val a = Array.fill(n * (n + 1) / 2) { rnd.nextDouble }

      runBLASBenchmark("dspr[U]", n * (n + 1) / 2) { impl =>
        impl.dspr("U", n, alpha, x, 1, a.clone)
      }
    }

    runBenchmark("dsyr[U]") {
      val n = 1e4.toInt
      val alpha = rnd.nextDouble
      val x = Array.fill(n) { rnd.nextDouble }
      val a = Array.fill(n * n) { rnd.nextDouble }

      runBLASBenchmark("dsyr[U]", n * (n + 1) / 2) { impl =>
        impl.dsyr("U", n, alpha, x, 1, a.clone, n)
      }
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
      val ldc = m

      runBLASBenchmark("dgemm[N,N]", m * n * k) { impl =>
        impl.dgemm("N", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }
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
      val ldc = m

      runBLASBenchmark("dgemm[N,T]", m * n * k) { impl =>
        impl.dgemm("N", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }
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
      val ldc = m

      runBLASBenchmark("dgemm[T,N]", m * n * k) { impl =>
        impl.dgemm("T", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }
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
      val ldc = m

      runBLASBenchmark("dgemm[T,T]", m * n * k) { impl =>
        impl.dgemm("T", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }
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
      val ldc = m

      runBLASBenchmark("sgemm[N,N]", m * n * k) { impl =>
        impl.sgemm("N", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }
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
      val ldc = m

      runBLASBenchmark("sgemm[N,T]", m * n * k) { impl =>
        impl.sgemm("N", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }
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
      val ldc = m

      runBLASBenchmark("sgemm[T,N]", m * n * k) { impl =>
        impl.sgemm("T", "N", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }
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
      val ldc = m

      runBLASBenchmark("sgemm[T,T]", m * n * k) { impl =>
        impl.sgemm("T", "T", m, n, k, alpha, a, lda, b, ldb, beta, c.clone, ldc)
      }
    }
  }
}
