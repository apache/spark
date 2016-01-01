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

package org.apache.spark.util

import scala.collection.mutable

import org.apache.commons.lang3.SystemUtils

/**
 * Utility class to benchmark components. An example of how to use this is:
 *  val benchmark = new Benchmark("My Benchmark", valuesPerIteration)
 *   benchmark.addCase("V1")(<function>)
 *   benchmark.addCase("V2")(<function>)
 *   benchmark.run
 * This will output the average time to run each function and the rate of each function.
 *
 * The benchmark function takes one argument that is the iteration that's being run.
 *
 * If outputPerIteration is true, the timing for each run will be printed to stdout.
 */
private[spark] class Benchmark(
    name: String, valuesPerIteration: Long,
    iters: Int = 5,
    outputPerIteration: Boolean = false) {
  val benchmarks = mutable.ArrayBuffer.empty[Benchmark.Case]

  def addCase(name: String)(f: Int => Unit): Unit = {
    benchmarks += Benchmark.Case(name, f)
  }

  /**
   * Runs the benchmark and outputs the results to stdout. This should be copied and added as
   * a comment with the benchmark. Although the results vary from machine to machine, it should
   * provide some baseline.
   */
  def run(): Unit = {
    require(benchmarks.nonEmpty)
    // scalastyle:off
    println("Running benchmark: " + name)

    val results = benchmarks.map { c =>
      println("  Running case: " + c.name)
      Benchmark.measure(valuesPerIteration, iters, outputPerIteration)(c.fn)
    }
    println

    val firstRate = results.head.avgRate
    // The results are going to be processor specific so it is useful to include that.
    println(Benchmark.getProcessorName())
    printf("%-30s %16s %16s %14s\n", name + ":", "Avg Time(ms)", "Avg Rate(M/s)", "Relative Rate")
    println("-------------------------------------------------------------------------------")
    results.zip(benchmarks).foreach { r =>
      printf("%-30s %16s %16s %14s\n",
        r._2.name,
        "%10.2f" format r._1.avgMs,
        "%10.2f" format r._1.avgRate,
        "%6.2f X" format (r._1.avgRate / firstRate))
    }
    println
    // scalastyle:on
  }
}

private[spark] object Benchmark {
  case class Case(name: String, fn: Int => Unit)
  case class Result(avgMs: Double, avgRate: Double)

  /**
   * This should return a user helpful processor information. Getting at this depends on the OS.
   * This should return something like "Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz"
   */
  def getProcessorName(): String = {
    if (SystemUtils.IS_OS_MAC_OSX) {
      Utils.executeAndGetOutput(Seq("/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"))
    } else if (SystemUtils.IS_OS_LINUX) {
      Utils.executeAndGetOutput(Seq("/usr/bin/grep", "-m", "1", "\"model name\"", "/proc/cpuinfo"))
    } else {
      System.getenv("PROCESSOR_IDENTIFIER")
    }
  }

  /**
   * Runs a single function `f` for iters, returning the average time the function took and
   * the rate of the function.
   */
  def measure(num: Long, iters: Int, outputPerIteration: Boolean)(f: Int => Unit): Result = {
    var totalTime = 0L
    for (i <- 0 until iters + 1) {
      val start = System.nanoTime()

      f(i)

      val end = System.nanoTime()
      if (i != 0) totalTime += end - start

      if (outputPerIteration) {
        // scalastyle:off
        println(s"Iteration $i took ${(end - start) / 1000} microseconds")
        // scalastyle:on
      }
    }
    Result(totalTime.toDouble / 1000000 / iters, num * iters / (totalTime.toDouble / 1000))
  }
}

