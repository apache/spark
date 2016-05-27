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
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

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
    name: String,
    valuesPerIteration: Long,
    defaultNumIters: Int = 5,
    outputPerIteration: Boolean = false) {
  val benchmarks = mutable.ArrayBuffer.empty[Benchmark.Case]

  /**
   * Adds a case to run when run() is called. The given function will be run for several
   * iterations to collect timing statistics.
   */
  def addCase(name: String, numIters: Int = 0)(f: Int => Unit): Unit = {
    addTimerCase(name, numIters) { timer =>
      timer.startTiming()
      f(timer.iteration)
      timer.stopTiming()
    }
  }

  /**
   * Adds a case with manual timing control. When the function is run, timing does not start
   * until timer.startTiming() is called within the given function. The corresponding
   * timer.stopTiming() method must be called before the function returns.
   */
  def addTimerCase(name: String, numIters: Int = 0)(f: Benchmark.Timer => Unit): Unit = {
    benchmarks += Benchmark.Case(name, f, if (numIters == 0) defaultNumIters else numIters)
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
      Benchmark.measure(valuesPerIteration, c.numIters, outputPerIteration)(c.fn)
    }
    println

    val firstBest = results.head.bestMs
    // The results are going to be processor specific so it is useful to include that.
    println(Benchmark.getJVMOSInfo())
    println(Benchmark.getProcessorName())
    printf("%-40s %16s %12s %13s %10s\n", name + ":", "Best/Avg Time(ms)", "Rate(M/s)",
      "Per Row(ns)", "Relative")
    println("-" * 96)
    results.zip(benchmarks).foreach { case (result, benchmark) =>
      printf("%-40s %16s %12s %13s %10s\n",
        benchmark.name,
        "%5.0f / %4.0f" format (result.bestMs, result.avgMs),
        "%10.1f" format result.bestRate,
        "%6.1f" format (1000 / result.bestRate),
        "%3.1fX" format (firstBest / result.bestMs))
    }
    println
    // scalastyle:on
  }
}

private[spark] object Benchmark {

  /**
   * Object available to benchmark code to control timing e.g. to exclude set-up time.
   *
   * @param iteration specifies this is the nth iteration of running the benchmark case
   */
  class Timer(val iteration: Int) {
    private var accumulatedTime: Long = 0L
    private var timeStart: Long = 0L

    def startTiming(): Unit = {
      assert(timeStart == 0L, "Already started timing.")
      timeStart = System.nanoTime
    }

    def stopTiming(): Unit = {
      assert(timeStart != 0L, "Have not started timing.")
      accumulatedTime += System.nanoTime - timeStart
      timeStart = 0L
    }

    def totalTime(): Long = {
      assert(timeStart == 0L, "Have not stopped timing.")
      accumulatedTime
    }
  }

  case class Case(name: String, fn: Timer => Unit, numIters: Int)
  case class Result(avgMs: Double, bestRate: Double, bestMs: Double)

  /**
   * This should return a user helpful processor information. Getting at this depends on the OS.
   * This should return something like "Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz"
   */
  def getProcessorName(): String = {
    val cpu = if (SystemUtils.IS_OS_MAC_OSX) {
      Utils.executeAndGetOutput(Seq("/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"))
    } else if (SystemUtils.IS_OS_LINUX) {
      Try {
        val grepPath = Utils.executeAndGetOutput(Seq("which", "grep")).stripLineEnd
        Utils.executeAndGetOutput(Seq(grepPath, "-m", "1", "model name", "/proc/cpuinfo"))
        .stripLineEnd.replaceFirst("model name[\\s*]:[\\s*]", "")
      }.getOrElse("Unknown processor")
    } else {
      System.getenv("PROCESSOR_IDENTIFIER")
    }
    cpu
  }

  /**
   * This should return a user helpful JVM & OS information.
   * This should return something like
   * "OpenJDK 64-Bit Server VM 1.8.0_65-b17 on Linux 4.1.13-100.fc21.x86_64"
   */
  def getJVMOSInfo(): String = {
    val vmName = System.getProperty("java.vm.name")
    val runtimeVersion = System.getProperty("java.runtime.version")
    val osName = System.getProperty("os.name")
    val osVersion = System.getProperty("os.version")
    s"${vmName} ${runtimeVersion} on ${osName} ${osVersion}"
  }

  /**
   * Runs a single function `f` for iters, returning the average time the function took and
   * the rate of the function.
   */
  def measure(num: Long, iters: Int, outputPerIteration: Boolean)(f: Timer => Unit): Result = {
    val runTimes = ArrayBuffer[Long]()
    for (i <- 0 until iters + 1) {
      val timer = new Benchmark.Timer(i)
      f(timer)
      val runTime = timer.totalTime()
      if (i > 0) {
        runTimes += runTime
      }

      if (outputPerIteration) {
        // scalastyle:off
        println(s"Iteration $i took ${runTime / 1000} microseconds")
        // scalastyle:on
      }
    }
    val best = runTimes.min
    val avg = runTimes.sum / iters
    Result(avg / 1000000.0, num / (best / 1000.0), best / 1000000.0)
  }
}

