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

package org.apache.spark.benchmark

import java.io.{OutputStream, PrintStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.Try

import org.apache.commons.io.output.TeeOutputStream
import org.apache.commons.lang3.SystemUtils

import org.apache.spark.util.Utils

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
 * @param name name of this benchmark.
 * @param valuesPerIteration number of values used in the test case, used to compute rows/s.
 * @param minNumIters the min number of iterations that will be run per case, not counting warm-up.
 * @param warmupTime amount of time to spend running dummy case iterations for JIT warm-up.
 * @param minTime further iterations will be run for each case until this time is used up.
 * @param outputPerIteration if true, the timing for each run will be printed to stdout.
 * @param output optional output stream to write benchmark results to
 */
private[spark] class Benchmark(
    name: String,
    valuesPerIteration: Long,
    minNumIters: Int = 2,
    warmupTime: FiniteDuration = 2.seconds,
    minTime: FiniteDuration = 2.seconds,
    outputPerIteration: Boolean = false,
    output: Option[OutputStream] = None) {
  import Benchmark._
  val benchmarks = mutable.ArrayBuffer.empty[Benchmark.Case]

  val out = if (output.isDefined) {
    new PrintStream(new TeeOutputStream(System.out, output.get))
  } else {
    System.out
  }

  /**
   * Adds a case to run when run() is called. The given function will be run for several
   * iterations to collect timing statistics.
   *
   * @param name of the benchmark case
   * @param numIters if non-zero, forces exactly this many iterations to be run
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
   *
   * @param name of the benchmark case
   * @param numIters if non-zero, forces exactly this many iterations to be run
   */
  def addTimerCase(name: String, numIters: Int = 0)(f: Benchmark.Timer => Unit): Unit = {
    benchmarks += Benchmark.Case(name, f, numIters)
  }

  /**
   * Runs the benchmark and outputs the results to stdout. This should be copied and added as
   * a comment with the benchmark. Although the results vary from machine to machine, it should
   * provide some baseline. If `relativeTime` is set to `true`, the `Relative` column will be
   * the relative time of each case relative to the first case (less is better). Otherwise, it
   * will be the relative execution speed of each case relative to the first case (more is better).
   */
  def run(relativeTime: Boolean = false): Unit = {
    require(benchmarks.nonEmpty)
    // scalastyle:off
    println("Running benchmark: " + name)

    val results = benchmarks.map { c =>
      println("  Running case: " + c.name)
      measure(valuesPerIteration, c.numIters)(c.fn)
    }
    println()

    val firstBest = results.head.bestMs
    // The results are going to be processor specific so it is useful to include that.
    out.println(Benchmark.getJVMOSInfo())
    out.println(Benchmark.getProcessorName())
    val nameLen = Math.max(40, Math.max(name.length, benchmarks.map(_.name.length).max))
    out.printf(s"%-${nameLen}s %14s %14s %11s %12s %13s %10s\n",
      name + ":", "Best Time(ms)", "Avg Time(ms)", "Stdev(ms)", "Rate(M/s)", "Per Row(ns)", "Relative")
    out.println("-" * (nameLen + 80))
    results.zip(benchmarks).foreach { case (result, benchmark) =>
      val relative = if (relativeTime) result.bestMs / firstBest else firstBest / result.bestMs
      out.printf(s"%-${nameLen}s %14s %14s %11s %12s %13s %10s\n",
        benchmark.name,
        "%5.0f" format result.bestMs,
        "%4.0f" format result.avgMs,
        "%5.0f" format result.stdevMs,
        "%10.1f" format result.bestRate,
        "%6.1f" format (1000 / result.bestRate),
        "%3.1fX" format relative)
    }
    out.println()
    // scalastyle:on
  }

  /**
   * Runs a single function `f` for iters, returning the average time the function took and
   * the rate of the function.
   */
  def measure(num: Long, overrideNumIters: Int)(f: Timer => Unit): Result = {
    System.gc()  // ensures garbage from previous cases don't impact this one
    val warmupDeadline = warmupTime.fromNow
    while (!warmupDeadline.isOverdue()) {
      f(new Benchmark.Timer(-1))
    }
    val minIters = if (overrideNumIters != 0) overrideNumIters else minNumIters
    val minDuration = if (overrideNumIters != 0) 0 else minTime.toNanos
    val runTimes = ArrayBuffer[Long]()
    var totalTime = 0L
    var i = 0
    while (i < minIters || totalTime < minDuration) {
      val timer = new Benchmark.Timer(i)
      f(timer)
      val runTime = timer.totalTime()
      runTimes += runTime
      totalTime += runTime

      if (outputPerIteration) {
        // scalastyle:off
        println(s"Iteration $i took ${NANOSECONDS.toMicros(runTime)} microseconds")
        // scalastyle:on
      }
      i += 1
    }
    // scalastyle:off
    println(s"  Stopped after $i iterations, ${NANOSECONDS.toMillis(runTimes.sum)} ms")
    // scalastyle:on
    assert(runTimes.nonEmpty)
    val best = runTimes.min
    val avg = runTimes.sum.toDouble / runTimes.size
    val stdev = if (runTimes.size > 1) {
      math.sqrt(runTimes.map(time => (time - avg) * (time - avg)).sum / (runTimes.size - 1))
    } else 0
    Result(avg / 1000000.0, num / (best / 1000.0), best / 1000000.0, stdev / 1000000.0)
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
  case class Result(avgMs: Double, bestRate: Double, bestMs: Double, stdevMs: Double)

  /**
   * This should return a user helpful processor information. Getting at this depends on the OS.
   * This should return something like "Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz"
   */
  def getProcessorName(): String = {
    val cpu = if (SystemUtils.IS_OS_MAC_OSX) {
      Utils.executeAndGetOutput(Seq("/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"))
        .stripLineEnd
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
}
