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

import java.io.PrintStream

/**
 * Util for getting some stats from a small sample of numeric values, with some handy
 * summary functions.
 *
 * Entirely in memory, not intended as a good way to compute stats over large data sets.
 *
 * Assumes you are giving it a non-empty set of data
 */
private[spark] class Distribution(val data: Array[Double], val startIdx: Int, val endIdx: Int) {
  require(startIdx < endIdx)
  def this(data: Iterable[Double]) = this(data.toArray, 0, data.size)
  java.util.Arrays.sort(data, startIdx, endIdx)
  val length = endIdx - startIdx

  val defaultProbabilities = Array(0, 0.25, 0.5, 0.75, 1.0)

  /**
   * Get the value of the distribution at the given probabilities.  Probabilities should be
   * given from 0 to 1
   * @param probabilities
   */
  def getQuantiles(probabilities: Iterable[Double] = defaultProbabilities)
      : IndexedSeq[Double] = {
    probabilities.toIndexedSeq.map { p: Double => data(closestIndex(p)) }
  }

  private def closestIndex(p: Double) = {
    math.min((p * length).toInt + startIdx, endIdx - 1)
  }

  def showQuantiles(out: PrintStream = System.out): Unit = {
    // scalastyle:off println
    out.println("min\t25%\t50%\t75%\tmax")
    getQuantiles(defaultProbabilities).foreach{q => out.print(s"$q\t")}
    out.println
    // scalastyle:on println
  }

  def statCounter: StatCounter = StatCounter(data.slice(startIdx, endIdx))

  /**
   * print a summary of this distribution to the given PrintStream.
   * @param out
   */
  def summary(out: PrintStream = System.out): Unit = {
    // scalastyle:off println
    out.println(statCounter)
    showQuantiles(out)
    // scalastyle:on println
  }
}

private[spark] object Distribution {

  def apply(data: Iterable[Double]): Option[Distribution] = {
    if (data.nonEmpty) {
      Some(new Distribution(data))
    } else {
      None
    }
  }

  def showQuantiles(out: PrintStream = System.out, quantiles: Iterable[Double]): Unit = {
    // scalastyle:off println
    out.println("min\t25%\t50%\t75%\tmax")
    quantiles.foreach{q => out.print(s"$q\t")}
    out.println
    // scalastyle:on println
  }
}
