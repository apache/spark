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

package org.apache.spark.sql.catalyst.util

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.SparkFunSuite

class QuantileSummariesSuite extends SparkFunSuite {

  private val r = new Random(1)
  private val n = 100
  private val increasing = "increasing" -> (0 until n).map(_.toDouble)
  private val decreasing = "decreasing" -> (n until 0 by -1).map(_.toDouble)
  private val random = "random" -> Seq.fill(n)(math.ceil(r.nextDouble() * 1000))

  private def buildSummary(
      data: Seq[Double],
      epsi: Double,
      threshold: Int): QuantileSummaries = {
    var summary = new QuantileSummaries(threshold, epsi)
    data.foreach { x =>
      summary = summary.insert(x)
    }
    summary.compress()
  }

  /**
   * Interleaves compression and insertions.
   */
  private def buildCompressSummary(
      data: Seq[Double],
      epsi: Double,
      threshold: Int): QuantileSummaries = {
    var summary = new QuantileSummaries(threshold, epsi)
    data.foreach { x =>
      summary = summary.insert(x).compress()
    }
    summary
  }

  private def validateQuantileApproximation(
      approx: Double,
      percentile: Double,
      data: Seq[Double],
      summary: QuantileSummaries): Unit = {
    assert(data.nonEmpty)

    val rankOfValue = data.count(_ <= approx)
    val rankOfPreValue = data.count(_ < approx)
    // `rankOfValue` is the last position of the quantile value. If the input repeats the value
    // chosen as the quantile, e.g. in (1,2,2,2,2,2,3), the 50% quantile is 2, then it's
    // improper to choose the last position as its rank. Instead, we get the rank by averaging
    // `rankOfValue` and `rankOfPreValue`.
    val rank = math.ceil((rankOfValue + rankOfPreValue) / 2.0)
    val lower = math.floor((percentile - summary.relativeError) * data.size)
    val upper = math.ceil((percentile + summary.relativeError) * data.size)
    val msg =
      s"$rank not in [$lower $upper], requested percentile: $percentile, approx returned: $approx"
    assert(rank >= lower, msg)
    assert(rank <= upper, msg)
  }

  private def checkQuantile(
      percentile: Double,
      data: Seq[Double],
      summary: QuantileSummaries): Unit = {
    if (data.nonEmpty) {
      val approx = summary.query(percentile).get
      validateQuantileApproximation(approx, percentile, data, summary)
    } else {
      assert(summary.query(percentile).isEmpty)
    }
  }

  private def checkQuantiles(
      percentiles: Seq[Double],
      data: Seq[Double],
      summary: QuantileSummaries): Unit = {
    if (data.nonEmpty) {
      val approx = summary.query(percentiles).get
      for ((q, a) <- percentiles zip approx) {
        validateQuantileApproximation(a, q, data, summary)
      }
    } else {
      assert(summary.query(percentiles).isEmpty)
    }
  }

  for {
    (seq_name, data) <- Seq(increasing, decreasing, random)
    epsi <- Seq(0.1, 0.0001) // With a significant value and with full precision
    compression <- Seq(1000, 10) // This interleaves n so that we test without and with compression
  } {

    test(s"Extremas with epsi=$epsi and seq=$seq_name, compression=$compression") {
      val s = buildSummary(data, epsi, compression)
      val min_approx = s.query(0.0).get
      assert(min_approx == data.min, s"Did not return the min: min=${data.min}, got $min_approx")
      val max_approx = s.query(1.0).get
      assert(max_approx == data.max, s"Did not return the max: max=${data.max}, got $max_approx")
    }

    test(s"Some quantile values with epsi=$epsi and seq=$seq_name, compression=$compression") {
      val s = buildSummary(data, epsi, compression)
      assert(s.count == data.size, s"Found count=${s.count} but data size=${data.size}")
      checkQuantile(0.9999, data, s)
      checkQuantile(0.9, data, s)
      checkQuantile(0.5, data, s)
      checkQuantile(0.1, data, s)
      checkQuantile(0.001, data, s)
      checkQuantiles(Seq(0.001, 0.1, 0.5, 0.9, 0.9999), data, s)
      checkQuantiles(Seq(0.9999, 0.9, 0.5, 0.1, 0.001), data, s)
    }

    test(s"Some quantile values with epsi=$epsi and seq=$seq_name, compression=$compression " +
      s"(interleaved)") {
      val s = buildCompressSummary(data, epsi, compression)
      assert(s.count == data.size, s"Found count=${s.count} but data size=${data.size}")
      checkQuantile(0.9999, data, s)
      checkQuantile(0.9, data, s)
      checkQuantile(0.5, data, s)
      checkQuantile(0.1, data, s)
      checkQuantile(0.001, data, s)
      checkQuantiles(Seq(0.001, 0.1, 0.5, 0.9, 0.9999), data, s)
      checkQuantiles(Seq(0.9999, 0.9, 0.5, 0.1, 0.001), data, s)
    }

    test(s"Tests on empty data with epsi=$epsi and seq=$seq_name, compression=$compression") {
      val emptyData = Seq.empty[Double]
      val s = buildSummary(emptyData, epsi, compression)
      assert(s.count == 0, s"Found count=${s.count} but data size=0")
      assert(s.sampled.isEmpty, s"if QuantileSummaries is empty, sampled should be empty")
      checkQuantile(0.9999, emptyData, s)
      checkQuantile(0.9, emptyData, s)
      checkQuantile(0.5, emptyData, s)
      checkQuantile(0.1, emptyData, s)
      checkQuantile(0.001, emptyData, s)
      checkQuantiles(Seq(0.001, 0.1, 0.5, 0.9, 0.9999), emptyData, s)
      checkQuantiles(Seq(0.9999, 0.9, 0.5, 0.1, 0.001), emptyData, s)
    }
  }

  // Tests for merging procedure
  for {
    (seq_name, data) <- Seq(increasing, decreasing, random)
    epsi <- Seq(0.1, 0.0001)
    compression <- Seq(1000, 10)
  } {

    val (data1, data2) = {
      val l = data.size
      data.take(l / 2) -> data.drop(l / 2)
    }

    test(s"Merging ordered lists with epsi=$epsi and seq=$seq_name, compression=$compression") {
      val s1 = buildSummary(data1, epsi, compression)
      val s2 = buildSummary(data2, epsi, compression)
      val s = s1.merge(s2)
      val min_approx = s.query(0.0).get
      assert(min_approx == data.min, s"Did not return the min: min=${data.min}, got $min_approx")
      val max_approx = s.query(1.0).get
      assert(max_approx == data.max, s"Did not return the max: max=${data.max}, got $max_approx")
      checkQuantile(0.9999, data, s)
      checkQuantile(0.9, data, s)
      checkQuantile(0.5, data, s)
      checkQuantile(0.1, data, s)
      checkQuantile(0.001, data, s)
      checkQuantiles(Seq(0.001, 0.1, 0.5, 0.9, 0.9999), data, s)
      checkQuantiles(Seq(0.9999, 0.9, 0.5, 0.1, 0.001), data, s)
    }

    val (data11, data12) = {
      data.sliding(2).map(_.head).toSeq -> data.sliding(2).map(_.last).toSeq
    }

    test(s"Merging interleaved lists with epsi=$epsi and seq=$seq_name, compression=$compression") {
      val s1 = buildSummary(data11, epsi, compression)
      val s2 = buildSummary(data12, epsi, compression)
      val s = s1.merge(s2)
      val min_approx = s.query(0.0).get
      assert(min_approx == data.min, s"Did not return the min: min=${data.min}, got $min_approx")
      val max_approx = s.query(1.0).get
      assert(max_approx == data.max, s"Did not return the max: max=${data.max}, got $max_approx")
      checkQuantile(0.9999, data, s)
      checkQuantile(0.9, data, s)
      checkQuantile(0.5, data, s)
      checkQuantile(0.1, data, s)
      checkQuantile(0.001, data, s)
      checkQuantiles(Seq(0.001, 0.1, 0.5, 0.9, 0.9999), data, s)
      checkQuantiles(Seq(0.9999, 0.9, 0.5, 0.1, 0.001), data, s)
    }

    // length of data21 is 4 * length of data22
    val data21 = data.zipWithIndex.filter(_._2 % 5 != 0).map(_._1).toSeq
    val data22 = data.zipWithIndex.filter(_._2 % 5 == 0).map(_._1).toSeq

    test(
      s"Merging unbalanced interleaved lists with epsi=$epsi and seq=$seq_name, " +
        s"compression=$compression") {
      val s1 = buildSummary(data21, epsi, compression)
      val s2 = buildSummary(data22, epsi, compression)
      val s = s1.merge(s2)
      // Check all quantiles
      val percentiles = ArrayBuffer[Double]()
      for (queryRank <- 1 to n) {
        val percentile = queryRank.toDouble / n.toDouble
        checkQuantile(percentile, data, s)
        percentiles += percentile
      }
      checkQuantiles(percentiles.toSeq, data, s)
      checkQuantiles(percentiles.reverse.toSeq, data, s)
    }
  }
}
