package org.apache.spark.sql.execution.stat

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.stat.StatFunctions.QuantileSummaries


class ApproxQuantileSuite extends SparkFunSuite {

  private val r = new Random(1)
  private val n = 100
  private val increasing = "increasing" -> (0 to n).map(_.toDouble)
  private val decreasing = "decreasing" -> (n to 0 by -1).map(_.toDouble)
  private val random = "random" -> Seq.fill(n)(math.ceil(r.nextDouble() * 1000))

  private def buildSummary(
      data: Seq[Double],
      epsi: Double,
      threshold: Int): QuantileSummaries = {
    val summary = new QuantileSummaries(threshold, epsi)
    data.foreach(summary.insert)
    summary
  }

  private def checkQuantile(quant: Double, data: Seq[Double], summary: QuantileSummaries): Unit = {
    val approx = summary.query(quant)
    // The rank of the approximation.
    val rank = data.count(_ < approx) // has to be <, not <= to be exact
    val lower = math.floor((quant - summary.epsilon) * data.size)
    assert(rank >= lower,
      s"approx_rank: $rank ! >= $lower, requested quantile = $quant")
    val upper = math.ceil((quant + summary.epsilon) * data.size)
    assert(rank <= upper,
      s"approx_rank: $rank ! <= $upper, requested quantile = $quant")
  }

  for {
    (seq_name, data) <- Seq(increasing, decreasing, random)
    epsi <- Seq(0.1, 0.0001)
    compression <- Seq(1000, 10)
  } {

    test(s"Extremas with epsi=$epsi and seq=$seq_name, compression=$compression") {
      val s = buildSummary(data, epsi, compression)
//      println(s"samples: ${s.sampled}")
      val min_approx = s.query(0.0)
      assert(min_approx == data.min, s"Did not return the min: min=${data.min}, got $min_approx")
      val max_approx = s.query(1.0)
      assert(max_approx == data.max, s"Did not return the max: max=${data.max}, got $max_approx")
    }

    test(s"Some quantile values with epsi=$epsi and seq=$seq_name, compression=$compression") {
      val s = buildSummary(data, epsi, compression)
      println(s"samples: ${s.sampled}")
      println(s"ranks: ${s.printBuffer(s.sampled)}")
      checkQuantile(0.9999, data, s)
      checkQuantile(0.9, data, s)
      checkQuantile(0.5, data, s)
      checkQuantile(0.1, data, s)
      checkQuantile(0.001, data, s)
    }
  }

  // Tests for merging procedure
}
