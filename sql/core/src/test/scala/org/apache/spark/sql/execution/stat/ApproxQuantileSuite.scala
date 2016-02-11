package org.apache.spark.sql.execution.stat

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.stat.StatFunctions.QuantileSummaries

class ApproxQuantileSuite extends SparkFunSuite {

  private val n = 10000
  private val increasing = (0 to n).map(_.toDouble)
  private val decreasing = (n to 0 by -1).map(_.toDouble)
  private val random = Seq.fill(n)(math.random)

  private def buildSummary(
      data: Seq[Double],
      epsi: Double,
      threshold: Int = QuantileSummaries.defaultCompressThreshold): QuantileSummaries = {
    val summary = QuantileSummaries(threshold, epsi)
    data.foreach(summary.insert)
    summary
  }

  // The naive implementation of quantile calculations, for reference checks.
  def naiveQuantile(quant: Double, sorted_data: Seq[Double]): Double = {
    val n = sorted_data.size
    sorted_data.take(math.floor(quant * n).toInt).last
  }

  private def checkQuantile(quant: Double, data: Seq[Double], summary: QuantileSummaries): Unit = {
    val approx = summary.query(quant)
    val approx_rank = data.count(_ <= approx)
    val lower = (quant - summary.epsilon) * data.size
    assert(approx_rank >= lower,
      s"approx_rank: $approx_rank ! >= $lower")
    val upper = (quant + summary.epsilon) * data.size
    assert(approx_rank <= upper,
      s"approx_rank: $approx_rank ! <= $upper")
  }

  for {
    (data, seq_name) <- Seq(increasing, decreasing, random).zip(Seq("increasing", "decreasing", "random"))
    epsi <- Seq(0.1, 0.01)
  } {

    test(s"Extremas with epsi=$epsi and seq=$seq_name") {
      val s = buildSummary(data, epsi)
      val min_approx = s.query(0.0)
      assert(min_approx == data.min, s"Did not return the min: min=${data.min}, got $min_approx")
      val max_approx = s.query(0.0)
      assert(max_approx == data.max, s"Did not return the max: max=${data.max}, got $max_approx")
    }

    test(s"Some quantile values with epsi=$epsi and seq=$seq_name") {
      val s = buildSummary(data, epsi)
      checkQuantile(0.9, data, s)
      checkQuantile(0.5, data, s)
      checkQuantile(0.1, data, s)
    }
  }
}
