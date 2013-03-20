package spark.util

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

/**
 *
 */

class DistributionSuite extends FunSuite with ShouldMatchers {
  test("summary") {
    val d = new Distribution((1 to 100).toArray.map{_.toDouble})
    val stats = d.statCounter
    stats.count should be (100)
    stats.mean should be (50.5)
    stats.sum should be (50 * 101)

    val quantiles = d.getQuantiles()
    quantiles(0) should be (1)
    quantiles(1) should be (26)
    quantiles(2) should be (51)
    quantiles(3) should be (76)
    quantiles(4) should be (100)
  }
}
