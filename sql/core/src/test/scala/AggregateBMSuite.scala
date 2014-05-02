package org.apache.spark.sql

import org.apache.spark.sql.TestData._
import org.apache.spark.sql.catalyst.expressions.Sum
import org.apache.spark.sql.test.TestSQLContext

/* Implicits */
import TestSQLContext._

/**
 * A simple benchmark to compare the performance of group by aggregate with and
 * without external sorting.
 */
class AggregateBMSuite extends QueryTest {

  test("agg random 10m") {
    val t0 = System.nanoTime()
    val sparkAnswerIterator = testDataLarge.groupBy('a)('a, Sum('b)).collect().iterator
    val t1 = System.nanoTime()
    println((t1 - t0)/1000000 + " ms")
    var isValid = true
    while (sparkAnswerIterator.hasNext) {
      val group = sparkAnswerIterator.next()
      // the sum is expected to be a 10k times the grouping attribute
      if (group.getLong(1) != group.getInt(0) * 10000) {
        isValid = false
      }
    }
    if (!isValid) {
      fail (
        "Invalid aggregation results"
      )
    }
  }
}
