package org.apache.spark

import org.scalatest.FunSuite

class AggregatorSuite extends FunSuite {

  private val testData = Seq(("k1", 1), ("k2", 1), ("k3", 1), ("k4", 1), ("k1", 1))

  test("combineValuesByKey with partial aggregation") {
    val agg = new Aggregator[String, Int, Int](v => v, (c, v) => c + v, _ + _).withConf(
      new SparkConf().set("spark.shuffle.spill", "false"))
    val output = agg.combineValuesByKey(testData.iterator, null).toMap
    assert(output("k1") === 2)
    assert(output("k2") === 1)
    assert(output("k3") === 1)
    assert(output("k4") === 1)
  }

  test("combineValuesByKey disabling partial aggregation") {
    val agg = new Aggregator[String, Int, Int](v => v, (c, v) => c + v, _ + _).withConf(
      new SparkConf().set("spark.shuffle.spill", "false")
        .set("spark.partialAgg.interval", "2")
        .set("spark.partialAgg.reduction", "0.5"))

    val output = agg.combineValuesByKey(testData.iterator, null).toSeq
    assert(output.count(record => record == ("k1", 1)) === 2)
    assert(output.count(record => record == ("k2", 1)) === 1)
    assert(output.count(record => record == ("k3", 1)) === 1)
    assert(output.count(record => record == ("k4", 1)) === 1)
  }

  test("partial aggregation check interval") {
    val testDataWithPartial = Seq(("k1", 1), ("k1", 1), ("k2", 1))
    val testDataWithoutPartial = Seq(("k1", 1), ("k2", 1), ("k1", 1))

    val agg = new Aggregator[String, Int, Int](v => v, (c, v) => c + v, _ + _).withConf(
      new SparkConf().set("spark.shuffle.spill", "false")
        .set("spark.partialAgg.interval", "2")
        .set("spark.partialAgg.reduction", "0.5"))

    assert(agg.combineValuesByKey(testDataWithPartial.iterator, null).size === 2)
    assert(agg.combineValuesByKey(testDataWithoutPartial.iterator, null).size === 3)
  }

}
