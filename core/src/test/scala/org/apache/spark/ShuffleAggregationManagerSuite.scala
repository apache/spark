package org.apache.spark

import org.apache.spark.shuffle.ShuffleAggregationManager
import org.apache.spark.shuffle.sort.SortShuffleWriter._
import org.mockito.Mockito._

/**
 * Created by vladio on 7/15/15.
 */
class ShuffleAggregationManagerSuite extends SparkFunSuite {

  test("conditions for doing the pre-aggregation") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.partialAgg.interval", "4")
    conf.set("spark.partialAgg.reduction", "0.5")

    // This test will pass if the first 4 elements of a set contains at most 2 unique keys.
    // Generate the records.
    val records = Iterator((1, "Vlad"), (2, "Marius"), (1, "Marian"), (2, "Cornel"), (3, "Patricia"), (4, "Georgeta"))

    // Test.
    val aggManager = new ShuffleAggregationManager[Int, String](records).withConf(conf)
    assert(aggManager.enableAggregation() == true)
  }

  test("conditions for skipping the pre-aggregation") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.partialAgg.interval", "4")
    conf.set("spark.partialAgg.reduction", "0.5")

    val records = Iterator((1, "Vlad"), (2, "Marius"), (3, "Marian"), (2, "Cornel"), (3, "Patricia"), (4, "Georgeta"))

    val aggManager = new ShuffleAggregationManager[Int, String](records).withConf(conf)
    assert(aggManager.enableAggregation() == false)
  }

  test("restoring the iterator") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.partialAgg.interval", "4")
    conf.set("spark.partialAgg.reduction", "0.5")

    val records = Iterator((1, "Vlad"), (2, "Marius"), (1, "Marian"), (2, "Cornel"), (3, "Patricia"), (4, "Georgeta"))
    val recordsCopy = Iterator((1, "Vlad"), (2, "Marius"), (1, "Marian"), (2, "Cornel"), (3, "Patricia"), (4, "Georgeta"))

    val aggManager = new ShuffleAggregationManager[Int, String](records).withConf(conf)
    assert(aggManager.enableAggregation() == true)

    val restoredRecords = aggManager.getRestoredIterator()
    assert(restoredRecords.hasNext)

    while (restoredRecords.hasNext && recordsCopy.hasNext) {
      val kv1 = restoredRecords.next()
      val kv2 = recordsCopy.next()

      assert(kv1 == kv2)
    }

    assert(!restoredRecords.hasNext)
    assert(!recordsCopy.hasNext)
  }
}
