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
