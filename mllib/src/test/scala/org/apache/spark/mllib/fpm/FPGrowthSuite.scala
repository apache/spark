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
package org.apache.spark.mllib.fpm

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext

class FPGrowthSuite extends SparkFunSuite with MLlibTestSparkContext {


  test("FP-Growth using String type") {
    val transactions = Seq(
      "r z h k p",
      "z y x w v u t s",
      "s x o n r",
      "x z y m t s q e",
      "z",
      "x z y r q t p")
      .map(_.split(" "))
    val rdd = sc.parallelize(transactions, 2).cache()

    val fpg = new FPGrowth()

    val model6 = fpg
      .setMinSupport(0.9)
      .setNumPartitions(1)
      .run(rdd)
    assert(model6.freqItemsets.count() === 0)

    val model3 = fpg
      .setMinSupport(0.5)
      .setNumPartitions(2)
      .run(rdd)
    val freqItemsets3 = model3.freqItemsets.collect().map { itemset =>
      (itemset.items.toSet, itemset.freq)
    }
    val expected = Set(
      (Set("s"), 3L), (Set("z"), 5L), (Set("x"), 4L), (Set("t"), 3L), (Set("y"), 3L),
      (Set("r"), 3L),
      (Set("x", "z"), 3L), (Set("t", "y"), 3L), (Set("t", "x"), 3L), (Set("s", "x"), 3L),
      (Set("y", "x"), 3L), (Set("y", "z"), 3L), (Set("t", "z"), 3L),
      (Set("y", "x", "z"), 3L), (Set("t", "x", "z"), 3L), (Set("t", "y", "z"), 3L),
      (Set("t", "y", "x"), 3L),
      (Set("t", "y", "x", "z"), 3L))
    assert(freqItemsets3.toSet === expected)

    val model2 = fpg
      .setMinSupport(0.3)
      .setNumPartitions(4)
      .run(rdd)
    assert(model2.freqItemsets.count() === 54)

    val model1 = fpg
      .setMinSupport(0.1)
      .setNumPartitions(8)
      .run(rdd)
    assert(model1.freqItemsets.count() === 625)
  }

  test("FP-Growth using Int type") {
    val transactions = Seq(
      "1 2 3",
      "1 2 3 4",
      "5 4 3 2 1",
      "6 5 4 3 2 1",
      "2 4",
      "1 3",
      "1 7")
      .map(_.split(" ").map(_.toInt).toArray)
    val rdd = sc.parallelize(transactions, 2).cache()

    val fpg = new FPGrowth()

    val model6 = fpg
      .setMinSupport(0.9)
      .setNumPartitions(1)
      .run(rdd)
    assert(model6.freqItemsets.count() === 0)

    val model3 = fpg
      .setMinSupport(0.5)
      .setNumPartitions(2)
      .run(rdd)
    assert(model3.freqItemsets.first().items.getClass === Array(1).getClass,
      "frequent itemsets should use primitive arrays")
    val freqItemsets3 = model3.freqItemsets.collect().map { itemset =>
      (itemset.items.toSet, itemset.freq)
    }
    val expected = Set(
      (Set(1), 6L), (Set(2), 5L), (Set(3), 5L), (Set(4), 4L),
      (Set(1, 2), 4L), (Set(1, 3), 5L), (Set(2, 3), 4L),
      (Set(2, 4), 4L), (Set(1, 2, 3), 4L))
    assert(freqItemsets3.toSet === expected)

    val model2 = fpg
      .setMinSupport(0.3)
      .setNumPartitions(4)
      .run(rdd)
    assert(model2.freqItemsets.count() === 15)

    val model1 = fpg
      .setMinSupport(0.1)
      .setNumPartitions(8)
      .run(rdd)
    assert(model1.freqItemsets.count() === 65)
  }
}
