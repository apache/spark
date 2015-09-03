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

    /* Verify results using the `R` code:
       transactions = as(sapply(
         list("r z h k p",
              "z y x w v u t s",
              "s x o n r",
              "x z y m t s q e",
              "z",
              "x z y r q t p"),
         FUN=function(x) strsplit(x," ",fixed=TRUE)),
         "transactions")
       > eclat(transactions, parameter = list(support = 0.9))
       ...
       eclat - zero frequent items
       set of 0 itemsets
     */
    assert(model6.freqItemsets.count() === 0)

    val model3 = fpg
      .setMinSupport(0.5)
      .setNumPartitions(2)
      .run(rdd)
    val freqItemsets3 = model3.freqItemsets.collect().map { itemset =>
      (itemset.items.toSet, itemset.freq)
    }

    /* Verify results using the `R` code:
       fp = eclat(transactions, parameter = list(support = 0.5))
       fpDF = as(sort(fp), "data.frame")
       fpDF$support = fpDF$support * length(transactions)
       names(fpDF)[names(fpDF) == "support"] = "freq"
       > fpDF
              items freq
       13       {z}    5
       14       {x}    4
       1      {s,x}    3
       2  {t,x,y,z}    3
       3    {t,y,z}    3
       4    {t,x,y}    3
       5    {x,y,z}    3
       6      {y,z}    3
       7      {x,y}    3
       8      {t,y}    3
       9    {t,x,z}    3
       10     {t,z}    3
       11     {t,x}    3
       12     {x,z}    3
       15       {t}    3
       16       {y}    3
       17       {s}    3
       18       {r}    3
     */
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

    /* Verify results using the `R` code:
       fp = eclat(transactions, parameter = list(support = 0.3))
       fpDF = as(fp, "data.frame")
       fpDF$support = fpDF$support * length(transactions)
       names(fpDF)[names(fpDF) == "support"] = "freq"
       > nrow(fpDF)
       [1] 54
     */
    assert(model2.freqItemsets.count() === 54)

    val model1 = fpg
      .setMinSupport(0.1)
      .setNumPartitions(8)
      .run(rdd)

    /* Verify results using the `R` code:
       fp = eclat(transactions, parameter = list(support = 0.1))
       fpDF = as(fp, "data.frame")
       fpDF$support = fpDF$support * length(transactions)
       names(fpDF)[names(fpDF) == "support"] = "freq"
       > nrow(fpDF)
       [1] 625
     */
    assert(model1.freqItemsets.count() === 625)
  }

  test("FP-Growth String type association rule generation") {
    val transactions = Seq(
      "r z h k p",
      "z y x w v u t s",
      "s x o n r",
      "x z y m t s q e",
      "z",
      "x z y r q t p")
      .map(_.split(" "))
    val rdd = sc.parallelize(transactions, 2).cache()

    /* Verify results using the `R` code:
       transactions = as(sapply(
         list("r z h k p",
              "z y x w v u t s",
              "s x o n r",
              "x z y m t s q e",
              "z",
              "x z y r q t p"),
         FUN=function(x) strsplit(x," ",fixed=TRUE)),
         "transactions")
       ars = apriori(transactions,
                     parameter = list(support = 0.0, confidence = 0.5, target="rules", minlen=2))
       arsDF = as(ars, "data.frame")
       arsDF$support = arsDF$support * length(transactions)
       names(arsDF)[names(arsDF) == "support"] = "freq"
       > nrow(arsDF)
       [1] 23
       > sum(arsDF$confidence == 1)
       [1] 23
     */
    val rules = (new FPGrowth())
      .setMinSupport(0.5)
      .setNumPartitions(2)
      .run(rdd)
      .generateAssociationRules(0.9)
      .collect()

    assert(rules.size === 23)
    assert(rules.count(rule => math.abs(rule.confidence - 1.0D) < 1e-6) == 23)
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

    /* Verify results using the `R` code:
       transactions = as(sapply(
         list("1 2 3",
              "1 2 3 4",
              "5 4 3 2 1",
              "6 5 4 3 2 1",
              "2 4",
              "1 3",
              "1 7"),
         FUN=function(x) strsplit(x," ",fixed=TRUE)),
         "transactions")
       > eclat(transactions, parameter = list(support = 0.9))
       ...
       eclat - zero frequent items
       set of 0 itemsets
     */
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

    /* Verify results using the `R` code:
       fp = eclat(transactions, parameter = list(support = 0.5))
       fpDF = as(sort(fp), "data.frame")
       fpDF$support = fpDF$support * length(transactions)
       names(fpDF)[names(fpDF) == "support"] = "freq"
       > fpDF
          items freq
      6     {1}    6
      3   {1,3}    5
      7     {2}    5
      8     {3}    5
      1   {2,4}    4
      2 {1,2,3}    4
      4   {2,3}    4
      5   {1,2}    4
      9     {4}    4
     */
    val expected = Set(
      (Set(1), 6L), (Set(2), 5L), (Set(3), 5L), (Set(4), 4L),
      (Set(1, 2), 4L), (Set(1, 3), 5L), (Set(2, 3), 4L),
      (Set(2, 4), 4L), (Set(1, 2, 3), 4L))
    assert(freqItemsets3.toSet === expected)

    val model2 = fpg
      .setMinSupport(0.3)
      .setNumPartitions(4)
      .run(rdd)

    /* Verify results using the `R` code:
       fp = eclat(transactions, parameter = list(support = 0.3))
       fpDF = as(fp, "data.frame")
       fpDF$support = fpDF$support * length(transactions)
       names(fpDF)[names(fpDF) == "support"] = "freq"
       > nrow(fpDF)
       [1] 15
     */
    assert(model2.freqItemsets.count() === 15)

    val model1 = fpg
      .setMinSupport(0.1)
      .setNumPartitions(8)
      .run(rdd)

    /* Verify results using the `R` code:
       fp = eclat(transactions, parameter = list(support = 0.1))
       fpDF = as(fp, "data.frame")
       fpDF$support = fpDF$support * length(transactions)
       names(fpDF)[names(fpDF) == "support"] = "freq"
       > nrow(fpDF)
       [1] 65
     */
    assert(model1.freqItemsets.count() === 65)
  }
}
