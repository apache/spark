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

class AssociationRulesSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("association rules using String type") {
    val freqItemsets = sc.parallelize(Seq(
      (Set("s"), 3L), (Set("z"), 5L), (Set("x"), 4L), (Set("t"), 3L), (Set("y"), 3L),
      (Set("r"), 3L),
      (Set("x", "z"), 3L), (Set("t", "y"), 3L), (Set("t", "x"), 3L), (Set("s", "x"), 3L),
      (Set("y", "x"), 3L), (Set("y", "z"), 3L), (Set("t", "z"), 3L),
      (Set("y", "x", "z"), 3L), (Set("t", "x", "z"), 3L), (Set("t", "y", "z"), 3L),
      (Set("t", "y", "x"), 3L),
      (Set("t", "y", "x", "z"), 3L)
    ).map {
      case (items, freq) => new FPGrowth.FreqItemset(items.toArray, freq)
    })

    val ar = new AssociationRules()

    val results1 = ar
      .setMinConfidence(0.9)
      .run(freqItemsets)
      .collect()

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
    assert(results1.size === 23)
    assert(results1.count(rule => math.abs(rule.confidence - 1.0D) < 1e-6) == 23)

    val results2 = ar
      .setMinConfidence(0)
      .run(freqItemsets)
      .collect()

    /* Verify results using the `R` code:
       ars = apriori(transactions,
                  parameter = list(support = 0.5, confidence = 0.5, target="rules", minlen=2))
       arsDF = as(ars, "data.frame")
       arsDF$support = arsDF$support * length(transactions)
       names(arsDF)[names(arsDF) == "support"] = "freq"
       nrow(arsDF)
       sum(arsDF$confidence == 1)
       > nrow(arsDF)
       [1] 30
       > sum(arsDF$confidence == 1)
       [1] 23
     */
    assert(results2.size === 30)
    assert(results2.count(rule => math.abs(rule.confidence - 1.0D) < 1e-6) == 23)
  }

  test("association rules with defined consequent-length") {
    val freqItemsets = sc.parallelize(Seq(
      (Set("12"), 3L), (Set("2"), 5L), (Set("7"), 4L), (Set("11"), 3L), (Set("6"), 3L),
      (Set("1"), 3L),
      (Set("7", "2"), 3L), (Set("11", "6"), 3L), (Set("11", "7"), 3L), (Set("12", "7"), 3L),
      (Set("6", "7"), 3L), (Set("6", "2"), 3L), (Set("11", "2"), 3L),
      (Set("6", "7", "2"), 3L), (Set("11", "7", "2"), 3L), (Set("11", "6", "2"), 3L),
      (Set("11", "6", "7"), 3L),
      (Set("11", "6", "7", "2"), 3L)
    ).map {
      case (items, freq) => new FPGrowth.FreqItemset(items.toArray, freq)
    })

    val ar = new AssociationRules()

    val results1 = ar
      .setMinConfidence(0.9)
      .setMaxConsequent(Int.MaxValue)
      .run(freqItemsets)
      .collect()

    /* Verify results using the CMD with SPMF library (http://www.philippe-fournier-viger.com/spmf/)
       Note: Arules package in R do not support rules with consequent-length larger than 1.
       Note: Because SPMF's implementation of Association Rules only support integer as item, the above dataset
        was encoded into integers.
       echo '1 2 3 4 5\n2 6 7 8 9 10 11 12\n12 7 13 14 1\n7 2 6 15 11 12 16 17\n2\n7 2 6 1 16 11 5' > dataset
       java -jar spmf.jar run FPGrowth_association_rules dataset rules1 50% 90%
       awk 'BEGIN{FS="[=#:>]+"; PROCINFO["sorted_in"]="@ind_num_asc"} {n=split($2,a," "); s[n]++}
        END{for(i in s) print i,s[i]}' rules1
       > 1 23
         2 12
         3 2
       awk 'BEGIN{FS="[=#:>]+"; PROCINFO["sorted_in"]="@ind_num_asc"} {n=split($2,a," "); s[n][$NF]++}
        END{for(i in s)for(j in s[i]) print i,j,s[i][j]}' rules2
       > 1  1.0 23
         2  1.0 12
         3  1.0 2
     */
    assert(results1.size === 37)
    assert(results1.filter(_.consequent.size == 1).size == 23)
    assert(results1.filter(_.consequent.size == 2).size == 12)
    assert(results1.filter(_.consequent.size == 3).size == 2)
    assert(results1.count(rule => math.abs(rule.confidence - 1.0D) < 1e-6) == 37)

    val results2 = ar
      .setMinConfidence(0.5)
      .setMaxConsequent(Int.MaxValue)
      .run(freqItemsets)
      .collect()

    /* Verify results using the CMD with SPMF library (http://www.philippe-fournier-viger.com/spmf/)
       Note: Arules package in R do not support rules with consequent-length larger than 1.
       Note: Because SPMF's implementation of Association Rules only support integer as item, the above dataset
        was encoded into integers.
       echo '1 2 3 4 5\n2 6 7 8 9 10 11 12\n12 7 13 14 1\n7 2 6 15 11 12 16 17\n2\n7 2 6 1 16 11 5' > dataset
       java -jar spmf.jar run FPGrowth_association_rules dataset rules2 50% 50%
       awk 'BEGIN{FS="[=#:>]+"; PROCINFO["sorted_in"]="@ind_num_asc"} {n=split($2,a," "); s[n]++}
        END{for(i in s) print i,s[i]}' rules2
       > 1 30
         2 18
         3 4
       awk 'BEGIN{FS="[=#:>]+"; PROCINFO["sorted_in"]="@ind_num_asc"} {n=split($2,a," "); s[n][$NF]++}
        END{for(i in s)for(j in s[i]) print i,j,s[i][j]}' rules2
       > 1  0.6 3
         1  0.75 4
         1  1.0 23
         2  0.6 3
         2  0.75 3
         2  1.0 12
         3  0.6 1
         3  0.75 1
         3  1.0 2
     */
    assert(results2.size === 52)
    assert(results2.filter(_.consequent.size == 1).size == 30)
    assert(results2.filter(_.consequent.size == 2).size == 18)
    assert(results2.filter(_.consequent.size == 3).size == 4)
    assert(results2.count(rule => rule.consequent.size == 1 &&
      math.abs(rule.confidence - 0.6D) < 1e-6) == 3)
    assert(results2.count(rule => rule.consequent.size == 1 &&
      math.abs(rule.confidence - 0.75D) < 1e-6) == 4)
    assert(results2.count(rule => rule.consequent.size == 1 &&
      math.abs(rule.confidence - 1.0D) < 1e-6) == 23)
    assert(results2.count(rule => rule.consequent.size == 2 &&
      math.abs(rule.confidence - 0.6D) < 1e-6) == 3)
    assert(results2.count(rule => rule.consequent.size == 2 &&
      math.abs(rule.confidence - 0.75D) < 1e-6) == 3)
    assert(results2.count(rule => rule.consequent.size == 2 &&
      math.abs(rule.confidence - 1.0D) < 1e-6) == 12)
    assert(results2.count(rule => rule.consequent.size == 3 &&
      math.abs(rule.confidence - 0.6D) < 1e-6) == 1)
    assert(results2.count(rule => rule.consequent.size == 3 &&
      math.abs(rule.confidence - 0.75D) < 1e-6) == 1)
    assert(results2.count(rule => rule.consequent.size == 3 &&
      math.abs(rule.confidence - 1.0D) < 1e-6) == 2)
  }
}

