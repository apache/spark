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
    val numTransactions = 6
    val freqItemsets = sc.parallelize(Seq(
      (Set("s"), 3L), (Set("z"), 5L), (Set("x"), 4L), (Set("t"), 3L), (Set("y"), 3L),
      (Set("r"), 3L),
      (Set("x", "z"), 3L), (Set("t", "y"), 3L), (Set("t", "x"), 3L), (Set("s", "x"), 3L),
      (Set("y", "x"), 3L), (Set("y", "z"), 3L), (Set("t", "z"), 3L),
      (Set("y", "x", "z"), 3L), (Set("t", "x", "z"), 3L), (Set("t", "y", "z"), 3L),
      (Set("t", "y", "x"), 3L),
      (Set("t", "y", "x", "z"), 3L))
      .map({ case (items, freq) => new FreqItemset(items.toArray, freq) }))

    val ar = new AssociationRules()

    val results1 = ar
      .setMinConfidence(0.9)
      .run(new FPGrowthModel[String](freqItemsets, numTransactions))
      .collect()

    assert(results1.size === 23)
    assert(results1.forall(rule => math.abs(rule.confidence - 1.0D) < 1e-6))

    val results2 = ar
      .setMinConfidence(0)
      .run(new FPGrowthModel[String](freqItemsets, numTransactions))
      .collect()

    assert(results2.size === 36)
    assert(results2.count(rule => math.abs(rule.confidence - 0.5D) < 1e-6) == 4)
  }
}

