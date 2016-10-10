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
package org.apache.spark.ml.fpm

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.fpm.{FPGrowth => MLlibFPGrowth, FPGrowthModel => MLlibFPGrowthModel}
import org.apache.spark.mllib.util.MLlibTestSparkContext

class AssociationRulesSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {
  test("association rules using String type") {

    val freqItems = spark.createDataFrame(Seq(
      (Array("a", "b"), 3L),
      (Array("a"), 3L),
      (Array("b"), 3L))
    ).toDF("items", "freq")

    val associationRules = new AssociationRules()
      .setMinConfidence(0.8)
      .setMinSupport(0.3)
      .setItemsCol("items")
      .setFreqCol("freq")
    val rules = associationRules.run(freqItems)

    val expectedRules = spark.createDataFrame(Seq(
      (Array("a"), Array("b"), 1.0),
      (Array("b"), Array("a"), 1.0))
    ).toDF("antecedent", "consequent", "confidence")
    assert(rules.sort("antecedent").rdd.collect() ===
      expectedRules.sort("antecedent").rdd.collect())

  }
}
