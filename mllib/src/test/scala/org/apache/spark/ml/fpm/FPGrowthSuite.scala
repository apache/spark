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
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class FPGrowthSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    dataset = FPGrowthSuite.getFPGrowthData(spark)
  }

  test("FPGrowth fit and transform") {
    val model = new FPGrowth().setMinSupport(0.8).fit(dataset)
    val generatedRules = model.setMinConfidence(0.8).getAssociationRules
    val expectedRules = spark.createDataFrame(Seq(
      (Array("2"), Array("1"), 1.0),
      (Array("1"), Array("2"), 1.0)
    )).toDF("antecedent", "consequent", "confidence")

    assert(expectedRules.sort("antecedent").rdd.collect().sameElements(
      generatedRules.sort("antecedent").rdd.collect()))
    val transformed = model.transform(dataset)
    assert(transformed.count() == 3 &&
      transformed.rdd.collect().forall(r => r.getSeq[String](2).sameElements(Array("1", "2"))))
  }

  test("FPGrowth getFreqItems") {
    val model = new FPGrowth().setMinSupport(0.8).fit(dataset)
    val expectedFreq = spark.createDataFrame(Seq(
      (Array("1"), 3L),
      (Array("2"), 3L),
      (Array("1", "2"), 3L)
    )).toDF("items", "freq")
    val freqItems = model.getFreqItems
    assert(freqItems.sort("items").rdd.collect()
      .sameElements(expectedFreq.sort("items").rdd.collect()))
  }

  test("FPGrowth get Association Rules") {
    val model = new FPGrowth().setMinSupport(0.8).fit(dataset)
    val expectedRules = spark.createDataFrame(Seq(
      (Array("2"), Array("1"), 1.0),
      (Array("1"), Array("2"), 1.0)
    )).toDF("antecedent", "consequent", "confidence")
    val associationRules = model.getAssociationRules

    assert(associationRules.sort("antecedent").rdd.collect()
      .sameElements(expectedRules.sort("antecedent").rdd.collect()))
  }

  test("FPGrowth parameter check") {
    val fpGrowth = new FPGrowth().setMinSupport(0.4567)
    val model = fpGrowth.fit(dataset)
                  .setMinConfidence(0.5678)
    assert(fpGrowth.getMinSupport === 0.4567)
    assert(model.getMinConfidence === 0.5678)
  }

  test("read/write") {
    def checkModelData(model: FPGrowthModel, model2: FPGrowthModel): Unit = {
      assert(model.getAssociationRules.collect() ===
        model2.getAssociationRules.collect())
    }
    val fPGrowth = new FPGrowth()
    testEstimatorAndModelReadWrite(
      fPGrowth, dataset, FPGrowthSuite.allParamSettings, checkModelData)
  }

}

object FPGrowthSuite {

  def getFPGrowthData(spark: SparkSession): DataFrame = {
    spark.createDataFrame(Seq(
      (0, Array("1", "2", "3", "5")),
      (0, Array("1", "2", "3", "6")),
      (0, Array("1", "2", "7"))
    )).toDF("id", "features")
  }

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "minSupport" -> 0.3
  )
}
