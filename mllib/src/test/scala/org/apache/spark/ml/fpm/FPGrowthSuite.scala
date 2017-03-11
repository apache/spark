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
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class FPGrowthSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    dataset = FPGrowthSuite.getFPGrowthData(spark)
  }

  test("FPGrowth fit and transform with different data types") {
    Array(IntegerType, StringType, ShortType, LongType, ByteType).foreach { dt =>
      val data = dataset.withColumn("features", col("features").cast(ArrayType(dt)))
      val model = new FPGrowth().setMinSupport(0.5).fit(data)
      val generatedRules = model.setMinConfidence(0.5).associationRules
      val expectedRules = spark.createDataFrame(Seq(
        (Array("2"), Array("1"), 1.0),
        (Array("1"), Array("2"), 0.75)
      )).toDF("antecedent", "consequent", "confidence")
        .withColumn("antecedent", col("antecedent").cast(ArrayType(dt)))
        .withColumn("consequent", col("consequent").cast(ArrayType(dt)))
      assert(expectedRules.sort("antecedent").rdd.collect().sameElements(
        generatedRules.sort("antecedent").rdd.collect()))

      val transformed = model.transform(data)
      val expectedTransformed = spark.createDataFrame(Seq(
        (0, Array("1", "2"), Array.emptyIntArray),
        (0, Array("1", "2"), Array.emptyIntArray),
        (0, Array("1", "2"), Array.emptyIntArray),
        (0, Array("1", "3"), Array(2))
      )).toDF("id", "features", "prediction")
        .withColumn("features", col("features").cast(ArrayType(dt)))
        .withColumn("prediction", col("prediction").cast(ArrayType(dt)))
      assert(expectedTransformed.collect().toSet.equals(
        transformed.collect().toSet))
    }
  }

  test("FPGrowth getFreqItems") {
    val model = new FPGrowth().setMinSupport(0.7).fit(dataset)
    val expectedFreq = spark.createDataFrame(Seq(
      (Array("1"), 4L),
      (Array("2"), 3L),
      (Array("1", "2"), 3L),
      (Array("2", "1"), 3L) // duplicate as the items sequence is not guaranteed
    )).toDF("items", "expectedFreq")
    val freqItems = model.freqItemsets

    val checkDF = freqItems.join(expectedFreq, "items")
    assert(checkDF.count() == 3 && checkDF.filter(col("freq") === col("expectedFreq")).count() == 3)
  }

  test("FPGrowth getFreqItems with Null") {
    val df = spark.createDataFrame(Seq(
      (1, Array("1", "2", "3", "5")),
      (2, Array("1", "2", "3", "4")),
      (3, null.asInstanceOf[Array[String]])
    )).toDF("id", "features")
    val model = new FPGrowth().setMinSupport(0.7).fit(dataset)
    val prediction = model.transform(df)
    assert(prediction.select("prediction").where("id=3").first().getSeq[String](0).isEmpty)
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
      assert(model.freqItemsets.sort("items").collect() ===
        model2.freqItemsets.sort("items").collect())
    }
    val fPGrowth = new FPGrowth()
    testEstimatorAndModelReadWrite(fPGrowth, dataset, FPGrowthSuite.allParamSettings,
      FPGrowthSuite.allParamSettings, checkModelData)
  }

}

object FPGrowthSuite {

  def getFPGrowthData(spark: SparkSession): DataFrame = {
    spark.createDataFrame(Seq(
      (0, Array("1", "2")),
      (0, Array("1", "2")),
      (0, Array("1", "2")),
      (0, Array("1", "3"))
    )).toDF("id", "features")
  }

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "minSupport" -> 0.321,
    "minConfidence" -> 0.456,
    "numPartitions" -> 5,
    "predictionCol" -> "myPrediction"
  )
}
