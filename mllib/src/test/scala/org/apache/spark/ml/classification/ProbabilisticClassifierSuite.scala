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

package org.apache.spark.ml.classification

import org.scalatest.Assertions._

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{Identifiable, MLTest}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.functions.udf

final class TestProbabilisticClassificationModel(
    override val uid: String,
    override val numFeatures: Int,
    override val numClasses: Int)
  extends ProbabilisticClassificationModel[Vector, TestProbabilisticClassificationModel] {

  override def copy(extra: org.apache.spark.ml.param.ParamMap): this.type = defaultCopy(extra)

  override def predictRaw(input: Vector): Vector = {
    input
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction
  }

  def friendlyPredict(values: Double*): Double = {
    predict(Vectors.dense(values.toArray))
  }
}

final class MockProbabilisticClassificationModelWithColumnFunctions(override val uid: String)
  extends ProbabilisticClassificationModel[
    Vector, MockProbabilisticClassificationModelWithColumnFunctions] {

  def this() = this(Identifiable.randomUID("mockprobabilisticmodelwithcolumnfunctions"))

  private def vectorColumn(input: Column): Column = udf((value: Vector) => value).apply(input)

  private def predictionFromVector(input: Column): Column = {
    udf((value: Vector) => value.argmax.toDouble).apply(input)
  }

  override protected def predictRawColumn(features: Column): Column = vectorColumn(features)

  override protected def raw2probabilityColumn(rawPrediction: Column): Column = {
    vectorColumn(rawPrediction)
  }

  override protected def predictProbabilityColumn(features: Column): Column = vectorColumn(features)

  override protected def raw2predictionColumn(rawPrediction: Column): Column = {
    predictionFromVector(rawPrediction)
  }

  override protected def probability2predictionColumn(probability: Column): Column = {
    predictionFromVector(probability)
  }

  override protected def predictionColumn(features: Column): Column = predictionFromVector(features)

  override def predict(features: Vector): Double = throw new UnsupportedOperationException()

  override def predictRaw(features: Vector): Vector = throw new UnsupportedOperationException()

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    throw new UnsupportedOperationException()
  }

  override protected def raw2probability(rawPrediction: Vector): Vector = {
    throw new UnsupportedOperationException()
  }

  override def predictProbability(features: Vector): Vector = {
    throw new UnsupportedOperationException()
  }

  override protected def raw2prediction(rawPrediction: Vector): Double = {
    throw new UnsupportedOperationException()
  }

  override protected def probability2prediction(probability: Vector): Double = {
    throw new UnsupportedOperationException()
  }

  override def copy(extra: ParamMap): MockProbabilisticClassificationModelWithColumnFunctions = {
    throw new UnsupportedOperationException()
  }

  override def numClasses: Int = 2
}


class ProbabilisticClassifierSuite extends SparkFunSuite with MLlibTestSparkContext {

  import testImplicits._

  test("test thresholding") {
    val testModel = new TestProbabilisticClassificationModel("myuid", 2, 2)
      .setThresholds(Array(0.5, 0.2))
    assert(testModel.friendlyPredict(1.0, 1.0) === 1.0)
    assert(testModel.friendlyPredict(1.0, 0.2) === 0.0)
  }

  test("test thresholding not required") {
    val testModel = new TestProbabilisticClassificationModel("myuid", 2, 2)
    assert(testModel.friendlyPredict(1.0, 2.0) === 1.0)
  }

  test("test tiebreak") {
    val testModel = new TestProbabilisticClassificationModel("myuid", 2, 2)
      .setThresholds(Array(0.4, 0.4))
    assert(testModel.friendlyPredict(0.6, 0.6) === 0.0)
  }

  test("test one zero threshold") {
    val testModel = new TestProbabilisticClassificationModel("myuid", 2, 2)
      .setThresholds(Array(0.0, 0.1))
    assert(testModel.friendlyPredict(1.0, 10.0) === 0.0)
    assert(testModel.friendlyPredict(0.0, 10.0) === 1.0)
  }

  test("bad thresholds") {
    intercept[IllegalArgumentException] {
      new TestProbabilisticClassificationModel("myuid", 2, 2).setThresholds(Array(0.0, 0.0))
    }
    intercept[IllegalArgumentException] {
      new TestProbabilisticClassificationModel("myuid", 2, 2).setThresholds(Array(-0.1, 0.1))
    }
  }

  test("normalizeToProbabilitiesInPlace") {
    val vec1 = Vectors.dense(1.0, 2.0, 3.0).toDense
    ProbabilisticClassificationModel.normalizeToProbabilitiesInPlace(vec1)
    assert(vec1 ~== Vectors.dense(1.0 / 6, 2.0 / 6, 3.0 / 6) relTol 1e-3)

    // all-0 input test
    val vec2 = Vectors.dense(0.0, 0.0, 0.0).toDense
    intercept[IllegalArgumentException] {
      ProbabilisticClassificationModel.normalizeToProbabilitiesInPlace(vec2)
    }

    // negative input test
    val vec3 = Vectors.dense(1.0, -1.0, 2.0).toDense
    intercept[IllegalArgumentException] {
      ProbabilisticClassificationModel.normalizeToProbabilitiesInPlace(vec3)
    }
  }

  test("transform uses probabilistic column prediction hooks") {
    val expected = Vectors.dense(0.25, 0.75)
    val data = Seq(Tuple1(expected)).toDF("features")

    val allOutputs = new MockProbabilisticClassificationModelWithColumnFunctions()
      .transform(data)
      .select("rawPrediction", "probability", "prediction")
      .head()
    assert(allOutputs.getAs[Vector](0) === expected)
    assert(allOutputs.getAs[Vector](1) === expected)
    assert(allOutputs.getDouble(2) === 1.0)

    val probabilityAndPrediction = new MockProbabilisticClassificationModelWithColumnFunctions()
      .setRawPredictionCol("")
      .transform(data)
      .select("probability", "prediction")
      .head()
    assert(probabilityAndPrediction.getAs[Vector](0) === expected)
    assert(probabilityAndPrediction.getDouble(1) === 1.0)

    val predictionOnly = new MockProbabilisticClassificationModelWithColumnFunctions()
      .setRawPredictionCol("")
      .setProbabilityCol("")
      .transform(data)
      .select("prediction")
      .head()
    assert(predictionOnly.getDouble(0) === 1.0)
  }
}

object ProbabilisticClassifierSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = ClassifierSuite.allParamSettings ++ Map(
    "probabilityCol" -> "myProbability",
    "thresholds" -> Array(0.4, 0.6)
  )

  /**
   * Helper for testing that a ProbabilisticClassificationModel computes
   * the same predictions across all combinations of output columns
   * (rawPrediction/probability/prediction) turned on/off. Makes sure the
   * output column values match by comparing vs. the case with all 3 output
   * columns turned on.
   */
  def testPredictMethods[
      FeaturesType,
      M <: ProbabilisticClassificationModel[FeaturesType, M]](
    mlTest: MLTest, model: M, testData: Dataset[_]): Unit = {

    val allColModel = model.copy(ParamMap.empty)
      .setRawPredictionCol("rawPredictionAll")
      .setProbabilityCol("probabilityAll")
      .setPredictionCol("predictionAll")

    val allColResult = allColModel.transform(testData.select(allColModel.getFeaturesCol))
      .select(allColModel.getFeaturesCol, "rawPredictionAll", "probabilityAll", "predictionAll")

    for (rawPredictionCol <- Seq("", "rawPredictionSingle")) {
      for (probabilityCol <- Seq("", "probabilitySingle")) {
        for (predictionCol <- Seq("", "predictionSingle")) {
          val newModel = model.copy(ParamMap.empty)
            .setRawPredictionCol(rawPredictionCol)
            .setProbabilityCol(probabilityCol)
            .setPredictionCol(predictionCol)

          import allColResult.sparkSession.implicits._

          mlTest.testTransformer[(Vector, Vector, Vector, Double)](allColResult, newModel,
            if (rawPredictionCol.isEmpty) "rawPredictionAll" else rawPredictionCol,
            "rawPredictionAll",
            if (probabilityCol.isEmpty) "probabilityAll" else probabilityCol, "probabilityAll",
            if (predictionCol.isEmpty) "predictionAll" else predictionCol, "predictionAll"
          ) {
            case Row(
              rawPredictionSingle: Vector, rawPredictionAll: Vector,
              probabilitySingle: Vector, probabilityAll: Vector,
              predictionSingle: Double, predictionAll: Double
            ) => {
              assert(rawPredictionSingle ~== rawPredictionAll relTol 1E-3)
              assert(probabilitySingle ~== probabilityAll relTol 1E-3)
              assert(predictionSingle ~== predictionAll relTol 1E-3)
            }
          }
        }
      }
    }
  }

}
