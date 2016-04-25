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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.classification.ClassifierSuite.MockClassifier
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

class ClassifierSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("extractLabeledPoints") {
    def getTestData(labels: Seq[Double]): DataFrame = {
      val data = labels.map { label: Double => LabeledPoint(label, Vectors.dense(0.0)) }
      sqlContext.createDataFrame(data)
    }

    val c = new MockClassifier
    // Valid dataset
    val df0 = getTestData(Seq(0.0, 2.0, 1.0, 5.0))
    c.extractLabeledPoints(df0).count()
    // Invalid datasets
    val df1 = getTestData(Seq(0.0, -2.0, 1.0, 5.0))
    withClue("Classifier should fail if label is negative") {
      val e: SparkException = intercept[SparkException] {
        c.extractLabeledPoints(df1).count()
      }
      assert(e.getMessage.contains("given dataset with invalid label"))
    }
    val df2 = getTestData(Seq(0.0, 2.1, 1.0, 5.0))
    withClue("Classifier should fail if label is not an integer") {
      val e: SparkException = intercept[SparkException] {
        c.extractLabeledPoints(df2).count()
      }
      assert(e.getMessage.contains("given dataset with invalid label"))
    }

    // Bad numClasses
    withClue("Classifier should fail for labels >= numClasses") {
      val e: SparkException = intercept[SparkException] {
        Classifier.extractLabeledPoints(df1, c.getLabelCol, c.getFeaturesCol, numClasses = 4)
          .count()
      }
      assert(e.getMessage.contains("where numClasses = 4"))
    }
  }
}

object ClassifierSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "predictionCol" -> "myPrediction",
    "rawPredictionCol" -> "myRawPrediction"
  )

  class MockClassifier(override val uid: String)
    extends Classifier[Vector, MockClassifier, MockClassificationModel] {

    def this() = this(Identifiable.randomUID("mockclassifier"))

    override def copy(extra: ParamMap): MockClassifier = throw new NotImplementedError()

    override def train(dataset: Dataset[_]): MockClassificationModel = throw new NotImplementedError()

    // Make method public
    override def extractLabeledPoints(dataset: Dataset[_]): RDD[LabeledPoint] =
      super.extractLabeledPoints(dataset)
  }

  class MockClassificationModel(override val uid: String)
    extends ClassificationModel[Vector, MockClassificationModel] {

    def this() = this(Identifiable.randomUID("mockclassificationmodel"))

    protected def predictRaw(features: Vector): Vector = throw new NotImplementedError()

    override def copy(extra: ParamMap): MockClassificationModel = throw new NotImplementedError()

    override def numClasses: Int = throw new NotImplementedError()
  }

}
