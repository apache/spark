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
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

class ClassifierSuite extends SparkFunSuite with MLlibTestSparkContext {
  import testImplicits._

  private def getTestData(labels: Seq[Double]): DataFrame = {
    labels.map { label: Double => LabeledPoint(label, Vectors.dense(0.0)) }.toDF()
  }

  test("extractLabeledPoints") {
    val c = new MockClassifier
    // Valid dataset
    val df0 = getTestData(Seq(0.0, 2.0, 1.0, 5.0))
    c.extractLabeledPoints(df0, 6).count()
    // Invalid datasets
    val df1 = getTestData(Seq(0.0, -2.0, 1.0, 5.0))
    withClue("Classifier should fail if label is negative") {
      val e: SparkException = intercept[SparkException] {
        c.extractLabeledPoints(df1, 6).count()
      }
      assert(e.getMessage.contains("given dataset with invalid label"))
    }
    val df2 = getTestData(Seq(0.0, 2.1, 1.0, 5.0))
    withClue("Classifier should fail if label is not an integer") {
      val e: SparkException = intercept[SparkException] {
        c.extractLabeledPoints(df2, 6).count()
      }
      assert(e.getMessage.contains("given dataset with invalid label"))
    }
    // extractLabeledPoints with numClasses specified
    withClue("Classifier should fail if label is >= numClasses") {
      val e: SparkException = intercept[SparkException] {
        c.extractLabeledPoints(df0, numClasses = 5).count()
      }
      assert(e.getMessage.contains("given dataset with invalid label"))
    }
    withClue("Classifier.extractLabeledPoints should fail if numClasses <= 0") {
      val e: IllegalArgumentException = intercept[IllegalArgumentException] {
        c.extractLabeledPoints(df0, numClasses = 0).count()
      }
      assert(e.getMessage.contains("but requires numClasses > 0"))
    }
  }

  test("getNumClasses") {
    val c = new MockClassifier
    // Valid dataset
    val df0 = getTestData(Seq(0.0, 2.0, 1.0, 5.0))
    assert(c.getNumClasses(df0) === 6)
    // Invalid datasets
    val df1 = getTestData(Seq(0.0, 2.0, 1.0, 5.1))
    withClue("getNumClasses should fail if label is max label not an integer") {
      val e: IllegalArgumentException = intercept[IllegalArgumentException] {
        c.getNumClasses(df1)
      }
      assert(e.getMessage.contains("requires integers in range"))
    }
    val df2 = getTestData(Seq(0.0, 2.0, 1.0, Int.MaxValue.toDouble))
    withClue("getNumClasses should fail if label is max label is >= Int.MaxValue") {
      val e: IllegalArgumentException = intercept[IllegalArgumentException] {
        c.getNumClasses(df2)
      }
      assert(e.getMessage.contains("requires integers in range"))
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

    override def train(dataset: Dataset[_]): MockClassificationModel =
      throw new NotImplementedError()

    // Make methods public
    override def extractLabeledPoints(dataset: Dataset[_], numClasses: Int): RDD[LabeledPoint] =
      super.extractLabeledPoints(dataset, numClasses)
    def getNumClasses(dataset: Dataset[_]): Int = super.getNumClasses(dataset)
  }

  class MockClassificationModel(override val uid: String)
    extends ClassificationModel[Vector, MockClassificationModel] {

    def this() = this(Identifiable.randomUID("mockclassificationmodel"))

    protected def predictRaw(features: Vector): Vector = throw new NotImplementedError()

    override def copy(extra: ParamMap): MockClassificationModel = throw new NotImplementedError()

    override def numClasses: Int = throw new NotImplementedError()
  }

}
