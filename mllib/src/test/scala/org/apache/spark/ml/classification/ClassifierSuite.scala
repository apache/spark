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
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Dataset}

class ClassifierSuite extends SparkFunSuite with MLlibTestSparkContext {

  import testImplicits._

  private def getTestData(labels: Seq[Double]): DataFrame = {
    labels.map { label: Double => LabeledPoint(label, Vectors.dense(0.0)) }.toDF()
  }

  test("getNumClasses") {
    val c = new MockClassifier
    // Valid dataset
    val df0 = getTestData(Seq(0.0, 2.0, 1.0, 5.0))
    assert(c.getNumClasses(df0) === 6)
    // Invalid datasets
    val df1 = getTestData(Seq(0.0, 2.0, 1.0, 5.1))
    withClue("getNumClasses should fail if label is max label not an integer") {
      val e = intercept[Exception] {
        c.getNumClasses(df1)
      }
      assert(e.getMessage.contains("Labels MUST be Integers"))
    }
    val df2 = getTestData(Seq(0.0, 2.0, 1.0, Int.MaxValue.toDouble))
    withClue("getNumClasses should fail if label is max label is >= Int.MaxValue") {
      val e = intercept[Exception] {
        c.getNumClasses(df2)
      }
      assert(e.getMessage.contains("Labels MUST be in [0"))
    }
    val df3 = getTestData(Seq.empty[Double])
    withClue("getNumClasses should fail if dataset is empty") {
      val e: SparkException = intercept[SparkException] {
        c.getNumClasses(df3)
      }
      assert(e.getMessage == "ML algorithm was given empty dataset.")
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

    override def copy(extra: ParamMap): MockClassifier = throw new UnsupportedOperationException()

    override def train(dataset: Dataset[_]): MockClassificationModel =
      throw new UnsupportedOperationException()

    def getNumClasses(dataset: Dataset[_]): Int =
      DatasetUtils.getNumClasses(dataset, $(labelCol))
  }

  class MockClassificationModel(override val uid: String)
    extends ClassificationModel[Vector, MockClassificationModel] {

    def this() = this(Identifiable.randomUID("mockclassificationmodel"))

    def predictRaw(features: Vector): Vector = throw new UnsupportedOperationException()

    override def copy(extra: ParamMap): MockClassificationModel =
      throw new UnsupportedOperationException()

    override def numClasses: Int = throw new UnsupportedOperationException()
  }

}
