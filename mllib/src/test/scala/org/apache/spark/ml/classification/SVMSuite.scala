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

import breeze.linalg.{DenseVector => BDV}
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext

class SVMSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("SVM binary classification") {
    val nPoints = 100
    // NOTE: Intercept should be small for generating equal 0s and 1s
    val A = 0.01
    val B = -1.5
    val C = 1.0
    val binaryDataset = {
      val testData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 42)
      spark.createDataFrame(sc.parallelize(testData, 4))
    }
    val svm = new SVM().setMaxIter(10).setRegParam(0.3)
    val model = svm.fit(binaryDataset)

    val validationData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 17)
    val validationDataFrame = spark.createDataFrame(sc.parallelize(validationData, 4))

    assert(model.transform(validationDataFrame).where("prediction=label").count() > nPoints * 0.8)
  }


  test("read/write: SVM") {
    def checkModelData(model: SVMModel, model2: SVMModel): Unit = {
      assert(model.intercept === model2.intercept)
      assert(model.weights.toArray === model2.weights.toArray)
      assert(model.numFeatures === model2.numFeatures)
    }
    val svm = new SVM()
    val nPoints = 100
    // NOTE: Intercept should be small for generating equal 0s and 1s
    val A = 0.01
    val B = -1.5
    val C = 1.0
    val binaryDataset = {
      val testData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 42)
      spark.createDataFrame(sc.parallelize(testData, 4))
    }
    testEstimatorAndModelReadWrite(svm, binaryDataset, SVMSuite.allParamSettings,
      checkModelData)
  }
}

object SVMSuite {

  val allParamSettings: Map[String, Any] = Map(
    "regParam" -> 0.01,
    "maxIter" -> 2,  // intentionally small
    "fitIntercept" -> true,
    "tol" -> 0.8,
    "standardization" -> false,
    "threshold" -> 0.6
  )

    // Generate noisy input of the form Y = signum(x.dot(weights) + intercept + noise)
  def generateSVMInput(
    intercept: Double,
    weights: Array[Double],
    nPoints: Int,
    seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)
    val weightsMat = new BDV(weights)
    val x = Array.fill[Array[Double]](nPoints)(
        Array.fill[Double](weights.length)(rnd.nextDouble() * 2.0 - 1.0))
    val y = x.map { xi =>
      val yD = new BDV(xi).dot(weightsMat) + intercept + 0.01 * rnd.nextGaussian()
      if (yD < 0) 0.0 else 1.0
    }
    y.zip(x).map(p => LabeledPoint(p._1, Vectors.dense(p._2)))
  }

}

