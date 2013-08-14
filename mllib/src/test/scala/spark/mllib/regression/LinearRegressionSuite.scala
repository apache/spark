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

package spark.mllib.regression

import scala.collection.JavaConversions._
import scala.util.Random

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import spark.SparkContext
import spark.SparkContext._
import spark.mllib.util.LinearRegressionDataGenerator
import spark.mllib.regression.LabeledPoint
import org.jblas.DoubleMatrix

object LinearRegressionSuite {

  def generateLinearRegressionInputAsList(
                                intercept: Double,
                                weights: Array[Double],
                                nPoints: Int,
                                seed: Int): java.util.List[LabeledPoint] = {
    seqAsJavaList(generateLinearRegressionInput(intercept, weights, nPoints, seed))
  }


  // Generate noisy input of the form Y = x.dot(weights) + intercept + noise
  def generateLinearRegressionInput(
                          intercept: Double,
                          weights: Array[Double],
                          nPoints: Int,
                          seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)
    val weightsMat = new DoubleMatrix(1, weights.length, weights:_*)
    val x = Array.fill[Array[Double]](nPoints)(
      Array.fill[Double](weights.length)(rnd.nextGaussian()))
    val y = x.map(xi =>
      (new DoubleMatrix(1, xi.length, xi:_*)).dot(weightsMat) + intercept + 0.1 * rnd.nextGaussian()
    )
    y.zip(x).map(p => LabeledPoint(p._1, p._2))
  }

}

class LinearRegressionSuite extends FunSuite with BeforeAndAfterAll {
  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  // Test if we can correctly learn Y = 3 + 10*X1 + 10*X2 when
  // X1 and X2 are collinear.
  test("multi-collinear variables") {
    val testRDD = LinearRegressionDataGenerator.generateLinearRDD(sc, 100, 2, 0.0, intercept=3.0).cache()
    val linReg = new LinearRegressionWithSGD()
    linReg.optimizer.setNumIterations(1000).setStepSize(1.0)

    val model = linReg.run(testRDD)

    assert(model.intercept >= 2.5 && model.intercept <= 3.5)
    assert(model.weights.length === 2)
    assert(model.weights(0) >= 9.0 && model.weights(0) <= 11.0)
    assert(model.weights(1) >= 9.0 && model.weights(1) <= 11.0)
  }
}
