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

package org.apache.spark.ml.regression

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.LogisticRegressionSuite._
import org.apache.spark.ml.feature.{Instance, LabeledPoint}
import org.apache.spark.ml.linalg.{BLAS, DenseVector, Vector, Vectors}
import org.apache.spark.ml.param.{ParamMap, ParamsSuite}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.random._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Random

class TestSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  private val seed: Int = 42
  @transient var datasetTweedie: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
  }


  test("glm summary: tweedie family with weight") {
    /*
       R code:

       A <- matrix(c(0, 1, 2, 3, 5, 2, 1, 3), 4, 2)
       b <- c(1, 0.5, 1, 0)
       w <- c(1, 2.0, 0.3, 4.7)
       df <- as.data.frame(cbind(A, b))
     */
    val datasetWithWeight = Seq(
      Instance(1.0, 1.0, Vectors.dense(0.0, 5.0).toSparse),
      Instance(0.5, 1.0, Vectors.dense(1.0, 2.0)),
      Instance(1.0, 1.0, Vectors.dense(2.0, 1.0)),
      Instance(0.0, 1.0, Vectors.dense(3.0, 3.0))
    ).toDF()

    val trainer = new GeneralizedLinearRegression()
      .setFamily("tweedie")
      .setVariancePower(1.5)

    val model = trainer.fit(datasetWithWeight)
    val coefficientsR = Vectors.dense(Array(-1.536, -0.683))
    val interceptR = 3.155
    /*
    val devianceResidualsR = Array(0.2404, 0.1965, 1.2824, -0.6916)
    val pearsonResidualsR = Array(0.171217, 0.197406, 2.085864, -0.495332)
    val workingResidualsR = Array(1.029315, 0.281881, 15.502768, -1.052203)
    val responseResidualsR = Array(0.02848, 0.069123, 0.935495, -0.049613)
    val seCoefR = Array(1.276417, 0.944934)
    val tValsR = Array(-1.324124, 0.747068)
    val pValsR = Array(0.185462, 0.455023)
    val dispersionR = 1.0
    val nullDevianceR = 8.3178
    val residualDevianceR = 2.2193
    val residualDegreeOfFreedomNullR = 4
    val residualDegreeOfFreedomR = 2
    val aicR = 5.991537

    val summary = model.summary
    val devianceResiduals = summary.residuals()
      .select(col("devianceResiduals"))
      .collect()
      .map(_.getDouble(0))
    val pearsonResiduals = summary.residuals("pearson")
      .select(col("pearsonResiduals"))
      .collect()
      .map(_.getDouble(0))
    val workingResiduals = summary.residuals("working")
      .select(col("workingResiduals"))
      .collect()
      .map(_.getDouble(0))
    val responseResiduals = summary.residuals("response")
      .select(col("responseResiduals"))
      .collect()
      .map(_.getDouble(0))
*/
    assert(model.coefficients ~== coefficientsR absTol 1E-3)
    assert(model.intercept ~== interceptR absTol 1E-3)
    /*   devianceResiduals.zip(devianceResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    pearsonResiduals.zip(pearsonResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    workingResiduals.zip(workingResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    responseResiduals.zip(responseResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    summary.coefficientStandardErrors.zip(seCoefR).foreach{ x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    summary.tValues.zip(tValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
    summary.pValues.zip(pValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
    assert(summary.dispersion ~== dispersionR absTol 1E-3)
    assert(summary.nullDeviance ~== nullDevianceR absTol 1E-3)
    assert(summary.deviance ~== residualDevianceR absTol 1E-3)
    assert(summary.residualDegreeOfFreedom === residualDegreeOfFreedomR)
    assert(summary.residualDegreeOfFreedomNull === residualDegreeOfFreedomNullR)
    assert(summary.aic ~== aicR absTol 1E-3)
    assert(summary.solver === "irls")
  */
  }
}

