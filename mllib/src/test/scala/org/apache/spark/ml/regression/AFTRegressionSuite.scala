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
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
// import org.apache.spark.mllib.random.{ExponentialGenerator, WeibullGenerator}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.Random

case class AFTExamplePoint(stage: Double, time: Double, age: Int, year: Int, censored: Double)

case class AFTPoint(features: Vector, censored: Double, label: Double)

class AFTRegressionSuite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var datasetUnivariate: DataFrame = _
  @transient var datasetMultivariate: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    /*
    datasetUnivariate = sqlContext.createDataFrame(
      sc.parallelize(generateAFTInput(
        1, Array(5.5), Array(0.8), 1000, 42, 1.0, 2.0)))
    datasetMultivariate = sqlContext.createDataFrame(
      sc.parallelize(generateAFTInput(
        2, Array(0.9, -1.3), Array(0.7, 1.2), 1000, 42, 1.5, 2.5)))
    */
  }

  test("params") {
    ParamsSuite.checkParams(new AFTRegression)
    val model = new AFTRegressionModel("aftReg", Vectors.dense(0.0), 0.0, 0.0)
    ParamsSuite.checkParams(model)
  }

  test("aft regression: default params") {
    val aftr = new AFTRegression
    assert(aftr.getLabelCol === "label")
    assert(aftr.getFeaturesCol === "features")
    assert(aftr.getPredictionCol === "prediction")
    assert(aftr.getFitIntercept)
//    val model = aftr.fit(datasetUnivariate)
//
//    // copied model must have the same parent.
//    MLTestingUtils.checkCopy(model)
//
//    assert(model.getFeaturesCol === "features")
//    assert(model.getPredictionCol === "prediction")
//    assert(model.getQuantileCol == "quantile")
//    assert(model.intercept !== 0.0)
//    assert(model.hasParent)
  }

  /*
     Currently disabled because the following test cases were blocked by SPARK-10464
     which will add WeibullGenerator for RandomDataGenerator.
   */
  /*
  def generateAFTInput(
      numFeatures: Int,
      xMean: Array[Double],
      xVariance: Array[Double],
      nPoints: Int,
      seed: Int,
      alpha: Double,
      beta: Double): Seq[AFTPoint] = {

    def censored(x: Double, y: Double): Double = {
      if (x <= y) 1.0 else 0.0
    }

    val weibull = new WeibullGenerator(alpha, beta)
    weibull.setSeed(seed)

    val exponential = new ExponentialGenerator(2.0)
    exponential.setSeed(seed)

    val rnd = new Random(seed)
    val x = Array.fill[Array[Double]](nPoints)(
      Array.fill[Double](numFeatures)(rnd.nextDouble()))

    x.foreach { v =>
      var i = 0
      val len = v.length
      while (i < len) {
        v(i) = (v(i) - 0.5) * math.sqrt(12.0 * xVariance(i)) + xMean(i)
        i += 1
      }
    }
    val y = (1 to nPoints).map { i =>
      (weibull.nextValue(), exponential.nextValue())
    }

    y.zip(x).map { p =>
      AFTPoint(Vectors.dense(p._2), censored(p._1._1, p._1._2), p._1._1)
    }
  }

  test("aft regression with univariate") {
    val trainer = new AFTRegression
    val model = trainer.fit(datasetUnivariate)

    /*
       Using the following R code to load the data and train the model using survival package.

       > library("survival")
       > data <- read.csv("path", header=FALSE, stringsAsFactors=FALSE)
       > features <- as.matrix(data.frame(as.numeric(data$V1)))
       > censored <- as.numeric(data$V2)
       > label <- as.numeric(data$V3)
       > sr.fit <- survreg(Surv(label, censored)~features, dist='weibull')
       > summary(sr.fit)

       survreg(formula = Surv(label, censored) ~ features, dist = "weibull")
                    Value Std. Error      z        p
       (Intercept)  1.759     0.4141  4.247 2.16e-05
       features    -0.039     0.0735 -0.531 5.96e-01
       Log(scale)   0.344     0.0379  9.073 1.16e-19

       Scale= 1.41

       Weibull distribution
       Loglik(model)= -1152.2   Loglik(intercept only)= -1152.3
           Chisq= 0.28 on 1 degrees of freedom, p= 0.6
       Number of Newton-Raphson Iterations: 5
       n= 1000
     */
    val weightsR = Vectors.dense(-0.039)
    val interceptR = 1.759
    val scaleR = 1.41

    assert(model.intercept ~== interceptR relTol 1E-3)
    assert(model.weights ~= weightsR relTol 1E-3)
    assert(model.scale ~= scaleR relTol 1E-3)

    val features = Vectors.dense(4.675290165370009)
    val quantile = Vectors.dense(Array(0.1, 0.5, 0.9))
    val expected = model.predict(features, quantile)
  }

  test("aft regression with multivariate") {
    val trainer = new AFTRegression
    val model = trainer.fit(datasetMultivariate)

    /*
       Using the following R code to load the data and train the model using survival package.

       > library("survival")
       > data <- read.csv("path", header=FALSE, stringsAsFactors=FALSE)
       > features <- as.matrix(data.frame(as.numeric(data$V1), as.numeric(data$V2)))
       > censored <- as.numeric(data$V3)
       > label <- as.numeric(data$V4)
       > sr.fit <- survreg(Surv(label, censored)~features, dist='weibull')
       > summary(sr.fit)

                                     Value Std. Error      z        p
       (Intercept)                  1.9206     0.1057 18.171 8.78e-74
       featuresas.numeric.data.V1. -0.0844     0.0611 -1.381 1.67e-01
       featuresas.numeric.data.V2.  0.0677     0.0468  1.447 1.48e-01
       Log(scale)                  -0.0236     0.0436 -0.542 5.88e-01

       Scale= 0.977

       Weibull distribution
       Loglik(model)= -1070.7   Loglik(intercept only)= -1072.7
           Chisq= 3.91 on 2 degrees of freedom, p= 0.14
       Number of Newton-Raphson Iterations: 5
       n= 1000
     */
    val weightsR = Vectors.dense(-0.0844, 0.0677)
    val interceptR = 1.9206
    val scaleR = 0.977

    assert(model.intercept ~== interceptR relTol 1E-3)
    assert(model.weights ~= weightsR relTol 1E-3)
    assert(model.scale ~= scaleR relTol 1E-3)

    val features = Vectors.dense(1.109175828579902,-0.5315711415960551)
    val quantile = Vectors.dense(Array(0.1, 0.5, 0.9))
    val expected = model.predict(features, quantile)
  }
  */

  /*
     This test case is only used to verify the AFTRegression on classical dataset.
     It is not a regular unit test because it depends on external data which need to be downloaded
     to your local disk firstly. I will move this test case to examples when this PR merged.
   */
  /*
  test("aft regression") {
    /*
       Larynx cancer data that are available in Klein and Moeschberger (2003)
       "Survival Analysis: Techniques for Censored and Truncated Data", Springer.

       The data can also be found here:
       http://www.mcw.edu/FileLibrary/Groups/Biostatistics/Publicfiles/DataFromSection
       /DataFromSectionTXT/Data_from_section_1.8.txt
     */
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val cancer = sc.textFile("your-path-to-the-larynx-dataset")
      .map(_.split(" "))
      .map(p => AFTExamplePoint(
      p(0).toDouble, p(1).toDouble, p(2).toInt, p(3).toInt, p(4).toDouble)).toDF()

    val dataset1 = cancer.select("stage", "age", "time", "censored")
      .withColumnRenamed("time", "label")

    val encoder = new OneHotEncoder()
      .setInputCol("stage")
      .setOutputCol("factorStage")
      .setDropLast(false)

    val dataset2 = encoder.transform(dataset1)

    val featuresUDF = udf {
      (factorStage: Vector, age: Double) => {
        Vectors.dense(factorStage.toArray.slice(2, factorStage.size) ++ Array(age))
      }
    }

    val dataset3 = dataset2.withColumn("features", featuresUDF(col("factorStage"), col("age")))

    val model = new AFTRegression().fit(dataset3)

    /*
       Using the following R code to load the data and train the model using survival package.
       You can follow the document:
       https://www.openintro.org/download.php?file=survival_analysis_in_R

       > library("survival")
       > data(larynx)
       > attach(larynx)
       > srFit <- survreg(Surv(time, delta) ~ as.factor(stage) + age, dist="weibull")
       > summary(srFit)

                           Value Std. Error      z        p
       (Intercept)        3.5288     0.9041  3.903 9.50e-05
       as.factor(stage)2 -0.1477     0.4076 -0.362 7.17e-01
       as.factor(stage)3 -0.5866     0.3199 -1.833 6.68e-02
       as.factor(stage)4 -1.5441     0.3633 -4.251 2.13e-05
       age               -0.0175     0.0128 -1.367 1.72e-01
       Log(scale)        -0.1223     0.1225 -0.999 3.18e-01

       Scale= 0.885

       Weibull distribution
       Loglik(model)= -141.4    Loglik(intercept only)= -151.1
           Chisq= 19.37 on 4 degrees of freedom, p= 0.00066
       Number of Newton-Raphson Iterations: 5
       n= 90
     */

    val weightsR = Vectors.dense(Array(-0.1477, -0.5866, -1.5441, -0.0175))
    val interceptR = 3.5288
    val scaleR = 0.885

    assert(model.intercept ~== interceptR relTol 1E-2)
    assert(model.weights ~= weightsR relTol 1E-2)
    assert(model.scale ~= scaleR relTol 1E-2)
  }
  */
}
