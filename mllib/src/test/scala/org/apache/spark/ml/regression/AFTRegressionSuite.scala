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
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.functions._

case class AFTExamplePoint(stage: Double, time: Double, age: Int, year: Int, censored: Double)

class AFTRegressionSuite extends SparkFunSuite with MLlibTestSparkContext {

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
  }

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
    val cancer = sc.textFile("/Users/yanboliang/data/test/pyspark_test/cancer.txt")
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
}
