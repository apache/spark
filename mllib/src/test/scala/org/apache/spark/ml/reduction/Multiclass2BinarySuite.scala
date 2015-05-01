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

package org.apache.spark.ml.reduction

import org.apache.spark.ml.classification.{LogisticRegressionModel, LogisticRegression}
import org.apache.spark.mllib.classification.LogisticRegressionSuite._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{MLUtils, MLlibTestSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FunSuite

class Multiclass2BinarySuite extends FunSuite with MLlibTestSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var dataset: DataFrame = _
  @transient var rdd: RDD[LabeledPoint] = _
  private val eps: Double = 1e-5

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
    val nPoints = 10000

    /**
     * The following weights and xMean/xVariance are computed from iris dataset with lambda = 0.2.
     * As a result, we are actually drawing samples from probability distribution of built model.
     */
    val weights = Array(
      -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
      -0.16624, -0.84355, -0.048509, -0.301789, 4.170682)

    val xMean = Array(5.843, 3.057, 3.758, 1.199)
    val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)
    rdd = sc.parallelize(generateMultinomialLogisticInput(
      weights, xMean, xVariance, true, nPoints, 42), 2)
    dataset = sqlContext.createDataFrame(rdd)
  }

  test("one-against-all: default params") {
    val ova = new Multiclass2Binary().
      setNumClasses(3).
      setBaseClassifier(new LogisticRegression)

    assert(ova.getLabelCol == "label")
    assert(ova.getPredictionCol == "prediction")
    val ovaModel = ova.fit(dataset)
    assert(ovaModel.baseClassificationModels.size == 3)
    val ovaResults = ovaModel.transform(dataset)
      .select("label", "prediction")
      .map (row => (row(0).asInstanceOf[Double], row(1).asInstanceOf[Double]))
      .collect()

    val lr = new LogisticRegressionWithLBFGS().setIntercept(true).setNumClasses(3)
    lr.optimizer.setRegParam(0.1).setNumIterations(100)

    val model = lr.run(rdd)
    val results = rdd.map(_.label).zip(model.predict(rdd.map(_.features))).collect()
    // determine the #confusion matrix in each class.

    println(confusionMatrix(results, 3).map(_.mkString("\t")).mkString("\n"))
    println(confusionMatrix(ovaResults, 3).map(_.mkString("\t")).mkString("\n"))
  }

  test("one-against-all: news20") {
    val k = 3
    val ova = new Multiclass2Binary().
      setNumClasses(k).
      setBaseClassifier(new LogisticRegression)

    assert(ova.getLabelCol == "label")
    assert(ova.getPredictionCol == "prediction")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val rdds = MLUtils.loadLibSVMFile(sc,
      "/Users/rsriharsha/projects/hortonworks/benchmark-datasets/multiclass-classification/connect-4.txt").
      map {case LabeledPoint(label, features) => LabeledPoint(label + 1, features)}.
      randomSplit(Array(0.7, 0.3))
    val Array(train, test) = rdds.map(_.toDF())
    val ovaModel = time(ova.fit(train))
    assert(ovaModel.baseClassificationModels.size == k)
    val ovaResults = ovaModel.transform(test)
      .select("label", "prediction")
      .map (row => (row(0).asInstanceOf[Double], row(1).asInstanceOf[Double]))
      .collect()

    val lr = new LogisticRegressionWithLBFGS().setIntercept(true).setNumClasses(k)
    lr.optimizer.setRegParam(0.1).setNumIterations(100)

    val model = time(lr.run(rdds(0)))
    val results = rdds(1).map(_.label).zip(model.predict(rdds(1).map(_.features))).collect()
    // determine the #confusion matrix in each class.

    println(confusionMatrix(results, k).map(_.mkString("\t")).mkString("\n"))
    println("**********************************************")
    println(confusionMatrix(ovaResults, k).map(_.mkString("\t")).mkString("\n"))
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1E9 + "s")
    result
  }

  private def confusionMatrix(results: Seq[(Double, Double)], numClasses: Int): Array[Array[Double]] = {
    val matrix = Array.fill(numClasses, 2)(0.0)
    for ((label, value) <- results) {
      val v = value.toInt
      val l = label.toInt
      if (l == v) {
        matrix(l).update(0, matrix(l)(0) + 1)
      } else {
        matrix(l).update(1, matrix(l)(1) + 1)
      }
    }
    matrix
  }
}
