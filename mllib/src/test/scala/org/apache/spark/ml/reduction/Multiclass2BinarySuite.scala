package org.apache.spark.ml.reduction

import org.apache.spark.ml.classification.{LogisticRegressionModel, LogisticRegression}
import org.apache.spark.mllib.classification.LogisticRegressionSuite._
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLlibTestSparkContext
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
