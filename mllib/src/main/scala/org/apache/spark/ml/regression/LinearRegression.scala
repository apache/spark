package org.apache.spark.ml.regression

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.LabeledPoint
import org.apache.spark.ml.param.{ParamMap, HasMaxIter, HasRegParam}
import org.apache.spark.mllib.linalg.{BLAS, Vector}
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * :: AlphaComponent ::
 * Params for linear regression.
 */
@AlphaComponent
private[regression] trait LinearRegressionParams extends RegressorParams
  with HasRegParam with HasMaxIter


/**
 * Logistic regression.
 */
class LinearRegression extends Regressor[LinearRegression, LinearRegressionModel]
  with LinearRegressionParams {

  // TODO: Extend IterativeEstimator

  setRegParam(0.1)
  setMaxIter(100)

  def setRegParam(value: Double): this.type = set(regParam, value)
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  def train(dataset: RDD[LabeledPoint], paramMap: ParamMap): LinearRegressionModel = {
    val oldDataset = dataset.map { case LabeledPoint(label: Double, features: Vector, weight) =>
      org.apache.spark.mllib.regression.LabeledPoint(label, features)
    }
    val handlePersistence = oldDataset.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      oldDataset.persist(StorageLevel.MEMORY_AND_DISK)
    }
    val lr = new LinearRegressionWithSGD()
    lr.optimizer
      .setRegParam(paramMap(regParam))
      .setNumIterations(paramMap(maxIter))
    val model = lr.run(oldDataset)
    val lrm = new LinearRegressionModel(this, paramMap, model.weights, model.intercept)
    if (handlePersistence) {
      oldDataset.unpersist()
    }
    lrm
  }
}


/**
 * :: AlphaComponent ::
 * Model produced by [[LinearRegression]].
 */
@AlphaComponent
class LinearRegressionModel private[ml] (
    override val parent: LinearRegression,
    override val fittingParamMap: ParamMap,
    val weights: Vector,
    val intercept: Double)
  extends RegressionModel[LinearRegressionModel]
  with LinearRegressionParams {

  override def predict(features: Vector): Double = {
    BLAS.dot(features, weights) + intercept
  }
}
