package org.apache.spark.ml.regression

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.Evaluator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.impl.estimator.{PredictionModel, HasDefaultEvaluator, Predictor,
  PredictorParams}
import org.apache.spark.mllib.linalg.Vector

@AlphaComponent
private[regression] trait RegressorParams extends PredictorParams

/**
 * Single-label regression
 */
abstract class Regressor[Learner <: Regressor[Learner, M], M <: RegressionModel[M]]
  extends Predictor[Learner, M]
  with RegressorParams
  with HasDefaultEvaluator {

  override def defaultEvaluator: Evaluator = new RegressionEvaluator
}


private[ml] abstract class RegressionModel[M <: RegressionModel[M]]
  extends PredictionModel[M] with RegressorParams {

  /**
   * Predict real-valued label for the given features.
   */
  def predict(features: Vector): Double

}
