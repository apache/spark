package org.apache.spark.ml.impl.estimator

import org.apache.spark.mllib.linalg.Vector

private[ml] trait ProbabilisticClassificationModel {

  /**
   * Predict the probability of each label.
   */
  def predictProbabilities(features: Vector): Vector

}
