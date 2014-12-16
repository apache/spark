package org.apache.spark.ml.impl.estimator

import org.apache.spark.ml.Evaluator

trait HasDefaultEvaluator {

  /**
   * Default evaluation metric usable for model validation (e.g., with CrossValidator).
   */
  def defaultEvaluator: Evaluator

}
