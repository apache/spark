package org.apache.spark.ml

import org.apache.spark.mllib.linalg.Vector

/**
 * Class that represents an instance (data point) for prediction tasks.
 *
 * @param label Label to predict
 * @param features List of features describing this instance
 * @param weight Instance weight
 */
case class LabeledPoint(label: Double, features: Vector, weight: Double) {

  override def toString: String = {
    "(%s,%s,%s)".format(label, features, weight)
  }
}

object LabeledPoint {
  /** Constructor which sets instance weight to 1.0 */
  def apply(label: Double, features: Vector) = new LabeledPoint(label, features, 1.0)
}
