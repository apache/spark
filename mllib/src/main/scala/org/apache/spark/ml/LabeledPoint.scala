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

  /** Default constructor which sets instance weight to 1.0 */
  def this(label: Double, features: Vector) = this(label, features, 1.0)

  override def toString: String = {
    "(%s,%s,%s)".format(label, features, weight)
  }
}

object LabeledPoint {
  def apply(label: Double, features: Vector) = new LabeledPoint(label, features)
}
