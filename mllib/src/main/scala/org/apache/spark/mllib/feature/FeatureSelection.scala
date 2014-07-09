package org.apache.spark.mllib.feature

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

trait FeatureSelection {

  /** make LabeledPoint or Vector a type parameter so it can select from both */
  def select(data: RDD[LabeledPoint]): Set[Int]
}

object FeatureSelection extends java.io.Serializable {

  /** make LabeledPoint or Vector a type parameter so it can filter both */
  def filter(data: RDD[LabeledPoint], indexes: Set[Int]): RDD[LabeledPoint] =
    data.map {labeledPoint =>
      new LabeledPoint(labeledPoint.label, filter(labeledPoint.features, indexes))
  }

  def filter(features: Vector, indexes: Set[Int]): Vector = {
    val (values, _) =
      features.toArray.zipWithIndex.filter { case (value, index) =>
        indexes.contains(index)}.unzip
    /**  probably make a sparse vector if it was initially sparse */
    Vectors.dense(values.toArray)
  }
}
