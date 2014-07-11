package org.apache.spark.mllib.feature

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

trait FeatureSelection[T] extends java.io.Serializable {
  def data: RDD[T]
  def select: Set[Int]
}

sealed trait FeatureFilter[T] extends FeatureSelection[T] {
  def filter: RDD[T]
}

trait LabeledPointFeatureFilter extends FeatureFilter[LabeledPoint] {
  lazy val indices = select
  def filter: RDD[LabeledPoint] =
    data.map { lp => new LabeledPoint(lp.label, Compress(lp.features, indices)) }
}

object Compress {
  def apply(features: Vector, indexes: Set[Int]): Vector = {
    val (values, _) =
      features.toArray.zipWithIndex.filter { case (value, index) =>
        indexes.contains(index)}.unzip
    /**  probably make a sparse vector if it was initially sparse */
    Vectors.dense(values.toArray)
  }
}
