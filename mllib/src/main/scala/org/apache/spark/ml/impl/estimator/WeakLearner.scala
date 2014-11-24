package org.apache.spark.ml.impl.estimator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.ml.LabeledPoint
import org.apache.spark.ml.param.ParamMap

/**
 * Trait indicating that this algorithm is optimized for being used as a weak learner for boosting,
 * bagging, and other meta-algorithms.
 */
trait WeakLearner[M <: PredictionModel[M]] {

  def getNativeFeatureRDD(dataset: SchemaRDD, paramMap: ParamMap): RDD[Vector]

  def trainNative(dataset: RDD[LabeledPoint], paramMap: ParamMap): M

}
