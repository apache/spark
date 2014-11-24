package org.apache.spark.ml.impl.estimator

import org.apache.spark.sql.SchemaRDD

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap

/**
 * Trait for an iterative estimator which permits access to its underlying iterative optimization
 * algorithm.
 * Classes implementing this trait can create an [[IterativeSolver]], which operates on a static
 * dataset using an iterative algorithm.
 */
trait IterativeEstimator[M <: Model[M]] {

  private[ml] def createSolver(dataset: SchemaRDD, paramMap: ParamMap): IterativeSolver[M]

}
