package org.apache.spark.mllib.admm

import org.apache.spark.SparkException
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

class PegasosSVM(val iterations: Integer = 10,
                 val rho: Double = .1,
                 val lambda: Double = 0.1) extends SVMWithSGD {

  override def run(input: RDD[LabeledPoint]): SVMModel = {
    // Check the data properties before running the optimizer
    if (validateData && !validators.forall(func => func(input))) {
      throw new SparkException("Input validation failed.")
    }
    // Prepend an extra variable consisting of all 1.0's for the intercept.
    val data = if (addIntercept) {
      input.map(labeledPoint => (labeledPoint.label, prependOne(labeledPoint.features)))
    } else {
      input.map(labeledPoint => (labeledPoint.label, labeledPoint.features))
    }
    val numFeatures: Int = input.first().features.size
    val initialWeights = Vectors.dense(new Array[Double](numFeatures))
    val initialWeightsWithIntercept = if (addIntercept) {
      prependOne(initialWeights)
    } else {
      initialWeights
    }

    val weightsWithIntercept =
      Vectors.fromBreeze(BSPADMMwithSGD.train(data, iterations, new PegasosBVGradient(lambda), initialWeights))

    val intercept = if (addIntercept) weightsWithIntercept(0) else 0.0
    val weights =
      if (addIntercept) {
        Vectors.dense(weightsWithIntercept.toArray.slice(1, weightsWithIntercept.size))
      } else {
        weightsWithIntercept
      }

    createModel(weights, intercept)

  }

  override protected def createModel(weights: Vector, intercept: Double) = {
    new SVMModel(weights, intercept)
  }
}
