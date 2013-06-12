package spark.ml

import org.jblas.DoubleMatrix

abstract class Updater extends Serializable {
  /**
   * Compute an updated value for weights given the gradient, stepSize and iteration number.
   *
   * @param weightsOld - Column matrix of size nx1 where n is the number of features.
   * @param gradient - Column matrix of size nx1 where n is the number of features.
   * @param stepSize - step size across iterations
   * @param iter - Iteration number
   *
   * @return weightsNew - Column matrix containing updated weights
   * @return reg_val - regularization value
   */
  def compute(weightsOlds: DoubleMatrix, gradient: DoubleMatrix, stepSize: Double, iter: Int):
      (DoubleMatrix, Double)
}

class SimpleUpdater extends Updater {
  override def compute(weightsOld: DoubleMatrix, gradient: DoubleMatrix,
      stepSize: Double, iter: Int): (DoubleMatrix, Double) = {
    val normGradient = gradient.mul(stepSize / math.sqrt(iter))
    (weightsOld.sub(normGradient), 0)
  }
}
