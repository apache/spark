package spark.mllib.optimization

import scala.math._
import org.jblas.DoubleMatrix

abstract class Updater extends Serializable {
  /**
   * Compute an updated value for weights given the gradient, stepSize and iteration number.
   *
   * @param weightsOld - Column matrix of size nx1 where n is the number of features.
   * @param gradient - Column matrix of size nx1 where n is the number of features.
   * @param stepSize - step size across iterations
   * @param iter - Iteration number
   * @param regParam - Regularization parameter
   *
   * @return weightsNew - Column matrix containing updated weights
   * @return reg_val - regularization value
   */
  def compute(weightsOld: DoubleMatrix, gradient: DoubleMatrix, stepSize: Double, iter: Int, regParam: Double):
      (DoubleMatrix, Double)
}

class SimpleUpdater extends Updater {
  override def compute(weightsOld: DoubleMatrix, gradient: DoubleMatrix,
      stepSize: Double, iter: Int, regParam: Double): (DoubleMatrix, Double) = {
    val normGradient = gradient.mul(stepSize / math.sqrt(iter))
    (weightsOld.sub(normGradient), 0)
  }
}

/**
L1 regularization -- corresponding proximal operator is the soft-thresholding function
**/
class L1Updater extends Updater {
  override def compute(weightsOld: DoubleMatrix, gradient: DoubleMatrix,
      stepSize: Double, iter: Int, regParam: Double): (DoubleMatrix, Double) = {
    val thisIterStepSize = stepSize / math.sqrt(iter)
    val normGradient = gradient.mul(thisIterStepSize)
    val newWeights = weightsOld.sub(normGradient)
    (0 until newWeights.length).foreach(i => {
      val wi = newWeights.get(i)
      newWeights.put(i, signum(wi) * max(0.0, abs(wi) - regParam * thisIterStepSize))
      })
    (newWeights, newWeights.norm1 * regParam)
  }
}

class SquaredL2Updater extends Updater {
  override def compute(weightsOld: DoubleMatrix, gradient: DoubleMatrix,
      stepSize: Double, iter: Int, regParam: Double): (DoubleMatrix, Double) = {
    val thisIterStepSize = stepSize / math.sqrt(iter)
    val normGradient = gradient.mul(thisIterStepSize)
    val newWeights = weightsOld.sub(normGradient).div(2.0 * thisIterStepSize * regParam + 1.0)
    (newWeights, pow(newWeights.norm2,2.0) * regParam)
  }
}

