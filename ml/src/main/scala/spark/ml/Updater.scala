package spark.ml

import org.jblas.DoubleMatrix

abstract class Updater extends Serializable {
  def compute(weightsOlds: DoubleMatrix, gradient: DoubleMatrix, stepSize: Double, iter: Int):
      (DoubleMatrix, Double)
}

class SimpleUpdater extends Updater {
  override def compute(weightsOld: DoubleMatrix, gradient: DoubleMatrix, stepSize: Double, iter: Int):
      (DoubleMatrix, Double) = {
    val normGradient = gradient.mul(stepSize / math.sqrt(iter))
    (weightsOld.sub(normGradient), 0)
  }
}
