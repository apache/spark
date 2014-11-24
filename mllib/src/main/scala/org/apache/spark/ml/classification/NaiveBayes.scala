package org.apache.spark.ml.classification

import org.apache.spark.ml.LabeledPoint
import org.apache.spark.ml.param.{HasSmoothingParam, ParamMap}
import org.apache.spark.mllib.linalg.{BLAS, DenseVector, Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.classification.{NaiveBayes => OldNaiveBayes}
import org.apache.spark.rdd.RDD


private[classification] trait NaiveBayesParams extends ClassifierParams with HasSmoothingParam {
  // TODO: override validateAndTransformSchema to check smoothingParam validity
}

class NaiveBayes extends Classifier[NaiveBayes, NaiveBayesModel] with NaiveBayesParams {

  setSmoothingParam(1.0)

  def setSmoothingParam(value: Double): this.type = set(smoothingParam, value)

  override def train(dataset: RDD[LabeledPoint], paramMap: ParamMap): NaiveBayesModel = {
    val oldDataset = dataset.map { case LabeledPoint(label: Double, features: Vector, weight) =>
      org.apache.spark.mllib.regression.LabeledPoint(label, features)
    }
    val nb = OldNaiveBayes.train(oldDataset, paramMap(smoothingParam))
    val numClasses = nb.theta.size
    val numFeatures = nb.theta(0).size
    // Arrange theta into column-major format.
    val thetaData = new Array[Double](numClasses * numFeatures)
    var j = 0
    var k = 0 // index into thetaData
    while (j < numFeatures) {
      var i = 0
      while (i < numClasses) {
        thetaData(k) = nb.theta(i)(j)
        i += 1
        k += 1
      }
      j += 1
    }
    val theta: Matrix = Matrices.dense(numClasses, numFeatures, thetaData)
    new NaiveBayesModel(this, paramMap, new DenseVector(nb.pi), theta)
  }
}

// TODO: Extend ProbabilisticClassificationModel
// TODO: I removed 'labels' since that functionality should be in Classifier.
/**
 * @param pi  log of class priors, whose dimension is C, number of labels
 * @param theta  log of class conditional probabilities, whose dimension is C-by-D,
 *               where D is number of features
 */
class NaiveBayesModel private[ml] (
    override val parent: NaiveBayes,
    override val fittingParamMap: ParamMap,
    val pi: DenseVector,
    val theta: Matrix)
  extends ClassificationModel[NaiveBayesModel] with NaiveBayesParams {

  override val numClasses: Int = pi.size

  override def predictRaw(features: Vector): Vector = {
    // TODO: Generalize BLAS.gemv to take Vector (not just DenseVector).
    val pred = theta.multiply(new DenseVector(features.toArray))
    BLAS.axpy(1.0, pi, pred)
    pred
  }
}
