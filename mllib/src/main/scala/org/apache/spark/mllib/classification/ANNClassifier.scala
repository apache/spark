package org.apache.spark.mllib.classification

import org.apache.spark.mllib.ann.{ArtificialNeuralNetworkModel, ArtificialNeuralNetwork}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import breeze.linalg.{argmax => Bargmax}

trait ANNClassifierHelper {

  protected val labelToIndex: Map[Double, Int]
  private val indexToLabel = labelToIndex.map(_.swap)
  private val labelCount = labelToIndex.size

  protected def labeledPointToVectorPair(labeledPoint: LabeledPoint) = {
    val output = Array.fill(labelCount){0.0}
    output(labelToIndex(labeledPoint.label)) = 1.0
    (labeledPoint.features, Vectors.dense(output))
  }

  protected def outputToLabel(output: Vector): Double = {
    val index = Bargmax(output.toBreeze.toDenseVector)
    indexToLabel(index)
  }
}

class ANNClassifierModel private[mllib](val annModel: ArtificialNeuralNetworkModel, val labelToIndex: Map[Double, Int])
  extends ClassificationModel with ANNClassifierHelper with Serializable {
  /**
   * Predict values for the given data set using the model trained.
   *
   * @param testData RDD representing data points to be predicted
   * @return an RDD[Double] where each entry contains the corresponding prediction
   */
  override def predict(testData: RDD[Vector]): RDD[Double] = testData.map(predict)

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param testData array representing a single data point
   * @return predicted category from the trained model
   */
  override def predict(testData: Vector): Double = {
    val output = annModel.predict(testData)
    outputToLabel(output)
  }
}

class ANNClassifier private(val labelToIndex: Map[Double, Int],
                             private val hiddenLayersTopology: Array[Int],
                             private val initialWeights: Vector,
                             private val maxIterations: Int,
                             private val stepSize: Double,
                             private val convergeTol: Double)
  extends ANNClassifierHelper with Serializable {

  def run(data: RDD[LabeledPoint]): ANNClassifierModel = {
    val annData = data.map(lp => labeledPointToVectorPair(lp))
    /* train the model */
    val model = ArtificialNeuralNetwork.train(annData, hiddenLayersTopology/*, initialWeights*/, maxIterations, convergeTol)
    new ANNClassifierModel(model, labelToIndex)
  }
}

object ANNClassifier {

  def train(data: RDD[LabeledPoint], hiddenLayersTopology: Array[Int],
            initialWeights: Vector, maxIterations: Int,
            stepSize: Double, convergenceTol: Double): ANNClassifierModel = {
    val labelToIndex = data.map( lp => lp.label).distinct().collect().zipWithIndex.toMap
    new ANNClassifier(labelToIndex, hiddenLayersTopology,
      initialWeights, maxIterations, stepSize, convergenceTol).run(data)
  }

  def train(data: RDD[LabeledPoint], hiddenLayersTopology: Array[Int], maxIterations: Int,
            stepSize: Double, convergenceTol: Double): ANNClassifierModel = {
    val initialWeights = randomWeights(data, hiddenLayersTopology)
    train(data, hiddenLayersTopology, initialWeights, maxIterations, stepSize, convergenceTol)
  }

  def train(data: RDD[LabeledPoint]): ANNClassifierModel = {
    val featureCount = data.first().features.size
    val hiddenSize = featureCount / 2 + 1
    val hiddenLayersTopology = Array[Int](hiddenSize)
    train(data, hiddenLayersTopology, 2000, 1.0, 1e-4)
  }

  /* TODO: remove duplication - the same analysis will be done in ANNClassifier.run() */
  def randomWeights(data: RDD[LabeledPoint], hiddenLayersTopology: Array[Int]) = {
    val labelCount = data.map( lp => lp.label).distinct().collect().length
    val featureCount = data.first().features.size
    null
  }
}
