/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.classification

import breeze.linalg.{argmax => Bargmax}

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.{PredictionModel, Predictor}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.regression.MultilayerPerceptronParams
import org.apache.spark.mllib.ann.{FeedForwardTrainer, FeedForwardTopology}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame

/**
 * :: Experimental ::
 * Label to vector converter.
 */
@Experimental
private object LabelConverter {

  /**
   * Encodes a label as a vector.
   * Returns a vector of given length with zeroes at all positions
   * and value 1.0 at the position that corresponds to the label.
   *
   * @param labeledPoint  labeled point
   * @param labelCount total number of labels
   * @return  vector encoding of a label
   */
  def apply(labeledPoint: LabeledPoint, labelCount: Int): (Vector, Vector) = {
    val output = Array.fill(labelCount){0.0}
    output(labeledPoint.label.toInt) = 1.0
    (labeledPoint.features, Vectors.dense(output))
  }

  /**
   * Converts a vector to a label.
   * Returns the position of the maximal element of a vector.
   *
   * @param output  label encoded with a vector
   * @return  label
   */
  def apply(output: Vector): Double = {
    Bargmax(output.toBreeze.toDenseVector).toDouble
  }
}

/**
 * :: Experimental ::
 * Classifier trainer based on the Multilayer Perceptron.
 * Each layer has sigmoid activation function, output layer has softmax.
 * Number of inputs has to be equal to the size of feature vectors.
 * Number of outputs has to be equal to the total number of labels.
 *
 */
@Experimental
class MultilayerPerceptronClassifier (override val uid: String)
  extends Predictor[Vector, MultilayerPerceptronClassifier, MultilayerPerceptronClassifierModel]
  with MultilayerPerceptronParams {

  override def copy(extra: ParamMap): MultilayerPerceptronClassifier = defaultCopy(extra)

  def this() = this(Identifiable.randomUID("mlpc"))

  /**
   * Train a model using the given dataset and parameters.
   * Developers can implement this instead of [[fit()]] to avoid dealing with schema validation
   * and copying parameters into the model.
   *
   * @param dataset  Training dataset
   * @return  Fitted model
   */
  override protected def train(dataset: DataFrame): MultilayerPerceptronClassifierModel = {
    val labels = getLayers.last.toInt
    val lpData = extractLabeledPoints(dataset)
    val data = lpData.map(lp => LabelConverter(lp, labels))
    val myLayers = getLayers.map(_.toInt)
    val topology = FeedForwardTopology.multiLayerPerceptron(myLayers, true)
    val FeedForwardTrainer = new FeedForwardTrainer(topology, myLayers(0), myLayers.last)
    FeedForwardTrainer.LBFGSOptimizer.setConvergenceTol(getTol).setNumIterations(getMaxIter)
    FeedForwardTrainer.setStackSize(getBlockSize)
    val mlpModel = FeedForwardTrainer.train(data)
    new MultilayerPerceptronClassifierModel(uid, myLayers, mlpModel.weights())
  }
}

/**
 * :: Experimental ::
 * Classifier model based on the Multilayer Perceptron.
 * Each layer has sigmoid activation function, output layer has softmax.
 */
@Experimental
class MultilayerPerceptronClassifierModel private[ml] (override val uid: String,
                                                      layers: Array[Int],
                                                      weights: Vector)
  extends PredictionModel[Vector, MultilayerPerceptronClassifierModel]
  with Serializable {

  private val mlpModel = FeedForwardTopology.multiLayerPerceptron(layers, true).getInstance(weights)

  /**
   * Predict label for the given features.
   * This internal method is used to implement [[transform()]] and output [[predictionCol]].
   */
  override protected def predict(features: Vector): Double = {
    LabelConverter(mlpModel.predict(features))
  }

  override def copy(extra: ParamMap): MultilayerPerceptronClassifierModel = {
    copyValues(new MultilayerPerceptronClassifierModel(uid, layers, weights), extra)
  }
}
