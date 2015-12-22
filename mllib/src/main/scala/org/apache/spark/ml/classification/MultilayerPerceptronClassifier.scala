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

import scala.collection.JavaConverters._

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param.shared.{HasTol, HasMaxIter, HasSeed}
import org.apache.spark.ml.{PredictorParams, PredictionModel, Predictor}
import org.apache.spark.ml.param.{IntParam, ParamValidators, IntArrayParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.ann.{FeedForwardTrainer, FeedForwardTopology}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame

/** Params for Multilayer Perceptron. */
private[ml] trait MultilayerPerceptronParams extends PredictorParams
  with HasSeed with HasMaxIter with HasTol {
  /**
   * Layer sizes including input size and output size.
   * Default: Array(1, 1)
   * @group param
   */
  final val layers: IntArrayParam = new IntArrayParam(this, "layers",
    "Sizes of layers from input layer to output layer" +
      " E.g., Array(780, 100, 10) means 780 inputs, " +
      "one hidden layer with 100 neurons and output layer of 10 neurons.",
    // TODO: how to check ALSO that all elements are greater than 0?
    ParamValidators.arrayLengthGt(1)
  )

  /** @group getParam */
  final def getLayers: Array[Int] = $(layers)

  /**
   * Block size for stacking input data in matrices to speed up the computation.
   * Data is stacked within partitions. If block size is more than remaining data in
   * a partition then it is adjusted to the size of this data.
   * Recommended size is between 10 and 1000.
   * Default: 128
   * @group expertParam
   */
  final val blockSize: IntParam = new IntParam(this, "blockSize",
    "Block size for stacking input data in matrices. Data is stacked within partitions." +
      " If block size is more than remaining data in a partition then " +
      "it is adjusted to the size of this data. Recommended size is between 10 and 1000",
    ParamValidators.gt(0))

  /** @group getParam */
  final def getBlockSize: Int = $(blockSize)

  setDefault(maxIter -> 100, tol -> 1e-4, layers -> Array(1, 1), blockSize -> 128)
}

/** Label to vector converter. */
private object LabelConverter {
  // TODO: Use OneHotEncoder instead
  /**
   * Encodes a label as a vector.
   * Returns a vector of given length with zeroes at all positions
   * and value 1.0 at the position that corresponds to the label.
   *
   * @param labeledPoint labeled point
   * @param labelCount total number of labels
   * @return pair of features and vector encoding of a label
   */
  def encodeLabeledPoint(labeledPoint: LabeledPoint, labelCount: Int): (Vector, Vector) = {
    val output = Array.fill(labelCount)(0.0)
    output(labeledPoint.label.toInt) = 1.0
    (labeledPoint.features, Vectors.dense(output))
  }

  /**
   * Converts a vector to a label.
   * Returns the position of the maximal element of a vector.
   *
   * @param output label encoded with a vector
   * @return label
   */
  def decodeLabel(output: Vector): Double = {
    output.argmax.toDouble
  }
}

/**
 * :: Experimental ::
 * Classifier trainer based on the Multilayer Perceptron.
 * Each layer has sigmoid activation function, output layer has softmax.
 * Number of inputs has to be equal to the size of feature vectors.
 * Number of outputs has to be equal to the total number of labels.
 */
@Since("1.5.0")
@Experimental
class MultilayerPerceptronClassifier @Since("1.5.0") (
    @Since("1.5.0") override val uid: String)
  extends Predictor[Vector, MultilayerPerceptronClassifier, MultilayerPerceptronClassificationModel]
  with MultilayerPerceptronParams {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("mlpc"))

  /** @group setParam */
  @Since("1.5.0")
  def setLayers(value: Array[Int]): this.type = set(layers, value)

  /** @group setParam */
  @Since("1.5.0")
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   * @group setParam
   */
  @Since("1.5.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-4.
   * @group setParam
   */
  @Since("1.5.0")
  def setTol(value: Double): this.type = set(tol, value)

  /**
   * Set the seed for weights initialization.
   * @group setParam
   */
  @Since("1.5.0")
  def setSeed(value: Long): this.type = set(seed, value)

  @Since("1.5.0")
  override def copy(extra: ParamMap): MultilayerPerceptronClassifier = defaultCopy(extra)

  /**
   * Train a model using the given dataset and parameters.
   * Developers can implement this instead of [[fit()]] to avoid dealing with schema validation
   * and copying parameters into the model.
   *
   * @param dataset Training dataset
   * @return Fitted model
   */
  override protected def train(dataset: DataFrame): MultilayerPerceptronClassificationModel = {
    val myLayers = $(layers)
    val labels = myLayers.last
    val lpData = extractLabeledPoints(dataset)
    val data = lpData.map(lp => LabelConverter.encodeLabeledPoint(lp, labels))
    val topology = FeedForwardTopology.multiLayerPerceptron(myLayers, true)
    val FeedForwardTrainer = new FeedForwardTrainer(topology, myLayers(0), myLayers.last)
    FeedForwardTrainer.LBFGSOptimizer.setConvergenceTol($(tol)).setNumIterations($(maxIter))
    FeedForwardTrainer.setStackSize($(blockSize))
    val mlpModel = FeedForwardTrainer.train(data)
    new MultilayerPerceptronClassificationModel(uid, myLayers, mlpModel.weights())
  }
}

/**
 * :: Experimental ::
 * Classification model based on the Multilayer Perceptron.
 * Each layer has sigmoid activation function, output layer has softmax.
 * @param uid uid
 * @param layers array of layer sizes including input and output layers
 * @param weights vector of initial weights for the model that consists of the weights of layers
 * @return prediction model
 */
@Since("1.5.0")
@Experimental
class MultilayerPerceptronClassificationModel private[ml] (
    @Since("1.5.0") override val uid: String,
    @Since("1.5.0") val layers: Array[Int],
    @Since("1.5.0") val weights: Vector)
  extends PredictionModel[Vector, MultilayerPerceptronClassificationModel]
  with Serializable {

  @Since("1.6.0")
  override val numFeatures: Int = layers.head

  private val mlpModel = FeedForwardTopology.multiLayerPerceptron(layers, true).getInstance(weights)

  /**
   * Returns layers in a Java List.
   */
  private[ml] def javaLayers: java.util.List[Int] = {
    layers.toList.asJava
  }

  /**
   * Predict label for the given features.
   * This internal method is used to implement [[transform()]] and output [[predictionCol]].
   */
  override protected def predict(features: Vector): Double = {
    LabelConverter.decodeLabel(mlpModel.predict(features))
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): MultilayerPerceptronClassificationModel = {
    copyValues(new MultilayerPerceptronClassificationModel(uid, layers, weights), extra)
  }
}
