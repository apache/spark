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

package org.apache.spark.ml.regression

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{PredictionModel, Predictor, PredictorParams}
import org.apache.spark.ml.ann.{FeedForwardTopology, FeedForwardTrainer}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

  /**
   * Params for Multilayer Perceptron.
   */
private[ml] trait MultilayerPerceptronParams extends PredictorParams
  with HasSeed with HasMaxIter with HasTol {
   /**
    * Layer sizes including input size and output size.
    *
    * @group param
    */
  final val layers: IntArrayParam = new IntArrayParam(this, "layers",
    "Sizes of layers including input and output from bottom to the top." +
      " E.g., Array(780, 100, 10) means 780 inputs, " +
      "hidden layer with 100 neurons and output layer of 10 neurons.",
     (t: Array[Int]) => t.forall(ParamValidators.gt(0)) && t.length > 1
  )

   /**
    * Block size for stacking input data in matrices. Speeds up the computations.
    * Cannot be more than the size of the dataset.
    *
    * @group expertParam
    */
  final val blockSize: IntParam = new IntParam(this, "blockSize",
    "Block size for stacking input data in matrices.",
    ParamValidators.gt(0))

  /** @group setParam */
  def setLayers(value: Array[Int]): this.type = set(layers, value)

  /** @group getParam */
  final def getLayers: Array[Int] = $(layers)

  /** @group setParam */
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  /** @group getParam */
  final def getBlockSize: Int = $(blockSize)

   /**
    * Set the maximum number of iterations.
    * Default is 100.
    *
    * @group setParam
    */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

   /**
    * Set the convergence tolerance of iterations.
    * Smaller value will lead to higher accuracy with the cost of more iterations.
    * Default is 1E-4.
    *
    * @group setParam
    */
  def setTol(value: Double): this.type = set(tol, value)

   /**
    * Set the seed for weights initialization.
    * Default is 11L.
    *
    * @group setParam
    */
  def setSeed(value: Long): this.type = set(seed, value)

  setDefault(seed -> 11L, maxIter -> 100, tol -> 1e-4, layers -> Array(1, 1), blockSize -> 128)
}



/** Label to vector converter. */
private object LabelConverter {

  var min = 0.0
  var max = 0.0

  def getMin(train: Dataset[_]): Unit = {
    min = train.select("label").rdd.map(x => x(0).asInstanceOf[Double]).min()
  }

  def getMax(train: Dataset[_]): Unit = {
    max = train.select("label").rdd.map(x => x(0).asInstanceOf[Double]).max()
  }

   /**
    * Encodes a label as a vector.
    * Returns a vector of length 1 with the label in the 0th position
    *
    * @param labeledPoint labeled point
    * @return pair of features and vector encoding of a label
    */

  def encodeLabeledPoint(labeledPoint: LabeledPoint): (Vector, Vector) = {
    val output = Array.fill(1)(0.0)
    output(0) = (labeledPoint.label-min)/(max-min)
    (labeledPoint.features, Vectors.dense(output))
  }

   /**
    * Converts a vector to a label.
    * Returns the value of the 0th element of the output vector.
    *
    * @param output label encoded with a vector
    * @return label
    */
  def decodeLabel(output: Vector): Double = {
     (output(0)*(max-min)) + min
  }
}
 /**
  * :: Experimental ::
  * Regression trainer based on Multi-layer perceptron regression.
  * Contains sigmoid activation function on all layers, output layer has a linear function.
  * Number of inputs has to be equal to the size of feature vectors.
  * Number of outputs has to be equal to one.
  */
@Since("2.0.0")
@Experimental
class MultilayerPerceptronRegressor @Since("2.0.0") (
    @Since("2.0.0") override val uid: String)
  extends Predictor[Vector, MultilayerPerceptronRegressor, MultilayerPerceptronRegressorModel]
    with MultilayerPerceptronParams {

   /**
    * Train a model using the given dataset and parameters.
    *
    * @param dataset Training dataset
    * @return Fitted model
    */
  override protected def train(dataset: Dataset[_]): MultilayerPerceptronRegressorModel = {
    val myLayers = getLayers
    val lpData: RDD[LabeledPoint] = extractLabeledPoints(dataset)
    LabelConverter.getMin(dataset)
    LabelConverter.getMax(dataset)
    val data = lpData.map(lp => LabelConverter.encodeLabeledPoint(lp))
    val topology = FeedForwardTopology.multiLayerPerceptronRegression(myLayers)
    val trainer = new FeedForwardTrainer(topology, myLayers(0), myLayers.last)
    // Set up conditional for setting weights here.
    trainer.setSeed($(seed))
    trainer.LBFGSOptimizer
      .setConvergenceTol($(tol))
      .setNumIterations($(maxIter))
    trainer.setStackSize($(blockSize))
     println("Beginning Training")
    val mlpModel = trainer.train(data)
    new MultilayerPerceptronRegressorModel(uid, myLayers, mlpModel.weights)
  }


  def this() = this(Identifiable.randomUID("mlpr"))

  override def copy(extra: ParamMap): MultilayerPerceptronRegressor = defaultCopy(extra)
}

   /**
    * :: Experimental ::
    * Multi-layer perceptron regression model.
    *
    * @param layers array of layer sizes including input and output
    * @param weights weights (or parameters) of the model
    */
@Experimental
class MultilayerPerceptronRegressorModel private[ml] (
    @Since("2.0.0") override val uid: String,
    @Since("2.0.0") layers: Array[Int],
    @Since("2.0.0")    weights: Vector)
  extends PredictionModel[Vector, MultilayerPerceptronRegressorModel]
    with Serializable{

  private val mlpModel =
    FeedForwardTopology.multiLayerPerceptronRegression(layers).model(weights)

   /**
    * Predict label for the given features.
    * This internal method is used to implement [[transform()]] and output [[predictionCol]].
    */
  override def predict(features: Vector): Double = {
    LabelConverter.decodeLabel(mlpModel.predict(features))
  }


  override def copy(extra: ParamMap): MultilayerPerceptronRegressorModel = {
    copyValues(new MultilayerPerceptronRegressorModel(uid, layers, weights), extra)
  }
}
