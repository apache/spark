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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{PredictionModel, Predictor, PredictorParams}
import org.apache.spark.ml.ann.{FeedForwardTopology, FeedForwardTrainer}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{max, min}

  /**
   * Params for Multilayer Perceptron.
   */
private[regression] trait MultilayerPerceptronParams extends PredictorParams
  with HasSeed with HasMaxIter with HasTol with HasStepSize {
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

    /** @group setParam */
    def setLayers(value: Array[Int]): this.type = set(layers, value)

    /** @group getParam */
    final def getLayers: Array[Int] = $(layers)

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
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  /** @group getParam */
  final def getBlockSize: Int = $(blockSize)

   /**
    * The solver algorithm for optimization.
    * Supported options: "gd" (minibatch gradient descent) or "l-bfgs".
    * Default: "l-bfgs"
    *
    * @group expertParam
    */
  @Since("2.0.0")
  final val solver: Param[String] = new Param[String](this, "solver",
    "The solver algorithm for optimization. Supported options: " +
      s"${MultilayerPerceptronRegressor.supportedSolvers.mkString(", ")}. (Default l-bfgs)",
    ParamValidators.inArray[String](MultilayerPerceptronRegressor.supportedSolvers))

  /** @group expertGetParam */
  @Since("2.0.0")
  final def getSolver: String = $(solver)

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

   /**
    * The initial weights of the model.
    *
    * @group expertParam
    */
    @Since("2.0.0")
    final val initialWeights: Param[Vector] = new Param[Vector](this, "initialWeights",
      "The initial weights of the model")

    /** @group expertGetParam */
    @Since("2.0.0")
    final def getInitialWeights: Vector = $(initialWeights)

  setDefault(seed -> 11L, maxIter -> 100, tol -> 1e-4, layers -> Array(1, 1),
    solver -> MultilayerPerceptronRegressor.LBFGS, stepSize -> 0.03, blockSize -> 128)
}

 /**
  * Params that need to mixin with both MultilayerPerceptronRegressorModel and
  * MultilayerPerceptronRegressor
  */
private[regression] trait MultilayerPerceptronRegressorParams extends PredictorParams {

  final val minimum: DoubleParam = new DoubleParam(this, "min",
    "Minimum value for scaling data.")

 /**
  * Set the minimum value in the training set labels.
  *
  * @group setParam
  */
  def setMin(value: Double): this.type = set(minimum, value)

  /** @group getParam */
  final def getMin: Double = $(minimum)

  final val maximum: DoubleParam = new DoubleParam(this, "max",
    "Max value for scaling data.")

 /**
  * Set the maximum value in the training set labels.
  *
  * @group setParam
  */
  def setMax(value: Double): this.type = set(maximum, value)

  /** @group getParam */
  final def getMax: Double = $(maximum)
}




/** Label to vector converter. */
private object LabelConverter {

  /* Consider using MinMaxScaler once it sets metadata, converting to column vector */
  /* Rewrite max and min with column aggregator methods */
//
//  var min = 0.0
//  var max = 0.0
//
//  def getMin(minimum: Double): Unit = {
//    min = train.select("label").rdd.map(x => x(0).asInstanceOf[Double]).min()
//    _min = min(train("label")).cast("Double").asInstanceOf[Double]
//    min = minimum
//  }
//
//  def getMax(maximum: Double): Unit = {
//    max = train.select("label").rdd.map(x => x(0).asInstanceOf[Double]).max()
//    _max = max(train("label")).cast("Double").asInstanceOf[Double]
//    max = maximum
//  }

   /**
    * Encodes a label as a vector.
    * Returns a vector of length 1 with the label in the 0th position
    *
    * @param labeledPoint labeled point
    * @return pair of features and vector encoding of a label
    */

  def encodeLabeledPoint(labeledPoint: LabeledPoint, min: Double, max: Double): (Vector, Vector) = {
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
  def decodeLabel(output: Vector, min: Double, max: Double): Double = {
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
    with MultilayerPerceptronParams with MultilayerPerceptronRegressorParams with Serializable
    with DefaultParamsWritable {

  /**
   * Sets the value of param [[initialWeights]].
   *
   * @group expertSetParam
   */
   @Since("2.0.0")
   def setInitialWeights(value: Vector): this.type = set(initialWeights, value)

  /**
   * Sets the value of param [[solver]].
   * Default is "l-bfgs".
   *
   * @group expertSetParam
   */
   @Since("2.0.0")
   def setSolver(value: String): this.type = set(solver, value)

  /**
   * Sets the value of param [[stepSize]] (applicable only for solver "gd").
   * Default is 0.03.
   *
   * @group setParam
   */
   @Since("2.0.0")
   def setStepSize(value: Double): this.type = set(stepSize, value)

   /**
    * Train a model using the given dataset and parameters.
    *
    * @param dataset Training dataset
    * @return Fitted model
    */
  override protected def train(dataset: Dataset[_]): MultilayerPerceptronRegressorModel = {
    val myLayers = getLayers
    val lpData: RDD[LabeledPoint] = extractLabeledPoints(dataset)
    setMin(dataset.select("label").rdd.map(x => x(0).asInstanceOf[Double]).min())
//    LabelConverter.getMin($(minimum))
    setMax(dataset.select("label").rdd.map(x => x(0).asInstanceOf[Double]).max())
//    LabelConverter.getMax($(maximum))
    val data = lpData.map(lp => LabelConverter.encodeLabeledPoint(lp, $(minimum), $(maximum)))
    val topology = FeedForwardTopology.multiLayerPerceptronRegression(myLayers)
    val trainer = new FeedForwardTrainer(topology, myLayers(0), myLayers.last)
    // Set up conditional for setting weights here.
    if (isDefined(initialWeights)) {
      trainer.setWeights($(initialWeights))
    } else {
      trainer.setSeed($(seed))
    }
     if ($(solver) == MultilayerPerceptronRegressor.LBFGS) {
       trainer.LBFGSOptimizer
         .setConvergenceTol($(tol))
         .setNumIterations($(maxIter))
     } else if ($(solver) == MultilayerPerceptronRegressor.GD) {
       trainer.SGDOptimizer
         .setNumIterations($(maxIter))
         .setConvergenceTol($(tol))
         .setStepSize($(stepSize))
     } else {
       throw new IllegalArgumentException(
         s"The solver $solver is not supported by MultilayerPerceptronClassifier.")
     }
    trainer.setStackSize($(blockSize))
     println("Beginning Training")
    val mlpModel = trainer.train(data)
    new MultilayerPerceptronRegressorModel(uid, myLayers, mlpModel.weights)
  }

  @Since("2.0.0")
  def this() = this(Identifiable.randomUID("mlpr"))

  override def copy(extra: ParamMap): MultilayerPerceptronRegressor = defaultCopy(extra)
}


@Since("2.0.0")
object MultilayerPerceptronRegressor
  extends DefaultParamsReadable[MultilayerPerceptronRegressor] {

  /** String name for "l-bfgs" solver. */
  private[regression] val LBFGS = "l-bfgs"

  /** String name for "gd" (minibatch gradient descent) solver. */
  private[regression] val GD = "gd"

  /** Set of solvers that MultilayerPerceptronRegressor supports. */
  private[regression] val supportedSolvers = Array(LBFGS, GD)

  @Since("2.0.0")
  override def load(path: String): MultilayerPerceptronRegressor = super.load(path)
}


   /**
    * :: Experimental ::
    * Multi-layer perceptron regression model.
    *
    * @param uid uid
    * @param layers array of layer sizes including input and output
    * @param weights weights (or parameters) of the model
    * @return prediction model
    */
@Experimental
class MultilayerPerceptronRegressorModel private[ml] (
    @Since("2.0.0") override val uid: String,
    @Since("2.0.0") val layers: Array[Int],
    @Since("2.0.0") val weights: Vector)
  extends PredictionModel[Vector, MultilayerPerceptronRegressorModel]
    with Serializable with MultilayerPerceptronRegressorParams with MLWritable {

  private val mlpModel =
    FeedForwardTopology.multiLayerPerceptronRegression(layers).model(weights)

  /** Returns layers in a Java List. */
  private[ml] def javaLayers: java.util.List[Int] = layers.toList.asJava

   /**
    * Predict label for the given features.
    * This internal method is used to implement [[transform()]] and output [[predictionCol]].
    */
  override def predict(features: Vector): Double = {
    LabelConverter.decodeLabel(mlpModel.predict(features), $(minimum), $(maximum))
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): MultilayerPerceptronRegressorModel = {
    copyValues(new MultilayerPerceptronRegressorModel(uid, layers, weights), extra)
  }

  @Since("2.0.0")
  override def write: MLWriter =
  new MultilayerPerceptronRegressorModel.MultilayerPerceptronRegressorModelWriter(this)
}

@Since("2.0.0")
object MultilayerPerceptronRegressorModel
  extends MLReadable[MultilayerPerceptronRegressorModel]{

  @Since("2.0.0")
  override def read: MLReader[MultilayerPerceptronRegressorModel] =
    new MultilayerPerceptronRegressorModelReader

  @Since("2.0.0")
  override def load(path: String): MultilayerPerceptronRegressorModel = super.load(path)

  /** [[MLWriter]] instance for [[MultilayerPerceptronRegressorModel]] */
  private[MultilayerPerceptronRegressorModel]
  class MultilayerPerceptronRegressorModelWriter(
    instance: MultilayerPerceptronRegressorModel) extends MLWriter {

    private case class Data(layers: Array[Int], weights: Vector)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: layers, weights
      val data = Data(instance.layers, instance.weights)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class MultilayerPerceptronRegressorModelReader
    extends MLReader[MultilayerPerceptronRegressorModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[MultilayerPerceptronRegressorModel].getName

    override def load(path: String): MultilayerPerceptronRegressorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath).select("layers", "weights").head()
      val layers = data.getAs[Seq[Int]](0).toArray
      val weights = data.getAs[Vector](1)
      val model = new MultilayerPerceptronRegressorModel(metadata.uid, layers, weights)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}
