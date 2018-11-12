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

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml.ann.{FeedForwardTopology, FeedForwardTrainer}
import org.apache.spark.ml.feature.OneHotEncoderModel
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.sql.{Dataset, Row}

/** Params for Multilayer Perceptron. */
private[classification] trait MultilayerPerceptronParams extends ProbabilisticClassifierParams
  with HasSeed with HasMaxIter with HasTol with HasStepSize with HasSolver {

  import MultilayerPerceptronClassifier._

  /**
   * Layer sizes including input size and output size.
   *
   * @group param
   */
  @Since("1.5.0")
  final val layers: IntArrayParam = new IntArrayParam(this, "layers",
    "Sizes of layers from input layer to output layer. " +
      "E.g., Array(780, 100, 10) means 780 inputs, " +
      "one hidden layer with 100 neurons and output layer of 10 neurons.",
    (t: Array[Int]) => t.forall(ParamValidators.gt(0)) && t.length > 1)

  /** @group getParam */
  @Since("1.5.0")
  final def getLayers: Array[Int] = $(layers)

  /**
   * Block size for stacking input data in matrices to speed up the computation.
   * Data is stacked within partitions. If block size is more than remaining data in
   * a partition then it is adjusted to the size of this data.
   * Recommended size is between 10 and 1000.
   * Default: 128
   *
   * @group expertParam
   */
  @Since("1.5.0")
  final val blockSize: IntParam = new IntParam(this, "blockSize",
    "Block size for stacking input data in matrices. Data is stacked within partitions." +
      " If block size is more than remaining data in a partition then " +
      "it is adjusted to the size of this data. Recommended size is between 10 and 1000",
    ParamValidators.gt(0))

  /** @group expertGetParam */
  @Since("1.5.0")
  final def getBlockSize: Int = $(blockSize)

  /**
   * The solver algorithm for optimization.
   * Supported options: "gd" (minibatch gradient descent) or "l-bfgs".
   * Default: "l-bfgs"
   *
   * @group expertParam
   */
  @Since("2.0.0")
  final override val solver: Param[String] = new Param[String](this, "solver",
    "The solver algorithm for optimization. Supported options: " +
      s"${supportedSolvers.mkString(", ")}. (Default l-bfgs)",
    ParamValidators.inArray[String](supportedSolvers))

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

  setDefault(maxIter -> 100, tol -> 1e-6, blockSize -> 128,
    solver -> LBFGS, stepSize -> 0.03)
}

/**
 * Classifier trainer based on the Multilayer Perceptron.
 * Each layer has sigmoid activation function, output layer has softmax.
 * Number of inputs has to be equal to the size of feature vectors.
 * Number of outputs has to be equal to the total number of labels.
 *
 */
@Since("1.5.0")
class MultilayerPerceptronClassifier @Since("1.5.0") (
    @Since("1.5.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, MultilayerPerceptronClassifier,
    MultilayerPerceptronClassificationModel]
  with MultilayerPerceptronParams with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("mlpc"))

  /**
   * Sets the value of param [[layers]].
   *
   * @group setParam
   */
  @Since("1.5.0")
  def setLayers(value: Array[Int]): this.type = set(layers, value)

  /**
   * Sets the value of param [[blockSize]].
   * Default is 128.
   *
   * @group expertSetParam
   */
  @Since("1.5.0")
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  /**
   * Sets the value of param [[solver]].
   * Default is "l-bfgs".
   *
   * @group expertSetParam
   */
  @Since("2.0.0")
  def setSolver(value: String): this.type = set(solver, value)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   *
   * @group setParam
   */
  @Since("1.5.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-6.
   *
   * @group setParam
   */
  @Since("1.5.0")
  def setTol(value: Double): this.type = set(tol, value)

  /**
   * Set the seed for weights initialization if weights are not set
   *
   * @group setParam
   */
  @Since("1.5.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /**
   * Sets the value of param [[initialWeights]].
   *
   * @group expertSetParam
   */
  @Since("2.0.0")
  def setInitialWeights(value: Vector): this.type = set(initialWeights, value)

  /**
   * Sets the value of param [[stepSize]] (applicable only for solver "gd").
   * Default is 0.03.
   *
   * @group setParam
   */
  @Since("2.0.0")
  def setStepSize(value: Double): this.type = set(stepSize, value)

  @Since("1.5.0")
  override def copy(extra: ParamMap): MultilayerPerceptronClassifier = defaultCopy(extra)

  /**
   * Train a model using the given dataset and parameters.
   * Developers can implement this instead of `fit()` to avoid dealing with schema validation
   * and copying parameters into the model.
   *
   * @param dataset Training dataset
   * @return Fitted model
   */
  override protected def train(
      dataset: Dataset[_]): MultilayerPerceptronClassificationModel = instrumented { instr =>
    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, labelCol, featuresCol, predictionCol, layers, maxIter, tol,
      blockSize, solver, stepSize, seed)

    val myLayers = $(layers)
    val labels = myLayers.last
    instr.logNumClasses(labels)
    instr.logNumFeatures(myLayers.head)

    // One-hot encoding for labels using OneHotEncoderModel.
    // As we already know the length of encoding, we skip fitting and directly create
    // the model.
    val encodedLabelCol = "_encoded" + $(labelCol)
    val encodeModel = new OneHotEncoderModel(uid, Array(labels))
      .setInputCols(Array($(labelCol)))
      .setOutputCols(Array(encodedLabelCol))
      .setDropLast(false)
    val encodedDataset = encodeModel.transform(dataset)
    val data = encodedDataset.select($(featuresCol), encodedLabelCol).rdd.map {
      case Row(features: Vector, encodedLabel: Vector) => (features, encodedLabel)
    }
    val topology = FeedForwardTopology.multiLayerPerceptron(myLayers, softmaxOnTop = true)
    val trainer = new FeedForwardTrainer(topology, myLayers(0), myLayers.last)
    if (isDefined(initialWeights)) {
      trainer.setWeights($(initialWeights))
    } else {
      trainer.setSeed($(seed))
    }
    if ($(solver) == MultilayerPerceptronClassifier.LBFGS) {
      trainer.LBFGSOptimizer
        .setConvergenceTol($(tol))
        .setNumIterations($(maxIter))
    } else if ($(solver) == MultilayerPerceptronClassifier.GD) {
      trainer.SGDOptimizer
        .setNumIterations($(maxIter))
        .setConvergenceTol($(tol))
        .setStepSize($(stepSize))
    } else {
      throw new IllegalArgumentException(
        s"The solver $solver is not supported by MultilayerPerceptronClassifier.")
    }
    trainer.setStackSize($(blockSize))
    val mlpModel = trainer.train(data)
    new MultilayerPerceptronClassificationModel(uid, myLayers, mlpModel.weights)
  }
}

@Since("2.0.0")
object MultilayerPerceptronClassifier
  extends DefaultParamsReadable[MultilayerPerceptronClassifier] {

  /** String name for "l-bfgs" solver. */
  private[classification] val LBFGS = "l-bfgs"

  /** String name for "gd" (minibatch gradient descent) solver. */
  private[classification] val GD = "gd"

  /** Set of solvers that MultilayerPerceptronClassifier supports. */
  private[classification] val supportedSolvers = Array(LBFGS, GD)

  @Since("2.0.0")
  override def load(path: String): MultilayerPerceptronClassifier = super.load(path)
}

/**
 * Classification model based on the Multilayer Perceptron.
 * Each layer has sigmoid activation function, output layer has softmax.
 *
 * @param uid uid
 * @param layers array of layer sizes including input and output layers
 * @param weights the weights of layers
 */
@Since("1.5.0")
class MultilayerPerceptronClassificationModel private[ml] (
    @Since("1.5.0") override val uid: String,
    @Since("1.5.0") val layers: Array[Int],
    @Since("2.0.0") val weights: Vector)
  extends ProbabilisticClassificationModel[Vector, MultilayerPerceptronClassificationModel]
  with Serializable with MLWritable {

  @Since("1.6.0")
  override val numFeatures: Int = layers.head

  private[ml] val mlpModel = FeedForwardTopology
    .multiLayerPerceptron(layers, softmaxOnTop = true)
    .model(weights)

  /**
   * Returns layers in a Java List.
   */
  private[ml] def javaLayers: java.util.List[Int] = {
    layers.toList.asJava
  }

  /**
   * Predict label for the given features.
   * This internal method is used to implement `transform()` and output [[predictionCol]].
   */
  override def predict(features: Vector): Double = {
    mlpModel.predict(features).argmax.toDouble
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): MultilayerPerceptronClassificationModel = {
    val copied = new MultilayerPerceptronClassificationModel(uid, layers, weights).setParent(parent)
    copyValues(copied, extra)
  }

  @Since("2.0.0")
  override def write: MLWriter =
    new MultilayerPerceptronClassificationModel.MultilayerPerceptronClassificationModelWriter(this)

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    mlpModel.raw2ProbabilityInPlace(rawPrediction)
  }

  override protected def predictRaw(features: Vector): Vector = mlpModel.predictRaw(features)

  override def numClasses: Int = layers.last
}

@Since("2.0.0")
object MultilayerPerceptronClassificationModel
  extends MLReadable[MultilayerPerceptronClassificationModel] {

  @Since("2.0.0")
  override def read: MLReader[MultilayerPerceptronClassificationModel] =
    new MultilayerPerceptronClassificationModelReader

  @Since("2.0.0")
  override def load(path: String): MultilayerPerceptronClassificationModel = super.load(path)

  /** [[MLWriter]] instance for [[MultilayerPerceptronClassificationModel]] */
  private[MultilayerPerceptronClassificationModel]
  class MultilayerPerceptronClassificationModelWriter(
      instance: MultilayerPerceptronClassificationModel) extends MLWriter {

    private case class Data(layers: Array[Int], weights: Vector)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: layers, weights
      val data = Data(instance.layers, instance.weights)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class MultilayerPerceptronClassificationModelReader
    extends MLReader[MultilayerPerceptronClassificationModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[MultilayerPerceptronClassificationModel].getName

    override def load(path: String): MultilayerPerceptronClassificationModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("layers", "weights").head()
      val layers = data.getAs[Seq[Int]](0).toArray
      val weights = data.getAs[Vector](1)
      val model = new MultilayerPerceptronClassificationModel(metadata.uid, layers, weights)

      metadata.getAndSetParams(model)
      model
    }
  }
}
