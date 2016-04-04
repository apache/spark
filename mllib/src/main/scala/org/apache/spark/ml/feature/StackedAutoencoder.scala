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

package org.apache.spark.ml.feature

import breeze.linalg.{DenseVector => BDV}

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.ann._
import org.apache.spark.ml.classification.MultilayerPerceptronParams
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.{BooleanParam, ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.storage.StorageLevel

/**
 * Params for [[StackedAutoencoder]].
 */
private[feature] trait StackedAutoencoderParams extends Params with HasInputCol with HasOutputCol {
  /**
   * True if data is in [0, 1] interval.
   * Default: false
   * @group expertParam
   */
  final val dataIn01Interval: BooleanParam = new BooleanParam(this, "dataIn01Interval",
    "True if data is in [0, 1] interval." +
      " Sets the layer on the top of the autoencoder: linear + sigmoid (true) " +
      " or linear (false)")

  /** @group getParam */
  final def getDataIn01Interval: Boolean = $(dataIn01Interval)

  /**
   * True if one wants to have decoder.
   * Default: false
   * @group expertParam
   */
  final val buildDecoder: BooleanParam = new BooleanParam(this, "buildDecoder",
    "True to produce a decoder.")

  /** @group getParam */
  final def getBuildDecoder: Boolean = $(buildDecoder)

  /**
   * True to cache the intermediate data in memory. Otherwise disk caching is used.
   * Default: true
   * @group expertParam
   */
  final val memoryOnlyCaching: BooleanParam = new BooleanParam(this, "memoryOnlyCaching",
    "True to cache the intermediate data in memory only.")

  /** @group getParam */
  final def getMemoryOnlyCaching: Boolean = $(memoryOnlyCaching)

  setDefault(dataIn01Interval -> true, buildDecoder -> false, memoryOnlyCaching -> true)
}


@Experimental
class StackedAutoencoder (override val uid: String)
  extends Estimator[StackedAutoencoderModel]
  with MultilayerPerceptronParams with StackedAutoencoderParams {

  def this() = this(Identifiable.randomUID("stackedAutoencoder"))

  /** @group setParam */
  def setDataIn01Interval(value: Boolean): this.type = set(dataIn01Interval, value)

  /** @group setParam */
  def setBuildDecoder(value: Boolean): this.type = set(buildDecoder, value)

  // TODO: make sure that user understands how to set it. Make correctness check
  /** @group setParam */
  def setLayers(value: Array[Int]): this.type = set(layers, value)

  /** @group setParam */
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   * @group setParam
   */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-4.
   * @group setParam
   */
  def setTol(value: Double): this.type = set(tol, value)

  /**
   * Set the seed for weights initialization.
   * @group setParam
   */
  def setSeed(value: Long): this.type = set(seed, value)

  /**
   * Set the model weights.
   * @group setParam
   */
  def setInitialWeights(value: Vector): this.type = set(initialWeights, value)

  /**
   * Fits a model to the input data.
   */
  override def fit(dataset: Dataset[_]): StackedAutoencoderModel = {
    val storageLevel =
      if ($(memoryOnlyCaching)) StorageLevel.MEMORY_ONLY else StorageLevel.DISK_ONLY
    var stackedEncoderOffset = 0
    val stackedEncoderWeights = if (!this.isSet(this.initialWeights)) {
      val size =
        FeedForwardTopology.multiLayerPerceptron($(layers)).layers.foldLeft(0)( (b, layer) =>
          b + layer.weightSize)
      new Array[Double](size)
    } else {
      $(initialWeights).toArray
    }
    // decoder if needed
    var stackedDecoderOffset = 0
    val decoderLayers = $(layers).reverse
    val stackedDecoderWeights: Array[Double] = if ($(buildDecoder)) {
      val size =
        FeedForwardTopology.multiLayerPerceptron(decoderLayers).layers.foldLeft(0)( (b, layer) =>
          b + layer.weightSize)
      stackedDecoderOffset = size
      new Array[Double](size)
    } else {
      new Array[Double](0)
    }
    // TODO: use single instance of vectors
    var data = dataset.select($(inputCol)).rdd.map { case Row(x: Vector) => (x, x) }
    var previousData = data
    val linearInput = !$(dataIn01Interval)
    // Train autoencoder for each layer except the last
    for (i <- 0 until $(layers).length - 1) {
      val currentLayers = Array($(layers)(i), $(layers)(i + 1), $(layers)(i))
      val currentTopology = FeedForwardTopology.multiLayerPerceptron(currentLayers, false)
      val isLastLayer = i == $(layers).length - 2
      val isFirstLayer = i == 0
      if (isFirstLayer && linearInput) {
        currentTopology.layers(currentTopology.layers.length - 1) = new EmptyLayerWithSquaredError()
      }
      val FeedForwardTrainer =
        new FeedForwardTrainer(currentTopology, currentLayers(0), currentLayers.last)
          .setStackSize($(blockSize))
          .setSeed($(seed))
      FeedForwardTrainer.LBFGSOptimizer
        .setConvergenceTol($(tol))
        .setNumIterations($(maxIter))
      val currentModel = FeedForwardTrainer.train(data)
      val currentWeights = currentModel.weights.toArray
      val encoderWeightSize = currentTopology.layers(0).weightSize
      System.arraycopy(
        currentWeights, 0, stackedEncoderWeights, stackedEncoderOffset, encoderWeightSize)
      stackedEncoderOffset += encoderWeightSize
      // input data for the next autoencoder in the stack
      if (!isLastLayer) { // intermediate layers
        val encoderTopology = FeedForwardTopology.multiLayerPerceptron(currentLayers.init, false)
        // Due to Vector inefficiency it will copy weights
        val encoderModel = encoderTopology.model(
          Vectors.fromBreeze(new BDV[Double](currentWeights, 0, 1, encoderWeightSize)))
        // TODO: perform block operations
        previousData = data
        data = data.map { x =>
          val y = encoderModel.predict(x._1)
          (y, y)
        }
        // persist and materialize the intermediate data
        data.persist(storageLevel)
        data.count()
        // unpersist the data that is persisted inside the loop
        if (!isFirstLayer) previousData.unpersist()
      } else { // last layer
        // unpersist the data that remains from the last intermediate layer
        if (!isFirstLayer) data.unpersist()
      }
      // if needs decoder
      if ($(buildDecoder)) {
        val decoderWeightSize = currentWeights.length - encoderWeightSize
        stackedDecoderOffset -= decoderWeightSize
        System.arraycopy(currentWeights, encoderWeightSize, stackedDecoderWeights,
          stackedDecoderOffset, decoderWeightSize)
      }
    }
    new StackedAutoencoderModel(uid + "model", $(layers), Vectors.dense(stackedEncoderWeights),
      Vectors.dense(stackedDecoderWeights), linearInput)
  }

  override def copy(extra: ParamMap): Estimator[StackedAutoencoderModel] = defaultCopy(extra)

  /**
   * :: DeveloperApi ::
   *
   * Derives the output schema from the input schema.
   */
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }
}

@Experimental
class StackedAutoencoderModel private[ml] (
    override val uid: String,
    val layers: Array[Int],
    val encoderWeights: Vector,
    val decoderWeights: Vector,
    linearOutput: Boolean) extends Model[StackedAutoencoderModel] with StackedAutoencoderParams {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  private val encoderModel = {
    val topology = FeedForwardTopology.multiLayerPerceptron(layers, false)
    topology.model(encoderWeights)
  }

  private val decoderModel = {
    if (decoderWeights != null && decoderWeights.size > 0) {
      val topology = FeedForwardTopology.multiLayerPerceptron(layers.reverse, false)
      if (linearOutput) {
        topology.layers(topology.layers.length - 1) = new EmptyLayerWithSquaredError()
      }
      topology.model(decoderWeights)
    } else {
      null
    }
  }

  override def copy(extra: ParamMap): StackedAutoencoderModel = {
    copyValues(
      new StackedAutoencoderModel(uid, layers, encoderWeights, decoderWeights, linearOutput), extra)
  }

  /**
   * Transforms the input dataset.
   */
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val pcaOp = udf { encoderModel.predict _ }
    dataset.withColumn($(outputCol), pcaOp(col($(inputCol))))
  }

  def encode(dataset: DataFrame): DataFrame = transform(dataset)

  def decode(dataset: DataFrame): DataFrame = {
    // TODO: show something if no decoder
    transformSchema(dataset.schema, logging = true)
    val pcaOp = udf { decoderModel.predict _ }
    dataset.withColumn($(outputCol), pcaOp(col($(inputCol))))
  }

  /**
   * :: DeveloperApi ::
   *
   * Derives the output schema from the input schema.
   */
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }
}
