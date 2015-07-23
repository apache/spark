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

import breeze.linalg.{argmax => Bargmax}

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.{Model, Transformer, Estimator, PredictorParams}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.ann.{FeedForwardTopology, FeedForwardTrainer}
import org.apache.spark.mllib.linalg.{VectorUDT, Vector}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Params for Multilayer Perceptron.
 */
private[ml] trait MultilayerPerceptronParams extends PredictorParams
with HasSeed with HasMaxIter with HasTol {
  /**
   * Layer sizes including input size and output size.
   * @group param
   */
  final val layers: IntArrayParam =
    // TODO: we need IntegerArrayParam!
    new IntArrayParam(this, "layers",
      "Sizes of layers including input and output from bottom to the top." +
      " E.g., Array(780, 100, 10) means 780 inputs, " +
      "hidden layer with 100 neurons and output layer of 10 neurons."
      // TODO: how to check that array is not empty?
      )

  /**
   * Block size for stacking input data in matrices. Speeds up the computations.
   * Cannot be more than the size of the dataset.
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
   * Default is 11L.
   * @group setParam
   */
  def setSeed(value: Long): this.type = set(seed, value)

  setDefault(seed -> 11L, maxIter -> 100, tol -> 1e-4, layers -> Array(1, 1), blockSize -> 1)
}

/**
 * :: Experimental ::
 * Multi-layer perceptron regression. Contains sigmoid activation function on all layers.
 * See https://en.wikipedia.org/wiki/Multilayer_perceptron for details.
 *
 */
@Experimental
class MultilayerPerceptronRegressor (override val uid: String)
  extends Estimator[MultilayerPerceptronRegressorModel]
  with MultilayerPerceptronParams with HasInputCol with HasOutputCol with HasRawPredictionCol
  with Logging {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * Fits a model to the input and output data.
   * InputCol has to contain input vectors.
   * OutputCol has to contain output vectors.
   */
  override def fit(dataset: DataFrame): MultilayerPerceptronRegressorModel = {
      val data = dataset.select($(inputCol), $(outputCol)).map {
        case Row(x: Vector, y: Vector) => (x, y)
      }
      val myLayers = getLayers
      val topology = FeedForwardTopology.multiLayerPerceptron(myLayers, false)
      val FeedForwardTrainer = new FeedForwardTrainer(topology, myLayers(0), myLayers.last)
      FeedForwardTrainer.LBFGSOptimizer.setConvergenceTol(getTol).setNumIterations(getMaxIter)
      FeedForwardTrainer.setStackSize(getBlockSize)
      val mlpModel = FeedForwardTrainer.train(data)
      new MultilayerPerceptronRegressorModel(uid, myLayers, mlpModel.weights())
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
    val outputType = schema($(outputCol)).dataType
    require(outputType.isInstanceOf[VectorUDT],
      s"Input column ${$(outputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(rawPredictionCol)),
      s"Output column ${$(rawPredictionCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(rawPredictionCol), new VectorUDT, false)
    StructType(outputFields)
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
class MultilayerPerceptronRegressorModel private[ml] (override val uid: String,
                                                      layers: Array[Int],
                                                      weights: Vector)
  extends Model[MultilayerPerceptronRegressorModel]
  with HasInputCol with HasRawPredictionCol {

  private val mlpModel =
    FeedForwardTopology.multiLayerPerceptron(layers, false).getInstance(weights)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /**
   * Transforms the input dataset.
   * InputCol has to contain input vectors.
   * RawPrediction column will contain predictions (outputs of the regressor).
   */
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val pcaOp = udf { mlpModel.predict _ }
    dataset.withColumn($(rawPredictionCol), pcaOp(col($(inputCol))))
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
    require(!schema.fieldNames.contains($(rawPredictionCol)),
      s"Output column ${$(rawPredictionCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(rawPredictionCol), new VectorUDT, false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): MultilayerPerceptronRegressorModel = {
    copyValues(new MultilayerPerceptronRegressorModel(uid, layers, weights), extra)
  }
}
