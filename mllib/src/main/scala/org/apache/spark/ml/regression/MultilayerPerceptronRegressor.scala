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

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.{PredictionModel, Predictor, PredictorParams}
import org.apache.spark.ml.ann.{FeedForwardTopology, FeedForwardTrainer}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}





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
    ParamValidators.arrayLengthGt(1)
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

   /**
    * :: Experimental ::
    * Multi-layer perceptron regression. Contains sigmoid activation function on all layers.
    * See https://en.wikipedia.org/wiki/Multilayer_perceptron for details.
    *
    */

/** Label to vector converter. */
private object LabelConverter {

  var min = 1.0
  var max = 1.0

  def getMin(train: Dataset[_]): Unit = {
    min = train.select("label").rdd.map(x => x(0).asInstanceOf[Double]).min()
  }

  def getMax(train: Dataset[_]): Unit = {
    max = train.select("label").rdd.map(x => x(0).asInstanceOf[Double]).max()
  }
   /**
    * Encodes a label as a vector.
    * Returns a vector of given length with zeroes at all positions
    * and value 1.0 at the position that corresponds to the label.
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
    * Returns the position of the maximal element of a vector.
    *
    * @param output label encoded with a vector
    * @return label
    */
  def decodeLabel(output: Vector): Double = {
     (output(0)*(max-min)) + min
  }
}

@Experimental
class MultilayerPerceptronRegressor (override val uid: String)
  extends Predictor[Vector, MultilayerPerceptronRegressor, MultilayerPerceptronRegressorModel]
    with MultilayerPerceptronParams with HasInputCol with HasOutputCol with HasRawPredictionCol {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

   /**
    * Fits a model to the input and output data.
    * InputCol has to contain input vectors.
    * OutputCol has to contain output vectors.
    */
  //  override def fit(dataset: DataFrame): MultilayerPerceptronRegressorModel = {
  //    val data = dataset.select($(inputCol), $(outputCol)).map {
  //      case Row(x: Vector, y: Vector) => (x, y)
  //    }
  //    data.take(5).foreach(println)
  //    println("Initialized data")
  //    data.take(5).foreach(println)
  //    val myLayers = getLayers
  //    val topology = FeedForwardTopology.multiLayerPerceptron(myLayers, false)
  //    val FeedForwardTrainer = new FeedForwardTrainer(topology, myLayers(0), myLayers.last)
  //    FeedForwardTrainer.LBFGSOptimizer.setConvergenceTol(getTol).setNumIterations(getMaxIter)
  //    FeedForwardTrainer.setStackSize(getBlockSize)
  //    println("Instantiated the FeedForwardTrainer")
  //    val mlpModel = FeedForwardTrainer.train(data)
  //    println("Model has been trained")
  //    new MultilayerPerceptronRegressorModel(uid, myLayers, mlpModel.weights())
  //  }

   /**
    * :: DeveloperApi ::
    *
    * Derives the output schema from the input schema.
    */
  //  override def transformSchema(schema: StructType): StructType = {
  //    val inputType = schema($(inputCol)).dataType
  //    require(inputType.isInstanceOf[VectorUDT],
  //      s"Input column ${$(inputCol)} must be a vector column")
  //    val outputType = schema($(outputCol)).dataType
  //    require(outputType.isInstanceOf[VectorUDT],
  //      s"Input column ${$(outputCol)} must be a vector column")
  //    require(!schema.fieldNames.contains($(rawPredictionCol)),
  //      s"Output column ${$(rawPredictionCol)} already exists.")
  //    val outputFields = schema.fields :+ StructField($(rawPredictionCol), new VectorUDT, false)
  //    StructType(outputFields)
  //  }

   /**
    * Train a model using the given dataset and parameters.
    * Developers can implement this instead of [[fit()]] to avoid dealing with schema validation
    * and copying parameters into the model.
    *
    * @param dataset Training dataset
    * @return Fitted model
    */
  override protected def train(dataset: Dataset[_]): MultilayerPerceptronRegressorModel = {
    val myLayers = getLayers
    //    println("Value in myLayers:")
    //    println(myLayers)
    val lpData: RDD[LabeledPoint] = extractLabeledPoints(dataset)
    //    lpData.take(2).foreach(println)
    LabelConverter.getMin(dataset)
    LabelConverter.getMax(dataset)
    val data = lpData.map(lp => LabelConverter.encodeLabeledPoint(lp))
    //    data.take(2).foreach(println)
    val topology = FeedForwardTopology.multiLayerPerceptron(myLayers, true)
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
    //    val FeedForwardTrainer = new FeedForwardTrainer(topology, myLayers(0), myLayers.last)
//    FeedForwardTrainer.LBFGSOptimizer.setConvergenceTol(getTol).setNumIterations(getMaxIter)
//    FeedForwardTrainer.setStackSize(getBlockSize)
//    //    println("Instantiated the FeedForwardTrainer")
//    val mlpModel = FeedForwardTrainer.train(data)
//    //    println("Model has been trained")
//    new MultilayerPerceptronRegressorModel(uid, myLayers, mlpModel.weights())
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
  extends PredictionModel[Vector, MultilayerPerceptronRegressorModel]
    with HasInputCol with HasRawPredictionCol with Serializable{

  private val mlpModel =
    FeedForwardTopology.multiLayerPerceptron(layers, true).model(weights)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

   /**
    * Transforms the input dataset.
    * InputCol has to contain input vectors.
    * RawPrediction column will contain predictions (outputs of the regressor).
    */
  //  override def transform(dataset: DataFrame): DataFrame = {
  //    transformSchema(dataset.schema, logging = true)
  //    val pcaOp = udf { mlpModel.predict _ }
  //    println(dataset.withColumn($(rawPredictionCol), pcaOp(col($(inputCol)))).take(5))
  //    dataset.withColumn($(rawPredictionCol), pcaOp(col($(inputCol))))
  //  }

  //  override def transform(dataset: DataFrame): Array[Double] = {
  //    val result = dataset.map(d => predict(d))
  //    result
  //  }

   /**
    * :: DeveloperApi ::
    *
    * Derives the output schema from the input schema.
    */
  //  override def transformSchema(schema: StructType): StructType = {
  //    val inputType = schema($(inputCol)).dataType
  //    require(inputType.isInstanceOf[VectorUDT],
  //      s"Input column ${$(inputCol)} must be a vector column")
  //    require(!schema.fieldNames.contains($(rawPredictionCol)),
  //      s"Output column ${$(rawPredictionCol)} already exists.")
  //    val outputFields = schema.fields :+ StructField($(rawPredictionCol), new VectorUDT, false)
  //    StructType(outputFields)
  //  }

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
