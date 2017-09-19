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

import breeze.linalg.{DenseVector => BDV, Vector => BV}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasMaxIter, HasRegParam, HasStepSize, HasThreshold}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.classification.{SVMModel => MLlibSVMWithSGDModel, SVMWithSGD => MLlibSVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.regression.{LabeledPoint => MllibLabledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType


private[classification] trait SVMParams extends Params
  with HasRegParam with HasMaxIter with  HasStepSize with HasThreshold {

  final val miniBatchFraction = new DoubleParam(this, "miniBatchFraction",
    "The miniBatchFraction " + "to create. " + "Must be > 0.",
    ParamValidators.gt(0))

  def getMiniBF: Double = $(miniBatchFraction)

  /**
    * Set threshold in binary classification, in range [0, 1].
    *
    * If the estimated probability of class label 1 is > threshold, then predict 1, else 0.
    * A high threshold encourages the model to predict 0 more often;
    * a low threshold encourages the model to predict 1 more often.
    * Default is 0.5.
    *
    * @group setParam
    */
  def setThreshold(value: Double): this.type = {
    set(threshold, value)
  }
  setDefault(threshold -> 0.5)
}


class SVMModel ( override val uid: String,
                 val weights: Vector,
                 val intercept: Double)
  extends ProbabilisticClassificationModel[Vector, SVMModel] with SVMParams with MLWritable {

  override def setThreshold(value: Double): this.type = super.setThreshold(value)

  override def getThreshold: Double = super.getThreshold

  private def asBreeze(v: Vector): BV[Double] = new BDV[Double](v.toArray)

  /** Margin (rawPrediction) for class label 1.  For binary classification only. */
  private val margin: Vector => Double = (features) => {
    asBreeze(weights).dot(asBreeze(features)) + intercept
  }

  /**
    * Predict label for the given features.
    * This internal method is used to implement [[transform()]] and output [[predictionCol]].
    */
  override protected def predict(features: Vector): Double = {
    if (margin(features) > getThreshold) 1.0 else 0.0
  }

  private var trainingSummary: Option[SVMSummary] = None

  /**
    * Gets summary of model on training set. An exception is
    * thrown if `trainingSummary == None`.
    */
  def summary: SVMSummary = trainingSummary.getOrElse {
    throw new SparkException("No training summary available for this LogisticRegressionModel")
  }

  def setSummary(summary: SVMSummary): this.type = {
    this.trainingSummary = Some(summary)
    this
  }

  /** Indicates whether a training summary exists for this model instance. */
  def hasSummary: Boolean = trainingSummary.isDefined

  override def copy(extra: ParamMap): SVMModel = {
    val newModel = copyValues(new SVMModel(uid, weights, intercept), extra)
    if (trainingSummary.isDefined) newModel.setSummary(trainingSummary.get)
    newModel.setParent(parent)
  }

  /**
    * Returns a [[MLWriter]] instance for this ML instance.
    */
  override def write: MLWriter = new SVMModel.SVMModelWriter(this)

  override def numClasses: Int = 2

  override protected def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(-m, m)
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        var i = 0
        val size = dv.size
        while (i < size) {
          dv.values(i) = 1.0 / (1.0 + math.exp(-dv.values(i)))
          i += 1
        }
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in LogisticRegressionModel:" +
          " raw2probabilitiesInPlace encountered SparseVector")
    }
  }
}

object SVMModel extends MLReadable[SVMModel] {

  override def read: MLReader[SVMModel] = new SVMModelReader

  override def load(path: String): SVMModel = super.load(path)

  /** [[MLWriter]] instance for [[SVMModel]] */
  private[SVMModel]
  class SVMModelWriter(instance: SVMModel)
    extends MLWriter with Logging {

    private case class Data(
                             weights: Vector,
                             intercept: Double
                           )

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: weights, intercept
      val data = Data(instance.weights, instance.intercept)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class SVMModelReader extends MLReader[SVMModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[SVMModel].getName

    override def load(path: String): SVMModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)
      val Row(coefficients: Vector, intercept: Double) =
        data.select("weights", "intercept").head()
      val model = new SVMModel(metadata.uid, coefficients, intercept)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}


class SVMWithSGD(override val uid: String)
  extends ProbabilisticClassifier[Vector, SVMWithSGD, SVMModel]
    with SVMParams with DefaultParamsWritable {

  setDefault(
    stepSize -> 1.0,
    maxIter -> 100,
    regParam -> 0.01,
    miniBatchFraction -> 1.0
  )


  def this() = this(Identifiable.randomUID("SVM"))

  def setStepSize(Value: Double): this.type = set(stepSize, Value)

  def setMaxIter(Value: Int): this.type = set(maxIter, Value)

  def setRegParam(Value: Double): this.type = set(regParam, Value)

  def setMiniBF(Value: Double): this.type = set(miniBatchFraction, Value)

  override protected def train(dataset: Dataset[_]): SVMModel = {
    val rdd: RDD[MllibLabledPoint] = dataset.select(col($(labelCol)).cast(DoubleType),
      col($(featuresCol))).rdd.map {
      case Row(label: Double, features: Vector) =>
        MllibLabledPoint(label, OldVectors.fromML(features))
    }.cache()

    val svm = new MLlibSVMWithSGD().setIntercept(true)
    svm.optimizer
      .setStepSize($(stepSize))
      .setNumIterations($(maxIter))
      .setRegParam($(regParam))
      .setMiniBatchFraction($(miniBatchFraction))

    val parentModel: MLlibSVMWithSGDModel = {
      svm.run(rdd)
    }

    val model = {
      copyValues(new SVMModel(uid, parentModel.weights.asML, parentModel.intercept).setParent(this))
    }

    val instr = {
      Instrumentation.create(this, rdd)
    }
    instr.logParams(labelCol, featuresCol, predictionCol, stepSize,
      maxIter, regParam, maxIter, miniBatchFraction)

    val summary = new SVMSummary(
      model.transform(dataset), $(labelCol), $(featuresCol), $(predictionCol))
    val m = model.setSummary(summary)
    instr.logSuccess(m)
    m
  }

  override def copy(extra: ParamMap): SVMWithSGD = defaultCopy(extra)
}

object SVMWithSGD extends DefaultParamsReadable[SVMWithSGD] {
  override def load(path: String): SVMWithSGD = super.load(path)
}

class SVMSummary( val predictions: DataFrame,
                  val labelCol: String,
                  val featuresCol: String,
                  val predictionCol: String
                ) extends Serializable {

  private val sparkSession = predictions.sparkSession

  import sparkSession.implicits._

  /**
    * Returns a BinaryClassificationMetrics object.
    */
  // TODO: Allow the user to vary the number of bins using a setBins method in
  // BinaryClassificationMetrics. For now the default is set to 100.
  @transient private val binaryMetrics = new BinaryClassificationMetrics(
    predictions.select(predictionCol, labelCol).rdd.map {
      case Row(label: Double, ground: Double) => (label, ground)
    }, 100
  )

  /**
    * Returns the receiver operating characteristic (ROC) curve,
    * which is a Dataframe having two fields (FPR, TPR)
    * with (0.0, 0.0) prepended and (1.0, 1.0) appended to it.
    * See http://en.wikipedia.org/wiki/Receiver_operating_characteristic
    *
    * Note: This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
    * This will change in later Spark versions.
    */
  @transient lazy val roc: DataFrame = binaryMetrics.roc().toDF("FPR", "TPR")

  /**
    * Computes the area under the receiver operating characteristic (ROC) curve.
    *
    * Note: This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
    * This will change in later Spark versions.
    */
  lazy val areaUnderROC: Double = binaryMetrics.areaUnderROC()

  /**
    * Returns the precision-recall curve, which is a Dataframe containing
    * two fields recall, precision with (0.0, 1.0) prepended to it.
    *
    * Note: This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
    * This will change in later Spark versions.
    */
  @transient lazy val pr: DataFrame = binaryMetrics.pr().toDF("recall", "precision")

  /**
    * Returns a dataframe with two fields (threshold, F-Measure) curve with beta = 1.0.
    *
    * Note: This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
    * This will change in later Spark versions.
    */
  @transient lazy val fMeasureByThreshold: DataFrame = {
    binaryMetrics.fMeasureByThreshold().toDF("threshold", "F-Measure")
  }
}

