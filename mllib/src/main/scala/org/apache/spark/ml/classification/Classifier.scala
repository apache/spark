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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.{PredictionModel, PredictorParams, Predictor}
import org.apache.spark.ml.param.shared.{HasRawPredictionCol, HasThreshold, HasThresholds}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}


/**
 * (private[spark]) Params for classification.
 */
private[spark] trait ClassifierParams
  extends PredictorParams with HasRawPredictionCol with HasThresholds with HasThreshold {

  override protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    val parentSchema = super.validateAndTransformSchema(schema, fitting, featuresDataType)
    SchemaUtils.appendColumn(parentSchema, $(rawPredictionCol), new VectorUDT)
  }

  /**
   * Customized version of getThreshold that looks at both threshold & thresholds param.
   * The priority order is thresholds assigned param, threshold assigned param
   * then thresholds default value.
   * When converting from threshold to thresholds the threshold for class 0 will be 0.5
   * and the threshold for class 1 will be the assigned threshold value.
   **/
  override def getThresholds: Array[Double] = {
    def thresholdToThresholds(threshold: Double): Array[Double] = {
      Array[Double](0.5, threshold)
    }
    if (isDefined(thresholds) || !isDefined(threshold)) {
      super.getThresholds
    } else {
      thresholdToThresholds(getThreshold)
    }
  }
  /**
   * Customized version of getThreshold that looks at both threshold & thresholds param.
   * The priority order is threshold assigned param, thresholds assigned param
   * then the threshold default value.
   * When converting from thresholds to threshold the threshold will be the ratio between
   * class 1 and class 0.
   **/
  override def getThreshold(): Double = {
    def thresholdsToThreshold(thresholds: Array[Double]): Double = {
      assert(thresholds.size == 2, "Attempting to use threshold array for binary classification, size " +
        "must be 2 instead of " + thresholds.size)
      thresholds(1)/thresholds(0)
    }
    if (isDefined(threshold) || !isDefined(thresholds)) {
      super.getThreshold
    } else {
      thresholdsToThreshold(getThresholds)
    }
  }
}

/**
 * :: DeveloperApi ::
 *
 * Single-label binary or multiclass classification.
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * @tparam FeaturesType  Type of input features.  E.g., [[Vector]]
 * @tparam E  Concrete Estimator type
 * @tparam M  Concrete Model type
 */
@DeveloperApi
abstract class Classifier[
    FeaturesType,
    E <: Classifier[FeaturesType, E, M],
    M <: ClassificationModel[FeaturesType, M]]
  extends Predictor[FeaturesType, E, M] with ClassifierParams {

  /** @group setParam */
  def setRawPredictionCol(value: String): E = set(rawPredictionCol, value).asInstanceOf[E]

  // TODO: defaultEvaluator (follow-up PR)
}

/**
 * :: DeveloperApi ::
 *
 * Model produced by a [[Classifier]].
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * @tparam FeaturesType  Type of input features.  E.g., [[Vector]]
 * @tparam M  Concrete Model type
 */
@DeveloperApi
abstract class ClassificationModel[FeaturesType, M <: ClassificationModel[FeaturesType, M]]
  extends PredictionModel[FeaturesType, M] with ClassifierParams {

  /** @group setParam */
  def setRawPredictionCol(value: String): M = set(rawPredictionCol, value).asInstanceOf[M]

  /** Number of classes (values which the label can take). */
  def numClasses: Int

  /**
   * Transforms dataset by reading from [[featuresCol]], and appending new columns as specified by
   * parameters:
   *  - predicted labels as [[predictionCol]] of type [[Double]]
   *  - raw predictions (confidences) as [[rawPredictionCol]] of type [[Vector]].
   *
   * @param dataset input dataset
   * @return transformed dataset
   */
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    // Output selected columns only.
    // This is a bit complicated since it tries to avoid repeated computation.
    var outputData = dataset
    var numColsOutput = 0
    if (getRawPredictionCol != "") {
      val predictRawUDF = udf { (features: Any) =>
        predictRaw(features.asInstanceOf[FeaturesType])
      }
      outputData = outputData.withColumn(getRawPredictionCol, predictRawUDF(col(getFeaturesCol)))
      numColsOutput += 1
    }
    if (getPredictionCol != "") {
      val predUDF = if (getRawPredictionCol != "") {
        udf(raw2prediction _).apply(col(getRawPredictionCol))
      } else {
        val predictUDF = udf { (features: Any) =>
          predict(features.asInstanceOf[FeaturesType])
        }
        predictUDF(col(getFeaturesCol))
      }
      outputData = outputData.withColumn(getPredictionCol, predUDF)
      numColsOutput += 1
    }

    if (numColsOutput == 0) {
      logWarning(s"$uid: ClassificationModel.transform() was called as NOOP" +
        " since no output columns were set.")
    }
    outputData
  }

  /**
   * Predict label for the given features.
   * This internal method is used to implement [[transform()]] and output [[predictionCol]].
   *
   * This default implementation for classification predicts the index of the maximum value
   * from [[predictRaw()]].
   */
  override protected def predict(features: FeaturesType): Double = {
    raw2prediction(predictRaw(features))
  }

  /**
   * Raw prediction for each possible label.
   * The meaning of a "raw" prediction may vary between algorithms, but it intuitively gives
   * a measure of confidence in each possible label (where larger = more confident).
   * This internal method is used to implement [[transform()]] and output [[rawPredictionCol]].
   *
   * @return  vector where element i is the raw prediction for label i.
   *          This raw prediction may be any real number, where a larger value indicates greater
   *          confidence for that label.
   */
  protected def predictRaw(features: FeaturesType): Vector

  /**
   * Given a vector of raw predictions, select the predicted label.
   * This may be overridden to support custom thresholds which favor particular labels.
   * @return  predicted label
   */
  protected def raw2prediction(rawPrediction: Vector): Double = {
    val modelScores = rawPrediction.toArray.zipWithIndex
    // Apply thresholding, or use votes if no thresholding
    val scores = Option(getThresholds).map{thresholds =>
      modelScores.map{case (score, index) =>
        (score/thresholds(index), index)
      }}.getOrElse(modelScores)
    scores.maxBy(_._1)._2
  }
}
