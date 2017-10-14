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

import org.apache.spark.SparkException
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{PredictionModel, Predictor, PredictorParams}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.shared.HasRawPredictionCol
import org.apache.spark.ml.util.{MetadataUtils, SchemaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * (private[spark]) Params for classification.
 */
private[spark] trait ClassifierParams
  extends PredictorParams with HasRawPredictionCol {

  override protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    val parentSchema = super.validateAndTransformSchema(schema, fitting, featuresDataType)
    SchemaUtils.appendColumn(parentSchema, $(rawPredictionCol), new VectorUDT)
  }
}

/**
 * :: DeveloperApi ::
 *
 * Single-label binary or multiclass classification.
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * @tparam FeaturesType  Type of input features.  E.g., `Vector`
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

  /**
   * Extract [[labelCol]] and [[featuresCol]] from the given dataset,
   * and put it in an RDD with strong types.
   *
   * @param dataset  DataFrame with columns for labels ([[org.apache.spark.sql.types.NumericType]])
   *                 and features (`Vector`).
   * @param numClasses  Number of classes label can take.  Labels must be integers in the range
   *                    [0, numClasses).
   * @note Throws `SparkException` if any label is a non-integer or is negative
   */
  protected def extractLabeledPoints(dataset: Dataset[_], numClasses: Int): RDD[LabeledPoint] = {
    require(numClasses > 0, s"Classifier (in extractLabeledPoints) found numClasses =" +
      s" $numClasses, but requires numClasses > 0.")
    dataset.select(col($(labelCol)), col($(featuresCol))).rdd.map {
      case Row(label: Double, features: Vector) =>
        require(label % 1 == 0 && label >= 0 && label < numClasses, s"Classifier was given" +
          s" dataset with invalid label $label.  Labels must be integers in range" +
          s" [0, $numClasses).")
        LabeledPoint(label, features)
    }
  }

  /**
   * Get the number of classes.  This looks in column metadata first, and if that is missing,
   * then this assumes classes are indexed 0,1,...,numClasses-1 and computes numClasses
   * by finding the maximum label value.
   *
   * Label validation (ensuring all labels are integers >= 0) needs to be handled elsewhere,
   * such as in `extractLabeledPoints()`.
   *
   * @param dataset  Dataset which contains a column [[labelCol]]
   * @param maxNumClasses  Maximum number of classes allowed when inferred from data.  If numClasses
   *                       is specified in the metadata, then maxNumClasses is ignored.
   * @return  number of classes
   * @throws IllegalArgumentException  if metadata does not specify numClasses, and the
   *                                   actual numClasses exceeds maxNumClasses
   */
  protected def getNumClasses(dataset: Dataset[_], maxNumClasses: Int = 100): Int = {
    MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) => n
      case None =>
        // Get number of classes from dataset itself.
        val maxLabelRow: Array[Row] = dataset.select(max($(labelCol))).take(1)
        if (maxLabelRow.isEmpty) {
          throw new SparkException("ML algorithm was given empty dataset.")
        }
        val maxDoubleLabel: Double = maxLabelRow.head.getDouble(0)
        require((maxDoubleLabel + 1).isValidInt, s"Classifier found max label value =" +
          s" $maxDoubleLabel but requires integers in range [0, ... ${Int.MaxValue})")
        val numClasses = maxDoubleLabel.toInt + 1
        require(numClasses <= maxNumClasses, s"Classifier inferred $numClasses from label values" +
          s" in column $labelCol, but this exceeded the max numClasses ($maxNumClasses) allowed" +
          s" to be inferred from values.  To avoid this error for labels with > $maxNumClasses" +
          s" classes, specify numClasses explicitly in the metadata; this can be done by applying" +
          s" StringIndexer to the label column.")
        logInfo(this.getClass.getCanonicalName + s" inferred $numClasses classes for" +
          s" labelCol=$labelCol since numClasses was not specified in the column metadata.")
        numClasses
    }
  }
}

/**
 * :: DeveloperApi ::
 *
 * Model produced by a [[Classifier]].
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * @tparam FeaturesType  Type of input features.  E.g., `Vector`
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
   *  - predicted labels as [[predictionCol]] of type `Double`
   *  - raw predictions (confidences) as [[rawPredictionCol]] of type `Vector`.
   *
   * @param dataset input dataset
   * @return transformed dataset
   */
  override def transform(dataset: Dataset[_]): DataFrame = {
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
    outputData.toDF
  }

  /**
   * Predict label for the given features.
   * This internal method is used to implement `transform()` and output [[predictionCol]].
   *
   * This default implementation for classification predicts the index of the maximum value
   * from `predictRaw()`.
   */
  override protected def predict(features: FeaturesType): Double = {
    raw2prediction(predictRaw(features))
  }

  /**
   * Raw prediction for each possible label.
   * The meaning of a "raw" prediction may vary between algorithms, but it intuitively gives
   * a measure of confidence in each possible label (where larger = more confident).
   * This internal method is used to implement `transform()` and output [[rawPredictionCol]].
   *
   * @return  vector where element i is the raw prediction for label i.
   *          This raw prediction may be any real number, where a larger value indicates greater
   *          confidence for that label.
   */
  protected def predictRaw(features: FeaturesType): Vector

  /**
   * Given a vector of raw predictions, select the predicted label.
   * This may be overridden to support thresholds which favor particular labels.
   * @return  predicted label
   */
  protected def raw2prediction(rawPrediction: Vector): Double = rawPrediction.argmax
}
