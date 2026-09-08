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

import org.apache.spark.annotation.Since
import org.apache.spark.internal.{LogKeys}
import org.apache.spark.ml.{PredictionModel, Predictor, PredictorParams}
import org.apache.spark.ml.linalg.{SQLDataTypes, Vector}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasRawPredictionCol
import org.apache.spark.ml.util._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
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
    SchemaUtils.appendColumn(parentSchema, $(rawPredictionCol), SQLDataTypes.VectorType)
  }
}

/**
 * Single-label binary or multiclass classification.
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * @tparam FeaturesType  Type of input features.  E.g., `Vector`
 * @tparam E  Concrete Estimator type
 * @tparam M  Concrete Model type
 */
abstract class Classifier[
    FeaturesType,
    E <: Classifier[FeaturesType, E, M],
    M <: ClassificationModel[FeaturesType, M]]
  extends Predictor[FeaturesType, E, M] with ClassifierParams {

  /**
   * Get the number of classes.  This looks in column metadata first, and if that is missing,
   * then this assumes classes are indexed 0,1,...,numClasses-1 and computes numClasses
   * by finding the maximum label value.
   *
   * Label validation (ensuring all labels are integers >= 0) needs to be handled elsewhere,
   * such as in `extractLabeledPoints()`.
   *
   * @param dataset       Dataset which contains a column [[labelCol]]
   * @param maxNumClasses Maximum number of classes allowed when inferred from data.  If numClasses
   *                      is specified in the metadata, then maxNumClasses is ignored.
   * @return number of classes
   * @throws IllegalArgumentException if metadata does not specify numClasses, and the
   *                                  actual numClasses exceeds maxNumClasses
   */
  protected def getNumClasses(dataset: Dataset[_], maxNumClasses: Int = 100): Int = {
    DatasetUtils.getNumClasses(dataset, $(labelCol), maxNumClasses)
  }

  /** @group setParam */
  def setRawPredictionCol(value: String): E = set(rawPredictionCol, value).asInstanceOf[E]

  // TODO: defaultEvaluator (follow-up PR)
}

/**
 * Model produced by a [[Classifier]].
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * `transform` selects column-producing methods based on which output columns are set:
 *
 * <table>
 *   <caption>Column-producing methods by requested output columns</caption>
 *   <tr>
 *     <th>Raw prediction</th>
 *     <th>Prediction</th>
 *     <th>Column-producing methods</th>
 *   </tr>
 *   <tr>
 *     <td>Not set</td>
 *     <td>Not set</td>
 *     <td>None</td>
 *   </tr>
 *   <tr>
 *     <td>Set</td>
 *     <td>Not set</td>
 *     <td><code>predictRawColumn</code></td>
 *   </tr>
 *   <tr>
 *     <td>Not set</td>
 *     <td>Set</td>
 *     <td><code>predictionColumn</code></td>
 *   </tr>
 *   <tr>
 *     <td>Set</td>
 *     <td>Set</td>
 *     <td><code>predictRawColumn</code> => <code>raw2predictionColumn</code></td>
 *   </tr>
 * </table>
 *
 * @tparam FeaturesType  Type of input features.  E.g., `Vector`
 * @tparam M  Concrete Model type
 */
abstract class ClassificationModel[FeaturesType, M <: ClassificationModel[FeaturesType, M]]
  extends PredictionModel[FeaturesType, M] with ClassifierParams {

  /**
   * Returns an expression that produces a raw-prediction vector from a features column.
   * The default wraps [[predictRaw]] in a UDF. Models may override this with a native expression or
   * a UDF that snapshots prediction state.
   *
   * `transform` uses this whenever [[rawPredictionCol]] is set. When [[predictionCol]] is also set,
   * it is used together with [[raw2predictionColumn]].
   *
   * @param features input features column
   * @return raw-prediction column of type `Vector`
   */
  protected def predictRawColumn(features: Column): Column = {
    udf { value: Any => predictRaw(value.asInstanceOf[FeaturesType]) }.apply(features)
  }

  /**
   * Returns an expression that produces a predicted label from a raw-prediction vector column.
   *
   * `transform` uses this when both [[rawPredictionCol]] and [[predictionCol]] are set. It consumes
   * the output of [[predictRawColumn]].
   *
   * @param rawPrediction input raw-prediction column
   * @return prediction column of type `Double`
   */
  protected def raw2predictionColumn(rawPrediction: Column): Column = {
    udf(raw2prediction _).apply(rawPrediction)
  }

  /**
   * Returns an expression that produces a predicted label directly from a features column.
   *
   * `transform` uses this when [[predictionCol]] is set and [[rawPredictionCol]] is not set.
   *
   * @param features input features column
   * @return prediction column of type `Double`
   */
  protected def predictionColumn(features: Column): Column = {
    udf { value: Any => predict(value.asInstanceOf[FeaturesType]) }.apply(features)
  }

  /** @group setParam */
  def setRawPredictionCol(value: String): M = set(rawPredictionCol, value).asInstanceOf[M]

  /** Number of classes (values which the label can take). */
  def numClasses: Int

  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = super.transformSchema(schema)
    if ($(predictionCol).nonEmpty) {
      outputSchema = SchemaUtils.updateNumValues(schema,
        $(predictionCol), numClasses)
    }
    if ($(rawPredictionCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(rawPredictionCol), numClasses)
    }
    outputSchema
  }

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
    val outputSchema = transformSchema(dataset.schema, logging = true)

    // Output selected columns only.
    // This is a bit complicated since it tries to avoid repeated computation.
    var outputData = dataset
    var numColsOutput = 0
    if ($(rawPredictionCol).nonEmpty) {
      outputData = outputData.withColumn($(rawPredictionCol),
        predictRawColumn(col($(featuresCol))),
        outputSchema($(rawPredictionCol)).metadata)
      numColsOutput += 1
    }
    if ($(predictionCol).nonEmpty) {
      val predCol = if ($(rawPredictionCol).nonEmpty) {
        raw2predictionColumn(col($(rawPredictionCol)))
      } else {
        predictionColumn(col($(featuresCol)))
      }
      outputData = outputData.withColumn($(predictionCol), predCol,
        outputSchema($(predictionCol)).metadata)
      numColsOutput += 1
    }

    if (numColsOutput == 0) {
      logWarning(log"${MDC(LogKeys.UUID, uid)}: ClassificationModel.transform() does nothing " +
        log"because no output columns were set.")
    }
    outputData.toDF()
  }

  final override def transformImpl(dataset: Dataset[_]): DataFrame =
    throw new UnsupportedOperationException(s"transformImpl is not supported in $getClass")

  /**
   * Predict label for the given features.
   * This method is used to implement `transform()` and output [[predictionCol]].
   *
   * This default implementation for classification predicts the index of the maximum value
   * from `predictRaw()`.
   */
  override def predict(features: FeaturesType): Double = {
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
  @Since("3.0.0")
  def predictRaw(features: FeaturesType): Vector

  /**
   * Given a vector of raw predictions, select the predicted label.
   * This may be overridden to support thresholds which favor particular labels.
   * @return  predicted label
   */
  protected def raw2prediction(rawPrediction: Vector): Double = rawPrediction.argmax

  /**
   * If the rawPrediction and prediction columns are set, this method returns the current model,
   * otherwise it generates new columns for them and sets them as columns on a new copy of
   * the current model
   */
  private[classification] def findSummaryModel():
  (ClassificationModel[FeaturesType, M], String, String) = {
    val model = if ($(rawPredictionCol).isEmpty && $(predictionCol).isEmpty) {
      copy(ParamMap.empty)
        .setRawPredictionCol(Identifiable.randomUID("rawPrediction"))
        .setPredictionCol(Identifiable.randomUID("prediction"))
    } else if ($(rawPredictionCol).isEmpty) {
      copy(ParamMap.empty).setRawPredictionCol(Identifiable.randomUID("rawPrediction"))
    } else if ($(predictionCol).isEmpty) {
      copy(ParamMap.empty).setPredictionCol(Identifiable.randomUID("prediction"))
    } else {
      this
    }
    (model, model.getRawPredictionCol, model.getPredictionCol)
  }
}
