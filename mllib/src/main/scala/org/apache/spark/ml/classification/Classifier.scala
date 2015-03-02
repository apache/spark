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

import org.apache.spark.annotation.{DeveloperApi, AlphaComponent}
import org.apache.spark.ml.impl.estimator.{PredictionModel, Predictor, PredictorParams}
import org.apache.spark.ml.param.{Params, ParamMap, HasRawPredictionCol}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}


/**
 * :: DeveloperApi ::
 * Params for classification.
 *
 * NOTE: This is currently private[spark] but will be made public later once it is stabilized.
 */
@DeveloperApi
private[spark] trait ClassifierParams extends PredictorParams
  with HasRawPredictionCol {

  override protected def validateAndTransformSchema(
      schema: StructType,
      paramMap: ParamMap,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    val parentSchema = super.validateAndTransformSchema(schema, paramMap, fitting, featuresDataType)
    val map = this.paramMap ++ paramMap
    addOutputColumn(parentSchema, map(rawPredictionCol), new VectorUDT)
  }
}

/**
 * :: AlphaComponent ::
 * Single-label binary or multiclass classification.
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * @tparam FeaturesType  Type of input features.  E.g., [[Vector]]
 * @tparam E  Concrete Estimator type
 * @tparam M  Concrete Model type
 *
 * NOTE: This is currently private[spark] but will be made public later once it is stabilized.
 */
@AlphaComponent
private[spark] abstract class Classifier[
    FeaturesType,
    E <: Classifier[FeaturesType, E, M],
    M <: ClassificationModel[FeaturesType, M]]
  extends Predictor[FeaturesType, E, M]
  with ClassifierParams {

  /** @group setParam */
  def setRawPredictionCol(value: String): E =
    set(rawPredictionCol, value).asInstanceOf[E]

  // TODO: defaultEvaluator (follow-up PR)
}

/**
 * :: AlphaComponent ::
 * Model produced by a [[Classifier]].
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * @tparam FeaturesType  Type of input features.  E.g., [[Vector]]
 * @tparam M  Concrete Model type
 *
 * NOTE: This is currently private[spark] but will be made public later once it is stabilized.
 */
@AlphaComponent
private[spark]
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
   * @param paramMap additional parameters, overwrite embedded params
   * @return transformed dataset
   */
  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    // This default implementation should be overridden as needed.

    // Check schema
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = this.paramMap ++ paramMap

    // Prepare model
    val tmpModel = if (paramMap.size != 0) {
      val tmpModel = this.copy()
      Params.inheritValues(paramMap, parent, tmpModel)
      tmpModel
    } else {
      this
    }

    val (numColsOutput, outputData) =
      ClassificationModel.transformColumnsImpl[FeaturesType](dataset, tmpModel, map)
    if (numColsOutput == 0) {
      logWarning(s"$uid: ClassificationModel.transform() was called as NOOP" +
        " since no output columns were set.")
    }
    outputData
  }

  /**
   * :: DeveloperApi ::
   *
   * Predict label for the given features.
   * This internal method is used to implement [[transform()]] and output [[predictionCol]].
   *
   * This default implementation for classification predicts the index of the maximum value
   * from [[predictRaw()]].
   */
  @DeveloperApi
  override protected def predict(features: FeaturesType): Double = {
    predictRaw(features).toArray.zipWithIndex.maxBy(_._1)._2
  }

  /**
   * :: DeveloperApi ::
   *
   * Raw prediction for each possible label.
   * The meaning of a "raw" prediction may vary between algorithms, but it intuitively gives
   * a measure of confidence in each possible label (where larger = more confident).
   * This internal method is used to implement [[transform()]] and output [[rawPredictionCol]].
   *
   * @return  vector where element i is the raw prediction for label i.
   *          This raw prediction may be any real number, where a larger value indicates greater
   *          confidence for that label.
   */
  @DeveloperApi
  protected def predictRaw(features: FeaturesType): Vector

}

private[ml] object ClassificationModel {

  /**
   * Added prediction column(s).  This is separated from [[ClassificationModel.transform()]]
   * since it is used by [[org.apache.spark.ml.classification.ProbabilisticClassificationModel]].
   * @param dataset  Input dataset
   * @param map  Parameter map.  This will NOT be merged with the embedded paramMap; the merge
   *             should already be done.
   * @return (number of columns added, transformed dataset)
   */
  def transformColumnsImpl[FeaturesType](
      dataset: DataFrame,
      model: ClassificationModel[FeaturesType, _],
      map: ParamMap): (Int, DataFrame) = {

    // Output selected columns only.
    // This is a bit complicated since it tries to avoid repeated computation.
    var tmpData = dataset
    var numColsOutput = 0
    if (map(model.rawPredictionCol) != "") {
      // output raw prediction
      val features2raw: FeaturesType => Vector = model.predictRaw
      tmpData = tmpData.withColumn(map(model.rawPredictionCol),
        callUDF(features2raw, new VectorUDT, col(map(model.featuresCol))))
      numColsOutput += 1
      if (map(model.predictionCol) != "") {
        val raw2pred: Vector => Double = (rawPred) => {
          rawPred.toArray.zipWithIndex.maxBy(_._1)._2
        }
        tmpData = tmpData.withColumn(map(model.predictionCol),
          callUDF(raw2pred, DoubleType, col(map(model.rawPredictionCol))))
        numColsOutput += 1
      }
    } else if (map(model.predictionCol) != "") {
      // output prediction
      val features2pred: FeaturesType => Double = model.predict
      tmpData = tmpData.withColumn(map(model.predictionCol),
        callUDF(features2pred, DoubleType, col(map(model.featuresCol))))
      numColsOutput += 1
    }
    (numColsOutput, tmpData)
  }

}
