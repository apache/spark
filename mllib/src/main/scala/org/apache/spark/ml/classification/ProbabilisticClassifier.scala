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

import org.apache.spark.annotation.{AlphaComponent, DeveloperApi}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Params for probabilistic classification.
 */
private[classification] trait ProbabilisticClassifierParams
  extends ClassifierParams with HasProbabilityCol {

  override protected def validateAndTransformSchema(
      schema: StructType,
      paramMap: ParamMap,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    val parentSchema = super.validateAndTransformSchema(schema, paramMap, fitting, featuresDataType)
    val map = extractParamMap(paramMap)
    SchemaUtils.appendColumn(parentSchema, map(probabilityCol), new VectorUDT)
  }
}


/**
 * :: AlphaComponent ::
 *
 * Single-label binary or multiclass classifier which can output class conditional probabilities.
 *
 * @tparam FeaturesType  Type of input features.  E.g., [[Vector]]
 * @tparam E  Concrete Estimator type
 * @tparam M  Concrete Model type
 *
 * NOTE: This is currently private[spark] but will be made public later once it is stabilized.
 */
@AlphaComponent
private[spark] abstract class ProbabilisticClassifier[
    FeaturesType,
    E <: ProbabilisticClassifier[FeaturesType, E, M],
    M <: ProbabilisticClassificationModel[FeaturesType, M]]
  extends Classifier[FeaturesType, E, M] with ProbabilisticClassifierParams {

  /** @group setParam */
  def setProbabilityCol(value: String): E = set(probabilityCol, value).asInstanceOf[E]
}


/**
 * :: AlphaComponent ::
 *
 * Model produced by a [[ProbabilisticClassifier]].
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * @tparam FeaturesType  Type of input features.  E.g., [[Vector]]
 * @tparam M  Concrete Model type
 *
 * NOTE: This is currently private[spark] but will be made public later once it is stabilized.
 */
@AlphaComponent
private[spark] abstract class ProbabilisticClassificationModel[
    FeaturesType,
    M <: ProbabilisticClassificationModel[FeaturesType, M]]
  extends ClassificationModel[FeaturesType, M] with ProbabilisticClassifierParams {

  /** @group setParam */
  def setProbabilityCol(value: String): M = set(probabilityCol, value).asInstanceOf[M]

  /**
   * Transforms dataset by reading from [[featuresCol]], and appending new columns as specified by
   * parameters:
   *  - predicted labels as [[predictionCol]] of type [[Double]]
   *  - raw predictions (confidences) as [[rawPredictionCol]] of type [[Vector]]
   *  - probability of each class as [[probabilityCol]] of type [[Vector]].
   *
   * @param dataset input dataset
   * @param paramMap additional parameters, overwrite embedded params
   * @return transformed dataset
   */
  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    // This default implementation should be overridden as needed.

    // Check schema
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = extractParamMap(paramMap)

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

    // Output selected columns only.
    if (map(probabilityCol) != "") {
      // output probabilities
      val features2probs: FeaturesType => Vector = (features) => {
        tmpModel.predictProbabilities(features)
      }
      outputData.withColumn(map(probabilityCol),
        callUDF(features2probs, new VectorUDT, col(map(featuresCol))))
    } else {
      if (numColsOutput == 0) {
        this.logWarning(s"$uid: ProbabilisticClassificationModel.transform() was called as NOOP" +
          " since no output columns were set.")
      }
      outputData
    }
  }

  /**
   * :: DeveloperApi ::
   *
   * Predict the probability of each class given the features.
   * These predictions are also called class conditional probabilities.
   *
   * WARNING: Not all models output well-calibrated probability estimates!  These probabilities
   *          should be treated as confidences, not precise probabilities.
   *
   * This internal method is used to implement [[transform()]] and output [[probabilityCol]].
   */
  @DeveloperApi
  protected def predictProbabilities(features: FeaturesType): Vector
}
