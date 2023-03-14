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

package org.apache.spark.ml

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Abstraction for prediction problems (regression and classification). It accepts all NumericType
 * labels and will automatically cast it to DoubleType in `fit()`. If this predictor supports
 * weights, it accepts all NumericType weights, which will be automatically casted to DoubleType
 * in `fit()`.
 *
 * @tparam FeaturesType
 *   Type of features. E.g., `VectorUDT` for vector features.
 * @tparam Learner
 *   Specialization of this class. If you subclass this type, use this type parameter to specify
 *   the concrete type.
 * @tparam M
 *   Specialization of [[PredictionModel]]. If you subclass this type, use this type parameter to
 *   specify the concrete type for the corresponding model.
 */
abstract class Predictor[
    FeaturesType,
    Learner <: Predictor[FeaturesType, Learner, M],
    M <: PredictionModel[FeaturesType, M]]
    extends Estimator[M]
    with PredictorParams {

  /** @group setParam */
  @Since("3.5.0")
  def setLabelCol(value: String): Learner = set(labelCol, value).asInstanceOf[Learner]

  /** @group setParam */
  @Since("3.5.0")
  def setFeaturesCol(value: String): Learner = set(featuresCol, value).asInstanceOf[Learner]

  /** @group setParam */
  @Since("3.5.0")
  def setPredictionCol(value: String): Learner = set(predictionCol, value).asInstanceOf[Learner]

  @Since("3.5.0")
  override def fit(dataset: Dataset[_]): M = {
    // TODO: should send the id of the input dataset and the latest params to the server,
    //  then invoke the 'fit' method of the remote predictor
    throw new NotImplementedError
  }

  @Since("3.5.0")
  override def copy(extra: ParamMap): Learner

  /**
   * Returns the SQL DataType corresponding to the FeaturesType type parameter.
   *
   * This is used by `validateAndTransformSchema()`. This workaround is needed since SQL has
   * different APIs for Scala and Java.
   *
   * The default value is VectorUDT, but it may be overridden if FeaturesType is not Vector.
   */
  private[ml] def featuresDataType: DataType = new VectorUDT

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true, featuresDataType)
  }
}

/**
 * Abstraction for a model for prediction tasks (regression and classification).
 *
 * @tparam FeaturesType
 *   Type of features. E.g., `VectorUDT` for vector features.
 * @tparam M
 *   Specialization of [[PredictionModel]]. If you subclass this type, use this type parameter to
 *   specify the concrete type for the corresponding model.
 */
abstract class PredictionModel[FeaturesType, M <: PredictionModel[FeaturesType, M]]
    extends Model[M]
    with PredictorParams {

  /** @group setParam */
  @Since("3.5.0")
  def setFeaturesCol(value: String): M = set(featuresCol, value).asInstanceOf[M]

  /** @group setParam */
  @Since("3.5.0")
  def setPredictionCol(value: String): M = set(predictionCol, value).asInstanceOf[M]

  /** Returns the number of features the model was trained on. If unknown, returns -1 */
  @Since("3.5.0")
  def numFeatures: Int = -1

  /**
   * Returns the SQL DataType corresponding to the FeaturesType type parameter.
   *
   * This is used by `validateAndTransformSchema()`. This workaround is needed since SQL has
   * different APIs for Scala and Java.
   *
   * The default value is VectorUDT, but it may be overridden if FeaturesType is not Vector.
   */
  protected def featuresDataType: DataType = new VectorUDT

  @Since("3.5.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema, fitting = false, featuresDataType)
    if ($(predictionCol).nonEmpty) {
      outputSchema = SchemaUtils.updateNumeric(outputSchema, $(predictionCol))
    }
    outputSchema
  }

  /**
   * Transforms dataset by reading from [[featuresCol]], calling `predict`, and storing the
   * predictions as a new column [[predictionCol]].
   *
   * @param dataset
   *   input dataset
   * @return
   *   transformed dataset with [[predictionCol]] of type `Double`
   */
  @Since("3.5.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    if ($(predictionCol).nonEmpty) {
      transformImpl(dataset)
    } else {
      this.logWarning(
        s"$uid: Predictor.transform() does nothing" +
          " because no output columns were set.")
      dataset.toDF
    }
  }

  protected def transformImpl(dataset: Dataset[_]): DataFrame = {
    // TODO: should send the id of the input dataset and the latest params to the server,
    //  then invoke the 'transform' method of the remote model
    throw new NotImplementedError
  }

  /**
   * Predict label for the given features. This method is used to implement `transform()` and
   * output [[predictionCol]].
   */
  @Since("3.5.0")
  def predict(features: FeaturesType): Double
}
