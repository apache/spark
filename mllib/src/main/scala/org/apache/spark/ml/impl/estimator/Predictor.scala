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

package org.apache.spark.ml.impl.estimator

import org.apache.spark.annotation.{AlphaComponent, DeveloperApi}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}

/**
 * :: DeveloperApi ::
 *
 * Trait for parameters for prediction (regression and classification).
 *
 * NOTE: This is currently private[spark] but will be made public later once it is stabilized.
 */
@DeveloperApi
private[spark] trait PredictorParams extends Params
  with HasLabelCol with HasFeaturesCol with HasPredictionCol {

  /**
   * Validates and transforms the input schema with the provided param map.
   * @param schema input schema
   * @param fitting whether this is in fitting
   * @param featuresDataType  SQL DataType for FeaturesType.
   *                          E.g., [[org.apache.spark.mllib.linalg.VectorUDT]] for vector features.
   * @return output schema
   */
  protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    // TODO: Support casting Array[Double] and Array[Float] to Vector when FeaturesType = Vector
    SchemaUtils.checkColumnType(schema, $(featuresCol), featuresDataType)
    if (fitting) {
      // TODO: Allow other numeric types
      SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)
    }
    SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)
  }
}

/**
 * :: AlphaComponent ::
 *
 * Abstraction for prediction problems (regression and classification).
 *
 * @tparam FeaturesType  Type of features.
 *                       E.g., [[org.apache.spark.mllib.linalg.VectorUDT]] for vector features.
 * @tparam Learner  Specialization of this class.  If you subclass this type, use this type
 *                  parameter to specify the concrete type.
 * @tparam M  Specialization of [[PredictionModel]].  If you subclass this type, use this type
 *            parameter to specify the concrete type for the corresponding model.
 *
 * NOTE: This is currently private[spark] but will be made public later once it is stabilized.
 */
@AlphaComponent
private[spark] abstract class Predictor[
    FeaturesType,
    Learner <: Predictor[FeaturesType, Learner, M],
    M <: PredictionModel[FeaturesType, M]]
  extends Estimator[M] with PredictorParams {

  /** @group setParam */
  def setLabelCol(value: String): Learner = set(labelCol, value).asInstanceOf[Learner]

  /** @group setParam */
  def setFeaturesCol(value: String): Learner = set(featuresCol, value).asInstanceOf[Learner]

  /** @group setParam */
  def setPredictionCol(value: String): Learner = set(predictionCol, value).asInstanceOf[Learner]

  override def fit(dataset: DataFrame): M = {
    // This handles a few items such as schema validation.
    // Developers only need to implement train().
    transformSchema(dataset.schema, logging = true)
    copyValues(train(dataset))
  }

  override def copy(extra: ParamMap): Learner = {
    super.copy(extra).asInstanceOf[Learner]
  }

  /**
   * :: DeveloperApi ::
   *
   * Train a model using the given dataset and parameters.
   * Developers can implement this instead of [[fit()]] to avoid dealing with schema validation
   * and copying parameters into the model.
   *
   * @param dataset  Training dataset
   * @return  Fitted model
   */
  @DeveloperApi
  protected def train(dataset: DataFrame): M

  /**
   * :: DeveloperApi ::
   *
   * Returns the SQL DataType corresponding to the FeaturesType type parameter.
   *
   * This is used by [[validateAndTransformSchema()]].
   * This workaround is needed since SQL has different APIs for Scala and Java.
   *
   * The default value is VectorUDT, but it may be overridden if FeaturesType is not Vector.
   */
  @DeveloperApi
  protected def featuresDataType: DataType = new VectorUDT

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true, featuresDataType)
  }

  /**
   * Extract [[labelCol]] and [[featuresCol]] from the given dataset,
   * and put it in an RDD with strong types.
   */
  protected def extractLabeledPoints(dataset: DataFrame): RDD[LabeledPoint] = {
    dataset.select($(labelCol), $(featuresCol))
      .map { case Row(label: Double, features: Vector) =>
      LabeledPoint(label, features)
    }
  }
}

/**
 * :: AlphaComponent ::
 *
 * Abstraction for a model for prediction tasks (regression and classification).
 *
 * @tparam FeaturesType  Type of features.
 *                       E.g., [[org.apache.spark.mllib.linalg.VectorUDT]] for vector features.
 * @tparam M  Specialization of [[PredictionModel]].  If you subclass this type, use this type
 *            parameter to specify the concrete type for the corresponding model.
 *
 * NOTE: This is currently private[spark] but will be made public later once it is stabilized.
 */
@AlphaComponent
private[spark] abstract class PredictionModel[FeaturesType, M <: PredictionModel[FeaturesType, M]]
  extends Model[M] with PredictorParams {

  /** @group setParam */
  def setFeaturesCol(value: String): M = set(featuresCol, value).asInstanceOf[M]

  /** @group setParam */
  def setPredictionCol(value: String): M = set(predictionCol, value).asInstanceOf[M]

  /**
   * :: DeveloperApi ::
   *
   * Returns the SQL DataType corresponding to the FeaturesType type parameter.
   *
   * This is used by [[validateAndTransformSchema()]].
   * This workaround is needed since SQL has different APIs for Scala and Java.
   *
   * The default value is VectorUDT, but it may be overridden if FeaturesType is not Vector.
   */
  @DeveloperApi
  protected def featuresDataType: DataType = new VectorUDT

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = false, featuresDataType)
  }

  /**
   * Transforms dataset by reading from [[featuresCol]], calling [[predict()]], and storing
   * the predictions as a new column [[predictionCol]].
   *
   * @param dataset input dataset
   * @return transformed dataset with [[predictionCol]] of type [[Double]]
   */
  override def transform(dataset: DataFrame): DataFrame = {
    // This default implementation should be overridden as needed.

    // Check schema
    transformSchema(dataset.schema, logging = true)

    if ($(predictionCol) != "") {
      dataset.withColumn($(predictionCol), callUDF(predict _, DoubleType, col($(featuresCol))))
    } else {
      this.logWarning(s"$uid: Predictor.transform() was called as NOOP" +
        " since no output columns were set.")
      dataset
    }
  }

  /**
   * :: DeveloperApi ::
   *
   * Predict label for the given features.
   * This internal method is used to implement [[transform()]] and output [[predictionCol]].
   */
  @DeveloperApi
  protected def predict(features: FeaturesType): Double
}
