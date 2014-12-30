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
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Star


/**
 * :: DeveloperApi ::
 *
 * Trait for parameters for prediction (regression and classification).
 */
@DeveloperApi
trait PredictorParams extends Params
  with HasLabelCol with HasFeaturesCol with HasPredictionCol {

  /**
   * Validates and transforms the input schema with the provided param map.
   * @param schema input schema
   * @param paramMap additional parameters
   * @param fitting whether this is in fitting
   * @param featuresDataType  SQL DataType for FeaturesType.
   *                          E.g., [[org.apache.spark.mllib.linalg.VectorUDT]] for vector features.
   * @return output schema
   */
  protected def validateAndTransformSchema(
      schema: StructType,
      paramMap: ParamMap,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    val map = this.paramMap ++ paramMap
    // TODO: Support casting Array[Double] and Array[Float] to Vector when FeaturesType = Vector
    checkInputColumn(schema, map(featuresCol), featuresDataType)
    if (fitting) {
      // TODO: Allow other numeric types
      checkInputColumn(schema, map(labelCol), DoubleType)
    }
    addOutputColumn(schema, map(predictionCol), DoubleType)
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
 */
@AlphaComponent
abstract class Predictor[
    FeaturesType,
    Learner <: Predictor[FeaturesType, Learner, M],
    M <: PredictionModel[FeaturesType, M]]
  extends Estimator[M] with PredictorParams {

  // TODO: Eliminate asInstanceOf and see if that works.
  def setLabelCol(value: String): Learner = set(labelCol, value).asInstanceOf[Learner]
  def setFeaturesCol(value: String): Learner = set(featuresCol, value).asInstanceOf[Learner]
  def setPredictionCol(value: String): Learner = set(predictionCol, value).asInstanceOf[Learner]

  /*
  // This will be useful for boosting.
  protected def selectLabelColumn(dataset: SchemaRDD, paramMap: ParamMap): RDD[Double] = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    dataset.select(map(labelCol).attr).map {
      case Row(label: Double) => label
      case Row(label: Int) => label.toDouble
    }
  }
  */

  /**
   * :: DeveloperApi ::
   *
   * Returns the SQL DataType corresponding to the FeaturesType type parameter.
   *
   * This is used by [[validateAndTransformSchema()]].
   * This workaround is needed since SQL has different APIs for Scala and Java.
   */
  @DeveloperApi
  protected def featuresDataType: DataType

  private[ml] override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap, fitting = true, featuresDataType)
  }

  /**
   * Extract [[labelCol]] and [[featuresCol]] from the given dataset,
   * and put it in an RDD with strong types.
   */
  protected def extractLabeledPoints(dataset: SchemaRDD, paramMap: ParamMap): RDD[LabeledPoint] = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    dataset.select(map(labelCol).attr, map(featuresCol).attr)
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
 */
@AlphaComponent
abstract class PredictionModel[FeaturesType, M <: PredictionModel[FeaturesType, M]]
  extends Model[M] with PredictorParams {

  def setFeaturesCol(value: String): M = set(featuresCol, value).asInstanceOf[M]

  def setPredictionCol(value: String): M = set(predictionCol, value).asInstanceOf[M]

  /**
   * :: DeveloperApi ::
   *
   * Returns the SQL DataType corresponding to the FeaturesType type parameter.
   *
   * This is used by [[validateAndTransformSchema()]].
   * This workaround is needed since SQL has different APIs for Scala and Java.
   */
  @DeveloperApi
  protected def featuresDataType: DataType

  private[ml] override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap, fitting = false, featuresDataType)
  }

  /**
   * Transforms dataset by reading from [[featuresCol]], calling [[predict()]], and storing
   * the predictions as a new column [[predictionCol]].
   *
   * @param dataset input dataset
   * @param paramMap additional parameters, overwrite embedded params
   * @return transformed dataset with [[predictionCol]] of type [[Double]]
   */
  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    // This default implementation should be overridden as needed.
    import org.apache.spark.sql.catalyst.dsl._
    import dataset.sqlContext._

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

    if (map(predictionCol) != "") {
      val pred: FeaturesType => Double = (features) => {
        tmpModel.predict(features)
      }
      dataset.select(Star(None), pred.call(map(featuresCol).attr) as map(predictionCol))
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

  /**
   * Create a copy of the model.
   * The copy is shallow, except for the embedded paramMap, which gets a deep copy.
   */
  protected def copy(): M
}
