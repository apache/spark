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

package org.apache.spark.ml.tuning

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasPredictionCol, HasSeed}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, rowNumber, udf}
import org.apache.spark.sql.types.StructType

/**
 * Params for [[Bagging]] and [[BaggingModel]].
 *
 * TODO?: similar typesafe specification of model over casting in CrossValidator
 */
private[ml] trait BaggingParams[M <: Model with HasPredictionCol] extends Params with HasSeed {
  /**
   * Param for the [[estimator]] to be validated
   * @group param
   */
  val estimator: Param[Estimator[M]] = new Param(this, "estimator", "estimator for bagging")

  /** @group getParam */
  def getEstimator: Estimator[M] = $(estimator)

  /**
   * Param for indicating whether bagged model is a classifier (true) or regressor (false). Voting
   * is used for classification (ties broken arbitrarily) and averaging is used for regression.
   * Default: true (Classification)
   * @group param
   */
  val isClassifier: BooleanParam = new BooleanParam(this, "isClassification",
    "indicates if bagged model is a classifier or regressor")

  /** @group getParam */
  def getIsClassifier: Boolean = $(isClassifier)

  /**
   * Param for number of bootstrap models.
   * Default: 3
   * @group param
   */
  val numModels: IntParam = new IntParam(this, "numModels",
    "number of models to train on bootstrapped samples (>=1)", ParamValidators.gtEq(1))

  /** @group getParam */
  def getNumModels: Int = $(numModels)


  setDefault(numModels-> 3, isClassifier->true)
}

/**
 * :: Experimental ::
 * Bootstrap aggregation.
 */
@Experimental
class Bagging[M <: Model[_] with HasPredictionCol](override val uid: String)
  extends Estimator[BaggingModel] with BaggingParams[M] with Logging {

  def this() = this(Identifiable.randomUID("bagging"))

  /** @group setParam */
  def setEstimator(value: Estimator[M]): this.type = set(estimator, value)

  /** @group setParam */
  def setNumModels(value: Int): this.type = set(numModels, value)

  /** @group setParam */
  def setIsClassifier(value: Boolean): this.type = set(isClassifier, value)

  override def fit(dataset: DataFrame): BaggingModel = {
    val models = (0 until $(numModels)).map { _ =>
      val bootstrapSample = dataset.sample(true, ???, $(seed))
      $(estimator).fit(bootstrapSample)
    }
    copyValues(new BaggingModel(uid, models).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    $(estimator).transformSchema(schema)
  }

  override def copy(extra: ParamMap): Bagging = {
    val copied = defaultCopy(extra).asInstanceOf[Bagging]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    copied
  }
}

/**
 * :: Experimental ::
 * Model from bootstrap aggregating (bagging).
 *
 * TODO: type-safe way to ensure models has at least one
 */
@Experimental
class BaggingModel[M <: Model[_] with HasPredictionCol] private[ml] (
  override val uid: String,
  val models: Seq[M])
  extends Model[BaggingModel] with BaggingParams {

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    // constant across models since estimator unchanged
    val predictionCol = models.head.getPredictionCol
    val instanceIdCol = "instanceId"
    val modelIdCol = "modelId"

    val numberedDataset = dataset.withColumn(instanceIdCol, rowNumber())
    val predictions = models.zipWithIndex.map { case (model: M, modelId: Int) =>
      val toModelId = udf { _ => modelId }
      model.transform(numberedDataset)
        .select(instanceIdCol, predictionCol)
        .withColumn(modelIdCol, toModelId(col(instanceIdCol)))
    }.reduce { case (a: DataFrame, b: DataFrame) =>
      a.join(b, instanceIdCol)
    }
    if ($(isClassifier)) {
      // Counts number of models voting for each (instance, prediction) pair
      val predictionCounts = predictions
        .groupBy(instanceIdCol, predictionCol)
        .agg(modelIdCol -> "count")
        .withColumnRenamed("count(" + modelIdCol + ")", "predCounts")

      // Gets the counts for the most predicted prediction
      val maxPredictionCounts = predictionCounts
        .groupBy(instanceIdCol)
        .agg("predCounts" -> "max")
        .withColumnRenamed("max(count(" + modelIdCol + "))", "predCounts")

      // Join and project to recover actual prediction
      maxPredictionCounts
        .join(predictionCounts, Seq(instanceIdCol, "predCounts"))
        .drop("predCounts")
    } else {
      predictions.groupBy(instanceIdCol)
        .agg(predictionCol -> "avg")
        .withColumnRenamed("avg(" + predictionCol + ")", predictionCol)
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    models.head.transformSchema(schema)
  }

  override def copy(extra: ParamMap): BaggingModel = {
    val copied = new BaggingModel(
      uid,
      models.map(_.copy(extra).asInstanceOf[Model[_]]))
    copyValues(copied, extra).setParent(parent)
  }
}

