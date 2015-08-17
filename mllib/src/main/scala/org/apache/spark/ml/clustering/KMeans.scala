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

package org.apache.spark.ml.clustering

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param.{Param, Params, IntParam, ParamMap}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans, KMeansModel => MLlibKMeansModel}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row}


/**
 * Common params for KMeans and KMeansModel
 */
private[clustering] trait KMeansParams extends Params with HasMaxIter with HasFeaturesCol
  with HasSeed with HasPredictionCol with HasTol {

  /**
   * Set the number of clusters to create (k). Must be > 1. Default: 2.
   * @group param
   */
  final val k = new IntParam(this, "k", "number of clusters to create", (x: Int) => x > 1)

  /** @group getParam */
  def getK: Int = $(k)

  /**
   * Param for the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   * @group expertParam
   */
  final val initMode = new Param[String](this, "initMode", "initialization algorithm",
    (value: String) => MLlibKMeans.validateInitMode(value))

  /** @group expertGetParam */
  def getInitMode: String = $(initMode)

  /**
   * Param for the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 5 is almost always enough. Must be > 0. Default: 5.
   * @group expertParam
   */
  final val initSteps = new IntParam(this, "initSteps", "number of steps for k-means||",
    (value: Int) => value > 0)

  /** @group expertGetParam */
  def getInitSteps: Int = $(initSteps)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(predictionCol), IntegerType)
  }
}

/**
 * :: Experimental ::
 * Model fitted by KMeans.
 *
 * @param parentModel a model trained by spark.mllib.clustering.KMeans.
 */
@Experimental
class KMeansModel private[ml] (
    override val uid: String,
    private val parentModel: MLlibKMeansModel) extends Model[KMeansModel] with KMeansParams {

  override def copy(extra: ParamMap): KMeansModel = {
    val copied = new KMeansModel(uid, parentModel)
    copyValues(copied, extra)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val predictUDF = udf((vector: Vector) => predict(vector))
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  private[clustering] def predict(features: Vector): Int = parentModel.predict(features)

  def clusterCenters: Array[Vector] = parentModel.clusterCenters
}

/**
 * :: Experimental ::
 * K-means clustering with support for k-means|| initialization proposed by Bahmani et al.
 *
 * @see [[http://dx.doi.org/10.14778/2180912.2180915 Bahmani et al., Scalable k-means++.]]
 */
@Experimental
class KMeans(override val uid: String) extends Estimator[KMeansModel] with KMeansParams {

  setDefault(
    k -> 2,
    maxIter -> 20,
    initMode -> MLlibKMeans.K_MEANS_PARALLEL,
    initSteps -> 5,
    tol -> 1e-4)

  override def copy(extra: ParamMap): KMeans = defaultCopy(extra)

  def this() = this(Identifiable.randomUID("kmeans"))

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setK(value: Int): this.type = set(k, value)

  /** @group expertSetParam */
  def setInitMode(value: String): this.type = set(initMode, value)

  /** @group expertSetParam */
  def setInitSteps(value: Int): this.type = set(initSteps, value)

  /** @group setParam */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  def setTol(value: Double): this.type = set(tol, value)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)

  override def fit(dataset: DataFrame): KMeansModel = {
    val rdd = dataset.select(col($(featuresCol))).map { case Row(point: Vector) => point }

    val algo = new MLlibKMeans()
      .setK($(k))
      .setInitializationMode($(initMode))
      .setInitializationSteps($(initSteps))
      .setMaxIterations($(maxIter))
      .setSeed($(seed))
      .setEpsilon($(tol))
    val parentModel = algo.run(rdd)
    val model = new KMeansModel(uid, parentModel)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

