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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, Params}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.mllib.clustering.
  {BisectingKMeans => MLlibBisectingKMeans, BisectingKMeansModel => MLlibBisectingKMeansModel}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StructType}


/**
 * Common params for BisectingKMeans and BisectingKMeansModel
 */
private[clustering] trait BisectingKMeansParams extends Params
  with HasMaxIter with HasFeaturesCol with HasSeed with HasPredictionCol {

  /**
   * Set the number of clusters to create (k). Must be > 1. Default: 2.
   * @group param
   */
  @Since("2.0.0")
  final val k = new IntParam(this, "k", "number of clusters to create", (x: Int) => x > 1)

  /** @group getParam */
  @Since("2.0.0")
  def getK: Int = $(k)

  /** @group expertParam */
  @Since("2.0.0")
  final val minDivisibleClusterSize = new Param[Double](
    this,
    "minDivisibleClusterSize",
    "the minimum number of points (if >= 1.0) or the minimum proportion",
    (value: Double) => value > 0)

  /** @group expertGetParam */
  @Since("2.0.0")
  def getMinDivisibleClusterSize: Double = $(minDivisibleClusterSize)

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
 * Model fitted by BisectingKMeans.
 *
 * @param parentModel a model trained by spark.mllib.clustering.BisectingKMeans.
 */
@Since("2.0.0")
@Experimental
class BisectingKMeansModel private[ml] (
    @Since("2.0.0") override val uid: String,
    private val parentModel: MLlibBisectingKMeansModel
  ) extends Model[BisectingKMeansModel] with BisectingKMeansParams {

  @Since("2.0.0")
  override def copy(extra: ParamMap): BisectingKMeansModel = {
    val copied = new BisectingKMeansModel(uid, parentModel)
    copyValues(copied, extra)
  }

  @Since("2.0.0")
  override def transform(dataset: DataFrame): DataFrame = {
    val predictUDF = udf((vector: Vector) => predict(vector))
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  private[clustering] def predict(features: Vector): Int = parentModel.predict(features)

  @Since("2.0.0")
  def clusterCenters: Array[Vector] = parentModel.clusterCenters

  /**
   * Computes the sum of squared distances between the input points and their corresponding cluster
   * centers.
   */
  @Since("2.0.0")
  def computeCost(dataset: DataFrame): Double = {
    SchemaUtils.checkColumnType(dataset.schema, $(featuresCol), new VectorUDT)
    val data = dataset.select(col($(featuresCol))).rdd.map { case Row(point: Vector) => point }
    parentModel.computeCost(data)
  }
}

/**
 * :: Experimental ::
 *
 * A bisecting k-means algorithm based on the paper "A comparison of document clustering techniques"
 * by Steinbach, Karypis, and Kumar, with modification to fit Spark.
 * The algorithm starts from a single cluster that contains all points.
 * Iteratively it finds divisible clusters on the bottom level and bisects each of them using
 * k-means, until there are `k` leaf clusters in total or no leaf clusters are divisible.
 * The bisecting steps of clusters on the same level are grouped together to increase parallelism.
 * If bisecting all divisible clusters on the bottom level would result more than `k` leaf clusters,
 * larger clusters get higher priority.
 *
 * @see [[http://glaros.dtc.umn.edu/gkhome/fetch/papers/docclusterKDDTMW00.pdf
 *     Steinbach, Karypis, and Kumar, A comparison of document clustering techniques,
 *     KDD Workshop on Text Mining, 2000.]]
 */
@Since("2.0.0")
@Experimental
class BisectingKMeans @Since("2.0.0") (
    @Since("2.0.0") override val uid: String)
  extends Estimator[BisectingKMeansModel] with BisectingKMeansParams {

  setDefault(
    k -> 4,
    maxIter -> 20,
    minDivisibleClusterSize -> 1.0)

  @Since("2.0.0")
  override def copy(extra: ParamMap): BisectingKMeans = defaultCopy(extra)

  @Since("2.0.0")
  def this() = this(Identifiable.randomUID("bisecting k-means"))

  /** @group setParam */
  @Since("2.0.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group setParam */
  @Since("2.0.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("2.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group expertSetParam */
  @Since("2.0.0")
  def setMinDivisibleClusterSize(value: Double): this.type = set(minDivisibleClusterSize, value)

  @Since("2.0.0")
  override def fit(dataset: DataFrame): BisectingKMeansModel = {
    val rdd = dataset.select(col($(featuresCol))).rdd.map { case Row(point: Vector) => point }

    val bkm = new MLlibBisectingKMeans()
      .setK($(k))
      .setMaxIterations($(maxIter))
      .setMinDivisibleClusterSize($(minDivisibleClusterSize))
      .setSeed($(seed))
    val parentModel = bkm.run(rdd)
    val model = new BisectingKMeansModel(uid, parentModel)
    copyValues(model.setParent(this))
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

