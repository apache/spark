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
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasMaxIter, HasPredictionCol, HasSeed}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.Utils


/**
 * Common params for KMeans and KMeansModel
 */
private[clustering] trait KMeansParams
    extends Params with HasMaxIter with HasFeaturesCol with HasSeed with HasPredictionCol {
  /**
   * Param for the column name for the number of clusters to create.
   * @group param
   */
  val k = new Param[Int](this, "k", "number of clusters to create")

  /** @group getParam */
  def getK: Int = $(k)

  /**
   * Param for the column name for the number of runs of the algorithm to execute in parallel.
   * @group param
   */
  val runs = new Param[Int](this, "runs", "number of runs of the algorithm to execute in parallel")

  /** @group getParam */
  def getRuns: Int = $(runs)

  /**
   * Param for the column name for the distance threshold
   * within which we've consider centers to have converged.
   * @group param
   */
  val epsilon = new Param[Double](this, "epsilon", "distance threshold")

  /** @group getParam */
  def getEpsilon: Double = $(epsilon)

  /**
   * Param for the initialization algorithm.
   * @group param
   */
  val initializationMode = new Param[String](this, "initializationMode", "initialization algorithm")

  /** @group getParam */
  def getInitializationMode: String = $(initializationMode)

  /**
   * Param for the number of steps for k-means initialization mode.
   * @group param
   */
  val initializationSteps =
    new Param[Int](this, "initializationSteps", "number of steps for k-means||")

  /** @group getParam */
  def getInitializationSteps: Int = $(initializationSteps)

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
 * @param paramMap a parameter map for fitting.
 * @param parentModel a model trained by spark.mllib.clustering.KMeans.
 */
@Experimental
class KMeansModel private[ml] (
  override val uid: String,
  val paramMap: ParamMap,
  val parentModel: mllib.clustering.KMeansModel
) extends Model[KMeansModel] with KMeansParams {
  /**
   * Transforms the input dataset.
   */
  override def transform(dataset: DataFrame): DataFrame = {
    dataset.select(
          dataset("*"),
          callUDF(predict _, IntegerType, col($(featuresCol))).as($(predictionCol))
        )
  }

  /**
   * :: DeveloperApi ::
   *
   * Derives the output schema from the input schema.
   */
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  def predict(features: Vector): Int = parentModel.predict(features)

  def clusterCenters: Array[Vector] = parentModel.clusterCenters
}

/**
 * :: Experimental ::
 * KMeans API for spark.ml Pipeline.
 */
@Experimental
class KMeans(override val uid: String) extends Estimator[KMeansModel] with KMeansParams {
  setK(2)
  setMaxIter(20)
  setRuns(1)
  setInitializationMode(KMeans.K_MEANS_PARALLEL)
  setInitializationSteps(5)
  setEpsilon(1e-4)
  setSeed(Utils.random.nextLong())

  def this() = this(Identifiable.randomUID("kmeans"))

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setK(value: Int): this.type = set(k, value)

  /** @group setParam */
  def setInitializationMode(value: String): this.type = {
    mllib.clustering.KMeans.validateInitializationMode(value)
    set(initializationMode, value)
  }

  /** @group setParam */
  def setInitializationSteps(value: Int): this.type = {
    require(value > 0, "Number of initialization steps must be positive")
    set(initializationSteps, value)
  }

  /** @group setParam */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  def setRuns(value: Int): this.type = set(runs, value)

  /** @group setParam */
  def setEpsilon(value: Double): this.type = set(epsilon, value)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)


  override def fit(dataset: DataFrame): KMeansModel = {
    val map = this.extractParamMap()
    val rdd = dataset.select(col(map(featuresCol))).map { case Row(point: Vector) => point}

    val algo = new mllib.clustering.KMeans()
        .setK(map(k))
        .setMaxIterations(map(maxIter))
        .setSeed(map(seed))
    val parentModel = algo.run(rdd)
    val model = new KMeansModel(uid, map, parentModel)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

