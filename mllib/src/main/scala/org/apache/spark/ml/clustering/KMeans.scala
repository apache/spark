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
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans, KMeansModel => MLlibKMeansModel}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.functions.{col, callUDF}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.Utils


/**
 * Common params for KMeans and KMeansModel
 */
private[clustering] trait KMeansParams
    extends Params with HasMaxIter with HasFeaturesCol with HasSeed with HasPredictionCol {

  /**
   * Set the number of clusters to create (k). Default: 2.
   * @group param
   */
  val k = new Param[Int](this, "k", "number of clusters to create")

  /** @group getParam */
  def getK: Int = $(k)

  /**
   * Param the number of runs of the algorithm to execute in parallel. We initialize the algorithm
   * this many times with random starting conditions (configured by the initialization mode), then
   * return the best clustering found over any run. Default: 1.
   * @group param
   */
  val runs = new Param[Int](this, "runs", "number of runs of the algorithm to execute in parallel")

  /** @group getParam */
  def getRuns: Int = $(runs)

  /**
   * Param the distance threshold within which we've consider centers to have converged.
   * If all centers move less than this Euclidean distance, we stop iterating one run.
   * @group param
   */
  val epsilon = new Param[Double](this, "epsilon", "distance threshold")

  /** @group getParam */
  def getEpsilon: Double = $(epsilon)

  /**
   * Param for the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   * @group param
   */
  val initMode = new Param[String](this, "initMode", "initialization algorithm")

  /** @group getParam */
  def getInitializationMode: String = $(initMode)

  /**
   * Param for the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 5 is almost always enough. Default: 5.
   * @group param
   */
  val initSteps = new Param[Int](this, "initSteps", "number of steps for k-means||")

  /** @group getParam */
  def getInitializationSteps: Int = $(initSteps)

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
    dataset.withColumn($(predictionCol), callUDF(predict _, IntegerType, col($(featuresCol))))
  }

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

  setDefault(k, 2)
  setDefault(maxIter, 20)
  setDefault(runs, 1)
  setDefault(initMode, MLlibKMeans.K_MEANS_PARALLEL)
  setDefault(initSteps, 5)
  setDefault(epsilon, 1e-4)
  setDefault(seed, Utils.random.nextLong())

  override def copy(extra: ParamMap): Estimator[KMeansModel] = defaultCopy(extra)

  def this() = this(Identifiable.randomUID("kmeans"))

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setK(value: Int): this.type = set(k, value)

  /** @group setParam */
  def setInitializationMode(value: String): this.type = {
    MLlibKMeans.validateInitializationMode(value)
    set(initMode, value)
  }

  /** @group setParam */
  def setInitializationSteps(value: Int): this.type = {
    require(value > 0, "Number of initialization steps must be positive")
    set(initSteps, value)
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

    val algo = new MLlibKMeans()
        .setK(map(k))
        .setMaxIterations(map(maxIter))
        .setSeed(map(seed))
    val parentModel = algo.run(rdd)
    val model = new KMeansModel(uid, parentModel)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

