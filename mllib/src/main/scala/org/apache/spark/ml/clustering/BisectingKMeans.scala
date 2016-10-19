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

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.clustering.{BisectingKMeans => MLlibBisectingKMeans, BisectingKMeansModel => MLlibBisectingKMeansModel}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StructType}


/**
 * Common params for BisectingKMeans and BisectingKMeansModel
 */
private[clustering] trait BisectingKMeansParams extends Params
  with HasMaxIter with HasFeaturesCol with HasSeed with HasPredictionCol {

  /**
   * The desired number of leaf clusters. Must be > 1. Default: 4.
   * The actual number could be smaller if there are no divisible leaf clusters.
   * @group param
   */
  @Since("2.0.0")
  final val k = new IntParam(this, "k", "The desired number of leaf clusters. " +
    "Must be > 1.", ParamValidators.gt(1))

  /** @group getParam */
  @Since("2.0.0")
  def getK: Int = $(k)

  /**
   * The minimum number of points (if >= 1.0) or the minimum proportion
   * of points (if < 1.0) of a divisible cluster (default: 1.0).
   * @group expertParam
   */
  @Since("2.0.0")
  final val minDivisibleClusterSize = new DoubleParam(this, "minDivisibleClusterSize",
    "The minimum number of points (if >= 1.0) or the minimum proportion " +
      "of points (if < 1.0) of a divisible cluster.", ParamValidators.gt(0.0))

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
 * @param parentModel a model trained by [[org.apache.spark.mllib.clustering.BisectingKMeans]].
 */
@Since("2.0.0")
@Experimental
class BisectingKMeansModel private[ml] (
    @Since("2.0.0") override val uid: String,
    private val parentModel: MLlibBisectingKMeansModel
  ) extends Model[BisectingKMeansModel] with BisectingKMeansParams with MLWritable {

  @Since("2.0.0")
  override def copy(extra: ParamMap): BisectingKMeansModel = {
    val copied = new BisectingKMeansModel(uid, parentModel)
    copyValues(copied, extra)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val predictUDF = udf((vector: Vector) => predict(vector))
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  private[clustering] def predict(features: Vector): Int = parentModel.predict(features)

  @Since("2.0.0")
  def clusterCenters: Array[Vector] = parentModel.clusterCenters.map(_.asML)

  /**
   * Computes the sum of squared distances between the input points and their corresponding cluster
   * centers.
   */
  @Since("2.0.0")
  def computeCost(dataset: Dataset[_]): Double = {
    SchemaUtils.checkColumnType(dataset.schema, $(featuresCol), new VectorUDT)
    val data = dataset.select(col($(featuresCol))).rdd.map { case Row(point: Vector) => point }
    parentModel.computeCost(data.map(OldVectors.fromML))
  }

  @Since("2.0.0")
  override def write: MLWriter = new BisectingKMeansModel.BisectingKMeansModelWriter(this)

  private var trainingSummary: Option[BisectingKMeansSummary] = None

  private[clustering] def setSummary(summary: BisectingKMeansSummary): this.type = {
    this.trainingSummary = Some(summary)
    this
  }

  /**
   * Return true if there exists summary of model.
   */
  @Since("2.1.0")
  def hasSummary: Boolean = trainingSummary.nonEmpty

  /**
   * Gets summary of model on training set. An exception is
   * thrown if `trainingSummary == None`.
   */
  @Since("2.1.0")
  def summary: BisectingKMeansSummary = trainingSummary.getOrElse {
    throw new SparkException(
      s"No training summary available for the ${this.getClass.getSimpleName}")
  }
}

object BisectingKMeansModel extends MLReadable[BisectingKMeansModel] {
  @Since("2.0.0")
  override def read: MLReader[BisectingKMeansModel] = new BisectingKMeansModelReader

  @Since("2.0.0")
  override def load(path: String): BisectingKMeansModel = super.load(path)

  /** [[MLWriter]] instance for [[BisectingKMeansModel]] */
  private[BisectingKMeansModel]
  class BisectingKMeansModelWriter(instance: BisectingKMeansModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.parentModel.save(sc, dataPath)
    }
  }

  private class BisectingKMeansModelReader extends MLReader[BisectingKMeansModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[BisectingKMeansModel].getName

    override def load(path: String): BisectingKMeansModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val mllibModel = MLlibBisectingKMeansModel.load(sc, dataPath)
      val model = new BisectingKMeansModel(metadata.uid, mllibModel)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
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
  extends Estimator[BisectingKMeansModel] with BisectingKMeansParams with DefaultParamsWritable {

  setDefault(
    k -> 4,
    maxIter -> 20,
    minDivisibleClusterSize -> 1.0)

  @Since("2.0.0")
  override def copy(extra: ParamMap): BisectingKMeans = defaultCopy(extra)

  @Since("2.0.0")
  def this() = this(Identifiable.randomUID("bisecting-kmeans"))

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
  override def fit(dataset: Dataset[_]): BisectingKMeansModel = {
    transformSchema(dataset.schema, logging = true)
    val rdd: RDD[OldVector] = dataset.select(col($(featuresCol))).rdd.map {
      case Row(point: Vector) => OldVectors.fromML(point)
    }

    val instr = Instrumentation.create(this, rdd)
    instr.logParams(featuresCol, predictionCol, k, maxIter, seed, minDivisibleClusterSize)

    val bkm = new MLlibBisectingKMeans()
      .setK($(k))
      .setMaxIterations($(maxIter))
      .setMinDivisibleClusterSize($(minDivisibleClusterSize))
      .setSeed($(seed))
    val parentModel = bkm.run(rdd)
    val model = copyValues(new BisectingKMeansModel(uid, parentModel).setParent(this))
    val summary = new BisectingKMeansSummary(
      model.transform(dataset), $(predictionCol), $(featuresCol), $(k))
    model.setSummary(summary)
    val m = model.setSummary(summary)
    instr.logSuccess(m)
    m
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}


@Since("2.0.0")
object BisectingKMeans extends DefaultParamsReadable[BisectingKMeans] {

  @Since("2.0.0")
  override def load(path: String): BisectingKMeans = super.load(path)
}


/**
 * :: Experimental ::
 * Summary of BisectingKMeans.
 *
 * @param predictions  [[DataFrame]] produced by [[BisectingKMeansModel.transform()]]
 * @param predictionCol  Name for column of predicted clusters in `predictions`
 * @param featuresCol  Name for column of features in `predictions`
 * @param k  Number of clusters
 */
@Since("2.1.0")
@Experimental
class BisectingKMeansSummary private[clustering] (
    predictions: DataFrame,
    predictionCol: String,
    featuresCol: String,
    k: Int)
  extends ClusteringSummary (
    predictions,
    predictionCol,
    featuresCol,
    k
  )
