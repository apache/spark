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
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans, VectorWithNorm}
import org.apache.spark.mllib.linalg.BLAS.{axpy, scal}
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.storage.StorageLevel

/**
 * Common params for MiniBatchKMeans and MiniBatchKMeansModel
 */
private[clustering] trait MiniBatchKMeansParams extends Params with HasMaxIter with HasFeaturesCol
  with HasSeed with HasPredictionCol with HasTol {

  /**
   * The number of clusters to create (k). Must be &gt; 1. Note that it is possible for fewer than
   * k clusters to be returned, for example, if there are fewer than k distinct points to cluster.
   * Default: 2.
   * @group param
   */
  @Since("2.3.0")
  final val k = new IntParam(this, "k", "The number of clusters to create. " +
    "Must be > 1.", ParamValidators.gt(1))

  /** @group getParam */
  @Since("2.3.0")
  def getK: Int = $(k)

  /**
   * Param for the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   * @group expertParam
   */
  @Since("2.3.0")
  final val initMode = new Param[String](this, "initMode", "The initialization algorithm. " +
    "Supported options: 'random' and 'k-means||'.",
    (value: String) => MLlibKMeans.validateInitMode(value))

  /** @group expertGetParam */
  @Since("2.3.0")
  def getInitMode: String = $(initMode)

  /**
   * Param for the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 2 is almost always enough. Must be &gt; 0. Default: 2.
   * @group expertParam
   */
  @Since("2.3.0")
  final val initSteps = new IntParam(this, "initSteps", "The number of steps for k-means|| " +
    "initialization mode. Must be > 0.", ParamValidators.gt(0))

  /** @group expertGetParam */
  @Since("2.3.0")
  def getInitSteps: Int = $(initSteps)

  /**
   * The fraction of data used to update centers per iteration. Must be &gt; 0 and &le; 1.
   * Default: 1.0.
   * @group param
   */
  @Since("2.3.0")
  final val fraction = new DoubleParam(this, "fraction", "The fraction of data used to " +
    "update cluster centers per iteration. Must be in (0, 1].",
    ParamValidators.inRange(0, 1, lowerInclusive = false, upperInclusive = true))

  /** @group getParam */
  @Since("2.3.0")
  def getFraction: Double = $(fraction)

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
 * Model fitted by MiniBatchKMeans.
 *
 * @param clusterCenters Centers of each cluster.
 */
@Since("2.3.0")
class MiniBatchKMeansModel private[ml] (
    @Since("2.3.0") override val uid: String,
    @Since("2.3.0") val clusterCenters: Array[Vector])
  extends Model[MiniBatchKMeansModel] with MiniBatchKMeansParams with MLWritable {

  private lazy val clusterCentersWithNorm =
    if (clusterCenters == null) null else clusterCenters.map(new VectorWithNorm(_))

  @Since("2.3.0")
  override def copy(extra: ParamMap): MiniBatchKMeansModel = {
    val copied = copyValues(new MiniBatchKMeansModel(uid, clusterCenters), extra)
    copied.setSummary(trainingSummary).setParent(this.parent)
  }

  /** @group setParam */
  @Since("2.3.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.3.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  @Since("2.3.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val predictUDF = udf((vector: Vector) => predict(vector))
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  @Since("2.3.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  private[clustering] def predict(features: Vector): Int =
    MLlibKMeans.findClosest(clusterCentersWithNorm, new VectorWithNorm(features))._1

  /**
   * Return the K-means cost (sum of squared distances of points to their nearest center) for this
   * model on the given data.
   */
  // TODO: Replace the temp fix when we have proper evaluators defined for clustering.
  @Since("2.3.0")
  def computeCost(dataset: Dataset[_]): Double = {
    SchemaUtils.checkColumnType(dataset.schema, $(featuresCol), new VectorUDT)
    val bcCentersWithNorm = dataset.sparkSession.sparkContext.broadcast(clusterCentersWithNorm)
    val cost = dataset.select(col($(featuresCol))).rdd.map {
      case Row(point: Vector) =>
        MLlibKMeans.pointCost(bcCentersWithNorm.value, new VectorWithNorm(point))
    }.sum()
    bcCentersWithNorm.destroy(blocking = false)
    cost
  }

  /**
   * Returns a [[org.apache.spark.ml.util.MLWriter]] instance for this ML instance.
   *
   * For [[MiniBatchKMeansModel]], this does NOT currently save the training [[summary]].
   * An option to save [[summary]] may be added in the future.
   *
   */
  @Since("2.3.0")
  override def write: MLWriter = new MiniBatchKMeansModel.MiniBatchKMeansModelWriter(this)

  private var trainingSummary: Option[MiniBatchKMeansSummary] = None

  private[clustering] def setSummary(summary: Option[MiniBatchKMeansSummary]): this.type = {
    this.trainingSummary = summary
    this
  }

  /**
   * Return true if there exists summary of model.
   */
  @Since("2.3.0")
  def hasSummary: Boolean = trainingSummary.nonEmpty

  /**
   * Gets summary of model on training set. An exception is
   * thrown if `trainingSummary == None`.
   */
  @Since("2.3.0")
  def summary: MiniBatchKMeansSummary = trainingSummary.getOrElse {
    throw new SparkException(
      s"No training summary available for the ${this.getClass.getSimpleName}")
  }
}

@Since("2.3.0")
object MiniBatchKMeansModel extends MLReadable[MiniBatchKMeansModel] {

  @Since("2.3.0")
  override def read: MLReader[MiniBatchKMeansModel] = new MiniBatchKMeansModelReader

  @Since("2.3.0")
  override def load(path: String): MiniBatchKMeansModel = super.load(path)

  /** Helper class for storing model data */
  private case class Data(clusterIdx: Int, clusterCenter: Vector)

  /** [[MLWriter]] instance for [[MiniBatchKMeansModel]] */
  private[MiniBatchKMeansModel] class MiniBatchKMeansModelWriter(instance: MiniBatchKMeansModel)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: cluster centers
      val data: Array[Data] = instance.clusterCenters.zipWithIndex.map { case (center, idx) =>
        Data(idx, center)
      }
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(data).repartition(1).write.parquet(dataPath)
    }
  }

  private class MiniBatchKMeansModelReader extends MLReader[MiniBatchKMeansModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[MiniBatchKMeansModel].getName

    override def load(path: String): MiniBatchKMeansModel = {
      // Import implicits for Dataset Encoder
      val sparkSession = super.sparkSession
      import sparkSession.implicits._

      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString

      val clusterCenters = {
        val data: Dataset[Data] = sparkSession.read.parquet(dataPath).as[Data]
        data.collect().sortBy(_.clusterIdx).map(_.clusterCenter)
      }

      val model = new MiniBatchKMeansModel(metadata.uid, clusterCenters)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

/**
 * MiniBatch K-means clustering proposed by Sculley.
 *
 * @see <a href="https://www.eecs.tufts.edu/~dsculley/papers/fastkmeans.pdf">Sculley, Web-Scale
 *      K-Means Clustering.</a>
 */
@Since("2.3.0")
class MiniBatchKMeans @Since("2.3.0") (
    @Since("2.3.0") override val uid: String)
  extends Estimator[MiniBatchKMeansModel] with MiniBatchKMeansParams
    with DefaultParamsWritable {

  setDefault(
    k -> 2,
    maxIter -> 20,
    initMode -> MLlibKMeans.K_MEANS_PARALLEL,
    initSteps -> 2,
    tol -> 1e-4,
    fraction -> 1.0)

  @Since("2.3.0")
  override def copy(extra: ParamMap): MiniBatchKMeans = defaultCopy(extra)

  @Since("2.3.0")
  def this() = this(Identifiable.randomUID("minibatch-kmeans"))

  /** @group setParam */
  @Since("2.3.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.3.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("2.3.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group expertSetParam */
  @Since("2.3.0")
  def setInitMode(value: String): this.type = set(initMode, value)

  /** @group expertSetParam */
  @Since("2.3.0")
  def setInitSteps(value: Int): this.type = set(initSteps, value)

  /** @group setParam */
  @Since("2.3.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("2.3.0")
  def setTol(value: Double): this.type = set(tol, value)

  /** @group setParam */
  @Since("2.3.0")
  def setFraction(value: Double): this.type = set(fraction, value)

  /** @group setParam */
  @Since("2.3.0")
  def setSeed(value: Long): this.type = set(seed, value)

  @Since("2.3.0")
  override def fit(dataset: Dataset[_]): MiniBatchKMeansModel = {
    transformSchema(dataset.schema, logging = true)

    val data = dataset.select(col($(featuresCol))).rdd.map {
       case Row(point: Vector) =>
         new VectorWithNorm(point)
    }
    data.persist(StorageLevel.MEMORY_AND_DISK)

    val instr = Instrumentation.create(this, dataset)
    instr.logParams(featuresCol, predictionCol, k, initMode, initSteps, maxIter, seed, tol,
      fraction)

    val initStartTime = System.nanoTime()
    val centers: Array[VectorWithNorm] = initCenters(data)
    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(f"Initialization with ${$(initMode)} took $initTimeInSeconds%.3f seconds.")

    val sc = dataset.sparkSession.sparkContext

    val iterationStartTime = System.nanoTime()

    val numFeatures = centers.head.vector.size
    instr.logNumFeatures(numFeatures)

    val numCenters = centers.length
    val counts = Array.ofDim[Long](numCenters)

    var converged = false
    var batchSize = 0L
    var iteration = 0

    // Execute iterations of Sculley's algorithm until converged
    while (iteration < $(maxIter) && !converged) {
      val singleIterationStartTime = (System.nanoTime() - initStartTime) / 1e9

      val costAccum = sc.doubleAccumulator
      val bcCenters = sc.broadcast(centers)

      val sampled = if ($(fraction) == 1.0) {
        data
      } else {
        data.sample(false, $(fraction), iteration + 42)
      }

      // Find the sum and count of points mapping to each center
      val totalContribs = sampled.mapPartitions { points =>
        val thisCenters = bcCenters.value
        val dims = thisCenters.head.vector.size
        val sums = Array.fill(thisCenters.length)(Vectors.zeros(dims))
        val counts = Array.fill(thisCenters.length)(0L)

        points.foreach { point =>
          val (bestCenter, cost) = MLlibKMeans.findClosest(thisCenters, point)
          costAccum.add(cost)
          val sum = sums(bestCenter)
          axpy(1.0, point.vector, sum)
          counts(bestCenter) += 1
        }

        counts.indices.filter(counts(_) > 0).map(j => (j, (sums(j), counts(j)))).iterator
      }.reduceByKey { case ((sum1, count1), (sum2, count2)) =>
        axpy(1.0, sum2, sum1)
        (sum1, count1 + count2)
      }.collectAsMap()

      // Update the cluster centers, costs and counts
      converged = true
      batchSize = 0
      totalContribs.foreach { case (j, (sum, count)) =>
        batchSize += count
        val newCount = counts(j) + count
        scal(1.0 / newCount, sum)
        axpy(counts(j).toDouble / newCount, centers(j).vector, sum)
        val newCenter = new VectorWithNorm(sum)
        if (converged &&
          MLlibKMeans.fastSquaredDistance(newCenter, centers(j)) > $(tol) * $(tol)) {
          converged = false
        }
        centers(j) = newCenter
        counts(j) = newCount
      }
      bcCenters.destroy(blocking = false)

      val cost = costAccum.value

      val singleIterationTimeInSeconds = (System.nanoTime() - singleIterationStartTime) / 1e9
      logInfo(f"Iteration $iteration took $singleIterationTimeInSeconds%.3f seconds, " +
        f"cost on $batchSize instances: $cost")
      iteration += 1
    }
    data.unpersist(blocking = false)

    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
    logInfo(f"Iterations took $iterationTimeInSeconds%.3f seconds.")

    if (iteration == $(maxIter)) {
      logInfo(s"MiniBatchKMeans reached the max number of iterations: ${$(maxIter)}.")
    } else {
      logInfo(s"MiniBatchKMeans converged in $iteration iterations.")
    }

    val model = copyValues(new MiniBatchKMeansModel(uid, centers.map(_.vector.asML))
      .setParent(this))
    val summary = new MiniBatchKMeansSummary(
      model.transform(dataset), $(predictionCol), $(featuresCol), $(k))
    model.setSummary(Some(summary))
    instr.logSuccess(model)
    model
  }

  private def initCenters(data: RDD[VectorWithNorm]): Array[VectorWithNorm] = {
    val algo = new MLlibKMeans()
      .setK($(k))
      .setInitializationMode($(initMode))
      .setInitializationSteps($(initSteps))
      .setSeed($(seed))

    $(initMode) match {
      case MLlibKMeans.RANDOM =>
        algo.initRandom(data)
      case MLlibKMeans.K_MEANS_PARALLEL =>
        algo.initKMeansParallel(data)
    }
  }

  @Since("2.3.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

@Since("2.3.0")
object MiniBatchKMeans extends DefaultParamsReadable[MiniBatchKMeans] {

  @Since("2.3.0")
  override def load(path: String): MiniBatchKMeans = super.load(path)
}

/**
 * :: Experimental ::
 * Summary of MiniBatchKMeans.
 *
 * @param predictions  `DataFrame` produced by `MiniBatchKMeansModel.transform()`.
 * @param predictionCol  Name for column of predicted clusters in `predictions`.
 * @param featuresCol  Name for column of features in `predictions`.
 * @param k  Number of clusters.
 */
@Since("2.3.0")
@Experimental
class MiniBatchKMeansSummary private[clustering] (
    predictions: DataFrame,
    predictionCol: String,
    featuresCol: String,
    k: Int) extends ClusteringSummary(predictions, predictionCol, featuresCol, k)
