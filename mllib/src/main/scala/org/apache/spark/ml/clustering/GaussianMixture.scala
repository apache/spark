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

import breeze.linalg.{DenseVector => BDV}
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.impl.Utils.EPSILON
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.stat.distribution.MultivariateGaussian
import org.apache.spark.ml.util._
import org.apache.spark.mllib.clustering.{GaussianMixture => MLlibGM}
import org.apache.spark.mllib.linalg.{Matrices => OldMatrices, Matrix => OldMatrix,
  Vector => OldVector, Vectors => OldVectors, VectorUDT => OldVectorUDT}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StructType}


/**
 * Common params for GaussianMixture and GaussianMixtureModel
 */
private[clustering] trait GaussianMixtureParams extends Params with HasMaxIter with HasFeaturesCol
  with HasSeed with HasPredictionCol with HasProbabilityCol with HasTol {

  /**
   * Number of independent Gaussians in the mixture model. Must be > 1. Default: 2.
   * @group param
   */
  @Since("2.0.0")
  final val k = new IntParam(this, "k", "Number of independent Gaussians in the mixture model. " +
    "Must be > 1.", ParamValidators.gt(1))

  /** @group getParam */
  @Since("2.0.0")
  def getK: Int = $(k)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new OldVectorUDT)
    SchemaUtils.appendColumn(schema, $(predictionCol), IntegerType)
    SchemaUtils.appendColumn(schema, $(probabilityCol), new OldVectorUDT)
  }
}

/**
 * :: Experimental ::
 *
 * Multivariate Gaussian Mixture Model (GMM) consisting of k Gaussians, where points
 * are drawn from each Gaussian i with probability weights(i).
 *
 * @param weights Weight for each Gaussian distribution in the mixture.
 *                This is a multinomial probability distribution over the k Gaussians,
 *                where weights(i) is the weight for Gaussian i, and weights sum to 1.
 * @param gaussians Array of [[MultivariateGaussian]] where gaussians(i) represents
 *                  the Multivariate Gaussian (Normal) Distribution for Gaussian i
 */
@Since("2.0.0")
@Experimental
class GaussianMixtureModel private[ml] (
    @Since("2.0.0") override val uid: String,
    @Since("2.0.0") val weights: Array[Double],
    @Since("2.0.0") val gaussians: Array[MultivariateGaussian])
  extends Model[GaussianMixtureModel] with GaussianMixtureParams with MLWritable {

  @Since("2.0.0")
  override def copy(extra: ParamMap): GaussianMixtureModel = {
    val copied = new GaussianMixtureModel(uid, weights, gaussians)
    copyValues(copied, extra).setParent(this.parent)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val predUDF = udf((vector: Vector) => predict(vector))
    val probUDF = udf((vector: Vector) => predictProbability(vector))
    dataset.withColumn($(predictionCol), predUDF(col($(featuresCol))))
      .withColumn($(probabilityCol), probUDF(col($(featuresCol))))
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  private[clustering] def predict(features: Vector): Int = {
    val r = predictProbability(features)
    r.argmax
  }

  private[clustering] def predictProbability(features: Vector): Vector = {
    val probs: Array[Double] =
      GaussianMixtureModel.computeProbabilities(features.asBreeze.toDenseVector, gaussians, weights)
    Vectors.dense(probs)
  }

  /**
   * Retrieve Gaussian distributions as a DataFrame.
   * Each row represents a Gaussian Distribution.
   * Two columns are defined: mean and cov.
   * Schema:
   * {{{
   *  root
   *   |-- mean: vector (nullable = true)
   *   |-- cov: matrix (nullable = true)
   * }}}
   */
  @Since("2.0.0")
  def gaussiansDF: DataFrame = {
    val modelGaussians = gaussians.map { gaussian =>
      (OldVectors.fromML(gaussian.mean), OldMatrices.fromML(gaussian.cov))
    }
    SparkSession.builder().getOrCreate().createDataFrame(modelGaussians).toDF("mean", "cov")
  }

  /**
   * Returns a [[org.apache.spark.ml.util.MLWriter]] instance for this ML instance.
   *
   * For [[GaussianMixtureModel]], this does NOT currently save the training [[summary]].
   * An option to save [[summary]] may be added in the future.
   *
   */
  @Since("2.0.0")
  override def write: MLWriter = new GaussianMixtureModel.GaussianMixtureModelWriter(this)

  private var trainingSummary: Option[GaussianMixtureSummary] = None

  private[clustering] def setSummary(summary: GaussianMixtureSummary): this.type = {
    this.trainingSummary = Some(summary)
    this
  }

  /**
   * Return true if there exists summary of model.
   */
  @Since("2.0.0")
  def hasSummary: Boolean = trainingSummary.nonEmpty

  /**
   * Gets summary of model on training set. An exception is
   * thrown if `trainingSummary == None`.
   */
  @Since("2.0.0")
  def summary: GaussianMixtureSummary = trainingSummary.getOrElse {
    throw new RuntimeException(
      s"No training summary available for the ${this.getClass.getSimpleName}")
  }
}

@Since("2.0.0")
object GaussianMixtureModel extends MLReadable[GaussianMixtureModel] {

  @Since("2.0.0")
  override def read: MLReader[GaussianMixtureModel] = new GaussianMixtureModelReader

  @Since("2.0.0")
  override def load(path: String): GaussianMixtureModel = super.load(path)

  /** [[MLWriter]] instance for [[GaussianMixtureModel]] */
  private[GaussianMixtureModel] class GaussianMixtureModelWriter(
      instance: GaussianMixtureModel) extends MLWriter {

    private case class Data(weights: Array[Double], mus: Array[OldVector], sigmas: Array[OldMatrix])

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: weights and gaussians
      val weights = instance.weights
      val gaussians = instance.gaussians
      val mus = gaussians.map(g => OldVectors.fromML(g.mean))
      val sigmas = gaussians.map(c => OldMatrices.fromML(c.cov))
      val data = Data(weights, mus, sigmas)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class GaussianMixtureModelReader extends MLReader[GaussianMixtureModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[GaussianMixtureModel].getName

    override def load(path: String): GaussianMixtureModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val row = sparkSession.read.parquet(dataPath).select("weights", "mus", "sigmas").head()
      val weights = row.getSeq[Double](0).toArray
      val mus = row.getSeq[OldVector](1).toArray
      val sigmas = row.getSeq[OldMatrix](2).toArray
      require(mus.length == sigmas.length, "Length of Mu and Sigma array must match")
      require(mus.length == weights.length, "Length of weight and Gaussian array must match")

      val gaussians = mus.zip(sigmas).map {
        case (mu, sigma) =>
          new MultivariateGaussian(mu.asML, sigma.asML)
      }
      val model = new GaussianMixtureModel(metadata.uid, weights, gaussians)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  /**
   * Compute the probability (partial assignment) for each cluster for the given data point.
   * @param features  Data point
   * @param dists  Gaussians for model
   * @param weights  Weights for each Gaussian
   * @return  Probability (partial assignment) for each of the k clusters
   */
  private[clustering]
  def computeProbabilities(
      features: BDV[Double],
      dists: Array[MultivariateGaussian],
      weights: Array[Double]): Array[Double] = {
    val p = weights.zip(dists).map {
      case (weight, dist) => EPSILON + weight * dist.pdf(features)
    }
    val pSum = p.sum
    var i = 0
    while (i < weights.length) {
      p(i) /= pSum
      i += 1
    }
    p
  }
}

/**
 * :: Experimental ::
 * Gaussian Mixture clustering.
 *
 * This class performs expectation maximization for multivariate Gaussian
 * Mixture Models (GMMs).  A GMM represents a composite distribution of
 * independent Gaussian distributions with associated "mixing" weights
 * specifying each's contribution to the composite.
 *
 * Given a set of sample points, this class will maximize the log-likelihood
 * for a mixture of k Gaussians, iterating until the log-likelihood changes by
 * less than convergenceTol, or until it has reached the max number of iterations.
 * While this process is generally guaranteed to converge, it is not guaranteed
 * to find a global optimum.
 *
 * Note: For high-dimensional data (with many features), this algorithm may perform poorly.
 *       This is due to high-dimensional data (a) making it difficult to cluster at all (based
 *       on statistical/theoretical arguments) and (b) numerical issues with Gaussian distributions.
 */
@Since("2.0.0")
@Experimental
class GaussianMixture @Since("2.0.0") (
    @Since("2.0.0") override val uid: String)
  extends Estimator[GaussianMixtureModel] with GaussianMixtureParams with DefaultParamsWritable {

  setDefault(
    k -> 2,
    maxIter -> 100,
    tol -> 0.01)

  @Since("2.0.0")
  override def copy(extra: ParamMap): GaussianMixture = defaultCopy(extra)

  @Since("2.0.0")
  def this() = this(Identifiable.randomUID("GaussianMixture"))

  /** @group setParam */
  @Since("2.0.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group setParam */
  @Since("2.0.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("2.0.0")
  def setTol(value: Double): this.type = set(tol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): GaussianMixtureModel = {
    val rdd: RDD[OldVector] = dataset.select(col($(featuresCol))).rdd.map {
      case Row(point: Vector) => OldVectors.fromML(point)
    }

    val algo = new MLlibGM()
      .setK($(k))
      .setMaxIterations($(maxIter))
      .setSeed($(seed))
      .setConvergenceTol($(tol))
    val parentModel = algo.run(rdd)
    val gaussians = parentModel.gaussians.map { case g =>
      new MultivariateGaussian(g.mu.asML, g.sigma.asML)
    }
    val model = copyValues(new GaussianMixtureModel(uid, parentModel.weights, gaussians))
      .setParent(this)
    val summary = new GaussianMixtureSummary(model.transform(dataset),
      $(predictionCol), $(probabilityCol), $(featuresCol), $(k))
    model.setSummary(summary)
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

@Since("2.0.0")
object GaussianMixture extends DefaultParamsReadable[GaussianMixture] {

  @Since("2.0.0")
  override def load(path: String): GaussianMixture = super.load(path)
}

/**
 * :: Experimental ::
 * Summary of GaussianMixture.
 *
 * @param predictions  [[DataFrame]] produced by [[GaussianMixtureModel.transform()]]
 * @param predictionCol  Name for column of predicted clusters in `predictions`
 * @param probabilityCol  Name for column of predicted probability of each cluster in `predictions`
 * @param featuresCol  Name for column of features in `predictions`
 * @param k  Number of clusters
 */
@Since("2.0.0")
@Experimental
class GaussianMixtureSummary private[clustering] (
    @Since("2.0.0") @transient val predictions: DataFrame,
    @Since("2.0.0") val predictionCol: String,
    @Since("2.0.0") val probabilityCol: String,
    @Since("2.0.0") val featuresCol: String,
    @Since("2.0.0") val k: Int) extends Serializable {

  /**
   * Cluster centers of the transformed data.
   */
  @Since("2.0.0")
  @transient lazy val cluster: DataFrame = predictions.select(predictionCol)

  /**
   * Probability of each cluster.
   */
  @Since("2.0.0")
  @transient lazy val probability: DataFrame = predictions.select(probabilityCol)

  /**
   * Size of (number of data points in) each cluster.
   */
  @Since("2.0.0")
  lazy val clusterSizes: Array[Long] = {
    val sizes = Array.fill[Long](k)(0)
    cluster.groupBy(predictionCol).count().select(predictionCol, "count").collect().foreach {
      case Row(cluster: Int, count: Long) => sizes(cluster) = count
    }
    sizes
  }
}
