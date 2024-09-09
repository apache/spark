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

import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.{LogKeys, MDC}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.impl.Utils.{unpackUpperTriangular, EPSILON}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.stat.distribution.MultivariateGaussian
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DatasetUtils._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.linalg.{Matrices => OldMatrices, Matrix => OldMatrix,
  Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ArrayImplicits._

/**
 * Common params for GaussianMixture and GaussianMixtureModel
 */
private[clustering] trait GaussianMixtureParams extends Params with HasMaxIter with HasFeaturesCol
  with HasSeed with HasPredictionCol with HasWeightCol with HasProbabilityCol with HasTol
  with HasAggregationDepth {

  /**
   * Number of independent Gaussians in the mixture model. Must be greater than 1. Default: 2.
   *
   * @group param
   */
  @Since("2.0.0")
  final val k = new IntParam(this, "k", "Number of independent Gaussians in the mixture model. " +
    "Must be > 1.", ParamValidators.gt(1))

  /** @group getParam */
  @Since("2.0.0")
  def getK: Int = $(k)

  setDefault(k -> 2, maxIter -> 100, tol -> 0.01)

  /**
   * Validates and transforms the input schema.
   *
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.validateVectorCompatibleColumn(schema, getFeaturesCol)
    val schemaWithPredictionCol = SchemaUtils.appendColumn(schema, $(predictionCol), IntegerType)
    SchemaUtils.appendColumn(schemaWithPredictionCol, $(probabilityCol), new VectorUDT)
  }
}

/**
 * Multivariate Gaussian Mixture Model (GMM) consisting of k Gaussians, where points
 * are drawn from each Gaussian i with probability weights(i).
 *
 * @param weights Weight for each Gaussian distribution in the mixture.
 *                This is a multinomial probability distribution over the k Gaussians,
 *                where weights(i) is the weight for Gaussian i, and weights sum to 1.
 * @param gaussians Array of `MultivariateGaussian` where gaussians(i) represents
 *                  the Multivariate Gaussian (Normal) Distribution for Gaussian i
 */
@Since("2.0.0")
class GaussianMixtureModel private[ml] (
    @Since("2.0.0") override val uid: String,
    @Since("2.0.0") val weights: Array[Double],
    @Since("2.0.0") val gaussians: Array[MultivariateGaussian])
  extends Model[GaussianMixtureModel] with GaussianMixtureParams with MLWritable
  with HasTrainingSummary[GaussianMixtureSummary] {

  @Since("3.0.0")
  lazy val numFeatures: Int = gaussians.head.mean.size

  /** @group setParam */
  @Since("2.1.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.1.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("2.1.0")
  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

  @Since("2.0.0")
  override def copy(extra: ParamMap): GaussianMixtureModel = {
    val copied = copyValues(new GaussianMixtureModel(uid, weights, gaussians), extra)
    copied.setSummary(trainingSummary).setParent(this.parent)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val vectorCol = columnToVector(dataset, $(featuresCol))
    var outputData = dataset
    var numColsOutput = 0

    if ($(probabilityCol).nonEmpty) {
      val probUDF = udf((vector: Vector) => predictProbability(vector))
      outputData = outputData.withColumn($(probabilityCol), probUDF(vectorCol),
        outputSchema($(probabilityCol)).metadata)
      numColsOutput += 1
    }

    if ($(predictionCol).nonEmpty) {
      if ($(probabilityCol).nonEmpty) {
        val predUDF = udf((vector: Vector) => vector.argmax)
        outputData = outputData.withColumn($(predictionCol), predUDF(col($(probabilityCol))),
          outputSchema($(predictionCol)).metadata)
      } else {
        val predUDF = udf((vector: Vector) => predict(vector))
        outputData = outputData.withColumn($(predictionCol), predUDF(vectorCol),
          outputSchema($(predictionCol)).metadata)
      }
      numColsOutput += 1
    }

    if (numColsOutput == 0) {
      this.logWarning(log"${MDC(LogKeys.UUID, uid)}: GaussianMixtureModel.transform() does " +
        log"nothing because no output columns were set.")
    }
    outputData.toDF()
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(predictionCol).nonEmpty) {
      outputSchema = SchemaUtils.updateNumValues(outputSchema,
        $(predictionCol), weights.length)
    }
    if ($(probabilityCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(probabilityCol), weights.length)
    }
    outputSchema
  }

  @Since("3.0.0")
  def predict(features: Vector): Int = {
    val r = predictProbability(features)
    r.argmax
  }

  @Since("3.0.0")
  def predictProbability(features: Vector): Vector = {
    val probs = GaussianMixtureModel.computeProbabilities(features, gaussians, weights)
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
    }.toImmutableArraySeq
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

  @Since("3.0.0")
  override def toString: String = {
    s"GaussianMixtureModel: uid=$uid, k=${weights.length}, numFeatures=$numFeatures"
  }

  /**
   * Gets summary of model on training set. An exception is
   * thrown if `hasSummary` is false.
   */
  @Since("2.0.0")
  override def summary: GaussianMixtureSummary = super.summary

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
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      // Save model data: weights and gaussians
      val weights = instance.weights
      val gaussians = instance.gaussians
      val mus = gaussians.map(g => OldVectors.fromML(g.mean))
      val sigmas = gaussians.map(c => OldMatrices.fromML(c.cov))
      val data = Data(weights, mus, sigmas)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).write.parquet(dataPath)
    }
  }

  private class GaussianMixtureModelReader extends MLReader[GaussianMixtureModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[GaussianMixtureModel].getName

    override def load(path: String): GaussianMixtureModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)

      val dataPath = new Path(path, "data").toString
      val row = sparkSession.read.parquet(dataPath).select("weights", "mus", "sigmas").head()
      val weights = row.getSeq[Double](0).toArray
      val mus = row.getSeq[OldVector](1).toArray
      val sigmas = row.getSeq[OldMatrix](2).toArray
      require(mus.length == sigmas.length, "Length of Mu and Sigma array must match")
      require(mus.length == weights.length, "Length of weight and Gaussian array must match")

      val gaussians = mus.zip(sigmas)
        .map { case (mu, sigma) => new MultivariateGaussian(mu.asML, sigma.asML) }
      val model = new GaussianMixtureModel(metadata.uid, weights, gaussians)

      metadata.getAndSetParams(model)
      model
    }
  }

  /**
   * Compute the probability (partial assignment) for each cluster for the given data point.
   *
   * @param features  Data point
   * @param dists  Gaussians for model
   * @param weights  Weights for each Gaussian
   * @return  Probability (partial assignment) for each of the k clusters
   */
  private[clustering] def computeProbabilities(
      features: Vector,
      dists: Array[MultivariateGaussian],
      weights: Array[Double]): Array[Double] = {
    val probArray = Array.ofDim[Double](weights.length)
    var probSum = 0.0
    var i = 0
    while (i < weights.length) {
      val p = EPSILON + weights(i) * dists(i).pdf(features)
      probArray(i) = p
      probSum += p
      i += 1
    }

    i = 0
    while (i < weights.length) {
      probArray(i) /= probSum
      i += 1
    }
    probArray
  }
}

/**
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
 * @note This algorithm is limited in its number of features since it requires storing a covariance
 * matrix which has size quadratic in the number of features. Even when the number of features does
 * not exceed this limit, this algorithm may perform poorly on high-dimensional data.
 * This is due to high-dimensional data (a) making it difficult to cluster at all (based
 * on statistical/theoretical arguments) and (b) numerical issues with Gaussian distributions.
 */
@Since("2.0.0")
class GaussianMixture @Since("2.0.0") (
    @Since("2.0.0") override val uid: String)
  extends Estimator[GaussianMixtureModel] with GaussianMixtureParams with DefaultParamsWritable {

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
  @Since("3.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

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

  /** @group expertSetParam */
  @Since("3.0.0")
  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)

  /**
   * Number of samples per cluster to use when initializing Gaussians.
   */
  private val numSamples = 5

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): GaussianMixtureModel = instrumented { instr =>
    transformSchema(dataset.schema, logging = true)

    val spark = dataset.sparkSession
    import spark.implicits._

    val numFeatures = getNumFeatures(dataset, $(featuresCol))
    require(numFeatures < GaussianMixture.MAX_NUM_FEATURES, s"GaussianMixture cannot handle more " +
      s"than ${GaussianMixture.MAX_NUM_FEATURES} features because the size of the covariance" +
      s" matrix is quadratic in the number of features.")

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, featuresCol, predictionCol, probabilityCol, weightCol, k, maxIter,
      seed, tol, aggregationDepth)
    instr.logNumFeatures(numFeatures)

    val instances = dataset.select(
      checkNonNanVectors(columnToVector(dataset, $(featuresCol))),
      checkNonNegativeWeights(get(weightCol))
    ).as[(Vector, Double)].rdd
     .setName("training instances")

    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    if (handlePersistence) { instances.persist(StorageLevel.MEMORY_AND_DISK) }
    // TODO: SPARK-15785 Support users supplied initial GMM.
    val (weights, gaussians) = initRandom(instances, $(k), numFeatures)
    val (logLikelihood, iteration) = trainImpl(instances, weights, gaussians, numFeatures, instr)
    if (handlePersistence) { instances.unpersist() }

    val gaussianDists = gaussians.map { case (mean, covVec) =>
      val cov = GaussianMixture.unpackUpperTriangularMatrix(numFeatures, covVec.values)
      new MultivariateGaussian(mean, cov)
    }

    val model = copyValues(new GaussianMixtureModel(uid, weights, gaussianDists))
      .setParent(this)
    val summary = new GaussianMixtureSummary(model.transform(dataset),
      $(predictionCol), $(probabilityCol), $(featuresCol), $(k), logLikelihood, iteration)
    instr.logNamedValue("logLikelihood", logLikelihood)
    instr.logNamedValue("clusterSizes", summary.clusterSizes)
    model.setSummary(Some(summary))
  }

  private def trainImpl(
      instances: RDD[(Vector, Double)],
      weights: Array[Double],
      gaussians: Array[(DenseVector, DenseVector)],
      numFeatures: Int,
      instr: Instrumentation): (Double, Int) = {
    val sc = instances.sparkContext
    var logLikelihood = Double.MinValue
    var logLikelihoodPrev = 0.0

    var iteration = 0
    while (iteration < $(maxIter) && math.abs(logLikelihood - logLikelihoodPrev) > $(tol)) {
      val weightSumAccum = if (iteration == 0) sc.doubleAccumulator else null
      val logLikelihoodAccum = sc.doubleAccumulator
      val bcWeights = sc.broadcast(weights)
      val bcGaussians = sc.broadcast(gaussians)

      // aggregate the cluster contribution for all sample points,
      // and then compute the new distributions
      instances.mapPartitions { iter =>
        if (iter.nonEmpty) {
          val agg = new ExpectationAggregator(numFeatures, bcWeights, bcGaussians)
          while (iter.hasNext) { agg.add(iter.next()) }
          // sum of weights in this partition
          val ws = agg.weights.sum
          if (iteration == 0) weightSumAccum.add(ws)
          logLikelihoodAccum.add(agg.logLikelihood)
          Iterator.tabulate(bcWeights.value.length) { i =>
            (i, (agg.means(i), agg.covs(i), agg.weights(i), ws))
          }
        } else Iterator.empty
      }.reduceByKey(GaussianMixture.mergeWeightsMeans).mapValues { case (mean, cov, w, ws) =>
        // Create new distributions based on the partial assignments
        // (often referred to as the "M" step in literature)
        GaussianMixture.updateWeightsAndGaussians(mean, cov, w, ws)
      }.collect().foreach { case (i, (weight, gaussian)) =>
        weights(i) = weight
        gaussians(i) = gaussian
      }

      bcWeights.destroy()
      bcGaussians.destroy()

      if (iteration == 0) {
        instr.logNumExamples(weightSumAccum.count)
        instr.logSumOfWeights(weightSumAccum.value)
      }

      logLikelihoodPrev = logLikelihood         // current becomes previous
      logLikelihood = logLikelihoodAccum.value  // this is the freshly computed log-likelihood
      instr.logNamedValue(s"logLikelihood@iter$iteration", logLikelihood)
      iteration += 1
    }

    (logLikelihood, iteration)
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  /**
   * Initialize weights and corresponding gaussian distributions at random.
   *
   * We start with uniform weights, a random mean from the data, and diagonal covariance matrices
   * using component variances derived from the samples.
   *
   * @param instances The training instances.
   * @param numClusters The number of clusters.
   * @param numFeatures The number of features of training instance.
   * @return The initialized weights and corresponding gaussian distributions. Note the
   *         covariance matrix of multivariate gaussian distribution is symmetric and
   *         we only save the upper triangular part as a dense vector (column major).
   */
  private def initRandom(
      instances: RDD[(Vector, Double)],
      numClusters: Int,
      numFeatures: Int): (Array[Double], Array[(DenseVector, DenseVector)]) = {
    val (samples, sampleWeights) = instances
      .takeSample(withReplacement = true, numClusters * numSamples, $(seed))
      .unzip

    val weights = new Array[Double](numClusters)
    val weightSum = sampleWeights.sum

    val gaussians = Array.tabulate(numClusters) { i =>
      val start = i * numSamples
      val end = start + numSamples
      val sampleSlice = samples.slice(start, end)
      val weightSlice = sampleWeights.slice(start, end)
      val localWeightSum = weightSlice.sum
      weights(i) = localWeightSum / weightSum

      val mean = {
        val v = new DenseVector(new Array[Double](numFeatures))
        var j = 0
        while (j < numSamples) {
          BLAS.axpy(weightSlice(j), sampleSlice(j), v)
          j += 1
        }
        BLAS.scal(1.0 / localWeightSum, v)
        v
      }
      /*
         Construct matrix where diagonal entries are element-wise
         variance of input vectors (computes biased variance).
         Since the covariance matrix of multivariate gaussian distribution is symmetric,
         only the upper triangular part of the matrix (column major) will be saved as
         a dense vector in order to reduce the shuffled data size.
       */
      val cov = {
        val ss = new DenseVector(new Array[Double](numFeatures)).asBreeze
        var j = 0
        while (j < numSamples) {
          val v = sampleSlice(j).asBreeze - mean.asBreeze
          ss += (v * v) * weightSlice(j)
          j += 1
        }
        val diagVec = Vectors.fromBreeze(ss)
        BLAS.scal(1.0 / localWeightSum, diagVec)
        val covVec = new DenseVector(Array.ofDim[Double](numFeatures * (numFeatures + 1) / 2))
        diagVec.foreach { (i, v) => covVec.values(i + i * (i + 1) / 2) = v }
        covVec
      }
      (mean, cov)
    }
    (weights, gaussians)
  }
}

@Since("2.0.0")
object GaussianMixture extends DefaultParamsReadable[GaussianMixture] {

  /** Limit number of features such that numFeatures^2^ < Int.MaxValue */
  private[clustering] val MAX_NUM_FEATURES = math.sqrt(Int.MaxValue).toInt

  @Since("2.0.0")
  override def load(path: String): GaussianMixture = super.load(path)

  /**
   * Convert an n * (n + 1) / 2 dimension array representing the upper triangular part of a matrix
   * into an n * n array representing the full symmetric matrix (column major).
   *
   * @param n The order of the n by n matrix.
   * @param triangularValues The upper triangular part of the matrix packed in an array
   *                         (column major).
   * @return A dense matrix which represents the symmetric matrix in column major.
   */
  private[clustering] def unpackUpperTriangularMatrix(
      n: Int,
      triangularValues: Array[Double]): DenseMatrix = {
    val symmetricValues = unpackUpperTriangular(n, triangularValues)
    new DenseMatrix(n, n, symmetricValues)
  }

  private def mergeWeightsMeans(
      a: (DenseVector, DenseVector, Double, Double),
      b: (DenseVector, DenseVector, Double, Double)): (DenseVector, DenseVector, Double, Double) =
  {
    // update the weights, means and covariances for i-th distributions
    BLAS.axpy(1.0, b._1, a._1)
    BLAS.axpy(1.0, b._2, a._2)
    (a._1, a._2, a._3 + b._3, a._4 + b._4)
  }

  /**
   * Update the weight, mean and covariance of gaussian distribution.
   *
   * @param mean The mean of the gaussian distribution.
   * @param cov The covariance matrix of the gaussian distribution. Note we only
   *            save the upper triangular part as a dense vector (column major).
   * @param weight The weight of the gaussian distribution.
   * @param sumWeights The sum of weights of all clusters.
   * @return The updated weight, mean and covariance.
   */
  private[clustering] def updateWeightsAndGaussians(
      mean: DenseVector,
      cov: DenseVector,
      weight: Double,
      sumWeights: Double): (Double, (DenseVector, DenseVector)) = {
    BLAS.scal(1.0 / weight, mean)
    BLAS.spr(-weight, mean, cov)
    BLAS.scal(1.0 / weight, cov)
    val newWeight = weight / sumWeights
    val newGaussian = (mean, cov)
    (newWeight, newGaussian)
  }
}

/**
 * ExpectationAggregator computes the partial expectation results.
 *
 * @param numFeatures The number of features.
 * @param bcWeights The broadcast weights for each Gaussian distribution in the mixture.
 * @param bcGaussians The broadcast array of Multivariate Gaussian (Normal) Distribution
 *                    in the mixture. Note only upper triangular part of the covariance
 *                    matrix of each distribution is stored as dense vector (column major)
 *                    in order to reduce shuffled data size.
 */
private class ExpectationAggregator(
    numFeatures: Int,
    bcWeights: Broadcast[Array[Double]],
    bcGaussians: Broadcast[Array[(DenseVector, DenseVector)]]) extends Serializable {

  private val k = bcWeights.value.length
  private var totalCnt = 0L
  private var newLogLikelihood = 0.0
  private val covSize = numFeatures * (numFeatures + 1) / 2
  private lazy val newWeights = Array.ofDim[Double](k)
  @transient private lazy val newMeans = Array.fill(k)(Vectors.zeros(numFeatures).toDense)
  @transient private lazy val newCovs = Array.fill(k)(Vectors.zeros(covSize).toDense)

  @transient private lazy val gaussians = {
    bcGaussians.value.map { case (mean, covVec) =>
      val cov = GaussianMixture.unpackUpperTriangularMatrix(numFeatures, covVec.values)
      new MultivariateGaussian(mean, cov)
    }
  }

  def count: Long = totalCnt

  def logLikelihood: Double = newLogLikelihood

  def weights: Array[Double] = newWeights

  def means: Array[DenseVector] = newMeans

  def covs: Array[DenseVector] = newCovs

  /**
   * Add a new training instance to this ExpectationAggregator, update the weights,
   * means and covariances for each distributions, and update the log likelihood.
   *
   * @param instance The instance of data point to be added.
   * @return This ExpectationAggregator object.
   */
  def add(instance: (Vector, Double)): this.type = {
    val (vector: Vector, weight: Double) = instance
    val localWeights = bcWeights.value
    val localGaussians = gaussians

    val prob = new Array[Double](k)
    var probSum = 0.0
    var i = 0
    while (i < k) {
      val p = EPSILON + localWeights(i) * localGaussians(i).pdf(vector)
      prob(i) = p
      probSum += p
      i += 1
    }

    newLogLikelihood += math.log(probSum) * weight
    val localNewWeights = newWeights
    val localNewMeans = newMeans
    val localNewCovs = newCovs
    i = 0
    while (i < k) {
      val w = prob(i) / probSum * weight
      localNewWeights(i) += w
      BLAS.axpy(w, vector, localNewMeans(i))
      BLAS.spr(w, vector, localNewCovs(i))
      i += 1
    }

    totalCnt += 1
    this
  }
}


/**
 * Summary of GaussianMixture.
 *
 * @param predictions  `DataFrame` produced by `GaussianMixtureModel.transform()`.
 * @param predictionCol  Name for column of predicted clusters in `predictions`.
 * @param probabilityCol  Name for column of predicted probability of each cluster
 *                        in `predictions`.
 * @param featuresCol  Name for column of features in `predictions`.
 * @param k  Number of clusters.
 * @param logLikelihood  Total log-likelihood for this model on the given data.
 * @param numIter  Number of iterations.
 */
@Since("2.0.0")
class GaussianMixtureSummary private[clustering] (
    predictions: DataFrame,
    predictionCol: String,
    @Since("2.0.0") val probabilityCol: String,
    featuresCol: String,
    k: Int,
    @Since("2.2.0") val logLikelihood: Double,
    numIter: Int)
  extends ClusteringSummary(predictions, predictionCol, featuresCol, k, numIter) {

  /**
   * Probability of each cluster.
   */
  @Since("2.0.0")
  @transient lazy val probability: DataFrame = predictions.select(probabilityCol)
}
