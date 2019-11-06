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

package org.apache.spark.ml.classification

import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.Since
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.VersionUtils

/**
 * Params for Naive Bayes Classifiers.
 */
private[classification] trait NaiveBayesParams extends PredictorParams with HasWeightCol {

  /**
   * The smoothing parameter.
   * (default = 1.0).
   * @group param
   */
  final val smoothing: DoubleParam = new DoubleParam(this, "smoothing", "The smoothing parameter.",
    ParamValidators.gtEq(0))

  /** @group getParam */
  final def getSmoothing: Double = $(smoothing)

  /**
   * The model type which is a string (case-sensitive).
   * Supported options: "multinomial", "bernoulli", "gaussian".
   * (default = multinomial)
   * @group param
   */
  final val modelType: Param[String] = new Param[String](this, "modelType", "The model type " +
    "which is a string (case-sensitive). Supported options: multinomial (default), bernoulli" +
    " and gaussian.",
    ParamValidators.inArray[String](NaiveBayes.supportedModelTypes.toArray))

  /** @group getParam */
  final def getModelType: String = $(modelType)
}

// scalastyle:off line.size.limit
/**
 * Naive Bayes Classifiers.
 * It supports Multinomial NB
 * (see <a href="http://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html">
 * here</a>)
 * which can handle finitely supported discrete data. For example, by converting documents into
 * TF-IDF vectors, it can be used for document classification. By making every vector a
 * binary (0/1) data, it can also be used as Bernoulli NB
 * (see <a href="http://nlp.stanford.edu/IR-book/html/htmledition/the-bernoulli-model-1.html">
 * here</a>).
 * The input feature values for Multinomial NB and Bernoulli NB must be nonnegative.
 * Since 3.0.0, it also supports Gaussian NB
 * (see <a href="https://en.wikipedia.org/wiki/Naive_Bayes_classifier#Gaussian_naive_Bayes">
 * here</a>)
 * which can handle continuous data.
 */
// scalastyle:on line.size.limit
@Since("1.5.0")
class NaiveBayes @Since("1.5.0") (
    @Since("1.5.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, NaiveBayes, NaiveBayesModel]
  with NaiveBayesParams with DefaultParamsWritable {

  import NaiveBayes._

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("nb"))

  /**
   * Set the smoothing parameter.
   * Default is 1.0.
   * @group setParam
   */
  @Since("1.5.0")
  def setSmoothing(value: Double): this.type = set(smoothing, value)
  setDefault(smoothing -> 1.0)

  /**
   * Set the model type using a string (case-sensitive).
   * Supported options: "multinomial" and "bernoulli".
   * Default is "multinomial"
   * @group setParam
   */
  @Since("1.5.0")
  def setModelType(value: String): this.type = set(modelType, value)
  setDefault(modelType -> NaiveBayes.Multinomial)

  /**
   * Sets the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  override protected def train(dataset: Dataset[_]): NaiveBayesModel = {
    trainWithLabelCheck(dataset, positiveLabel = true)
  }

  /**
   * ml assumes input labels in range [0, numClasses). But this implementation
   * is also called by mllib NaiveBayes which allows other kinds of input labels
   * such as {-1, +1}. `positiveLabel` is used to determine whether the label
   * should be checked and it should be removed when we remove mllib NaiveBayes.
   */
  private[spark] def trainWithLabelCheck(
      dataset: Dataset[_],
      positiveLabel: Boolean): NaiveBayesModel = instrumented { instr =>
    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, labelCol, featuresCol, weightCol, predictionCol, rawPredictionCol,
      probabilityCol, modelType, smoothing, thresholds)

    if (positiveLabel && isDefined(thresholds)) {
      val numClasses = getNumClasses(dataset)
      instr.logNumClasses(numClasses)
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".train() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    $(modelType) match {
      case Bernoulli | Multinomial =>
        trainBernoulliMultinomialImpl(dataset, instr)
      case Gaussian =>
        trainGaussianImpl(dataset, instr)
      case _ =>
        // This should never happen.
        throw new IllegalArgumentException(s"Invalid modelType: ${$(modelType)}.")
    }
  }

  private def trainBernoulliMultinomialImpl(
      dataset: Dataset[_],
      instr: Instrumentation): NaiveBayesModel = {
    val validateInstance = $(modelType) match {
      case Multinomial =>
        (instance: Instance) => requireNonnegativeValues(instance.features)
      case Bernoulli =>
        (instance: Instance) => requireZeroOneBernoulliValues(instance.features)
    }

    val numFeatures = dataset.select(col($(featuresCol))).head().getAs[Vector](0).size
    instr.logNumFeatures(numFeatures)

    // Aggregates term frequencies per label.
    // TODO: Calling aggregateByKey and collect creates two stages, we can implement something
    // TODO: similar to reduceByKeyLocally to save one stage.
    val aggregated = extractInstances(dataset, validateInstance).map { instance =>
      (instance.label, (instance.weight, instance.features))
    }.aggregateByKey[(Double, DenseVector, Long)]((0.0, Vectors.zeros(numFeatures).toDense, 0L))(
      seqOp = {
        case ((weightSum, featureSum, count), (weight, features)) =>
          BLAS.axpy(weight, features, featureSum)
          (weightSum + weight, featureSum, count + 1)
      },
      combOp = {
        case ((weightSum1, featureSum1, count1), (weightSum2, featureSum2, count2)) =>
          BLAS.axpy(1.0, featureSum2, featureSum1)
          (weightSum1 + weightSum2, featureSum1, count1 + count2)
      }).collect().sortBy(_._1)

    val numSamples = aggregated.map(_._2._3).sum
    instr.logNumExamples(numSamples)
    val numLabels = aggregated.length
    instr.logNumClasses(numLabels)
    val numDocuments = aggregated.map(_._2._1).sum

    val labelArray = new Array[Double](numLabels)
    val piArray = new Array[Double](numLabels)
    val thetaArray = new Array[Double](numLabels * numFeatures)

    val lambda = $(smoothing)
    val piLogDenom = math.log(numDocuments + numLabels * lambda)
    var i = 0
    aggregated.foreach { case (label, (n, sumTermFreqs, _)) =>
      labelArray(i) = label
      piArray(i) = math.log(n + lambda) - piLogDenom
      val thetaLogDenom = $(modelType) match {
        case Multinomial => math.log(sumTermFreqs.values.sum + numFeatures * lambda)
        case Bernoulli => math.log(n + 2.0 * lambda)
      }
      var j = 0
      while (j < numFeatures) {
        thetaArray(i * numFeatures + j) = math.log(sumTermFreqs(j) + lambda) - thetaLogDenom
        j += 1
      }
      i += 1
    }

    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(numLabels, numFeatures, thetaArray, true)
    new NaiveBayesModel(uid, pi, theta.compressed, null).setOldLabels(labelArray)
  }

  private def trainGaussianImpl(
      dataset: Dataset[_],
      instr: Instrumentation): NaiveBayesModel = {
    val spark = dataset.sparkSession
    import spark.implicits._

    val w = if (isDefined(weightCol) && $(weightCol).nonEmpty) {
      col($(weightCol)).cast(DoubleType)
    } else {
      lit(1.0)
    }

    // Aggregates mean vector and square-sum vector per label.
    // TODO: Summarizer directly returns square-sum vector.
    val aggregated = dataset.groupBy(col($(labelCol)))
      .agg(sum(w).as("weightSum"), Summarizer.metrics("mean", "normL2")
        .summary(col($(featuresCol)), w).as("summary"))
      .select($(labelCol), "weightSum", "summary.mean", "summary.normL2")
      .as[(Double, Double, Vector, Vector)]
      .map { case (label, weightSum, mean, normL2) =>
        (label, weightSum, mean, Vectors.dense(normL2.toArray.map(v => v * v)))
      }.collect().sortBy(_._1)

    val numFeatures = aggregated.head._3.size
    instr.logNumFeatures(numFeatures)

    val numLabels = aggregated.length
    instr.logNumClasses(numLabels)

    val numInstances = aggregated.map(_._2).sum

    // If the ratio of data variance between dimensions is too small, it
    // will cause numerical errors. To address this, we artificially
    // boost the variance by epsilon, a small fraction of the standard
    // deviation of the largest dimension.
    // Refer to scikit-learn's implementation
    // [https://github.com/scikit-learn/scikit-learn/blob/0.21.X/sklearn/naive_bayes.py#L348]
    // and discussion [https://github.com/scikit-learn/scikit-learn/pull/5349] for detail.
    val epsilon = Iterator.range(0, numFeatures).map { j =>
      val globalSum = aggregated.map(t => t._3(j) * t._2).sum
      val globalSqrSum = aggregated.map(t => t._4(j)).sum
      globalSqrSum / numInstances -
        globalSum * globalSum / numInstances / numInstances
    }.max * 1e-9

    val piArray = new Array[Double](numLabels)

    // thetaArray in Gaussian NB store the means of features per label
    val thetaArray = new Array[Double](numLabels * numFeatures)

    // thetaArray in Gaussian NB store the variances of features per label
    val sigmaArray = new Array[Double](numLabels * numFeatures)

    var i = 0
    aggregated.foreach { case (label, n, mean, squareSum) =>
      piArray(i) = math.log(n / numInstances)
      var j = 0
      while (j < numFeatures) {
        val m = mean(j)
        thetaArray(i * numFeatures + j) = m
        sigmaArray(i * numFeatures + j) = epsilon + squareSum(j) / n - m * m
        j += 1
      }
      i += 1
    }

    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(numLabels, numFeatures, thetaArray, true)
    val sigma = new DenseMatrix(numLabels, numFeatures, sigmaArray, true)
    new NaiveBayesModel(uid, pi, theta.compressed, sigma.compressed)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): NaiveBayes = defaultCopy(extra)
}

@Since("1.6.0")
object NaiveBayes extends DefaultParamsReadable[NaiveBayes] {
  /** String name for multinomial model type. */
  private[classification] val Multinomial: String = "multinomial"

  /** String name for Bernoulli model type. */
  private[classification] val Bernoulli: String = "bernoulli"

  /** String name for Gaussian model type. */
  private[classification] val Gaussian: String = "gaussian"

  /* Set of modelTypes that NaiveBayes supports */
  private[classification] val supportedModelTypes = Set(Multinomial, Bernoulli, Gaussian)

  private[NaiveBayes] def requireNonnegativeValues(v: Vector): Unit = {
    val values = v match {
      case sv: SparseVector => sv.values
      case dv: DenseVector => dv.values
    }

    require(values.forall(_ >= 0.0),
      s"Naive Bayes requires nonnegative feature values but found $v.")
  }

  private[NaiveBayes] def requireZeroOneBernoulliValues(v: Vector): Unit = {
    val values = v match {
      case sv: SparseVector => sv.values
      case dv: DenseVector => dv.values
    }

    require(values.forall(v => v == 0.0 || v == 1.0),
      s"Bernoulli naive Bayes requires 0 or 1 feature values but found $v.")
  }

  @Since("1.6.0")
  override def load(path: String): NaiveBayes = super.load(path)
}

/**
 * Model produced by [[NaiveBayes]]
 *
 * @param pi    log of class priors, whose dimension is C (number of classes)
 * @param theta log of class conditional probabilities, whose dimension is C (number of classes)
 *              by D (number of features)
 * @param sigma variance of each feature, whose dimension is C (number of classes)
 *              by D (number of features). This matrix is only available when modelType
 *              is set Gaussian.
 */
@Since("1.5.0")
class NaiveBayesModel private[ml] (
    @Since("1.5.0") override val uid: String,
    @Since("2.0.0") val pi: Vector,
    @Since("2.0.0") val theta: Matrix,
    @Since("3.0.0") val sigma: Matrix)
  extends ProbabilisticClassificationModel[Vector, NaiveBayesModel]
  with NaiveBayesParams with MLWritable {

  import NaiveBayes.{Bernoulli, Multinomial, Gaussian}

  /**
   * mllib NaiveBayes is a wrapper of ml implementation currently.
   * Input labels of mllib could be {-1, +1} and mllib NaiveBayesModel exposes labels,
   * both of which are different from ml, so we should store the labels sequentially
   * to be called by mllib. This should be removed when we remove mllib NaiveBayes.
   */
  private[spark] var oldLabels: Array[Double] = null

  private[spark] def setOldLabels(labels: Array[Double]): this.type = {
    this.oldLabels = labels
    this
  }

  /**
   * Bernoulli scoring requires log(condprob) if 1, log(1-condprob) if 0.
   * This precomputes log(1.0 - exp(theta)) and its sum which are used for the linear algebra
   * application of this condition (in predict function).
   */
  @transient private lazy val (thetaMinusNegTheta, negThetaSum) = $(modelType) match {
    case Bernoulli =>
      val negTheta = theta.map(value => math.log1p(-math.exp(value)))
      val ones = new DenseVector(Array.fill(theta.numCols) {1.0})
      val thetaMinusNegTheta = theta.map { value =>
        value - math.log1p(-math.exp(value))
      }
      (Option(thetaMinusNegTheta), Option(negTheta.multiply(ones)))
    case Multinomial => (None, None)
    case Gaussian => (None, None)
    case _ =>
      // This should never happen.
      throw new IllegalArgumentException(s"Invalid modelType: ${$(modelType)}.")
  }

  /**
   * Gaussian scoring requires sum of log(Variance).
   * This precomputes sum of log(Variance) which are used for the linear algebra
   * application of this condition (in predict function).
   */
  @transient private lazy val logVarSum = $(modelType) match {
    case Gaussian =>
      val logVarSum = (0 until numClasses).map { i =>
        (0 until numFeatures).map { j =>
          math.log(sigma(i, j))
        }.sum
      }.toArray
      Some(Vectors.dense(logVarSum))
    case Multinomial => None
    case Bernoulli => None
    case _ =>
      // This should never happen.
      throw new IllegalArgumentException(s"Invalid modelType: ${$(modelType)}.")
  }

  @Since("1.6.0")
  override val numFeatures: Int = theta.numCols

  @Since("1.5.0")
  override val numClasses: Int = pi.size

  private def multinomialCalculation(features: Vector) = {
    val prob = theta.multiply(features)
    BLAS.axpy(1.0, pi, prob)
    prob
  }

  private def bernoulliCalculation(features: Vector) = {
    features.foreachActive((_, value) =>
      require(value == 0.0 || value == 1.0,
        s"Bernoulli naive Bayes requires 0 or 1 feature values but found $features.")
    )
    val prob = thetaMinusNegTheta.get.multiply(features)
    BLAS.axpy(1.0, pi, prob)
    BLAS.axpy(1.0, negThetaSum.get, prob)
    prob
  }

  private def gaussianCalculation(features: Vector) = {
    val probArray = (0 until numClasses).map { i =>
      // sum of (value_j - mean_j) ^2^ / variance_j
      val s = (0 until numFeatures).map { j =>
        val d = features(j) - theta(i, j)
        d * d / sigma(i, j)
      }.sum
      pi(i) - (s + logVarSum.get(i)) / 2
    }.toArray
    Vectors.dense(probArray)
  }

  override protected def predictRaw(features: Vector): Vector = {
    $(modelType) match {
      case Multinomial =>
        multinomialCalculation(features)
      case Bernoulli =>
        bernoulliCalculation(features)
      case Gaussian =>
        gaussianCalculation(features)
      case _ =>
        // This should never happen.
        throw new IllegalArgumentException(s"Invalid modelType: ${$(modelType)}.")
    }
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        var i = 0
        val size = dv.size
        val maxLog = dv.values.max
        while (i < size) {
          dv.values(i) = math.exp(dv.values(i) - maxLog)
          i += 1
        }
        val probSum = dv.values.sum
        i = 0
        while (i < size) {
          dv.values(i) = dv.values(i) / probSum
          i += 1
        }
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in NaiveBayesModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): NaiveBayesModel = {
    copyValues(new NaiveBayesModel(uid, pi, theta, sigma).setParent(this.parent), extra)
  }

  @Since("1.5.0")
  override def toString: String = {
    s"NaiveBayesModel (uid=$uid) with ${pi.size} classes"
  }

  @Since("1.6.0")
  override def write: MLWriter = new NaiveBayesModel.NaiveBayesModelWriter(this)
}

@Since("1.6.0")
object NaiveBayesModel extends MLReadable[NaiveBayesModel] {

  @Since("1.6.0")
  override def read: MLReader[NaiveBayesModel] = new NaiveBayesModelReader

  @Since("1.6.0")
  override def load(path: String): NaiveBayesModel = super.load(path)

  /** [[MLWriter]] instance for [[NaiveBayesModel]] */
  private[NaiveBayesModel] class NaiveBayesModelWriter(instance: NaiveBayesModel) extends MLWriter {

    private case class Data(pi: Vector, theta: Matrix)
    private case class GaussianData(pi: Vector, theta: Matrix, sigma: Matrix)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString

      if (instance.getModelType != NaiveBayes.Gaussian) {
        // Save model data: pi, theta
        require(instance.sigma == null)
        val data = Data(instance.pi, instance.theta)
        sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
      } else {
        // Save model data: pi, theta, sigma
        require(instance.sigma != null)
        val data = GaussianData(instance.pi, instance.theta, instance.sigma)
        sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
      }
    }
  }

  private class NaiveBayesModelReader extends MLReader[NaiveBayesModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[NaiveBayesModel].getName

    override def load(path: String): NaiveBayesModel = {
      implicit val format = DefaultFormats
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val (major, minor) = VersionUtils.majorMinorVersion(metadata.sparkVersion)
      val modelTypeJson = metadata.getParamValue("modelType")
      val modelType = Param.jsonDecode[String](compact(render(modelTypeJson)))

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val vecConverted = MLUtils.convertVectorColumnsToML(data, "pi")

      val model = if (major.toInt < 3 || modelType != NaiveBayes.Gaussian) {
        val Row(pi: Vector, theta: Matrix) =
          MLUtils.convertMatrixColumnsToML(vecConverted, "theta")
            .select("pi", "theta")
            .head()
        new NaiveBayesModel(metadata.uid, pi, theta, null)
      } else {
        val Row(pi: Vector, theta: Matrix, sigma: Matrix) =
          MLUtils.convertMatrixColumnsToML(vecConverted, "theta", "sigma")
            .select("pi", "theta", "sigma")
            .head()
        new NaiveBayesModel(metadata.uid, pi, theta, sigma)
      }

      metadata.getAndSetParams(model)
      model
    }
  }
}
