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

import org.apache.spark.annotation.Since
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.impl.Utils
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DatasetUtils._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql._
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
   * Supported options: "multinomial", "complement", "bernoulli", "gaussian".
   * (default = multinomial)
   * @group param
   */
  final val modelType: Param[String] = new Param[String](this, "modelType", "The model type " +
    "which is a string (case-sensitive). Supported options: multinomial (default), complement, " +
    "bernoulli and gaussian.",
    ParamValidators.inArray[String](NaiveBayes.supportedModelTypes.toArray))

  /** @group getParam */
  final def getModelType: String = $(modelType)

  setDefault(smoothing -> 1.0, modelType -> NaiveBayes.Multinomial)
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
 * Since 3.0.0, it supports Complement NB which is an adaptation of the Multinomial NB. Specifically,
 * Complement NB uses statistics from the complement of each class to compute the model's coefficients
 * The inventors of Complement NB show empirically that the parameter estimates for CNB are more stable
 * than those for Multinomial NB. Like Multinomial NB, the input feature values for Complement NB must
 * be nonnegative.
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

  /**
   * Set the model type using a string (case-sensitive).
   * Supported options: "multinomial", "complement", "bernoulli", and "gaussian".
   * Default is "multinomial"
   * @group setParam
   */
  @Since("1.5.0")
  def setModelType(value: String): this.type = set(modelType, value)

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
    trainWithLabelCheck(dataset, nonNegativeLabel = true)
  }

  /**
   * ml assumes input labels in range [0, numClasses). But this implementation
   * is also called by mllib NaiveBayes which allows other kinds of input labels
   * such as {-1, +1}. `positiveLabel` is used to determine whether the label
   * should be checked and it should be removed when we remove mllib NaiveBayes.
   */
  private[spark] def trainWithLabelCheck(
      dataset: Dataset[_],
      nonNegativeLabel: Boolean): NaiveBayesModel = instrumented { instr =>
    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, labelCol, featuresCol, weightCol, predictionCol, rawPredictionCol,
      probabilityCol, modelType, smoothing, thresholds)

    val validatedLabelCol = if (nonNegativeLabel) {
      checkClassificationLabels($(labelCol), get(thresholds).map(_.length))
    } else {
      checkRegressionLabels($(labelCol))
    }

    val validatedWeightCol = checkNonNegativeWeights(get(weightCol))

    val validatedfeaturesCol = $(modelType) match {
      case Multinomial | Complement =>
        val checkNonNegativeVector = udf { vector: Vector =>
          vector match {
            case dv: DenseVector => dv.values.forall(v => v >= 0 && !v.isInfinity)
            case sv: SparseVector => sv.values.forall(v => v >= 0 && !v.isInfinity)
          }
        }
        val vecCol = col($(featuresCol))
        when(vecCol.isNull, raise_error(lit("Vectors MUST NOT be Null")))
          .when(!checkNonNegativeVector(vecCol),
            raise_error(concat(
              lit("Vector values MUST NOT be Negative, NaN or Infinity, but got "),
              vecCol.cast(StringType))))
          .otherwise(vecCol)

      case Bernoulli =>
        val checkBinaryVector = udf { vector: Vector =>
          vector match {
            case dv: DenseVector => dv.values.forall(v => v == 0 || v == 1)
            case sv: SparseVector => sv.values.forall(v => v == 0 || v == 1)
          }
        }
        val vecCol = col($(featuresCol))
        when(vecCol.isNull, raise_error(lit("Vectors MUST NOT be Null")))
          .when(!checkBinaryVector(vecCol),
            raise_error(concat(
              lit("Vector values MUST be in {0, 1}, but got "),
              vecCol.cast(StringType))))
          .otherwise(vecCol)

      case _ => checkNonNanVectors($(featuresCol))
    }

    val validated = dataset.select(
      validatedLabelCol.as("_validated_label_"),
      validatedWeightCol.as("_validated_weight_"),
      validatedfeaturesCol.as("_validated_features_")
    )

    $(modelType) match {
      case Bernoulli | Multinomial | Complement =>
        trainDiscreteImpl(validated, instr)
      case Gaussian =>
        trainGaussianImpl(validated, instr)
      case _ =>
        // This should never happen.
        throw new IllegalArgumentException(s"Invalid modelType: ${$(modelType)}.")
    }
  }

  private def trainDiscreteImpl(
      dataset: Dataset[_],
      instr: Instrumentation): NaiveBayesModel = {
    val spark = dataset.sparkSession
    import spark.implicits._
    val Array(label, weight, featuers) = dataset.schema.fieldNames

    // Aggregates term frequencies per label.
    val aggregated = dataset.groupBy(col(label))
      .agg(sum(col(weight)).as("weightSum"),
        Summarizer.metrics("sum", "count").summary(col(featuers), col(weight)).as("summary"))
      .select(label, "weightSum", "summary.sum", "summary.count")
      .as[(Double, Double, Vector, Long)]
      .collect().sortBy(_._1)

    val numFeatures = aggregated.head._3.size
    instr.logNumFeatures(numFeatures)
    val numSamples = aggregated.map(_._4).sum
    instr.logNumExamples(numSamples)
    val numLabels = aggregated.length
    instr.logNumClasses(numLabels)
    val numDocuments = aggregated.map(_._2).sum
    instr.logSumOfWeights(numDocuments)

    val labelArray = new Array[Double](numLabels)
    val piArray = new Array[Double](numLabels)
    val thetaArray = new Array[Double](numLabels * numFeatures)

    val aggIter = $(modelType) match {
      case Multinomial | Bernoulli => aggregated.iterator
      case Complement =>
        val featureSum = Vectors.zeros(numFeatures)
        aggregated.foreach { case (_, _, sumTermFreqs, _) =>
          BLAS.axpy(1.0, sumTermFreqs, featureSum)
        }
        aggregated.iterator.map { case (label, n, sumTermFreqs, count) =>
          val comp = featureSum.copy
          BLAS.axpy(-1.0, sumTermFreqs, comp)
          (label, n, comp, count)
        }
    }

    val lambda = $(smoothing)
    val piLogDenom = math.log(numDocuments + numLabels * lambda)
    var i = 0
    aggIter.foreach { case (label, n, sumTermFreqs, _) =>
      labelArray(i) = label
      piArray(i) = math.log(n + lambda) - piLogDenom
      val thetaLogDenom = $(modelType) match {
        case Multinomial | Complement =>
          math.log(sumTermFreqs.toArray.sum + numFeatures * lambda)
        case Bernoulli => math.log(n + 2.0 * lambda)
      }
      var j = 0
      val offset = i * numFeatures
      while (j < numFeatures) {
        thetaArray(offset + j) = math.log(sumTermFreqs(j) + lambda) - thetaLogDenom
        j += 1
      }
      i += 1
    }

    val pi = Vectors.dense(piArray)
    $(modelType) match {
      case Multinomial | Bernoulli =>
        val theta = new DenseMatrix(numLabels, numFeatures, thetaArray, true)
        new NaiveBayesModel(uid, pi.compressed, theta.compressed, Matrices.zeros(0, 0))
          .setOldLabels(labelArray)
      case Complement =>
        // Since the CNB compute the coefficient in a complement way.
        val theta = new DenseMatrix(numLabels, numFeatures, thetaArray.map(v => -v), true)
        new NaiveBayesModel(uid, pi.compressed, theta.compressed, Matrices.zeros(0, 0))
    }
  }

  private def trainGaussianImpl(
      dataset: Dataset[_],
      instr: Instrumentation): NaiveBayesModel = {
    val spark = dataset.sparkSession
    import spark.implicits._
    val Array(label, weight, featuers) = dataset.schema.fieldNames

    // Aggregates mean vector and square-sum vector per label.
    val aggregated = dataset.groupBy(col(label))
      .agg(sum(col(weight)).as("weightSum"),
        Summarizer.metrics("mean", "normL2").summary(col(featuers), col(weight)).as("summary"))
      .select(label, "weightSum", "summary.mean", "summary.normL2")
      .as[(Double, Double, Vector, Vector)]
      .map { case (label, weightSum, mean, normL2) =>
        (label, weightSum, mean, Vectors.dense(normL2.toArray.map(v => v * v)))
      }.collect().sortBy(_._1)

    val numFeatures = aggregated.head._3.size
    instr.logNumFeatures(numFeatures)

    val numLabels = aggregated.length
    instr.logNumClasses(numLabels)

    val numInstances = aggregated.map(_._2).sum
    instr.logSumOfWeights(numInstances)

    // If the ratio of data variance between dimensions is too small, it
    // will cause numerical errors. To address this, we artificially
    // boost the variance by epsilon, a small fraction of the standard
    // deviation of the largest dimension.
    // Refer to scikit-learn's implementation
    // [https://github.com/scikit-learn/scikit-learn/blob/0.21.X/sklearn/naive_bayes.py#L348]
    // and discussion [https://github.com/scikit-learn/scikit-learn/pull/5349] for detail.
    val epsilon = Iterator.range(0, numFeatures).map { j =>
      var globalSum = 0.0
      var globalSqrSum = 0.0
      aggregated.foreach { case (_, weightSum, mean, squareSum) =>
        globalSum += mean(j) * weightSum
        globalSqrSum += squareSum(j)
      }
      globalSqrSum / numInstances -
        globalSum * globalSum / numInstances / numInstances
    }.max * 1e-9

    val piArray = new Array[Double](numLabels)

    // thetaArray in Gaussian NB store the means of features per label
    val thetaArray = new Array[Double](numLabels * numFeatures)

    // thetaArray in Gaussian NB store the variances of features per label
    val sigmaArray = new Array[Double](numLabels * numFeatures)

    var i = 0
    val logNumInstances = math.log(numInstances)
    aggregated.foreach { case (_, weightSum, mean, squareSum) =>
      piArray(i) = math.log(weightSum) - logNumInstances
      var j = 0
      val offset = i * numFeatures
      while (j < numFeatures) {
        val m = mean(j)
        thetaArray(offset + j) = m
        sigmaArray(offset + j) = epsilon + squareSum(j) / weightSum - m * m
        j += 1
      }
      i += 1
    }

    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(numLabels, numFeatures, thetaArray, true)
    val sigma = new DenseMatrix(numLabels, numFeatures, sigmaArray, true)
    new NaiveBayesModel(uid, pi.compressed, theta.compressed, sigma.compressed)
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

  /** String name for Complement model type. */
  private[classification] val Complement: String = "complement"

  /* Set of modelTypes that NaiveBayes supports */
  private[classification] val supportedModelTypes =
    Set(Multinomial, Bernoulli, Gaussian, Complement)

  private[ml] def requireNonnegativeValues(v: Vector): Unit = {
    require(v.nonZeroIterator.forall(_._2 > 0.0),
      s"Naive Bayes requires nonnegative feature values but found $v.")
  }

  private[ml] def requireZeroOneBernoulliValues(v: Vector): Unit = {
    require(v.nonZeroIterator.forall(_._2 == 1.0),
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

  import NaiveBayes._

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
  @transient private lazy val thetaMinusNegTheta = $(modelType) match {
    case Bernoulli =>
      theta.map(value => value - math.log1p(-math.exp(value)))
    case _ =>
      // This should never happen.
      throw new IllegalArgumentException(s"Invalid modelType: ${$(modelType)}. " +
        "Variables thetaMinusNegTheta should only be precomputed in Bernoulli NB.")
  }

  @transient private lazy val piMinusThetaSum = $(modelType) match {
    case Bernoulli =>
      val negTheta = theta.map(value => math.log1p(-math.exp(value)))
      val ones = new DenseVector(Array.fill(theta.numCols)(1.0))
      val piMinusThetaSum = pi.toDense.copy
      BLAS.gemv(1.0, negTheta, ones, 1.0, piMinusThetaSum)
      piMinusThetaSum
    case _ =>
      // This should never happen.
      throw new IllegalArgumentException(s"Invalid modelType: ${$(modelType)}. " +
        "Variables piMinusThetaSum should only be precomputed in Bernoulli NB.")
  }

  /**
   * Gaussian scoring requires sum of log(Variance).
   * This precomputes sum of log(Variance) which are used for the linear algebra
   * application of this condition (in predict function).
   */
  @transient private lazy val logVarSum = $(modelType) match {
    case Gaussian =>
      Array.tabulate(numClasses) { i =>
        Iterator.range(0, numFeatures).map { j =>
          math.log(sigma(i, j))
        }.sum
      }
    case _ =>
      // This should never happen.
      throw new IllegalArgumentException(s"Invalid modelType: ${$(modelType)}. " +
        "Variables logVarSum should only be precomputed in Gaussian NB.")
  }

  @Since("1.6.0")
  override val numFeatures: Int = theta.numCols

  @Since("1.5.0")
  override val numClasses: Int = pi.size

  private def multinomialCalculation(features: Vector) = {
    requireNonnegativeValues(features)
    val prob = pi.toDense.copy
    BLAS.gemv(1.0, theta, features, 1.0, prob)
    prob
  }

  private def complementCalculation(features: Vector) = {
    requireNonnegativeValues(features)
    val probArray = theta.multiply(features).toArray
    // the following lines equal to:
    // val logSumExp = math.log(probArray.map(math.exp).sum)
    // However, it easily returns Infinity/NaN values.
    // Here follows 'scipy.special.logsumexp' (which is used in Scikit-Learn's ComplementNB)
    // to compute the log of the sum of exponentials of elements in a numeric-stable way.
    val max = probArray.max
    var sumExp = 0.0
    var j = 0
    while (j < probArray.length) {
      sumExp += math.exp(probArray(j) - max)
      j += 1
    }
    val logSumExp = math.log(sumExp) + max

    j = 0
    while (j < probArray.length) {
      probArray(j) -= logSumExp
      j += 1
    }
    Vectors.dense(probArray)
  }

  private def bernoulliCalculation(features: Vector) = {
    requireZeroOneBernoulliValues(features)
    val prob = piMinusThetaSum.copy
    BLAS.gemv(1.0, thetaMinusNegTheta, features, 1.0, prob)
    prob
  }

  private def gaussianCalculation(features: Vector) = {
    val prob = Array.ofDim[Double](numClasses)
    var i = 0
    while (i < numClasses) {
      var s = 0.0
      var j = 0
      while (j < numFeatures) {
        val d = features(j) - theta(i, j)
        s += d * d / sigma(i, j)
        j += 1
      }
      prob(i) = pi(i) - (s + logVarSum(i)) / 2
      i += 1
    }
    Vectors.dense(prob)
  }

  @transient private lazy val predictRawFunc = {
    $(modelType) match {
      case Multinomial =>
        features: Vector => multinomialCalculation(features)
      case Complement =>
        features: Vector => complementCalculation(features)
      case Bernoulli =>
        features: Vector => bernoulliCalculation(features)
      case Gaussian =>
        features: Vector => gaussianCalculation(features)
    }
  }

  @Since("3.0.0")
  override def predictRaw(features: Vector): Vector = predictRawFunc(features)

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        Utils.softmax(dv.values)
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
    s"NaiveBayesModel: uid=$uid, modelType=${$(modelType)}, numClasses=$numClasses, " +
      s"numFeatures=$numFeatures"
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
    import NaiveBayes._

    private case class Data(pi: Vector, theta: Matrix, sigma: Matrix)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString

      instance.getModelType match {
        case Multinomial | Bernoulli | Complement =>
          require(instance.sigma.numRows == 0 && instance.sigma.numCols == 0)
        case Gaussian =>
          require(instance.sigma.numRows != 0 && instance.sigma.numCols != 0)
      }

      val data = Data(instance.pi, instance.theta, instance.sigma)
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class NaiveBayesModelReader extends MLReader[NaiveBayesModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[NaiveBayesModel].getName

    override def load(path: String): NaiveBayesModel = {
      implicit val format = DefaultFormats
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val (major, minor) = VersionUtils.majorMinorVersion(metadata.sparkVersion)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val vecConverted = MLUtils.convertVectorColumnsToML(data, "pi")

      val model = if (major < 3) {
        val Row(pi: Vector, theta: Matrix) =
          MLUtils.convertMatrixColumnsToML(vecConverted, "theta")
            .select("pi", "theta")
            .head()
        new NaiveBayesModel(metadata.uid, pi, theta, Matrices.zeros(0, 0))
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
