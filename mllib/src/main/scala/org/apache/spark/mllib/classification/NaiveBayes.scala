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

package org.apache.spark.mllib.classification

import java.lang.{Iterable => JIterable}

import scala.jdk.CollectionConverters._

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.classification.{NaiveBayes => NewNaiveBayes}
import org.apache.spark.mllib.linalg.{BLAS, DenseMatrix, DenseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Model for Naive Bayes Classifiers.
 *
 * @param labels list of labels
 * @param pi log of class priors, whose dimension is C, number of labels
 * @param theta log of class conditional probabilities, whose dimension is C-by-D,
 *              where D is number of features
 * @param modelType The type of NB model to fit  can be "multinomial" or "bernoulli"
 */
@Since("0.9.0")
class NaiveBayesModel private[spark] (
    @Since("1.0.0") val labels: Array[Double],
    @Since("0.9.0") val pi: Array[Double],
    @Since("0.9.0") val theta: Array[Array[Double]],
    @Since("1.4.0") val modelType: String)
  extends ClassificationModel with Serializable with Saveable {

  import NaiveBayes.{Bernoulli, Multinomial, supportedModelTypes}

  private val piVector = new DenseVector(pi)
  private val thetaMatrix = new DenseMatrix(labels.length, theta(0).length, theta.flatten, true)

  private[mllib] def this(labels: Array[Double], pi: Array[Double], theta: Array[Array[Double]]) =
    this(labels, pi, theta, NaiveBayes.Multinomial)

  /** A Java-friendly constructor that takes three Iterable parameters. */
  private[mllib] def this(
      labels: JIterable[Double],
      pi: JIterable[Double],
      theta: JIterable[JIterable[Double]]) =
    this(labels.asScala.toArray, pi.asScala.toArray, theta.asScala.toArray.map(_.asScala.toArray))

  require(supportedModelTypes.contains(modelType),
    s"Invalid modelType $modelType. Supported modelTypes are $supportedModelTypes.")

  // Bernoulli scoring requires log(condprob) if 1, log(1-condprob) if 0.
  // This precomputes log(1.0 - exp(theta)) and its sum which are used for the linear algebra
  // application of this condition (in predict function).
  private val (thetaMinusNegTheta, negThetaSum) = modelType match {
    case Multinomial => (None, None)
    case Bernoulli =>
      val negTheta = thetaMatrix.map(value => math.log1p(-math.exp(value)))
      val ones = new DenseVector(Array.fill(thetaMatrix.numCols)(1.0))
      val thetaMinusNegTheta = thetaMatrix.map { value =>
        value - math.log1p(-math.exp(value))
      }
      (Option(thetaMinusNegTheta), Option(negTheta.multiply(ones)))
    case _ =>
      // This should never happen.
      throw new IllegalArgumentException(s"Invalid modelType: $modelType.")
  }

  @Since("1.0.0")
  override def predict(testData: RDD[Vector]): RDD[Double] = {
    val bcModel = testData.context.broadcast(this)
    testData.mapPartitions { iter =>
      val model = bcModel.value
      iter.map(model.predict)
    }
  }

  @Since("1.0.0")
  override def predict(testData: Vector): Double = {
    modelType match {
      case Multinomial =>
        labels(multinomialCalculation(testData).argmax)
      case Bernoulli =>
        labels(bernoulliCalculation(testData).argmax)
    }
  }

  /**
   * Predict values for the given data set using the model trained.
   *
   * @param testData RDD representing data points to be predicted
   * @return an RDD[Vector] where each entry contains the predicted posterior class probabilities,
   *         in the same order as class labels
   */
  @Since("1.5.0")
  def predictProbabilities(testData: RDD[Vector]): RDD[Vector] = {
    val bcModel = testData.context.broadcast(this)
    testData.mapPartitions { iter =>
      val model = bcModel.value
      iter.map(model.predictProbabilities)
    }
  }

  /**
   * Predict posterior class probabilities for a single data point using the model trained.
   *
   * @param testData array representing a single data point
   * @return predicted posterior class probabilities from the trained model,
   *         in the same order as class labels
   */
  @Since("1.5.0")
  def predictProbabilities(testData: Vector): Vector = {
    modelType match {
      case Multinomial =>
        posteriorProbabilities(multinomialCalculation(testData))
      case Bernoulli =>
        posteriorProbabilities(bernoulliCalculation(testData))
    }
  }

  private def multinomialCalculation(testData: Vector) = {
    val prob = thetaMatrix.multiply(testData)
    BLAS.axpy(1.0, piVector, prob)
    prob
  }

  private def bernoulliCalculation(testData: Vector) = {
    testData.foreachNonZero((_, value) =>
      if (value != 1.0) {
        throw new SparkException(
          s"Bernoulli naive Bayes requires 0 or 1 feature values but found $testData.")
      }
    )
    val prob = thetaMinusNegTheta.get.multiply(testData)
    BLAS.axpy(1.0, piVector, prob)
    BLAS.axpy(1.0, negThetaSum.get, prob)
    prob
  }

  private def posteriorProbabilities(logProb: DenseVector) = {
    val logProbArray = logProb.toArray
    val maxLog = logProbArray.max
    val scaledProbs = logProbArray.map(lp => math.exp(lp - maxLog))
    val probSum = scaledProbs.sum
    new DenseVector(scaledProbs.map(_ / probSum))
  }

  @Since("1.3.0")
  override def save(sc: SparkContext, path: String): Unit = {
    val data = NaiveBayesModel.SaveLoadV2_0.Data(labels, pi, theta, modelType)
    NaiveBayesModel.SaveLoadV2_0.save(sc, path, data)
  }
}

@Since("1.3.0")
object NaiveBayesModel extends Loader[NaiveBayesModel] {

  import org.apache.spark.mllib.util.Loader._

  private[mllib] object SaveLoadV2_0 {

    def thisFormatVersion: String = "2.0"

    /** Hard-code class name string in case it changes in the future */
    def thisClassName: String = "org.apache.spark.mllib.classification.NaiveBayesModel"

    /** Model data for model import/export */
    case class Data(
        labels: Array[Double],
        pi: Array[Double],
        theta: Array[Array[Double]],
        modelType: String)

    def save(sc: SparkContext, path: String, data: Data): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      // Create JSON metadata.
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~
          ("numFeatures" -> data.theta(0).length) ~ ("numClasses" -> data.pi.length)))
      spark.createDataFrame(Seq(Tuple1(metadata))).write.text(metadataPath(path))

      // Create Parquet data.
      spark.createDataFrame(Seq(data)).write.parquet(dataPath(path))
    }

    @Since("1.3.0")
    def load(sc: SparkContext, path: String): NaiveBayesModel = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      // Load Parquet data.
      val dataRDD = spark.read.parquet(dataPath(path))
      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      checkSchema[Data](dataRDD.schema)
      val dataArray = dataRDD.select("labels", "pi", "theta", "modelType").take(1)
      assert(dataArray.length == 1, s"Unable to load NaiveBayesModel data from: ${dataPath(path)}")
      val data = dataArray(0)
      val labels = data.getAs[Seq[Double]](0).toArray
      val pi = data.getAs[Seq[Double]](1).toArray
      val theta = data.getSeq[scala.collection.Seq[Double]](2).map(_.toArray).toArray
      val modelType = data.getString(3)
      new NaiveBayesModel(labels, pi, theta, modelType)
    }

  }

  private[mllib] object SaveLoadV1_0 {

    def thisFormatVersion: String = "1.0"

    /** Hard-code class name string in case it changes in the future */
    def thisClassName: String = "org.apache.spark.mllib.classification.NaiveBayesModel"

    /** Model data for model import/export */
    case class Data(
        labels: Array[Double],
        pi: Array[Double],
        theta: Array[Array[Double]])

    def save(sc: SparkContext, path: String, data: Data): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      // Create JSON metadata.
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~
          ("numFeatures" -> data.theta(0).length) ~ ("numClasses" -> data.pi.length)))
      spark.createDataFrame(Seq(Tuple1(metadata))).write.text(metadataPath(path))

      // Create Parquet data.
      spark.createDataFrame(Seq(data)).write.parquet(dataPath(path))
    }

    def load(sc: SparkContext, path: String): NaiveBayesModel = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      // Load Parquet data.
      val dataRDD = spark.read.parquet(dataPath(path))
      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      checkSchema[Data](dataRDD.schema)
      val dataArray = dataRDD.select("labels", "pi", "theta").take(1)
      assert(dataArray.length == 1, s"Unable to load NaiveBayesModel data from: ${dataPath(path)}")
      val data = dataArray(0)
      val labels = data.getAs[Seq[Double]](0).toArray
      val pi = data.getAs[Seq[Double]](1).toArray
      val theta = data.getSeq[scala.collection.Seq[Double]](2).map(_.toArray).toArray
      new NaiveBayesModel(labels, pi, theta)
    }
  }

  override def load(sc: SparkContext, path: String): NaiveBayesModel = {
    val (loadedClassName, version, metadata) = loadMetadata(sc, path)
    val classNameV1_0 = SaveLoadV1_0.thisClassName
    val classNameV2_0 = SaveLoadV2_0.thisClassName
    val (model, numFeatures, numClasses) = (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        val (numFeatures, numClasses) = ClassificationModel.getNumFeaturesClasses(metadata)
        val model = SaveLoadV1_0.load(sc, path)
        (model, numFeatures, numClasses)
      case (className, "2.0") if className == classNameV2_0 =>
        val (numFeatures, numClasses) = ClassificationModel.getNumFeaturesClasses(metadata)
        val model = SaveLoadV2_0.load(sc, path)
        (model, numFeatures, numClasses)
      case _ => throw new Exception(
        s"NaiveBayesModel.load did not recognize model with (className, format version):" +
        s"($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
    assert(model.pi.length == numClasses,
      s"NaiveBayesModel.load expected $numClasses classes," +
        s" but class priors vector pi had ${model.pi.length} elements")
    assert(model.theta.length == numClasses,
      s"NaiveBayesModel.load expected $numClasses classes," +
        s" but class conditionals array theta had ${model.theta.length} elements")
    assert(model.theta.forall(_.length == numFeatures),
      s"NaiveBayesModel.load expected $numFeatures features," +
        s" but class conditionals array theta had elements of size:" +
        s" ${model.theta.map(_.length).mkString(",")}")
    model
  }
}

/**
 * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
 *
 * This is the Multinomial NB (see <a href="http://tinyurl.com/lsdw6p">here</a>) which can
 * handle all kinds of discrete data. For example, by converting documents into TF-IDF
 * vectors, it can be used for document classification. By making every vector a 0-1 vector,
 * it can also be used as Bernoulli NB (see <a href="http://tinyurl.com/p7c96j6">here</a>).
 * The input feature values must be nonnegative.
 */
@Since("0.9.0")
class NaiveBayes private (
    private var lambda: Double,
    private var modelType: String) extends Serializable with Logging {

  @Since("1.4.0")
  def this(lambda: Double) = this(lambda, NaiveBayes.Multinomial)

  @Since("0.9.0")
  def this() = this(1.0, NaiveBayes.Multinomial)

  /** Set the smoothing parameter. Default: 1.0. */
  @Since("0.9.0")
  def setLambda(lambda: Double): NaiveBayes = {
    require(lambda >= 0,
      s"Smoothing parameter must be nonnegative but got $lambda")
    this.lambda = lambda
    this
  }

  /** Get the smoothing parameter. */
  @Since("1.4.0")
  def getLambda: Double = lambda

  /**
   * Set the model type using a string (case-sensitive).
   * Supported options: "multinomial" (default) and "bernoulli".
   */
  @Since("1.4.0")
  def setModelType(modelType: String): NaiveBayes = {
    require(NaiveBayes.supportedModelTypes.contains(modelType),
      s"NaiveBayes was created with an unknown modelType: $modelType.")
    this.modelType = modelType
    this
  }

  /** Get the model type. */
  @Since("1.4.0")
  def getModelType: String = this.modelType

  /**
   * Run the algorithm with the configured parameters on an input RDD of LabeledPoint entries.
   *
   * @param data RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   */
  @Since("0.9.0")
  def run(data: RDD[LabeledPoint]): NaiveBayesModel = {
    val spark = SparkSession
      .builder()
      .sparkContext(data.context)
      .getOrCreate()

    import spark.implicits._

    val nb = new NewNaiveBayes()
      .setModelType(modelType)
      .setSmoothing(lambda)

    val dataset = data.map { case LabeledPoint(label, features) => (label, features.asML) }
      .toDF("label", "features")

    // mllib NaiveBayes allows input labels like {-1, +1}, so set `nonNegativeLabel` as false.
    val newModel = nb.trainWithLabelCheck(dataset, nonNegativeLabel = false)

    val pi = newModel.pi.toArray
    val theta = Array.ofDim[Double](newModel.numClasses, newModel.numFeatures)
    newModel.theta.foreachActive {
      case (i, j, v) =>
        theta(i)(j) = v
    }

    assert(newModel.oldLabels != null,
      "The underlying ML NaiveBayes training does not produce labels.")
    new NaiveBayesModel(newModel.oldLabels, pi, theta, modelType)
  }
}

/**
 * Top-level methods for calling naive Bayes.
 */
@Since("0.9.0")
object NaiveBayes {

  /** String name for multinomial model type. */
  private[classification] val Multinomial: String = "multinomial"

  /** String name for Bernoulli model type. */
  private[classification] val Bernoulli: String = "bernoulli"

  /* Set of modelTypes that NaiveBayes supports */
  private[classification] val supportedModelTypes = Set(Multinomial, Bernoulli)

  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * This is the default Multinomial NB (see <a href="http://tinyurl.com/lsdw6p">here</a>)
   * which can handle all kinds of discrete data. For example, by converting documents into
   * TF-IDF vectors, it can be used for document classification.
   *
   * This version of the method uses a default smoothing parameter of 1.0.
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   */
  @Since("0.9.0")
  def train(input: RDD[LabeledPoint]): NaiveBayesModel = {
    new NaiveBayes().run(input)
  }

  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * This is the default Multinomial NB (see <a href="http://tinyurl.com/lsdw6p">here</a>)
   * which can handle all kinds of discrete data. For example, by converting documents
   * into TF-IDF vectors, it can be used for document classification.
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   * @param lambda The smoothing parameter
   */
  @Since("0.9.0")
  def train(input: RDD[LabeledPoint], lambda: Double): NaiveBayesModel = {
    new NaiveBayes(lambda, Multinomial).run(input)
  }

  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * The model type can be set to either Multinomial NB (see <a href="http://tinyurl.com/lsdw6p">
   * here</a>) or Bernoulli NB (see <a href="http://tinyurl.com/p7c96j6">here</a>).
   * The Multinomial NB can handle discrete count data and can be called by setting the model
   * type to "multinomial".
   * For example, it can be used with word counts or TF_IDF vectors of documents.
   * The Bernoulli model fits presence or absence (0-1) counts. By making every vector a
   * 0-1 vector and setting the model type to "bernoulli", the  fits and predicts as
   * Bernoulli NB.
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   * @param lambda The smoothing parameter
   *
   * @param modelType The type of NB model to fit from the enumeration NaiveBayesModels, can be
   *              multinomial or bernoulli
   */
  @Since("1.4.0")
  def train(input: RDD[LabeledPoint], lambda: Double, modelType: String): NaiveBayesModel = {
    require(supportedModelTypes.contains(modelType),
      s"NaiveBayes was created with an unknown modelType: $modelType.")
    new NaiveBayes(lambda, modelType).run(input)
  }

}
