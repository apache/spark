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

import scala.collection.JavaConverters._

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, argmax => brzArgmax, sum => brzSum, Axis}
import breeze.numerics.{exp => brzExp, log => brzLog}

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, JValue}

import org.apache.spark.{Logging, SparkContext, SparkException}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
 * Model for Naive Bayes Classifiers.
 *
 * @param labels list of labels
 * @param pi log of class priors, whose dimension is C, number of labels
 * @param theta log of class conditional probabilities, whose dimension is C-by-D,
 *              where D is number of features
 * @param modelType The type of NB model to fit  can be "Multinomial" or "Bernoulli"
 */
class NaiveBayesModel private[mllib] (
    val labels: Array[Double],
    val pi: Array[Double],
    val theta: Array[Array[Double]],
    val modelType: String)
  extends ClassificationModel with Serializable with Saveable {

  private[mllib] def this(labels: Array[Double], pi: Array[Double], theta: Array[Array[Double]]) =
    this(labels, pi, theta, "Multinomial")

  /** A Java-friendly constructor that takes three Iterable parameters. */
  private[mllib] def this(
      labels: JIterable[Double],
      pi: JIterable[Double],
      theta: JIterable[JIterable[Double]]) =
    this(labels.asScala.toArray, pi.asScala.toArray, theta.asScala.toArray.map(_.asScala.toArray))

  private val brzPi = new BDV[Double](pi)
  private val brzTheta = new BDM(theta(0).length, theta.length, theta.flatten).t

  // Bernoulli scoring requires log(condprob) if 1, log(1-condprob) if 0.
  // This precomputes log(1.0 - exp(theta)) and its sum  which are used for the  linear algebra
  // application of this condition (in predict function).
  private val (brzNegTheta, brzNegThetaSum) = modelType match {
    case "Multinomial" => (None, None)
    case "Bernoulli" =>
      val negTheta = brzLog((brzExp(brzTheta.copy) :*= (-1.0)) :+= 1.0) // log(1.0 - exp(x))
      (Option(negTheta), Option(brzSum(negTheta, Axis._1)))
    case _ =>
      // This should never happen.
      throw new UnknownError(s"NaiveBayesModel was created with an unknown ModelType: $modelType")
  }

  override def predict(testData: RDD[Vector]): RDD[Double] = {
    val bcModel = testData.context.broadcast(this)
    testData.mapPartitions { iter =>
      val model = bcModel.value
      iter.map(model.predict)
    }
  }

  override def predict(testData: Vector): Double = {
    modelType match {
      case "Multinomial" =>
        labels (brzArgmax (brzPi + brzTheta * testData.toBreeze) )
      case "Bernoulli" =>
        labels (brzArgmax (brzPi +
          (brzTheta - brzNegTheta.get) * testData.toBreeze + brzNegThetaSum.get))
      case _ =>
        // This should never happen.
        throw new UnknownError(s"NaiveBayesModel was created with an unknown ModelType: $modelType")
    }
  }

  override def save(sc: SparkContext, path: String): Unit = {
    val data = NaiveBayesModel.SaveLoadV2_0.Data(labels, pi, theta, modelType)
    NaiveBayesModel.SaveLoadV2_0.save(sc, path, data)
  }

  override protected def formatVersion: String = "2.0"
}

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
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // Create JSON metadata.
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~
          ("numFeatures" -> data.theta(0).length) ~ ("numClasses" -> data.pi.length)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(metadataPath(path))

      // Create Parquet data.
      val dataRDD: DataFrame = sc.parallelize(Seq(data), 1).toDF()
      dataRDD.saveAsParquetFile(dataPath(path))
    }

    def load(sc: SparkContext, path: String): NaiveBayesModel = {
      val sqlContext = new SQLContext(sc)
      // Load Parquet data.
      val dataRDD = sqlContext.parquetFile(dataPath(path))
      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      checkSchema[Data](dataRDD.schema)
      val dataArray = dataRDD.select("labels", "pi", "theta", "modelType").take(1)
      assert(dataArray.size == 1, s"Unable to load NaiveBayesModel data from: ${dataPath(path)}")
      val data = dataArray(0)
      val labels = data.getAs[Seq[Double]](0).toArray
      val pi = data.getAs[Seq[Double]](1).toArray
      val theta = data.getAs[Seq[Seq[Double]]](2).map(_.toArray).toArray
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
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // Create JSON metadata.
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~
          ("numFeatures" -> data.theta(0).length) ~ ("numClasses" -> data.pi.length)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(metadataPath(path))

      // Create Parquet data.
      val dataRDD: DataFrame = sc.parallelize(Seq(data), 1).toDF()
      dataRDD.saveAsParquetFile(dataPath(path))
    }

    def load(sc: SparkContext, path: String): NaiveBayesModel = {
      val sqlContext = new SQLContext(sc)
      // Load Parquet data.
      val dataRDD = sqlContext.parquetFile(dataPath(path))
      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      checkSchema[Data](dataRDD.schema)
      val dataArray = dataRDD.select("labels", "pi", "theta").take(1)
      assert(dataArray.size == 1, s"Unable to load NaiveBayesModel data from: ${dataPath(path)}")
      val data = dataArray(0)
      val labels = data.getAs[Seq[Double]](0).toArray
      val pi = data.getAs[Seq[Double]](1).toArray
      val theta = data.getAs[Seq[Seq[Double]]](2).map(_.toArray).toArray
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
    assert(model.pi.size == numClasses,
      s"NaiveBayesModel.load expected $numClasses classes," +
        s" but class priors vector pi had ${model.pi.size} elements")
    assert(model.theta.size == numClasses,
      s"NaiveBayesModel.load expected $numClasses classes," +
        s" but class conditionals array theta had ${model.theta.size} elements")
    assert(model.theta.forall(_.size == numFeatures),
      s"NaiveBayesModel.load expected $numFeatures features," +
        s" but class conditionals array theta had elements of size:" +
        s" ${model.theta.map(_.size).mkString(",")}")
    model
  }
}

/**
 * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
 *
 * This is the Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all kinds of
 * discrete data.  For example, by converting documents into TF-IDF vectors, it can be used for
 * document classification.  By making every vector a 0-1 vector, it can also be used as
 * Bernoulli NB ([[http://tinyurl.com/p7c96j6]]). The input feature values must be nonnegative.
 */

class NaiveBayes private (
    private var lambda: Double,
    private var modelType: String) extends Serializable with Logging {

  def this(lambda: Double) = this(lambda, "Multinomial")

  def this() = this(1.0, "Multinomial")

  /** Set the smoothing parameter. Default: 1.0. */
  def setLambda(lambda: Double): NaiveBayes = {
    this.lambda = lambda
    this
  }

  /** Get the smoothing parameter. */
  def getLambda: Double = lambda

  /**
   * Set the model type using a string (case-sensitive).
   * Supported options: "Multinomial" and "Bernoulli".
   * (default: Multinomial)
   */
  def setModelType(modelType:String): NaiveBayes = {
    require(NaiveBayes.supportedModelTypes.contains(modelType),
      s"NaiveBayes was created with an unknown ModelType: $modelType")
    this.modelType = modelType
    this
  }

  /** Get the model type. */
  def getModelType: String = this.modelType

  /**
   * Run the algorithm with the configured parameters on an input RDD of LabeledPoint entries.
   *
   * @param data RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   */
  def run(data: RDD[LabeledPoint]): NaiveBayesModel = {
    val requireNonnegativeValues: Vector => Unit = (v: Vector) => {
      val values = v match {
        case SparseVector(size, indices, values) =>
          values
        case DenseVector(values) =>
          values
      }
      if (!values.forall(_ >= 0.0)) {
        throw new SparkException(s"Naive Bayes requires nonnegative feature values but found $v.")
      }
    }

    // Aggregates term frequencies per label.
    // TODO: Calling combineByKey and collect creates two stages, we can implement something
    // TODO: similar to reduceByKeyLocally to save one stage.
    val aggregated = data.map(p => (p.label, p.features)).combineByKey[(Long, BDV[Double])](
      createCombiner = (v: Vector) => {
        requireNonnegativeValues(v)
        (1L, v.toBreeze.toDenseVector)
      },
      mergeValue = (c: (Long, BDV[Double]), v: Vector) => {
        requireNonnegativeValues(v)
        (c._1 + 1L, c._2 += v.toBreeze)
      },
      mergeCombiners = (c1: (Long, BDV[Double]), c2: (Long, BDV[Double])) =>
        (c1._1 + c2._1, c1._2 += c2._2)
    ).collect()

    val numLabels = aggregated.length
    var numDocuments = 0L
    aggregated.foreach { case (_, (n, _)) =>
      numDocuments += n
    }
    val numFeatures = aggregated.head match { case (_, (_, v)) => v.size }

    val labels = new Array[Double](numLabels)
    val pi = new Array[Double](numLabels)
    val theta = Array.fill(numLabels)(new Array[Double](numFeatures))

    val piLogDenom = math.log(numDocuments + numLabels * lambda)
    var i = 0
    aggregated.foreach { case (label, (n, sumTermFreqs)) =>
      labels(i) = label
      pi(i) = math.log(n + lambda) - piLogDenom
      val thetaLogDenom = modelType match {
        case "Multinomial" => math.log(brzSum(sumTermFreqs) + numFeatures * lambda)
        case "Bernoulli" => math.log(n + 2.0 * lambda)
        case _ =>
          // This should never happen.
          throw new UnknownError(s"NaiveBayes was created with an unknown ModelType: $modelType")
      }
      var j = 0
      while (j < numFeatures) {
        theta(i)(j) = math.log(sumTermFreqs(j) + lambda) - thetaLogDenom
        j += 1
      }
      i += 1
    }

    new NaiveBayesModel(labels, pi, theta, modelType)
  }
}

/**
 * Top-level methods for calling naive Bayes.
 */
object NaiveBayes {

  /* Set of modelTypes that NaiveBayes supports */
  private[mllib] val supportedModelTypes = Set("Multinomial", "Bernoulli")

  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * This is the default Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all
   * kinds of discrete data.  For example, by converting documents into TF-IDF vectors, it
   * can be used for document classification.
   *
   * This version of the method uses a default smoothing parameter of 1.0.
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   */
  def train(input: RDD[LabeledPoint]): NaiveBayesModel = {
    new NaiveBayes().run(input)
  }

  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * This is the default Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all
   * kinds of discrete data.  For example, by converting documents into TF-IDF vectors, it
   * can be used for document classification.
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   * @param lambda The smoothing parameter
   */
  def train(input: RDD[LabeledPoint], lambda: Double): NaiveBayesModel = {
    new NaiveBayes(lambda, "Multinomial").run(input)
  }

  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * The model type can be set to either Multinomial NB ([[http://tinyurl.com/lsdw6p]])
   * or Bernoulli NB ([[http://tinyurl.com/p7c96j6]]). The Multinomial NB can handle
   * discrete count data and can be called by setting the model type to "multinomial".
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
  def train(input: RDD[LabeledPoint], lambda: Double, modelType: String): NaiveBayesModel = {
    require(supportedModelTypes.contains(modelType),
      s"NaiveBayes was created with an unknown ModelType: $modelType")
    new NaiveBayes(lambda, modelType).run(input)
  }

}
