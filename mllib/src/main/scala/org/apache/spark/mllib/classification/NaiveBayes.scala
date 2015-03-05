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
 * @param modelType The type of NB model to fit from the enumeration NaiveBayesModels, can be
 *              Multinomial or Bernoulli
 */

class NaiveBayesModel private[mllib] (
    val labels: Array[Double],
    val pi: Array[Double],
    val theta: Array[Array[Double]],
    val modelType: NaiveBayes.ModelType)
  extends ClassificationModel with Serializable with Saveable {

  def this(labels: Array[Double], pi: Array[Double], theta: Array[Array[Double]]) =
    this(labels, pi, theta, NaiveBayes.Multinomial)

  private val brzPi = new BDV[Double](pi)
  private val brzTheta = new BDM(theta(0).length, theta.length, theta.flatten).t

  private val brzNegTheta: Option[BDM[Double]] = modelType match {
    case NaiveBayes.Multinomial => None
    case NaiveBayes.Bernoulli =>
      val negTheta = brzLog((brzExp(brzTheta.copy) :*= (-1.0)) :+= 1.0) // log(1.0 - exp(x))
      Option(negTheta)
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
      case NaiveBayes.Multinomial =>
        labels (brzArgmax (brzPi + brzTheta * testData.toBreeze) )
      case NaiveBayes.Bernoulli =>
        labels (brzArgmax (brzPi +
          (brzTheta - brzNegTheta.get) * testData.toBreeze +
          brzSum(brzNegTheta.get, Axis._1)))
    }
  }

  override def save(sc: SparkContext, path: String): Unit = {
    val data = NaiveBayesModel.SaveLoadV1_0.Data(labels, pi, theta, modelType.toString)
    NaiveBayesModel.SaveLoadV1_0.save(sc, path, data)
  }

  override protected def formatVersion: String = "1.0"
}

object NaiveBayesModel extends Loader[NaiveBayesModel] {

  import org.apache.spark.mllib.util.Loader._

  private object SaveLoadV1_0 {

    def thisFormatVersion = "1.0"

    /** Hard-code class name string in case it changes in the future */
    def thisClassName = "org.apache.spark.mllib.classification.NaiveBayesModel"

    /** Model data for model import/export */
    case class Data(labels: Array[Double],
                    pi: Array[Double],
                    theta: Array[Array[Double]],
                    modelType: String)

    def save(sc: SparkContext, path: String, data: Data): Unit = {
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // Create JSON metadata.
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~
          ("numFeatures" -> data.theta(0).length) ~ ("numClasses" -> data.pi.length) ~
          ("modelType" -> data.modelType)))
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
      val modelType = NaiveBayes.ModelType.fromString(data.getString(3))
      new NaiveBayesModel(labels, pi, theta, modelType)
    }
  }

  override def load(sc: SparkContext, path: String): NaiveBayesModel = {
    def getModelType(metadata: JValue): NaiveBayes.ModelType = {
      implicit val formats = DefaultFormats
      NaiveBayes.ModelType.fromString((metadata \ "modelType").extract[String])
    }
    val (loadedClassName, version, metadata) = loadMetadata(sc, path)
    val classNameV1_0 = SaveLoadV1_0.thisClassName
    (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        val (numFeatures, numClasses) = ClassificationModel.getNumFeaturesClasses(metadata)
        val model = SaveLoadV1_0.load(sc, path)
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
        assert(model.modelType == getModelType(metadata))
        model
      case _ => throw new Exception(
        s"NaiveBayesModel.load did not recognize model with (className, format version):" +
        s"($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
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
    var modelType: NaiveBayes.ModelType) extends Serializable with Logging {

  def this(lambda: Double) = this(lambda, NaiveBayes.Multinomial)

  def this() = this(1.0, NaiveBayes.Multinomial)

  /** Set the smoothing parameter. Default: 1.0. */
  def setLambda(lambda: Double): NaiveBayes = {
    this.lambda = lambda
    this
  }

  /** Set the model type. Default: Multinomial. */
  def setModelType(model: NaiveBayes.ModelType): NaiveBayes = {
    this.modelType = model
    this
  }


  /**
   * Run the algorithm with the configured parameters on an input RDD of LabeledPoint entries.
   *
   * @param data RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   */
  def run(data: RDD[LabeledPoint]) = {
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
        case NaiveBayes.Multinomial => math.log(brzSum(sumTermFreqs) + numFeatures * lambda)
        case NaiveBayes.Bernoulli => math.log(n + 2.0 * lambda)
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
  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * This is the Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all kinds of
   * discrete data.  For example, by converting documents into TF-IDF vectors, it can be used for
   * document classification.
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
   * This is the Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all kinds of
   * discrete data.  For example, by converting documents into TF-IDF vectors, it can be used for
   * document classification.
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   * @param lambda The smoothing parameter
   */
  def train(input: RDD[LabeledPoint], lambda: Double): NaiveBayesModel = {
    new NaiveBayes(lambda).run(input)
  }


  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * This is by default the Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle
   * all kinds of discrete data.  For example, by converting documents into TF-IDF vectors,
   * it can be used for document classification.  By making every vector a 0-1 vector and
   * setting the model type to NaiveBayesModels.Bernoulli, it fits and predicts as
   * Bernoulli NB ([[http://tinyurl.com/p7c96j6]]).
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   * @param lambda The smoothing parameter
   *
   * @param modelType The type of NB model to fit from the enumeration NaiveBayesModels, can be
   *              Multinomial or Bernoulli
   */
  def train(input: RDD[LabeledPoint], lambda: Double, modelType: String): NaiveBayesModel = {
    new NaiveBayes(lambda, Multinomial).run(input)
  }

  sealed abstract class ModelType

  object MODELTYPE {
    final val MULTINOMIAL_STRING = "multinomial"
    final val BERNOULLI_STRING = "bernoulli"

    def fromString(modelType: String): ModelType = modelType match {
      case MULTINOMIAL_STRING => Multinomial
      case BERNOULLI_STRING => Bernoulli
      case _ =>
        throw new IllegalArgumentException(s"Cannot recognize NaiveBayes ModelType: $modelType")
    }
  }

  final val ModelType = MODELTYPE

  final val Multinomial: ModelType = new ModelType {
    override def toString: String = ModelType.MULTINOMIAL_STRING
  }

  final val Bernoulli: ModelType = new ModelType {
    override def toString: String = ModelType.BERNOULLI_STRING
  }

}

