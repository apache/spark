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

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, argmax => brzArgmax, sum => brzSum}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, StructField, StructType}

import org.apache.spark.{SparkContext, SparkException, Logging}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{Importable, Exportable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

import scala.collection.mutable.ArrayBuffer


/**
 * Model for Naive Bayes Classifiers.
 *
 * @param labels list of labels
 * @param pi log of class priors, whose dimension is C, number of labels
 * @param theta log of class conditional probabilities, whose dimension is C-by-D,
 *              where D is number of features
 */
class NaiveBayesModel private[mllib] (
    val labels: Array[Double],
    val pi: Array[Double],
    val theta: Array[Array[Double]]) extends ClassificationModel with Serializable with Exportable {

  private val brzPi = new BDV[Double](pi)
  private val brzTheta = new BDM[Double](theta.length, theta(0).length)

  {
    // Need to put an extra pair of braces to prevent Scala treating `i` as a member.
    var i = 0
    while (i < theta.length) {
      var j = 0
      while (j < theta(i).length) {
        brzTheta(i, j) = theta(i)(j)
        j += 1
      }
      i += 1
    }
  }

  override def predict(testData: RDD[Vector]): RDD[Double] = {
    val bcModel = testData.context.broadcast(this)
    testData.mapPartitions { iter =>
      val model = bcModel.value
      iter.map(model.predict)
    }
  }

  override def predict(testData: Vector): Double = {
    labels(brzArgmax(brzPi + brzTheta * testData.toBreeze))
  }

  override def save(sc: SparkContext, path: String): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    // Create JSON metadata.
    val metadata = NaiveBayesModel.Metadata(
      clazz = this.getClass.getName, version = latestVersion)
    val metadataRDD: DataFrame = sc.parallelize(Seq(metadata))
    metadataRDD.toJSON.saveAsTextFile(path + "/metadata")

    // Create Parquet data.
    val data = NaiveBayesModel.Data(labels, pi, theta)
    val dataRDD: DataFrame = sc.parallelize(Seq(data))
    dataRDD.saveAsParquetFile(path + "/data")
  }

  override protected def latestVersion: String = NaiveBayesModel.latestVersion
}

object NaiveBayesModel extends Importable[NaiveBayesModel] {

  /** Metadata for model import/export */
  private case class Metadata(clazz: String, version: String)

  /** Model data for model import/export */
  private case class Data(labels: Array[Double], pi: Array[Double], theta: Array[Array[Double]])

  override def load(sc: SparkContext, path: String): NaiveBayesModel = {
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    // Load JSON metadata.
    val metadataRDD = sqlContext.jsonFile(path + "/metadata")
    val metadataArray = metadataRDD.select("clazz", "version").take(1)
    assert(metadataArray.size == 1,
      s"Unable to load NaiveBayesModel metadata from: ${path + "/metadata"}")
    metadataArray(0) match {
      case Row(clazz: String, version: String) =>
        assert(clazz == classOf[NaiveBayesModel].getName, s"NaiveBayesModel.load" +
          s" was given model file with metadata specifying a different model class: $clazz")
        assert(version == latestVersion, // only 1 version exists currently
          s"NaiveBayesModel.load did not recognize model format version: $version")
    }

    // Load Parquet data.
    val dataRDD = sqlContext.parquetFile(path + "/data")
    val dataArray = dataRDD.select("labels", "pi", "theta").take(1)
    assert(dataArray.size == 1, s"Unable to load NaiveBayesModel data from: ${path + "/data"}")
    val data = dataArray(0)
    assert(data.size == 3, s"Unable to load NaiveBayesModel data from: ${path + "/data"}")
    // Check schema explicitly since erasure makes it hard to use match-case for checking.
    Importable.checkSchema[Data](dataRDD.schema)
    val labels = data.getAs[Seq[Double]](0).toArray
    val pi = data.getAs[Seq[Double]](1).toArray
    val theta = data.getAs[Seq[Seq[Double]]](2).map(_.toArray).toArray
    new NaiveBayesModel(labels, pi, theta)
  }

  override protected def latestVersion: String = "1.0"
}

/**
 * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
 *
 * This is the Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all kinds of
 * discrete data.  For example, by converting documents into TF-IDF vectors, it can be used for
 * document classification.  By making every vector a 0-1 vector, it can also be used as
 * Bernoulli NB ([[http://tinyurl.com/p7c96j6]]). The input feature values must be nonnegative.
 */
class NaiveBayes private (private var lambda: Double) extends Serializable with Logging {

  def this() = this(1.0)

  /** Set the smoothing parameter. Default: 1.0. */
  def setLambda(lambda: Double): NaiveBayes = {
    this.lambda = lambda
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
      val thetaLogDenom = math.log(brzSum(sumTermFreqs) + numFeatures * lambda)
      pi(i) = math.log(n + lambda) - piLogDenom
      var j = 0
      while (j < numFeatures) {
        theta(i)(j) = math.log(sumTermFreqs(j) + lambda) - thetaLogDenom
        j += 1
      }
      i += 1
    }

    new NaiveBayesModel(labels, pi, theta)
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
   * document classification.  By making every vector a 0-1 vector, it can also be used as
   * Bernoulli NB ([[http://tinyurl.com/p7c96j6]]).
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
   * document classification.  By making every vector a 0-1 vector, it can also be used as
   * Bernoulli NB ([[http://tinyurl.com/p7c96j6]]).
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   * @param lambda The smoothing parameter
   */
  def train(input: RDD[LabeledPoint], lambda: Double): NaiveBayesModel = {
    new NaiveBayes(lambda).run(input)
  }
}
