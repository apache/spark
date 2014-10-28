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

import scala.reflect.ClassTag

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, argmax => brzArgmax, sum => brzSum}

import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.{SparkException, Logging}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Abstract model for a naive bayes classifier.
 */
abstract class NaiveBayesModel extends ClassificationModel with Serializable {
  /**
   * Predict values for the given data set using the trained model.
   *
   * @param testData PairRDD with values representing data points to be predicted
   * @return an RDD[(K, Double)] where each entry contains the corresponding prediction,
   *              partitioned consistently with testData.
   */
  def predictValues[K: ClassTag](testData: RDD[(K, Vector)]): RDD[(K, Double)]
}

/**
 * Local model for a naive bayes classifier.
 *
 * @param labels list of labels
 * @param pi log of class priors, whose dimension is C, number of labels
 * @param theta log of class conditional probabilities, whose dimension is C-by-D,
 *              where D is number of features
 */
private class LocalNaiveBayesModel(
    val labels: Array[Double],
    val pi: Array[Double],
    val theta: Array[Array[Double]]) extends NaiveBayesModel {

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

  def predict(testData: RDD[Vector]): RDD[Double] = {
    val bcModel = testData.context.broadcast(this)
    testData.mapPartitions { iter =>
      val model = bcModel.value
      iter.map(model.predict)
    }
  }

  def predict(testData: Vector): Double = {
    labels(brzArgmax(brzPi + brzTheta * testData.toBreeze))
  }

  def predictValues[K: ClassTag](testData: RDD[(K, Vector)]): RDD[(K, Double)] = {
    val bcModel = testData.context.broadcast(this)
    testData.mapValues { test =>
      bcModel.value.predict(test)
    }    
  }
}

/**
 * Distributed model for a naive bayes classifier.
 *
 * @param model RDD of (label, pi, theta) rows comprising the model.
 */
private class DistNaiveBayesModel(val model: RDD[(Double, Double, BDV[Double])])
  extends NaiveBayesModel {

  def predict(testData: RDD[Vector]): RDD[Double] = {
    val indexed = testData.zipWithIndex().map(_.swap)
    // Predict, reorder the results to match the input order, then project the labels.
    predictValues(indexed).sortByKey().map(_._2)
  }

  def predict(testData: Vector): Double = {
    val testBreeze = testData.toBreeze
    model.map { case (label, pi, theta) =>
      (pi + theta.dot(testBreeze), label)
    }.max._2
  }

  def predictValues[K: ClassTag](testData: RDD[(K, Vector)]): RDD[(K, Double)] = {
    // Pair each test data point with all model rows.
    val testXModel = testData.mapValues(_.toBreeze).cartesian(model)

    // Compute the posterior distribution for every test point.
    val posterior = testXModel.map { case ((key, test), (label, pi, theta)) =>
      (key, (pi + theta.dot(test), label))
    }

    // Find the maximum a posteriori value for each test data point, then project labels.
    val partitioner = testData.partitioner.getOrElse(defaultPartitioner(posterior))
    posterior.reduceByKey(partitioner, Ordering[(Double, Double)].max _).mapValues(_._2)
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
class NaiveBayes private (private var lambda: Double) extends Serializable with Logging {

  private var distMode = "local"

  def this() = this(1.0)

  /** Set the smoothing parameter. Default: 1.0. */
  def setLambda(lambda: Double): NaiveBayes = {
    this.lambda = lambda
    this
  }

  /** Set the model distribution mode, either "local" or "dist" (for distributed). */
  def setDistMode(distMode: String): NaiveBayes = {
    this.distMode = distMode
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
        case sv: SparseVector =>
          sv.values
        case dv: DenseVector =>
          dv.values
      }
      if (!values.forall(_ >= 0.0)) {
        throw new SparkException(s"Naive Bayes requires nonnegative feature values but found $v.")
      }
    }

    // Sum the document counts and feature frequencies for each label.
    val labelAggregates = data.map(p => (p.label, p.features)).combineByKey[(Long, BDV[Double])](
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
    )

    distMode match {
      case "local" => trainLocalModel(labelAggregates)
      case "dist" => trainDistModel(labelAggregates)
      case _ =>
        throw new SparkException(s"Naive Bayes requires a valid distMode but found $distMode.")
    }
  }

  private def trainLocalModel(labelAggregates: RDD[(Double, (Long, BDV[Double]))]) = {
    // TODO: Calling combineByKey and collect creates two stages, we can implement something
    // TODO: similar to reduceByKeyLocally to save one stage.
    val aggregated = labelAggregates.collect()
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

    new LocalNaiveBayesModel(labels, pi, theta)
  }

  private def trainDistModel(labelAggregates: RDD[(Double, (Long, BDV[Double]))]) = {
    // Compute the model's prior (pi) value and conditional (theta) vector for each label.
    // NOTE In contrast to the local trainer, the piLogDenom normalization term is omitted here.
    // Computing this term requires an additional aggregation on 'aggregated', and because the
    // term is an additive constant it does not affect maximum a posteriori model prediction.
    val model = labelAggregates.map { case (label, (numDocuments, sumFeatures)) =>
      val pi = math.log(numDocuments + lambda)
      val thetaLogDenom = math.log(brzSum(sumFeatures) + sumFeatures.length * lambda)
      val theta = new Array[Double](sumFeatures.length)
      sumFeatures.iterator.map(f => math.log(f._2 + lambda) - thetaLogDenom).copyToArray(theta)
      (label, pi, new BDV[Double](theta))
    }

    // Materialize and persist the model, check that it is nonempty.
    if (model.persist(StorageLevel.MEMORY_AND_DISK).count() == 0) {
      throw new SparkException("Naive Bayes requires a nonempty training RDD.")
    }

    new DistNaiveBayesModel(model)
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
   * @param distMode The model distribution mode, either "local" or "dist" (for distributed)
   */
  def train(input: RDD[LabeledPoint], lambda: Double, distMode: String): NaiveBayesModel = {
    new NaiveBayes(lambda).setDistMode(distMode).run(input)
  }
}
