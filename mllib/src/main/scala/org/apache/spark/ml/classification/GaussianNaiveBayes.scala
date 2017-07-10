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

import org.apache.spark.annotation.Since
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.util._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, lit}


/**
 * Params for Gaussian Naive Bayes Classifiers.
 */
private[classification] trait GaussianNaiveBayesParams extends PredictorParams with HasWeightCol


/**
 * Gaussian Naive Bayes Classifiers.
 * It supports Gaussian NB
 * (see <a href="https://en.wikipedia.org/wiki/Naive_Bayes_classifier#Gaussian_naive_Bayes">
 * here</a>)
 * which can handle continuous data.
 */
@Since("2.2.0")
class GaussianNaiveBayes @Since("2.2.0") (
    @Since("2.2.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, GaussianNaiveBayes, GaussianNaiveBayesModel]
  with GaussianNaiveBayesParams with DefaultParamsWritable {

  @Since("2.2.0")
  def this() = this(Identifiable.randomUID("gnb"))

  /**
   * Sets the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  override protected def train(dataset: Dataset[_]): GaussianNaiveBayesModel = {
    if (isDefined(thresholds)) {
      val numClasses = getNumClasses(dataset)
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".train() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    val instr = Instrumentation.create(this, dataset)
    instr.logParams(labelCol, featuresCol, weightCol, predictionCol, rawPredictionCol,
      probabilityCol, thresholds)

    val numFeatures = dataset.select(col($(featuresCol))).head().getAs[Vector](0).size
    instr.logNumFeatures(numFeatures)

    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))

    // Aggregates sum and square-sum per label.
    // TODO: Calling aggregateByKey and collect creates two stages, we can implement something
    // TODO: similar to reduceByKeyLocally to save one stage.
    val aggregated = dataset.select(col($(labelCol)), w, col($(featuresCol))).rdd
      .map { row => (row.getDouble(0), (row.getDouble(1), row.getAs[Vector](2)))
      }.aggregateByKey((0.0, Vectors.zeros(numFeatures).toDense,
      Vectors.zeros(numFeatures).toDense))(
      seqOp = {
        case ((weightSum, featureSum, squareSum), (weight, features)) =>
          BLAS.axpy(weight, features, featureSum)
          features.foreachActive {
            case (i, v) =>
              squareSum.values(i) += v * v * weight
          }
          (weightSum + weight, featureSum, squareSum)
      },
      combOp = {
        case ((weightSum1, featureSum1, squareSum1), (weightSum2, featureSum2, squareSum2)) =>
          BLAS.axpy(1.0, featureSum2, featureSum1)
          BLAS.axpy(1.0, squareSum2, squareSum1)
          (weightSum1 + weightSum2, featureSum1, squareSum1)
      }).collect().sortBy(_._1)

    val numLabels = aggregated.length
    instr.logNumClasses(numLabels)

    val numInstances = aggregated.map(_._2._1).sum

    // If the ratio of data variance between dimensions is too small, it
    // will cause numerical errors. To address this, we artificially
    // boost the variance by epsilon, a small fraction of the standard
    // deviation of the largest dimension.
    // Refer to scikit-learn's implement
    // [https://github.com/scikit-learn/scikit-learn/blob/master/sklearn/naive_bayes.py#L337]
    // and discussion [https://github.com/scikit-learn/scikit-learn/pull/5349] for detail.
    var epsilon = Double.MinValue
    for (j <- 0 until numFeatures) {
      val globalSum = aggregated.map(t => t._2._2(j)).sum
      val globalSqrSum = aggregated.map(t => t._2._3(j)).sum
      val globalVar = globalSqrSum / numInstances -
        globalSum * globalSum / numInstances / numInstances
      if (globalVar > epsilon) epsilon = globalVar
    }
    epsilon *= 1e-9

    val piArray = new Array[Double](numLabels)

    // thetaArray in Gaussian NB store the means of features per label
    val thetaArray = new Array[Double](numLabels * numFeatures)

    // thetaArray in Gaussian NB store the variances of features per label
    val sigmaArray = new Array[Double](numLabels * numFeatures)

    var i = 0
    aggregated.foreach { case (label, (n, featureSum, squareSum)) =>
      piArray(i) = math.log(n / numInstances)
      var j = 0
      while (j < numFeatures) {
        val m = featureSum(j) / n
        thetaArray(i * numFeatures + j) = m
        sigmaArray(i * numFeatures + j) = epsilon + squareSum(j) / n - m * m
        j += 1
      }
      i += 1
    }

    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(numLabels, numFeatures, thetaArray, true)
    val sigma = new DenseMatrix(numLabels, numFeatures, sigmaArray, true)

    val model = new GaussianNaiveBayesModel(uid, pi, theta, sigma)
    instr.logSuccess(model)
    model
  }

  @Since("2.2.0")
  override def copy(extra: ParamMap): GaussianNaiveBayes = defaultCopy(extra)
}


@Since("2.2.0")
object GaussianNaiveBayes extends DefaultParamsReadable[GaussianNaiveBayes] {
  @Since("2.2.0")
  override def load(path: String): GaussianNaiveBayes = super.load(path)
}


/**
 * Model produced by [[GaussianNaiveBayes]]
 * @param pi log of class priors, whose dimension is C (number of classes)
 * @param theta mean of each feature, whose dimension is C (number of classes)
 *              by D (number of features)
 * @param sigma variance of each feature, whose dimension is C (number of classes)
 *              by D (number of features)
 */
@Since("2.2.0")
class GaussianNaiveBayesModel private[ml] (
    @Since("2.2.0") override val uid: String,
    @Since("2.2.0") val pi: Vector,
    @Since("2.2.0") val theta: Matrix,
    @Since("2.2.0") val sigma: Matrix)
  extends ProbabilisticClassificationModel[Vector, GaussianNaiveBayesModel]
  with GaussianNaiveBayesParams with MLWritable {

  require(pi.size == theta.numRows)
  require(theta.numRows == sigma.numRows)
  require(theta.numCols == sigma.numCols)

  import GaussianNaiveBayesModel._

  @Since("2.2.0")
  override val numFeatures: Int = theta.numCols

  @Since("2.2.0")
  override val numClasses: Int = pi.size

  /**
   * Gaussian scoring requires sum of log(Variance).
   * This precomputes sum of log(Variance) which are used for the linear algebra
   * application of this condition (in predict function).
   */
  private lazy val logVarSum = {
    val logVarSum = (0 until numClasses).map { i =>
      (0 until numFeatures).map { j =>
        math.log(sigma(i, j))
      }.sum
    }.toArray
    Vectors.dense(logVarSum)
  }

  override protected def predictRaw(features: Vector): Vector = {
    val probArray = (0 until numClasses).map { i =>
      // sum of (value_j - mean_j) ^2^ / variance_j
      val s = (0 until numFeatures).map { j =>
        val d = features(j) - theta(i, j)
        d * d / sigma(i, j)
      }.sum

      pi(i) - (s + logVarSum(i)) / 2
    }.toArray

    Vectors.dense(probArray)
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
        throw new RuntimeException("Unexpected error in GaussianNaiveBayesModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  @Since("2.2.0")
  override def write: MLWriter = new GaussianNaiveBayesModelWriter(this)

  @Since("2.2.0")
  override def copy(extra: ParamMap): GaussianNaiveBayesModel = {
    copyValues(new GaussianNaiveBayesModel(uid, pi, theta, sigma).setParent(this.parent), extra)
  }

  @Since("2.2.0")
  override def toString: String = {
    s"GaussianNaiveBayesModel (uid=$uid) with ${pi.size} classes"
  }
}


@Since("2.2.0")
object GaussianNaiveBayesModel extends MLReadable[GaussianNaiveBayesModel] {

  @Since("2.2.0")
  override def read: MLReader[GaussianNaiveBayesModel] = new GaussianNaiveBayesModelReader

  @Since("2.2.0")
  override def load(path: String): GaussianNaiveBayesModel = super.load(path)

  /** [[MLWriter]] instance for [[GaussianNaiveBayesModel]] */
  private[GaussianNaiveBayesModel] class GaussianNaiveBayesModelWriter(
    instance: GaussianNaiveBayesModel) extends MLWriter {

    private case class Data(pi: Vector, theta: Matrix, sigma: Matrix)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: pi, theta, sigma
      val data = Data(instance.pi, instance.theta, instance.sigma)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class GaussianNaiveBayesModelReader extends MLReader[GaussianNaiveBayesModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[GaussianNaiveBayesModel].getName

    override def load(path: String): GaussianNaiveBayesModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("pi", "theta", "sigma")
        .head()
      val pi = data.getAs[Vector](0)
      val theta = data.getAs[Matrix](1)
      val sigma = data.getAs[Matrix](2)
      val model = new GaussianNaiveBayesModel(metadata.uid, pi, theta, sigma)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}
