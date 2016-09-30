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
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.util._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType

/**
 * Params for Naive Bayes Classifiers.
 */
private[ml] trait NaiveBayesParams extends PredictorParams with HasWeightCol {

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
   * Supported options: "multinomial" and "bernoulli".
   * (default = multinomial)
   * @group param
   */
  final val modelType: Param[String] = new Param[String](this, "modelType", "The model type " +
    "which is a string (case-sensitive). Supported options: multinomial (default) and bernoulli.",
    ParamValidators.inArray[String](NaiveBayes.supportedModelTypes.toArray))

  /** @group getParam */
  final def getModelType: String = $(modelType)
}

/**
 * Naive Bayes Classifiers.
 * It supports Multinomial NB
 * ([[http://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html]])
 * which can handle finitely supported discrete data. For example, by converting documents into
 * TF-IDF vectors, it can be used for document classification. By making every vector a
 * binary (0/1) data, it can also be used as Bernoulli NB
 * ([[http://nlp.stanford.edu/IR-book/html/htmledition/the-bernoulli-model-1.html]]).
 * The input feature values must be nonnegative.
 */
@Since("1.5.0")
class NaiveBayes @Since("1.5.0") (
    @Since("1.5.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, NaiveBayes, NaiveBayesModel]
  with NaiveBayesParams with DefaultParamsWritable {

  import NaiveBayes.{Bernoulli, Multinomial}

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
    val numClasses = getNumClasses(dataset)

    if (isDefined(thresholds)) {
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".train() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    val numFeatures = dataset.select(col($(featuresCol))).head().getAs[Vector](0).size

    val requireNonnegativeValues: Vector => Unit = (v: Vector) => {
      val values = v match {
        case sv: SparseVector => sv.values
        case dv: DenseVector => dv.values
      }

      require(values.forall(_ >= 0.0),
        s"Naive Bayes requires nonnegative feature values but found $v.")
    }

    val requireZeroOneBernoulliValues: Vector => Unit = (v: Vector) => {
      val values = v match {
        case sv: SparseVector => sv.values
        case dv: DenseVector => dv.values
      }

      require(values.forall(v => v == 0.0 || v == 1.0),
        s"Bernoulli naive Bayes requires 0 or 1 feature values but found $v.")
    }

    val requireValues: Vector => Unit = {
      $(modelType) match {
        case Multinomial =>
          requireNonnegativeValues
        case Bernoulli =>
          requireZeroOneBernoulliValues
        case _ =>
          // This should never happen.
          throw new UnknownError(s"Invalid modelType: ${$(modelType)}.")
      }
    }

    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))

    // Aggregates term frequencies per label.
    // TODO: Calling aggregateByKey and collect creates two stages, we can implement something
    // TODO: similar to reduceByKeyLocally to save one stage.
    val aggregated = dataset.select(col($(labelCol)).cast(DoubleType), w, col($(featuresCol))).rdd
      .map { row => (row.getDouble(0), (row.getDouble(1), row.getAs[Vector](2)))
      }.aggregateByKey[(Double, DenseVector)]((0.0, Vectors.zeros(numFeatures).toDense))(
      seqOp = {
         case ((weightSum: Double, featureSum: DenseVector), (weight, features)) =>
           requireValues(features)
           BLAS.axpy(weight, features, featureSum)
           (weightSum + weight, featureSum)
      },
      combOp = {
         case ((weightSum1, featureSum1), (weightSum2, featureSum2)) =>
           BLAS.axpy(1.0, featureSum2, featureSum1)
           (weightSum1 + weightSum2, featureSum1)
      }).collect().sortBy(_._1)

    val numLabels = aggregated.length
    val numDocuments = aggregated.map(_._2._1).sum

    val piArray = Array.fill[Double](numLabels)(0.0)
    val thetaArrays = Array.fill[Double](numLabels, numFeatures)(0.0)

    val lambda = $(smoothing)
    val piLogDenom = math.log(numDocuments + numLabels * lambda)
    var i = 0
    aggregated.foreach { case (label, (n, sumTermFreqs)) =>
      piArray(i) = math.log(n + lambda) - piLogDenom
      val thetaLogDenom = $(modelType) match {
        case Multinomial => math.log(sumTermFreqs.values.sum + numFeatures * lambda)
        case Bernoulli => math.log(n + 2.0 * lambda)
        case _ =>
          // This should never happen.
          throw new UnknownError(s"Invalid modelType: ${$(modelType)}.")
      }
      var j = 0
      while (j < numFeatures) {
        thetaArrays(i)(j) = math.log(sumTermFreqs(j) + lambda) - thetaLogDenom
        j += 1
      }
      i += 1
    }

    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(numLabels, thetaArrays(0).length, thetaArrays.flatten, true)
    new NaiveBayesModel(uid, pi, theta)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): NaiveBayes = defaultCopy(extra)
}

@Since("1.6.0")
object NaiveBayes extends DefaultParamsReadable[NaiveBayes] {
  /** String name for multinomial model type. */
  private[spark] val Multinomial: String = "multinomial"

  /** String name for Bernoulli model type. */
  private[spark] val Bernoulli: String = "bernoulli"

  /* Set of modelTypes that NaiveBayes supports */
  private[spark] val supportedModelTypes = Set(Multinomial, Bernoulli)

  @Since("1.6.0")
  override def load(path: String): NaiveBayes = super.load(path)
}

/**
 * Model produced by [[NaiveBayes]]
 * @param pi log of class priors, whose dimension is C (number of classes)
 * @param theta log of class conditional probabilities, whose dimension is C (number of classes)
 *              by D (number of features)
 */
@Since("1.5.0")
class NaiveBayesModel private[ml] (
    @Since("1.5.0") override val uid: String,
    @Since("2.0.0") val pi: Vector,
    @Since("2.0.0") val theta: Matrix)
  extends ProbabilisticClassificationModel[Vector, NaiveBayesModel]
  with NaiveBayesParams with MLWritable {

  import NaiveBayes.{Bernoulli, Multinomial}

  /**
   * Bernoulli scoring requires log(condprob) if 1, log(1-condprob) if 0.
   * This precomputes log(1.0 - exp(theta)) and its sum which are used for the linear algebra
   * application of this condition (in predict function).
   */
  private lazy val (thetaMinusNegTheta, negThetaSum) = $(modelType) match {
    case Multinomial => (None, None)
    case Bernoulli =>
      val negTheta = theta.map(value => math.log(1.0 - math.exp(value)))
      val ones = new DenseVector(Array.fill(theta.numCols) {1.0})
      val thetaMinusNegTheta = theta.map { value =>
        value - math.log(1.0 - math.exp(value))
      }
      (Option(thetaMinusNegTheta), Option(negTheta.multiply(ones)))
    case _ =>
      // This should never happen.
      throw new UnknownError(s"Invalid modelType: ${$(modelType)}.")
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

  override protected def predictRaw(features: Vector): Vector = {
    $(modelType) match {
      case Multinomial =>
        multinomialCalculation(features)
      case Bernoulli =>
        bernoulliCalculation(features)
      case _ =>
        // This should never happen.
        throw new UnknownError(s"Invalid modelType: ${$(modelType)}.")
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
    copyValues(new NaiveBayesModel(uid, pi, theta).setParent(this.parent), extra)
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

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: pi, theta
      val data = Data(instance.pi, instance.theta)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class NaiveBayesModelReader extends MLReader[NaiveBayesModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[NaiveBayesModel].getName

    override def load(path: String): NaiveBayesModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("pi", "theta").head()
      val pi = data.getAs[Vector](0)
      val theta = data.getAs[Matrix](1)
      val model = new NaiveBayesModel(metadata.uid, pi, theta)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}
