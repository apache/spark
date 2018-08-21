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

import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, OWLQN => BreezeOWLQN}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.optim.aggregator.HingeAggregator
import org.apache.spark.ml.optim.loss.{L2Regularization, RDDLossFunction}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{col, lit}

/** Params for linear SVM Classifier. */
private[classification] trait LinearSVCParams extends ClassifierParams with HasRegParam
  with HasMaxIter with HasFitIntercept with HasTol with HasStandardization with HasWeightCol
  with HasAggregationDepth with HasThreshold {

  /**
   * Param for threshold in binary classification prediction.
   * For LinearSVC, this threshold is applied to the rawPrediction, rather than a probability.
   * This threshold can be any real number, where Inf will make all predictions 0.0
   * and -Inf will make all predictions 1.0.
   * Default: 0.0
   *
   * @group param
   */
  final override val threshold: DoubleParam = new DoubleParam(this, "threshold",
    "threshold in binary classification prediction applied to rawPrediction")
}

/**
 * :: Experimental ::
 *
 * <a href = "https://en.wikipedia.org/wiki/Support_vector_machine#Linear_SVM">
 *   Linear SVM Classifier</a>
 *
 * This binary classifier optimizes the Hinge Loss using the OWLQN optimizer.
 * Only supports L2 regularization currently.
 *
 */
@Since("2.2.0")
@Experimental
class LinearSVC @Since("2.2.0") (
    @Since("2.2.0") override val uid: String)
  extends Classifier[Vector, LinearSVC, LinearSVCModel]
  with LinearSVCParams with DefaultParamsWritable {

  @Since("2.2.0")
  def this() = this(Identifiable.randomUID("linearsvc"))

  /**
   * Set the regularization parameter.
   * Default is 0.0.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 100)

  /**
   * Whether to fit an intercept term.
   * Default is true.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)
  setDefault(fitIntercept -> true)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller values will lead to higher accuracy at the cost of more iterations.
   * Default is 1E-6.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  /**
   * Whether to standardize the training features before fitting the model.
   * Default is true.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setStandardization(value: Boolean): this.type = set(standardization, value)
  setDefault(standardization -> true)

  /**
   * Set the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /**
   * Set threshold in binary classification.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setThreshold(value: Double): this.type = set(threshold, value)
  setDefault(threshold -> 0.0)

  /**
   * Suggested depth for treeAggregate (greater than or equal to 2).
   * If the dimensions of features or the number of partitions are large,
   * this param could be adjusted to a larger size.
   * Default is 2.
   *
   * @group expertSetParam
   */
  @Since("2.2.0")
  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)
  setDefault(aggregationDepth -> 2)

  @Since("2.2.0")
  override def copy(extra: ParamMap): LinearSVC = defaultCopy(extra)

  override protected def train(dataset: Dataset[_]): LinearSVCModel = instrumented { instr =>
    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Instance] =
      dataset.select(col($(labelCol)), w, col($(featuresCol))).rdd.map {
        case Row(label: Double, weight: Double, features: Vector) =>
          Instance(label, weight, features)
      }

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, regParam, maxIter, fitIntercept, tol, standardization, threshold,
      aggregationDepth)

    val (summarizer, labelSummarizer) = {
      val seqOp = (c: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        instance: Instance) =>
          (c._1.add(instance.features, instance.weight), c._2.add(instance.label, instance.weight))

      val combOp = (c1: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        c2: (MultivariateOnlineSummarizer, MultiClassSummarizer)) =>
          (c1._1.merge(c2._1), c1._2.merge(c2._2))

      instances.treeAggregate(
        (new MultivariateOnlineSummarizer, new MultiClassSummarizer)
      )(seqOp, combOp, $(aggregationDepth))
    }
    instr.logNumExamples(summarizer.count)
    instr.logNamedValue("lowestLabelWeight", labelSummarizer.histogram.min.toString)
    instr.logNamedValue("highestLabelWeight", labelSummarizer.histogram.max.toString)

    val histogram = labelSummarizer.histogram
    val numInvalid = labelSummarizer.countInvalid
    val numFeatures = summarizer.mean.size
    val numFeaturesPlusIntercept = if (getFitIntercept) numFeatures + 1 else numFeatures

    val numClasses = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) =>
        require(n >= histogram.length, s"Specified number of classes $n was " +
          s"less than the number of unique labels ${histogram.length}.")
        n
      case None => histogram.length
    }
    require(numClasses == 2, s"LinearSVC only supports binary classification." +
      s" $numClasses classes detected in $labelCol")
    instr.logNumClasses(numClasses)
    instr.logNumFeatures(numFeatures)

    val (coefficientVector, interceptVector, objectiveHistory) = {
      if (numInvalid != 0) {
        val msg = s"Classification labels should be in [0 to ${numClasses - 1}]. " +
          s"Found $numInvalid invalid labels."
        instr.logError(msg)
        throw new SparkException(msg)
      }

      val featuresStd = summarizer.variance.toArray.map(math.sqrt)
      val getFeaturesStd = (j: Int) => featuresStd(j)
      val regParamL2 = $(regParam)
      val bcFeaturesStd = instances.context.broadcast(featuresStd)
      val regularization = if (regParamL2 != 0.0) {
        val shouldApply = (idx: Int) => idx >= 0 && idx < numFeatures
        Some(new L2Regularization(regParamL2, shouldApply,
          if ($(standardization)) None else Some(getFeaturesStd)))
      } else {
        None
      }

      val getAggregatorFunc = new HingeAggregator(bcFeaturesStd, $(fitIntercept))(_)
      val costFun = new RDDLossFunction(instances, getAggregatorFunc, regularization,
        $(aggregationDepth))

      def regParamL1Fun = (index: Int) => 0D
      val optimizer = new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))
      val initialCoefWithIntercept = Vectors.zeros(numFeaturesPlusIntercept)

      val states = optimizer.iterations(new CachedDiffFunction(costFun),
        initialCoefWithIntercept.asBreeze.toDenseVector)

      val scaledObjectiveHistory = mutable.ArrayBuilder.make[Double]
      var state: optimizer.State = null
      while (states.hasNext) {
        state = states.next()
        scaledObjectiveHistory += state.adjustedValue
      }

      bcFeaturesStd.destroy(blocking = false)
      if (state == null) {
        val msg = s"${optimizer.getClass.getName} failed."
        instr.logError(msg)
        throw new SparkException(msg)
      }

      /*
         The coefficients are trained in the scaled space; we're converting them back to
         the original space.
         Note that the intercept in scaled space and original space is the same;
         as a result, no scaling is needed.
       */
      val rawCoefficients = state.x.toArray
      val coefficientArray = Array.tabulate(numFeatures) { i =>
        if (featuresStd(i) != 0.0) {
          rawCoefficients(i) / featuresStd(i)
        } else {
          0.0
        }
      }

      val intercept = if ($(fitIntercept)) {
        rawCoefficients(numFeaturesPlusIntercept - 1)
      } else {
        0.0
      }
      (Vectors.dense(coefficientArray), intercept, scaledObjectiveHistory.result())
    }

    copyValues(new LinearSVCModel(uid, coefficientVector, interceptVector))
  }
}

@Since("2.2.0")
object LinearSVC extends DefaultParamsReadable[LinearSVC] {

  @Since("2.2.0")
  override def load(path: String): LinearSVC = super.load(path)
}

/**
 * :: Experimental ::
 * Linear SVM Model trained by [[LinearSVC]]
 */
@Since("2.2.0")
@Experimental
class LinearSVCModel private[classification] (
    @Since("2.2.0") override val uid: String,
    @Since("2.2.0") val coefficients: Vector,
    @Since("2.2.0") val intercept: Double)
  extends ClassificationModel[Vector, LinearSVCModel]
  with LinearSVCParams with MLWritable {

  @Since("2.2.0")
  override val numClasses: Int = 2

  @Since("2.2.0")
  override val numFeatures: Int = coefficients.size

  @Since("2.2.0")
  def setThreshold(value: Double): this.type = set(threshold, value)
  setDefault(threshold, 0.0)

  @Since("2.2.0")
  def setWeightCol(value: Double): this.type = set(threshold, value)

  private val margin: Vector => Double = (features) => {
    BLAS.dot(features, coefficients) + intercept
  }

  override def predict(features: Vector): Double = {
    if (margin(features) > $(threshold)) 1.0 else 0.0
  }

  override protected def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(-m, m)
  }

  override protected def raw2prediction(rawPrediction: Vector): Double = {
    if (rawPrediction(1) > $(threshold)) 1.0 else 0.0
  }

  @Since("2.2.0")
  override def copy(extra: ParamMap): LinearSVCModel = {
    copyValues(new LinearSVCModel(uid, coefficients, intercept), extra).setParent(parent)
  }

  @Since("2.2.0")
  override def write: MLWriter = new LinearSVCModel.LinearSVCWriter(this)

}


@Since("2.2.0")
object LinearSVCModel extends MLReadable[LinearSVCModel] {

  @Since("2.2.0")
  override def read: MLReader[LinearSVCModel] = new LinearSVCReader

  @Since("2.2.0")
  override def load(path: String): LinearSVCModel = super.load(path)

  /** [[MLWriter]] instance for [[LinearSVCModel]] */
  private[LinearSVCModel]
  class LinearSVCWriter(instance: LinearSVCModel)
    extends MLWriter with Logging {

    private case class Data(coefficients: Vector, intercept: Double)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.coefficients, instance.intercept)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class LinearSVCReader extends MLReader[LinearSVCModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[LinearSVCModel].getName

    override def load(path: String): LinearSVCModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)
      val Row(coefficients: Vector, intercept: Double) =
        data.select("coefficients", "intercept").head()
      val model = new LinearSVCModel(metadata.uid, coefficients, intercept)
      metadata.getAndSetParams(model)
      model
    }
  }
}
