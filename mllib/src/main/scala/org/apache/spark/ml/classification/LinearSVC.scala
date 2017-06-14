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

import java.util.Locale

import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS,
  OWLQN => BreezeOWLQN}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.optim.aggregator.{HingeAggregator, SquaredHingeAggregator}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{col, lit}

/** Params for linear SVM Classifier. */
private[classification] trait LinearSVCParams extends ClassifierParams with HasRegParam
  with HasMaxIter with HasFitIntercept with HasTol with HasStandardization with HasWeightCol
  with HasThreshold with HasAggregationDepth with HasSolver {

  /**
   * Specifies the loss function. Currently "hinge" and "squared_hinge" are supported.
   * "hinge" is the standard SVM loss (a.k.a. L1 loss) while "squared_hinge" is the square of
   * the hinge loss (a.k.a. L2 loss).
   *
   * @see <a href="https://en.wikipedia.org/wiki/Hinge_loss">Hinge loss (Wikipedia)</a>
   *
   * @group param
   */
  @Since("2.3.0")
  final val loss: Param[String] = new Param(this, "loss", "Specifies the loss " +
    "function. hinge is the standard SVM loss while squared_hinge is the square of the hinge loss.",
    (s: String) => LinearSVC.supportedLoss.contains(s.toLowerCase(Locale.ROOT)))

  setDefault(loss -> "squared_hinge")

  /** @group getParam */
  @Since("2.3.0")
  def getLoss: String = $(loss)
}

/**
 * :: Experimental ::
 *
 * <a href = "https://en.wikipedia.org/wiki/Support_vector_machine#Linear_SVM">
 *   Linear SVM Classifier</a>
 *
 * This binary classifier implements a linear SVM classifier. Currently "hinge" and
 * "squared_hinge" loss functions are supported. "hinge" is the standard SVM loss (a.k.a. L1 loss)
 * while "squared_hinge" is the square of the hinge loss (a.k.a. L2 loss). Both LBFGS and OWL-QN
 * optimizers are supported and can be specified via setting the solver param.
 * By default, L2 SVM (Squared Hinge Loss) and L-BFGS optimizer are used.
 *
 */
@Since("2.2.0")
@Experimental
class LinearSVC @Since("2.2.0") (
    @Since("2.2.0") override val uid: String)
  extends Classifier[Vector, LinearSVC, LinearSVCModel]
  with LinearSVCParams with DefaultParamsWritable {

  import LinearSVC._

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
   * Set threshold in binary classification, in range [0, 1].
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

  /**
   * Set the loss function. Default is "squared_hinge".
   *
   * @group setParam
   */
  @Since("2.3.0")
  def setLoss(value: String): this.type = set(loss, value)

  /**
   * Set solver for LinearSVC. Supported options: "l-bfgs" and "owlqn" (case insensitve).
   * - "l-bfgs" denotes Limited-memory BFGS which is a limited-memory quasi-Newton
   * optimization method.
   * - "owlqn" denotes Orthant-Wise Limited-memory Quasi-Newton algorithm .
   * (default: "owlqn")
   * @group setParam
   */
  @Since("2.2.0")
  def setSolver(value: String): this.type = {
    val lowercaseValue = value.toLowerCase(Locale.ROOT)
    require(supportedOptimizers.contains(lowercaseValue),
      s"Solver $value was not supported. Supported options: l-bfgs, owlqn")
    set(solver, lowercaseValue)
  }
  setDefault(solver -> "l-bfgs")

  @Since("2.2.0")
  override def copy(extra: ParamMap): LinearSVC = defaultCopy(extra)

  override protected def train(dataset: Dataset[_]): LinearSVCModel = {
    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Instance] =
      dataset.select(col($(labelCol)), w, col($(featuresCol))).rdd.map {
        case Row(label: Double, weight: Double, features: Vector) =>
          Instance(label, weight, features)
      }

    val instr = Instrumentation.create(this, instances)
    instr.logParams(regParam, maxIter, fitIntercept, tol, standardization, threshold,
      aggregationDepth)

    val (summarizer, labelSummarizer) = {
      val seqOp = (c: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        instance: Instance) =>
          (c._1.add(instance.features, instance.weight), c._2.add(instance.label, instance.weight))

      val combOp = (c1: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        c2: (MultivariateOnlineSummarizer, MultiClassSummarizer)) =>
          (c1._1.merge(c2._1), c1._2.merge(c2._2))

      instances.treeAggregate(
        new MultivariateOnlineSummarizer, new MultiClassSummarizer
      )(seqOp, combOp, $(aggregationDepth))
    }

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
        logError(msg)
        throw new SparkException(msg)
      }

      val featuresStd = summarizer.variance.toArray.map(math.sqrt)
      val regParamL2 = $(regParam)
      val bcFeaturesStd = instances.context.broadcast(featuresStd)
      val costFun = new LinearSVCCostFun(instances, $(fitIntercept), $(standardization),
        bcFeaturesStd, regParamL2, $(aggregationDepth), $(loss).toLowerCase(Locale.ROOT))

      val optimizer = $(solver).toLowerCase(Locale.ROOT) match {
        case LBFGS => new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))
        case OWLQN =>
          def regParamL1Fun = (index: Int) => 0D
          new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))
        case _ => throw new SparkException ("unexpected optimizer: " + $(solver))
      }

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
        logError(msg)
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

    val model = copyValues(new LinearSVCModel(uid, coefficientVector, interceptVector))
    instr.logSuccess(model)
    model
  }
}

@Since("2.2.0")
object LinearSVC extends DefaultParamsReadable[LinearSVC] {

  /** String name for Limited-memory BFGS. */
  private[classification] val LBFGS: String = "l-bfgs".toLowerCase(Locale.ROOT)

  /** String name for Orthant-Wise Limited-memory Quasi-Newton. */
  private[classification] val OWLQN: String = "owlqn".toLowerCase(Locale.ROOT)

  /* Set of optimizers that LinearSVC supports */
  private[classification] val supportedOptimizers = Array(LBFGS, OWLQN)

  @Since("2.2.0")
  override def load(path: String): LinearSVC = super.load(path)

  private[classification] val supportedLoss = Array("hinge", "squared_hinge")
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

  @Since("2.2.0")
  def setWeightCol(value: Double): this.type = set(threshold, value)

  private val margin: Vector => Double = (features) => {
    BLAS.dot(features, coefficients) + intercept
  }

  override protected def predict(features: Vector): Double = {
    if (margin(features) > $(threshold)) 1.0 else 0.0
  }

  override protected def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(-m, m)
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
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

/**
 * LinearSVCCostFun implements Breeze's DiffFunction[T] for loss function ("hinge" or
 * "squared_hinge").
 */
private class LinearSVCCostFun(
    instances: RDD[Instance],
    fitIntercept: Boolean,
    standardization: Boolean,
    bcFeaturesStd: Broadcast[Array[Double]],
    regParamL2: Double,
    aggregationDepth: Int,
    loss: String) extends DiffFunction[BDV[Double]] {

  override def calculate(coefficients: BDV[Double]): (Double, BDV[Double]) = {
    val coeffs = Vectors.fromBreeze(coefficients)
    val bcCoeffs = instances.context.broadcast(coeffs)
    val featuresStd = bcFeaturesStd.value
    val numFeatures = featuresStd.length

    val svmAggregator = loss match {
      case "hinge" =>
        val seqOp = (c: HingeAggregator, instance: Instance) => c.add(instance)
        val combOp = (c1: HingeAggregator, c2: HingeAggregator) => c1.merge(c2)
        instances.treeAggregate(
          new HingeAggregator(bcFeaturesStd, fitIntercept)(bcCoeffs)
        )(seqOp, combOp, aggregationDepth)
      case "squared_hinge" =>
        val seqOp = (c: SquaredHingeAggregator, instance: Instance) => c.add(instance)
        val combOp = (c1: SquaredHingeAggregator, c2: SquaredHingeAggregator) => c1.merge(c2)

        instances.treeAggregate(
          new SquaredHingeAggregator(bcFeaturesStd, fitIntercept)(bcCoeffs)
        )(seqOp, combOp, aggregationDepth)
      case unexpected => throw new SparkException(
        s"unexpected lossFunction in LinearSVCAggregator: $unexpected")
    }

    val totalGradientArray = svmAggregator.gradient.toArray
    // regVal is the sum of coefficients squares excluding intercept for L2 regularization.
    val regVal = if (regParamL2 == 0.0) {
      0.0
    } else {
      var sum = 0.0
      coeffs.foreachActive { case (index, value) =>
        // We do not apply regularization to the intercepts
        if (index != numFeatures) {
          // The following code will compute the loss of the regularization; also
          // the gradient of the regularization, and add back to totalGradientArray.
          sum += {
            if (standardization) {
              totalGradientArray(index) += regParamL2 * value
              value * value
            } else {
              if (featuresStd(index) != 0.0) {
                // If `standardization` is false, we still standardize the data
                // to improve the rate of convergence; as a result, we have to
                // perform this reverse standardization by penalizing each component
                // differently to get effectively the same objective function when
                // the training dataset is not standardized.
                val temp = value / (featuresStd(index) * featuresStd(index))
                totalGradientArray(index) += regParamL2 * temp
                value * temp
              } else {
                0.0
              }
            }
          }
        }
      }
      0.5 * regParamL2 * sum
    }
    bcCoeffs.destroy(blocking = false)

    (svmAggregator.loss + regVal, new BDV(totalGradientArray))
  }
}
