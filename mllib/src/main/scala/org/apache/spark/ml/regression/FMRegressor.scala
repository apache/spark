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

package org.apache.spark.ml.regression

import scala.util.Random

import breeze.linalg.{axpy => brzAxpy, norm => brzNorm, Vector => BV}
import breeze.numerics.{sqrt => brzSqrt}
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.linalg.BLAS._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.regression.FactorizationMachines._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DatasetUtils._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.{linalg => OldLinalg}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.optimization.{Gradient, GradientDescent, SquaredL2Updater, Updater}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.storage.StorageLevel

/**
 * Params for Factorization Machines
 */
private[ml] trait FactorizationMachinesParams extends PredictorParams
  with HasMaxIter with HasStepSize with HasTol with HasSolver with HasSeed
  with HasFitIntercept with HasRegParam with HasWeightCol {

  /**
   * Param for dimensionality of the factors (&gt;= 0)
   * @group param
   */
  @Since("3.0.0")
  final val factorSize: IntParam = new IntParam(this, "factorSize",
    "Dimensionality of the factor vectors, " +
      "which are used to get pairwise interactions between variables",
    ParamValidators.gt(0))

  /** @group getParam */
  @Since("3.0.0")
  final def getFactorSize: Int = $(factorSize)

  /**
   * Param for whether to fit linear term (aka 1-way term)
   * @group param
   */
  @Since("3.0.0")
  final val fitLinear: BooleanParam = new BooleanParam(this, "fitLinear",
    "whether to fit linear term (aka 1-way term)")

  /** @group getParam */
  @Since("3.0.0")
  final def getFitLinear: Boolean = $(fitLinear)

  /**
   * Param for mini-batch fraction, must be in range (0, 1]
   * @group param
   */
  @Since("3.0.0")
  final val miniBatchFraction: DoubleParam = new DoubleParam(this, "miniBatchFraction",
    "fraction of the input data set that should be used for one iteration of gradient descent",
    ParamValidators.inRange(0, 1, false, true))

  /** @group getParam */
  @Since("3.0.0")
  final def getMiniBatchFraction: Double = $(miniBatchFraction)

  /**
   * Param for standard deviation of initial coefficients
   * @group param
   */
  @Since("3.0.0")
  final val initStd: DoubleParam = new DoubleParam(this, "initStd",
    "standard deviation of initial coefficients", ParamValidators.gt(0))

  /** @group getParam */
  @Since("3.0.0")
  final def getInitStd: Double = $(initStd)

  /**
   * The solver algorithm for optimization.
   * Supported options: "gd", "adamW".
   * Default: "adamW"
   *
   * @group param
   */
  @Since("3.0.0")
  final override val solver: Param[String] = new Param[String](this, "solver",
    "The solver algorithm for optimization. Supported options: " +
      s"${supportedSolvers.mkString(", ")}. (Default adamW)",
    ParamValidators.inArray[String](supportedSolvers))

  setDefault(factorSize -> 8, fitIntercept -> true, fitLinear -> true, regParam -> 0.0,
    miniBatchFraction -> 1.0, initStd -> 0.01, maxIter -> 100, stepSize -> 1.0, tol -> 1E-6,
    solver -> AdamW)
}

private[ml] trait FactorizationMachines extends FactorizationMachinesParams {

  private[ml] def initCoefficients(numFeatures: Int): OldVector = {
    val rnd = new Random($(seed))
    val initialCoefficients =
      OldVectors.dense(
        Array.fill($(factorSize) * numFeatures)(rnd.nextGaussian() * $(initStd)) ++
        (if ($(fitLinear)) new Array[Double](numFeatures) else Array.emptyDoubleArray) ++
        (if ($(fitIntercept)) new Array[Double](1) else Array.emptyDoubleArray))
    initialCoefficients
  }

  private[ml] def trainImpl(
      data: RDD[(Double, OldVector)],
      numFeatures: Int,
      loss: String
    ): (Vector, Array[Double]) = {

    // initialize coefficients
    val initialCoefficients = initCoefficients(numFeatures)
    val coefficientsSize = initialCoefficients.size

    // optimize coefficients with gradient descent
    val gradient = parseLoss(loss, $(factorSize), $(fitIntercept), $(fitLinear), numFeatures)

    val updater = parseSolver($(solver), coefficientsSize)

    val optimizer = new GradientDescent(gradient, updater)
      .setStepSize($(stepSize))
      .setNumIterations($(maxIter))
      .setRegParam($(regParam))
      .setMiniBatchFraction($(miniBatchFraction))
      .setConvergenceTol($(tol))
    val (coefficients, lossHistory) = optimizer.optimizeWithLossReturned(data, initialCoefficients)
    (coefficients.asML, lossHistory)
  }
}

private[ml] object FactorizationMachines {

  /** String name for "gd". */
  val GD = "gd"

  /** String name for "adamW". */
  val AdamW = "adamW"

  /** Set of solvers that FactorizationMachines supports. */
  val supportedSolvers = Array(GD, AdamW)

  /** String name for "logisticLoss". */
  val LogisticLoss = "logisticLoss"

  /** String name for "squaredError". */
  val SquaredError = "squaredError"

  /** Set of loss function names that FactorizationMachines supports. */
  val supportedRegressorLosses = Array(SquaredError)
  val supportedClassifierLosses = Array(LogisticLoss)
  val supportedLosses = supportedRegressorLosses ++ supportedClassifierLosses

  def parseSolver(solver: String, coefficientsSize: Int): Updater = {
    solver match {
      case GD => new SquaredL2Updater()
      case AdamW => new AdamWUpdater(coefficientsSize)
    }
  }

  def parseLoss(
      lossFunc: String,
      factorSize: Int,
      fitIntercept: Boolean,
      fitLinear: Boolean,
      numFeatures: Int
    ): BaseFactorizationMachinesGradient = {

    lossFunc match {
      case LogisticLoss =>
        new LogisticFactorizationMachinesGradient(factorSize, fitIntercept, fitLinear, numFeatures)
      case SquaredError =>
        new MSEFactorizationMachinesGradient(factorSize, fitIntercept, fitLinear, numFeatures)
      case _ => throw new IllegalArgumentException(s"loss function type $lossFunc is invalidation")
    }
  }

  def splitCoefficients(
      coefficients: Vector,
      numFeatures: Int,
      factorSize: Int,
      fitIntercept: Boolean,
      fitLinear: Boolean
    ): (Double, Vector, Matrix) = {

    val coefficientsSize = numFeatures * factorSize +
      (if (fitLinear) numFeatures else 0) + (if (fitIntercept) 1 else 0)
    require(coefficientsSize == coefficients.size,
      s"coefficients.size did not match the excepted size ${coefficientsSize}")

    val intercept = if (fitIntercept) coefficients(coefficients.size - 1) else 0.0
    val linear: Vector = if (fitLinear) {
      new DenseVector(coefficients.toArray.slice(
        numFeatures * factorSize, numFeatures * factorSize + numFeatures))
    } else {
      Vectors.sparse(numFeatures, Seq.empty)
    }
    val factors = new DenseMatrix(numFeatures, factorSize,
      coefficients.toArray.slice(0, numFeatures * factorSize), true)
    (intercept, linear, factors)
  }

  def combineCoefficients(
      intercept: Double,
      linear: Vector,
      factors: Matrix,
      fitIntercept: Boolean,
      fitLinear: Boolean
    ): Vector = {

    val coefficients = factors.toDense.values ++
      (if (fitLinear) linear.toArray else Array.emptyDoubleArray) ++
      (if (fitIntercept) Array(intercept) else Array.emptyDoubleArray)
    new DenseVector(coefficients)
  }

  def getRawPrediction(
      features: Vector,
      intercept: Double,
      linear: Vector,
      factors: Matrix
    ): Double = {
    var rawPrediction = intercept + features.dot(linear)
    (0 until factors.numCols).foreach { f =>
      var sumSquare = 0.0
      var sum = 0.0
      features.foreachNonZero { case (index, value) =>
        val vx = factors(index, f) * value
        sumSquare += vx * vx
        sum += vx
      }
      rawPrediction += 0.5 * (sum * sum - sumSquare)
    }

    rawPrediction
  }
}

/**
 * Params for FMRegressor
 */
private[regression] trait FMRegressorParams extends FactorizationMachinesParams {
}

/**
 * Factorization Machines learning algorithm for regression.
 * It supports normal gradient descent and AdamW solver.
 *
 * The implementation is based upon:
 * <a href="https://www.csie.ntu.edu.tw/~b97053/paper/Rendle2010FM.pdf">
 * S. Rendle. "Factorization machines" 2010</a>.
 *
 * FM is able to estimate interactions even in problems with huge sparsity
 * (like advertising and recommendation system).
 * FM formula is:
 * <blockquote>
 *   $$
 *   \begin{align}
 *   y = w_0 + \sum\limits^n_{i-1} w_i x_i +
 *     \sum\limits^n_{i=1} \sum\limits^n_{j=i+1} \langle v_i, v_j \rangle x_i x_j
 *   \end{align}
 *   $$
 * </blockquote>
 * First two terms denote global bias and linear term (as same as linear regression),
 * and last term denotes pairwise interactions term. v_i describes the i-th variable
 * with k factors.
 *
 * FM regression model uses MSE loss which can be solved by gradient descent method, and
 * regularization terms like L2 are usually added to the loss function to prevent overfitting.
 */
@Since("3.0.0")
class FMRegressor @Since("3.0.0") (
    @Since("3.0.0") override val uid: String)
  extends Regressor[Vector, FMRegressor, FMRegressionModel]
  with FactorizationMachines with FMRegressorParams with DefaultParamsWritable with Logging {

  @Since("3.0.0")
  def this() = this(Identifiable.randomUID("fmr"))

  /**
   * Set the dimensionality of the factors.
   * Default is 8.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setFactorSize(value: Int): this.type = set(factorSize, value)

  /**
   * Set whether to fit intercept term.
   * Default is true.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  /**
   * Set whether to fit linear term.
   * Default is true.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setFitLinear(value: Boolean): this.type = set(fitLinear, value)

  /**
   * Set the L2 regularization parameter.
   * Default is 0.0.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setRegParam(value: Double): this.type = set(regParam, value)

  /**
   * Set the mini-batch fraction parameter.
   * Default is 1.0.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setMiniBatchFraction(value: Double): this.type = set(miniBatchFraction, value)

  /**
   * Set the standard deviation of initial coefficients.
   * Default is 0.01.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setInitStd(value: Double): this.type = set(initStd, value)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Set the initial step size for the first step (like learning rate).
   * Default is 1.0.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setStepSize(value: Double): this.type = set(stepSize, value)

  /**
   * Set the convergence tolerance of iterations.
   * Default is 1E-6.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setTol(value: Double): this.type = set(tol, value)

  /**
   * Set the solver algorithm used for optimization.
   * Supported options: "gd", "adamW".
   * Default: "adamW"
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setSolver(value: String): this.type = set(solver, value)

  /**
   * Set the random seed for weight initialization.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  override protected def train(
      dataset: Dataset[_]
    ): FMRegressionModel = instrumented { instr =>

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, factorSize, fitIntercept, fitLinear, regParam,
      miniBatchFraction, initStd, maxIter, stepSize, tol, solver)

    val numFeatures = getNumFeatures(dataset, $(featuresCol))
    instr.logNumFeatures(numFeatures)

    val handlePersistence = dataset.storageLevel == StorageLevel.NONE

    val data = dataset.select(
      checkRegressionLabels($(labelCol)),
      checkNonNanVectors($(featuresCol))
    ).rdd.map { case Row(l: Double, v: Vector) => (l, OldVectors.fromML(v))
    }.setName("training instances")

    if (handlePersistence) data.persist(StorageLevel.MEMORY_AND_DISK)

    val (coefficients, _) = trainImpl(data, numFeatures, SquaredError)

    val (intercept, linear, factors) = splitCoefficients(
      coefficients, numFeatures, $(factorSize), $(fitIntercept), $(fitLinear))

    if (handlePersistence) data.unpersist()

    copyValues(new FMRegressionModel(uid, intercept, linear, factors))
  }

  @Since("3.0.0")
  override def copy(extra: ParamMap): FMRegressor = defaultCopy(extra)
}

@Since("3.0.0")
object FMRegressor extends DefaultParamsReadable[FMRegressor] {

  @Since("3.0.0")
  override def load(path: String): FMRegressor = super.load(path)
}

/**
 * Model produced by [[FMRegressor]].
 */
@Since("3.0.0")
class FMRegressionModel private[regression] (
    @Since("3.0.0") override val uid: String,
    @Since("3.0.0") val intercept: Double,
    @Since("3.0.0") val linear: Vector,
    @Since("3.0.0") val factors: Matrix)
  extends RegressionModel[Vector, FMRegressionModel]
  with FMRegressorParams with MLWritable {

  @Since("3.0.0")
  override val numFeatures: Int = linear.size

  @Since("3.0.0")
  override def predict(features: Vector): Double = {
    getRawPrediction(features, intercept, linear, factors)
  }

  @Since("3.0.0")
  override def copy(extra: ParamMap): FMRegressionModel = {
    copyValues(new FMRegressionModel(uid, intercept, linear, factors), extra)
  }

  @Since("3.0.0")
  override def write: MLWriter =
    new FMRegressionModel.FMRegressionModelWriter(this)

  override def toString: String = {
    s"FMRegressionModel: " +
      s"uid=${super.toString}, numFeatures=$numFeatures, " +
      s"factorSize=${$(factorSize)}, fitLinear=${$(fitLinear)}, fitIntercept=${$(fitIntercept)}"
  }
}

@Since("3.0.0")
object FMRegressionModel extends MLReadable[FMRegressionModel] {

  @Since("3.0.0")
  override def read: MLReader[FMRegressionModel] = new FMRegressionModelReader

  @Since("3.0.0")
  override def load(path: String): FMRegressionModel = super.load(path)

  /** [[MLWriter]] instance for [[FMRegressionModel]] */
  private[FMRegressionModel] class FMRegressionModelWriter(
      instance: FMRegressionModel) extends MLWriter with Logging {

    private case class Data(
        intercept: Double,
        linear: Vector,
        factors: Matrix)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.intercept, instance.linear, instance.factors)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class FMRegressionModelReader extends MLReader[FMRegressionModel] {

    private val className = classOf[FMRegressionModel].getName

    override def load(path: String): FMRegressionModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)

      val Row(intercept: Double, linear: Vector, factors: Matrix) = data
        .select("intercept", "linear", "factors").head()
      val model = new FMRegressionModel(metadata.uid, intercept, linear, factors)
      metadata.getAndSetParams(model)
      model
    }
  }
}

/**
 * Factorization Machines base gradient class
 * Implementing the raw FM formula, include raw prediction and raw gradient,
 * then inherit the base class to implement special gradient class(like logloss, mse).
 *
 * Factorization Machines raw formula:
 * {{{
 *   y_{fm} = w_0 + \sum\limits^n_{i-1} w_i x_i +
 *     \sum\limits^n_{i=1} \sum\limits^n_{j=i+1} \langle v_i, v_j \rangle x_i x_j
 * }}}
 * the pairwise interactions (2-way term) can be reformulated:
 * {{{
 *   \sum\limits^n_{i=1} \sum\limits^n_{j=i+1} \langle v_i, v_j \rangle x_i x_j
 *   = \frac{1}{2}\sum\limits^k_{f=1}
 *   \left(\left( \sum\limits^n_{i=1}v_{i,f}x_i \right)^2 -
 *     \sum\limits^n_{i=1}v_{i,f}^2x_i^2 \right)
 * }}}
 * and the gradients are:
 * {{{
 *   \frac{\partial}{\partial\theta}y_{fm} = \left\{
 *   \begin{align}
 *   &1, & if\ \theta\ is\ w_0 \\
 *   &x_i, & if\ \theta\ is\ w_i \\
 *   &x_i{\sum}^n_{j=1}v_{j,f}x_j - v_{i,f}x_i^2, & if\ \theta\ is\ v_{i,j} \\
 *   \end{align}
 *   \right.
 * }}}
 *
 * Factorization Machines formula with prediction task:
 * {{{
 *   \hat{y} = p\left( y_{fm} \right)
 * }}}
 * p is the prediction function, for binary classification task is sigmoid.
 * The loss function gradient formula:
 * {{{
 *   \frac{\partial}{\partial\theta} l\left( \hat{y},y \right) =
 *   \frac{\partial}{\partial\theta} l\left( p\left( y_{fm} \right),y \right) =
 *   \frac{\partial l}{\partial \hat{y}} \cdot
 *   \frac{\partial \hat{y}}{\partial y_{fm}} \cdot
 *   \frac{\partial y_{fm}}{\partial\theta}
 * }}}
 * Last term is same for all task, so be implemented in base gradient class.
 * last term named rawGradient in following code, and first two term named multiplier.
 */
private[ml] abstract class BaseFactorizationMachinesGradient(
    factorSize: Int,
    fitIntercept: Boolean,
    fitLinear: Boolean,
    numFeatures: Int) extends Gradient {

  override def compute(
      data: OldVector,
      label: Double,
      weights: OldVector,
      cumGradient: OldVector): Double = {
    val (rawPrediction, sumVX) = getRawPrediction(data, weights)
    val rawGradient = getRawGradient(data, weights, sumVX)
    val multiplier = getMultiplier(rawPrediction, label)
    axpy(multiplier, rawGradient, cumGradient)
    val loss = getLoss(rawPrediction, label)
    loss
  }

  def getPrediction(rawPrediction: Double): Double

  protected def getMultiplier(rawPrediction: Double, label: Double): Double

  protected def getLoss(rawPrediction: Double, label: Double): Double

  def getRawPrediction(data: OldVector, weights: OldVector): (Double, Array[Double]) = {
    val sumVX = new Array[Double](factorSize)
    var rawPrediction = 0.0
    val vWeightsSize = numFeatures * factorSize

    if (fitIntercept) rawPrediction += weights(weights.size - 1)
    if (fitLinear) {
      data.foreachNonZero { case (index, value) =>
        rawPrediction += weights(vWeightsSize + index) * value
      }
    }
    (0 until factorSize).foreach { f =>
      var sumSquare = 0.0
      var sum = 0.0
      data.foreachNonZero { case (index, value) =>
        val vx = weights(index * factorSize + f) * value
        sumSquare += vx * vx
        sum += vx
      }
      sumVX(f) = sum
      rawPrediction += 0.5 * (sum * sum - sumSquare)
    }

    (rawPrediction, sumVX)
  }

  private def getRawGradient(
      data: OldVector,
      weights: OldVector,
      sumVX: Array[Double]
    ): OldVector = {
    data match {
      // Usually Factorization Machines is used, there will be a lot of sparse features.
      // So need to optimize the gradient descent of sparse vector.
      case data: OldLinalg.SparseVector =>
        val gardSize = data.indices.length * factorSize +
          (if (fitLinear) data.indices.length else 0) +
          (if (fitIntercept) 1 else 0)
        val gradIndex = Array.ofDim[Int](gardSize)
        val gradValue = Array.ofDim[Double](gardSize)
        var gradI = 0
        val vWeightsSize = numFeatures * factorSize

        data.foreachNonZero { case (index, value) =>
          (0 until factorSize).foreach { f =>
            gradIndex(gradI) = index * factorSize + f
            gradValue(gradI) = value * sumVX(f) - weights(index * factorSize + f) * value * value
            gradI += 1
          }
        }
        if (fitLinear) {
          data.foreachNonZero { case (index, value) =>
            gradIndex(gradI) = vWeightsSize + index
            gradValue(gradI) = value
            gradI += 1
          }
        }
        if (fitIntercept) {
          gradIndex(gradI) = weights.size - 1
          gradValue(gradI) = 1.0
        }

        OldVectors.sparse(weights.size, gradIndex, gradValue)
      case data: OldLinalg.DenseVector =>
        val gradient = Array.ofDim[Double](weights.size)
        val vWeightsSize = numFeatures * factorSize

        if (fitIntercept) gradient(weights.size - 1) += 1.0
        if (fitLinear) {
          data.foreachNonZero { case (index, value) =>
            gradient(vWeightsSize + index) += value
          }
        }
        (0 until factorSize).foreach { f =>
          data.foreachNonZero { case (index, value) =>
            gradient(index * factorSize + f) +=
              value * sumVX(f) - weights(index * factorSize + f) * value * value
          }
        }

        OldVectors.dense(gradient)
    }
  }
}

/**
 * FM with logistic loss
 * prediction formula:
 * {{{
 *   \hat{y} = \sigmoid(y_{fm})
 * }}}
 * loss formula:
 * {{{
 *   - y * log(\hat{y}) - (1 - y) * log(1 - \hat{y})
 * }}}
 * multiplier formula:
 * {{{
 *   \frac{\partial l}{\partial \hat{y}} \cdot
 *   \frac{\partial \hat{y}}{\partial y_{fm}} =
 *   \hat{y} - y
 * }}}
 */
private[ml] class LogisticFactorizationMachinesGradient(
    factorSize: Int,
    fitIntercept: Boolean,
    fitLinear: Boolean,
    numFeatures: Int)
  extends BaseFactorizationMachinesGradient(
    factorSize: Int,
    fitIntercept: Boolean,
    fitLinear: Boolean,
    numFeatures: Int) with Logging {

  override def getPrediction(rawPrediction: Double): Double = {
    1.0 / (1.0 + math.exp(-rawPrediction))
  }

  override protected def getMultiplier(rawPrediction: Double, label: Double): Double = {
    getPrediction(rawPrediction) - label
  }

  override protected def getLoss(rawPrediction: Double, label: Double): Double = {
    if (label > 0) MLUtils.log1pExp(-rawPrediction)
    else MLUtils.log1pExp(rawPrediction)
  }
}

/**
 * FM with mse
 * prediction formula:
 * {{{
 *   \hat{y} = y_{fm}
 * }}}
 * loss formula:
 * {{{
 *   (\hat{y} - y) ^ 2
 * }}}
 * multiplier formula:
 * {{{
 *   \frac{\partial l}{\partial \hat{y}} \cdot
 *   \frac{\partial \hat{y}}{\partial y_{fm}} =
 *   2 * (\hat{y} - y)
 * }}}
 */
private[ml] class MSEFactorizationMachinesGradient(
    factorSize: Int,
    fitIntercept: Boolean,
    fitLinear: Boolean,
    numFeatures: Int)
  extends BaseFactorizationMachinesGradient(
    factorSize: Int,
    fitIntercept: Boolean,
    fitLinear: Boolean,
    numFeatures: Int) with Logging {

  override def getPrediction(rawPrediction: Double): Double = {
    rawPrediction
  }

  override protected def getMultiplier(rawPrediction: Double, label: Double): Double = {
    2 * (rawPrediction - label)
  }

  override protected def getLoss(rawPrediction: Double, label: Double): Double = {
    (rawPrediction - label) * (rawPrediction - label)
  }
}

/**
 * AdamW optimizer.
 *
 * The implementation is based upon:
 * <a href="https://arxiv.org/pdf/1711.05101.pdf">
 * Loshchilov I, Hutter F. "DECOUPLED WEIGHT DECAY REGULARIZATION" 2019</a>.
 *
 * The main contribution of this paper is to improve regularization in Adam
 * by decoupling the weight decay from the gradient-based update.
 * This paper proposed a simple modification to recover the original formulation of
 * weight decay regularization by decoupling the weight decay from the optimization steps
 * taken w.r.t. the loss function.
 */
private[ml] class AdamWUpdater(weightSize: Int) extends Updater with Logging {
  val beta1: Double = 0.9
  val beta2: Double = 0.999
  val epsilon: Double = 1e-8

  val m: BV[Double] = BV.zeros[Double](weightSize).toDenseVector
  val v: BV[Double] = BV.zeros[Double](weightSize).toDenseVector
  var beta1T: Double = 1.0
  var beta2T: Double = 1.0

  override def compute(
    weightsOld: OldVector,
    gradient: OldVector,
    stepSize: Double,
    iter: Int,
    regParam: Double
  ): (OldVector, Double) = {
    val w: BV[Double] = weightsOld.asBreeze.toDenseVector
    val lr = stepSize // learning rate
    if (stepSize > 0) {
      val g: BV[Double] = gradient.asBreeze.toDenseVector
      m *= beta1
      brzAxpy(1 - beta1, g, m)
      v *= beta2
      brzAxpy(1 - beta2, g * g, v)
      beta1T *= beta1
      beta2T *= beta2
      val mHat = m / (1 - beta1T)
      val vHat = v / (1 - beta2T)
      w -= lr * mHat / (brzSqrt(vHat) + epsilon) + regParam * w
    }
    val norm = brzNorm(w, 2.0)

    (Vectors.fromBreeze(w), 0.5 * regParam * norm * norm)
  }
}
