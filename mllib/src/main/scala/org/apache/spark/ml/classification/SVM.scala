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


import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS, OWLQN => BreezeOWLQN}
import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{PredictionModel, Predictor, PredictorParams}
import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.mllib.linalg.{Vector, Vectors, _}
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable

/** Params for Multilayer Perceptron. */
private[ml] trait SVMParams extends PredictorParams
  with HasSeed with HasStepSize
with HasRegParam with HasElasticNetParam with HasMaxIter with HasFitIntercept with HasTol
with HasStandardization with HasWeightCol with HasThreshold {

}

/**
 * :: Experimental ::
 * Classifier trainer based on the Multilayer Perceptron.
 * Each layer has sigmoid activation function, output layer has softmax.
 * Number of inputs has to be equal to the size of feature vectors.
 * Number of outputs has to be equal to the total number of labels.
 *
 */
@Since("1.5.0")
@Experimental
class SVM @Since("1.5.0") (
    @Since("1.5.0") override val uid: String)
  extends Predictor[Vector, SVM, SVMModel]
  with SVMParams with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("mlpc"))

  /**
   * Set the maximum number of iterations.
   * Default is 100.
    *
    * @group setParam
   */
  @Since("1.5.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-4.
    *
    * @group setParam
   */
  @Since("1.5.0")
  def setTol(value: Double): this.type = set(tol, value)

  /**
   * Set the seed for weights initialization if weights are not set
    *
    * @group setParam
   */
  @Since("1.5.0")
  def setSeed(value: Long): this.type = set(seed, value)

  @Since("1.5.0")
  override def copy(extra: ParamMap): SVM = defaultCopy(extra)
  setDefault(weightCol -> "")
  setDefault(regParam -> 0.0)
  /**
   * Train a model using the given dataset and parameters.
   * Developers can implement this instead of [[fit()]] to avoid dealing with schema validation
   * and copying parameters into the model.
   *
   * @param dataset Training dataset
   * @return Fitted model
   */
  override protected def train(dataset: Dataset[_]): SVMModel = {
    val w = if ($(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Instance] =
      dataset.select(col($(labelCol)).cast(DoubleType), w, col($(featuresCol))).rdd.map {
        case Row(label: Double, weight: Double, features: Vector) =>
          Instance(label, weight, features)
      }

    val (summarizer, labelSummarizer) = {
      val seqOp = (c: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        instance: Instance) =>
          (c._1.add(instance.features, instance.weight), c._2.add(instance.label, instance.weight))

      val combOp = (c1: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        c2: (MultivariateOnlineSummarizer, MultiClassSummarizer)) =>
          (c1._1.merge(c2._1), c1._2.merge(c2._2))

      instances.treeAggregate(
        new MultivariateOnlineSummarizer, new MultiClassSummarizer)(seqOp, combOp)
    }

    val histogram = labelSummarizer.histogram
    val numInvalid = labelSummarizer.countInvalid
    val numClasses = histogram.length
    val numFeatures = summarizer.mean.size

    val (coefficients, intercept, objectiveHistory) = {
      if (numInvalid != 0) {
        val msg = s"Classification labels should be in {0 to ${numClasses - 1} " +
          s"Found $numInvalid invalid labels."
        logError(msg)
        throw new SparkException(msg)
      }

      if (numClasses > 2) {
        val msg = s"Currently, LogisticRegression with ElasticNet in ML package only supports " +
          s"binary classification. Found $numClasses in the input dataset."
        logError(msg)
        throw new SparkException(msg)
      } else if ($(fitIntercept) && numClasses == 2 && histogram(0) == 0.0) {
        logWarning(s"All labels are one and fitIntercept=true, so the coefficients will be " +
          s"zeros and the intercept will be positive infinity; as a result, " +
          s"training is not needed.")
        (Vectors.sparse(numFeatures, Seq()), Double.PositiveInfinity, Array.empty[Double])
      } else if ($(fitIntercept) && numClasses == 1) {
        logWarning(s"All labels are zero and fitIntercept=true, so the coefficients will be " +
          s"zeros and the intercept will be negative infinity; as a result, " +
          s"training is not needed.")
        (Vectors.sparse(numFeatures, Seq()), Double.NegativeInfinity, Array.empty[Double])
      } else {
        if (!$(fitIntercept) && numClasses == 2 && histogram(0) == 0.0) {
          logWarning(s"All labels are one and fitIntercept=false. It's a dangerous ground, " +
            s"so the algorithm may not converge.")
        } else if (!$(fitIntercept) && numClasses == 1) {
          logWarning(s"All labels are zero and fitIntercept=false. It's a dangerous ground, " +
            s"so the algorithm may not converge.")
        }

        val featuresMean = summarizer.mean.toArray
        val featuresStd = summarizer.variance.toArray.map(math.sqrt)

        val regParamL1 = $(regParam)
        val regParamL2 = $(regParam)

        val costFun = new LogisticCostFun(instances, numClasses, $(fitIntercept),
          $(standardization), featuresStd, featuresMean, regParamL2)

        val standardizationParam = $(standardization)
        def regParamL1Fun = (index: Int) => {
          // Remove the L1 penalization on the intercept
          if (index == numFeatures) {
            0.0
          } else {
            if (standardizationParam) {
              regParamL1
            } else {
              // If `standardization` is false, we still standardize the data
              // to improve the rate of convergence; as a result, we have to
              // perform this reverse standardization by penalizing each component
              // differently to get effectively the same objective function when
              // the training dataset is not standardized.
              if (featuresStd(index) != 0.0) regParamL1 / featuresStd(index) else 0.0
            }
          }
        }
        val optimizer =
          new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))


        val initialCoefficientsWithIntercept =
          Vectors.zeros(if ($(fitIntercept)) numFeatures + 1 else numFeatures)



        if ($(fitIntercept)) {
          /*
             For binary logistic regression, when we initialize the coefficients as zeros,
             it will converge faster if we initialize the intercept such that
             it follows the distribution of the labels.

             {{{
               P(0) = 1 / (1 + \exp(b)), and
               P(1) = \exp(b) / (1 + \exp(b))
             }}}, hence
             {{{
               b = \log{P(1) / P(0)} = \log{count_1 / count_0}
             }}}
           */
          initialCoefficientsWithIntercept.toArray(numFeatures) = math.log(
            histogram(1) / histogram(0))
        }

        val states = optimizer.iterations(new CachedDiffFunction(costFun),
          initialCoefficientsWithIntercept.toBreeze.toDenseVector)

        /*
           Note that in Logistic Regression, the objective history (loss + regularization)
           is log-likelihood which is invariance under feature standardization. As a result,
           the objective history from optimizer is the same as the one in the original space.
         */
        val arrayBuilder = mutable.ArrayBuilder.make[Double]
        var state: optimizer.State = null
        while (states.hasNext) {
          state = states.next()
          arrayBuilder += state.adjustedValue
        }

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
        val rawCoefficients = state.x.toArray.clone()
        var i = 0
        while (i < numFeatures) {
          rawCoefficients(i) *= { if (featuresStd(i) != 0.0) 1.0 / featuresStd(i) else 0.0 }
          i += 1
        }

        if ($(fitIntercept)) {
          (Vectors.dense(rawCoefficients.dropRight(1)).compressed, rawCoefficients.last,
            arrayBuilder.result())
        } else {
          (Vectors.dense(rawCoefficients).compressed, 0.0, arrayBuilder.result())
        }
      }
    }

    copyValues(new SVMModel(uid, coefficients, intercept))

  }
}

@Since("2.0.0")
object SVM
  extends DefaultParamsReadable[SVM] {

  @Since("2.0.0")
  override def load(path: String): SVM = super.load(path)
}

/**
 * :: Experimental ::
 * Classification model based on the Multilayer Perceptron.
 * Each layer has sigmoid activation function, output layer has softmax.
  *
  * @param uid uid
 * @param weights vector of initial weights for the model that consists of the weights of layers
 * @return prediction model
 */
@Since("1.5.0")
@Experimental
class SVMModel private[ml] (
    @Since("1.5.0") override val uid: String,
    @Since("1.0.0") val weights: Vector,
    @Since("0.8.0") val intercept: Double)
  extends PredictionModel[Vector, SVMModel]
  with Serializable  {



  /**
   * Predict label for the given features.
   * This internal method is used to implement [[transform()]] and output [[predictionCol]].
   */
  override protected def predict(features: Vector): Double = {
//    LabelConverter.decodeLabel(mlpModel.predict(features))
    val margin = features.toBreeze.dot(weights.toBreeze) + intercept
    if (margin > 0.5) 1.0 else 0.0
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): SVMModel = {
    copyValues(new SVMModel(uid, weights, intercept), extra)
  }

}


/**
 * LogisticCostFun implements Breeze's DiffFunction[T] for a multinomial logistic loss function,
 * as used in multi-class classification (it is also used in binary logistic regression).
 * It returns the loss and gradient with L2 regularization at a particular point (coefficients).
 * It's used in Breeze's convex optimization routines.
 */
private class SVMCostFun(
                               instances: RDD[Instance],
                               numClasses: Int,
                               fitIntercept: Boolean,
                               standardization: Boolean,
                               featuresStd: Array[Double],
                               featuresMean: Array[Double],
                               regParamL2: Double) extends DiffFunction[BDV[Double]] {

  override def calculate(coefficients: BDV[Double]): (Double, BDV[Double]) = {
    val numFeatures = featuresStd.length
    val coeffs = Vectors.fromBreeze(coefficients)

    val logisticAggregator = {
      val seqOp = (c: SVMAggregator, instance: Instance) => c.add(instance)
      val combOp = (c1: SVMAggregator, c2: SVMAggregator) => c1.merge(c2)

      instances.treeAggregate(
        new SVMAggregator(coeffs, numClasses, fitIntercept, featuresStd, featuresMean)
      )(seqOp, combOp)
    }

    val totalGradientArray = logisticAggregator.gradient.toArray

    // regVal is the sum of coefficients squares excluding intercept for L2 regularization.
    val regVal = if (regParamL2 == 0.0) {
      0.0
    } else {
      var sum = 0.0
      coeffs.foreachActive { (index, value) =>
        // If `fitIntercept` is true, the last term which is intercept doesn't
        // contribute to the regularization.
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

    (logisticAggregator.loss + regVal, new BDV(totalGradientArray))
  }
}


/**
 * LogisticAggregator computes the gradient and loss for binary logistic loss function, as used
 * in binary classification for instances in sparse or dense vector in a online fashion.
 *
 * Note that multinomial logistic loss is not supported yet!
 *
 * Two LogisticAggregator can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * @param coefficients The coefficients corresponding to the features.
 * @param numClasses the number of possible outcomes for k classes classification problem in
 *                   Multinomial Logistic Regression.
 * @param fitIntercept Whether to fit an intercept term.
 * @param featuresStd The standard deviation values of the features.
 * @param featuresMean The mean values of the features.
 */
private class SVMAggregator(
    coefficients: Vector,
    numClasses: Int,
    fitIntercept: Boolean,
    featuresStd: Array[Double],
    featuresMean: Array[Double]) extends Serializable {

  private var weightSum = 0.0
  private var lossSum = 0.0

  private val coefficientsArray = coefficients match {
    case dv: DenseVector => dv.values
    case _ =>
      throw new IllegalArgumentException(
        s"coefficients only supports dense vector but got type ${coefficients.getClass}.")
  }

  private val dim = if (fitIntercept) coefficientsArray.length - 1 else coefficientsArray.length

  private val gradientSumArray = Array.ofDim[Double](coefficientsArray.length)

  /**
   * Add a new training instance to this LogisticAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param instance The instance of data point to be added.
   * @return This LogisticAggregator object.
   */
  def add(instance: Instance): this.type = {
    instance match { case Instance(label, weight, features) =>
      require(dim == features.size, s"Dimensions mismatch when adding new instance." +
        s" Expecting $dim but got ${features.size}.")
      require(weight >= 0.0, s"instance weight, $weight has to be >= 0.0")

      if (weight == 0.0) return this

      val localCoefficientsArray = coefficientsArray
      val localGradientSumArray = gradientSumArray

      numClasses match {
        case 2 =>
          // For Binary Logistic Regression.
//          val margin = - {
//            var sum = 0.0
//            features.foreachActive { (index, value) =>
//              if (featuresStd(index) != 0.0 && value != 0.0) {
//                sum += localCoefficientsArray(index) * (value / featuresStd(index))
//              }
//            }
//            sum + {
//              if (fitIntercept) localCoefficientsArray(dim) else 0.0
//            }
//          }
//
//          val multiplier = weight * (1.0 / (1.0 + math.exp(margin)) - label)



          val dotProduct = dot(features, new DenseVector(localCoefficientsArray))
          // Our loss function with {0, 1} labels is max(0, 1 - (2y - 1) (f_w(x)))
          // Therefore the gradient is -(2y - 1)*x
          val labelScaled = 2 * label - 1.0
          val (gradient, loss) = if (1.0 > labelScaled * dotProduct) {
            val gradient = features.copy
            scal(-labelScaled, gradient)
            (gradient, 1.0 - labelScaled * dotProduct)
          } else {
            (Vectors.sparse(localCoefficientsArray.size, Array.empty, Array.empty), 0.0)
          }

          features.foreachActive { (index, value) =>
            if (featuresStd(index) != 0.0 && value != 0.0) {
              localGradientSumArray(index) += gradient(index)
            }
          }

          if (fitIntercept) {
            localGradientSumArray(dim) += gradient.toArray.last
          }

//          if (label > 0) {
//            // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
//            lossSum += weight * MLUtils.log1pExp(margin)
//          } else {
//            lossSum += weight * (MLUtils.log1pExp(margin) - margin)
//          }
          lossSum += loss
        case _ =>
          new NotImplementedError("LogisticRegression with ElasticNet in ML package " +
            "only supports binary classification for now.")
      }
      weightSum += weight
      this
    }
  }

  /**
   * Merge another LogisticAggregator, and update the loss and gradient
   * of the objective function.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other LogisticAggregator to be merged.
   * @return This LogisticAggregator object.
   */
  def merge(other: SVMAggregator): this.type = {
    require(dim == other.dim, s"Dimensions mismatch when merging with another " +
      s"LeastSquaresAggregator. Expecting $dim but got ${other.dim}.")

    if (other.weightSum != 0.0) {
      weightSum += other.weightSum
      lossSum += other.lossSum

      var i = 0
      val localThisGradientSumArray = this.gradientSumArray
      val localOtherGradientSumArray = other.gradientSumArray
      val len = localThisGradientSumArray.length
      while (i < len) {
        localThisGradientSumArray(i) += localOtherGradientSumArray(i)
        i += 1
      }
    }
    this
  }

  def loss: Double = {
    require(weightSum > 0.0, s"The effective number of instances should be " +
      s"greater than 0.0, but $weightSum.")
    lossSum / weightSum
  }

  def gradient: Vector = {
    require(weightSum > 0.0, s"The effective number of instances should be " +
      s"greater than 0.0, but $weightSum.")
    val result = Vectors.dense(gradientSumArray.clone())
    scal(1.0 / weightSum, result)
    result
  }
}