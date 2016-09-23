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
import org.apache.hadoop.fs.Path
import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{PredictionModel, Predictor, PredictorParams}
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.linalg.BLAS._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.util.VersionUtils

/** Params for SVM. */
private[ml] trait SVMParams extends ProbabilisticClassifierParams
  with HasSeed with HasStepSize with HasRegParam with HasMaxIter
  with HasFitIntercept with HasTol with HasStandardization with HasWeightCol with HasThreshold
  with HasAggregationDepth {

}

/**
 * :: Experimental ::
 * SVM with Hinge and OWLQN
 */
@Since("2.1.0")
@Experimental
class SVM @Since("2.1.0") (
    @Since("2.1.0") override val uid: String)
  extends Predictor[Vector, SVM, SVMModel]
  with SVMParams with DefaultParamsWritable {

  @Since("2.1.0")
  def this() = this(Identifiable.randomUID("svm"))

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 100)

  /**
   * Set the regularization parameter.
   * Default is 0.0.
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-4.
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  /**
   * Set the seed for weights initialization if weights are not set
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setSeed(value: Long): this.type = set(seed, value)

  @Since("2.1.0")
  override def copy(extra: ParamMap): SVM = defaultCopy(extra)

  /**
   * Sets the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /**
   * Train a model using the given dataset and parameters.
   * Developers can implement this instead of [[fit()]] to avoid dealing with schema validation
   * and copying parameters into the model.
   *
   * @param dataset Training dataset
   * @return Fitted model
   */
  override protected def train(dataset: Dataset[_]): SVMModel = {
    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Instance] =
      dataset.select(col($(labelCol)).cast(DoubleType), w, col($(featuresCol))).rdd.map {
        case Row(label: Double, weight: Double, features: Vector) =>
          Instance(label, weight, features)
      }

    val instr = Instrumentation.create(this, instances)
    instr.logParams(regParam, standardization, threshold, maxIter, tol, fitIntercept)

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

    if (isDefined(thresholds)) {
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".train() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    instr.logNumClasses(numClasses)
    instr.logNumFeatures(numFeatures)

    val (coefficientMatrix, interceptVector, objectiveHistory) = {
      if (numInvalid != 0) {
        val msg = s"Classification labels should be in [0 to ${numClasses - 1}]. " +
          s"Found $numInvalid invalid labels."
        logError(msg)
        throw new SparkException(msg)
      }

      val featuresStd = summarizer.variance.toArray.map(math.sqrt)
      val regParamL2 = $(regParam)
      val bcFeaturesStd = instances.context.broadcast(featuresStd)
      val costFun = new SVMCostFun(instances, numClasses, $(fitIntercept),
        $(standardization), bcFeaturesStd, regParamL2, false,
        $(aggregationDepth))

      def regParamL1Fun = (index: Int) => 0D
      val optimizer = new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))

      val initialCoefficientsWithIntercept = Vectors.zeros(numFeaturesPlusIntercept)

      initialCoefficientsWithIntercept.toArray(numFeatures) = math.log(
        histogram(1) / histogram(0))

      val states = optimizer.iterations(new CachedDiffFunction(costFun),
        initialCoefficientsWithIntercept.asBreeze.toDenseVector)

      val arrayBuilder = mutable.ArrayBuilder.make[Double]
      var state: optimizer.State = null
      while (states.hasNext) {
        state = states.next()
        arrayBuilder += state.adjustedValue
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
      val rawCoefficients = state.x.toArray.clone()
      val coefficientArray = Array.tabulate(numFeatures) { i =>
        // flatIndex will loop though rawCoefficients, and skip the intercept terms.
        val flatIndex = if ($(fitIntercept)) i + i / numFeatures else i
        val featureIndex = i % numFeatures
        if (featuresStd(featureIndex) != 0.0) {
          rawCoefficients(flatIndex) / featuresStd(featureIndex)
        } else {
          0.0
        }
      }

      val intercept = rawCoefficients(numFeaturesPlusIntercept - 1)
      (Vectors.dense(coefficientArray), intercept, arrayBuilder.result())
    }

    val model = copyValues(new SVMModel(uid, coefficientMatrix, interceptVector))
    instr.logSuccess(model)
    model
  }
}

@Since("2.1.0")
object SVM extends DefaultParamsReadable[SVM] {

  @Since("2.1.0")
  override def load(path: String): SVM = super.load(path)
}

/**
 * :: Experimental ::
 * SVM Model trained by [[SVM]]
 */
@Since("2.1.0")
@Experimental
class SVMModel private[ml] (
    @Since("2.1.0") override val uid: String,
    @Since("2.1.0") val weights: Vector,
    @Since("2.1.0") val intercept: Double)
  extends PredictionModel[Vector, SVMModel]
  with SVMParams with MLWritable {

  /**
   * Predict label for the given features.
   * This internal method is used to implement [[transform()]] and output [[predictionCol]].
   */
  override protected def predict(features: Vector): Double = {
    val margin = features.asBreeze.dot(weights.asBreeze) + intercept
    if (margin > $(threshold)) 1.0 else 0.0
  }

  @Since("2.1.0")
  override def copy(extra: ParamMap): SVMModel = {
    copyValues(new SVMModel(uid, weights, intercept), extra)
  }

  /**
   * Returns a [[org.apache.spark.ml.util.MLWriter]] instance for this ML instance.
   */
  @Since("2.1.0")
  override def write: MLWriter = new SVMModel.SVMModelWriter(this)

}


@Since("2.1.0")
object SVMModel extends MLReadable[SVMModel] {

  @Since("2.1.0")
  override def read: MLReader[SVMModel] = new SVMModelReader

  @Since("2.1.0")
  override def load(path: String): SVMModel = super.load(path)

  /** [[MLWriter]] instance for [[SVMModel]] */
  private[SVMModel]
  class SVMModelWriter(instance: SVMModel)
    extends MLWriter with Logging {

    private case class Data(
        coefficients: Vector,
        intercept: Double)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: numClasses, numFeatures, intercept, coefficients
      val data = Data(instance.weights, instance.intercept)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class SVMModelReader extends MLReader[SVMModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[SVMModel].getName

    override def load(path: String): SVMModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)
      val Row(coefficients: Vector, intercept: Double) =
        data.select("coefficients", "intercept").head()
      val model = new SVMModel(metadata.uid, coefficients, intercept)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}


/**
 * SVMCostFun implements Breeze's DiffFunction[T] for hinge loss function
 */
private class SVMCostFun(
    instances: RDD[Instance],
    numClasses: Int,
    fitIntercept: Boolean,
    standardization: Boolean,
    bcFeaturesStd: Broadcast[Array[Double]],
    regParamL2: Double,
    multinomial: Boolean,
    aggregationDepth: Int) extends DiffFunction[BDV[Double]] {

  override def calculate(coefficients: BDV[Double]): (Double, BDV[Double]) = {
    val coeffs = Vectors.fromBreeze(coefficients)
    val bcCoeffs = instances.context.broadcast(coeffs)
    val featuresStd = bcFeaturesStd.value
    val numFeatures = featuresStd.length

    val svmAggregator = {
      val seqOp = (c: SVMAggregator, instance: Instance) => c.add(instance)
      val combOp = (c1: SVMAggregator, c2: SVMAggregator) => c1.merge(c2)

      instances.treeAggregate(
        new SVMAggregator(bcCoeffs, bcFeaturesStd, numClasses, fitIntercept, false)
      )(seqOp, combOp, aggregationDepth)
    }

    val totalGradientArray = svmAggregator.gradient.toArray
    // regVal is the sum of coefficients squares excluding intercept for L2 regularization.
    val regVal = if (regParamL2 == 0.0) {
      0.0
    } else {
      var sum = 0.0
      coeffs.foreachActive { case (index, value) =>
        // We do not apply regularization to the intercepts
        val isIntercept = fitIntercept && ((index + 1) % (numFeatures + 1) == 0)
        if (!isIntercept) {
          // The following code will compute the loss of the regularization; also
          // the gradient of the regularization, and add back to totalGradientArray.
          sum += {
            if (standardization) {
              totalGradientArray(index) += regParamL2 * value
              value * value
            } else {
              val featureIndex = if (fitIntercept) {
                index % (numFeatures + 1)
              } else {
                index % numFeatures
              }
              if (featuresStd(featureIndex) != 0.0) {
                // If `standardization` is false, we still standardize the data
                // to improve the rate of convergence; as a result, we have to
                // perform this reverse standardization by penalizing each component
                // differently to get effectively the same objective function when
                // the training dataset is not standardized.
                val temp = value / (featuresStd(featureIndex) * featuresStd(featureIndex))
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


/**
 * SVMAggregator computes the gradient and loss for hinge loss function, as used
 * in binary classification for instances in sparse or dense vector in a online fashion.
 *
 * Two SVMAggregator can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * @param bcCoefficients The coefficients corresponding to the features.
 * @param fitIntercept Whether to fit an intercept term.
 * @param bcFeaturesStd The standard deviation values of the features.
 */
private class SVMAggregator(
    bcCoefficients: Broadcast[Vector],
    bcFeaturesStd: Broadcast[Array[Double]],
    numClasses: Int,
    fitIntercept: Boolean,
    multinomial: Boolean) extends Serializable {

  private val numFeatures = bcFeaturesStd.value.length
  private val numFeaturesPlusIntercept = if (fitIntercept) numFeatures + 1 else numFeatures
  private val coefficients = bcCoefficients.value
  private val featuresStd = bcFeaturesStd.value
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
   * Add a new training instance to this SVMAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param instance The instance of data point to be added.
   * @return This SVMAggregator object.
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
          val dotProduct = {
          var sum = 0.0
          features.foreachActive { (index, value) =>
            if (featuresStd(index) != 0.0 && value != 0.0) {
              sum += coefficients(index) * value / featuresStd(index)
            }
          }
          if (fitIntercept) sum += coefficients(numFeaturesPlusIntercept - 1)
          sum
        }
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
          lossSum += loss
        case _ =>
          new NotImplementedError("SVM in ML package only supports binary classification for now.")
      }
      weightSum += weight
      this
    }
  }

  /**
   * Merge another SVMAggregator, and update the loss and gradient
   * of the objective function.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other SVMAggregator to be merged.
   * @return This SVMAggregator object.
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
