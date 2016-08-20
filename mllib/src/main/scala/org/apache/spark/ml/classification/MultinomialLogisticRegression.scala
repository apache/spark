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
import breeze.optimize.{CachedDiffFunction, LBFGS => BreezeLBFGS, OWLQN => BreezeOWLQN}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.storage.StorageLevel

/**
 * Params for multinomial logistic (softmax) regression.
 */
private[classification] trait MultinomialLogisticRegressionParams
  extends ProbabilisticClassifierParams with HasRegParam with HasElasticNetParam with HasMaxIter
    with HasFitIntercept with HasTol with HasStandardization with HasWeightCol
    with HasAggregationDepth {

  /**
   * Set thresholds in multiclass (or binary) classification to adjust the probability of
   * predicting each class. Array must have length equal to the number of classes, with values >= 0.
   * The class with largest value p/t is predicted, where p is the original probability of that
   * class and t is the class' threshold.
   *
   * @group setParam
   */
  def setThresholds(value: Array[Double]): this.type = {
    set(thresholds, value)
  }

  /**
   * Get thresholds for binary or multiclass classification.
   *
   * @group getParam
   */
  override def getThresholds: Array[Double] = {
    $(thresholds)
  }
}

/**
 * :: Experimental ::
 * Multinomial Logistic (softmax) regression.
 */
@Since("2.1.0")
@Experimental
class MultinomialLogisticRegression @Since("2.1.0") (
    @Since("2.1.0") override val uid: String)
  extends ProbabilisticClassifier[Vector,
    MultinomialLogisticRegression, MultinomialLogisticRegressionModel]
    with MultinomialLogisticRegressionParams with DefaultParamsWritable with Logging {

  @Since("2.1.0")
  def this() = this(Identifiable.randomUID("mlogreg"))

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
   * Set the ElasticNet mixing parameter.
   * For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty.
   * For 0 < alpha < 1, the penalty is a combination of L1 and L2.
   * Default is 0.0 which is an L2 penalty.
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setElasticNetParam(value: Double): this.type = set(elasticNetParam, value)
  setDefault(elasticNetParam -> 0.0)

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
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-6.
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  /**
   * Whether to fit an intercept term.
   * Default is true.
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)
  setDefault(fitIntercept -> true)

  /**
   * Whether to standardize the training features before fitting the model.
   * The coefficients of models will be always returned on the original scale,
   * so it will be transparent for users. Note that with/without standardization,
   * the models should always converge to the same solution when no regularization
   * is applied. In R's GLMNET package, the default behavior is true as well.
   * Default is true.
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setStandardization(value: Boolean): this.type = set(standardization, value)
  setDefault(standardization -> true)

  /**
   * Sets the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  @Since("2.1.0")
  override def setThresholds(value: Array[Double]): this.type = super.setThresholds(value)

  /**
   * Suggested depth for treeAggregate (>= 2).
   * If the dimensions of features or the number of partitions are large,
   * this param could be adjusted to a larger size.
   * Default is 2.
   * @group expertSetParam
   */
  @Since("2.1.0")
  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)
  setDefault(aggregationDepth -> 2)

  override protected[spark] def train(dataset: Dataset[_]): MultinomialLogisticRegressionModel = {
    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Instance] =
      dataset.select(col($(labelCol)).cast(DoubleType), w, col($(featuresCol))).rdd.map {
        case Row(label: Double, weight: Double, features: Vector) =>
          Instance(label, weight, features)
      }

    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val instr = Instrumentation.create(this, instances)
    instr.logParams(regParam, elasticNetParam, standardization, thresholds,
      maxIter, tol, fitIntercept)

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
    val numFeatures = summarizer.mean.size
    val numFeaturesPlusIntercept = if (getFitIntercept) numFeatures + 1 else numFeatures

    val numClasses = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) =>
        require(n >= histogram.length, s"Specified number of classes $n was " +
          s"less than the number of unique labels ${histogram.length}")
        n
      case None => histogram.length
    }

    instr.logNumClasses(numClasses)
    instr.logNumFeatures(numFeatures)

    val (coefficients, intercepts, objectiveHistory) = {
      if (numInvalid != 0) {
        val msg = s"Classification labels should be in {0 to ${numClasses - 1} " +
          s"Found $numInvalid invalid labels."
        logError(msg)
        throw new SparkException(msg)
      }

      val isConstantLabel = histogram.count(_ != 0) == 1

      if ($(fitIntercept) && isConstantLabel) {
        // we want to produce a model that will always predict the constant label so all the
        // coefficients will be zero, and the constant label class intercept will be +inf
        val constantLabelIndex = Vectors.dense(histogram).argmax
        (Matrices.sparse(numClasses, numFeatures, Array.fill(numFeatures + 1)(0),
          Array.empty[Int], Array.empty[Double]),
          Vectors.sparse(numClasses, Seq((constantLabelIndex, Double.PositiveInfinity))),
          Array.empty[Double])
      } else {
        if (!$(fitIntercept) && isConstantLabel) {
          logWarning(s"All labels belong to a single class and fitIntercept=false. It's" +
            s"a dangerous ground, so the algorithm may not converge.")
        }

        val featuresStd = summarizer.variance.toArray.map(math.sqrt)
        val featuresMean = summarizer.mean.toArray
        if (!$(fitIntercept) && (0 until numFeatures).exists { i =>
          featuresStd(i) == 0.0 && featuresMean(i) != 0.0 }) {
          logWarning("Fitting MultinomialLogisticRegressionModel without intercept on dataset " +
            "with constant nonzero column, Spark MLlib outputs zero coefficients for constant " +
            "nonzero columns. This behavior is the same as R glmnet but different from LIBSVM.")
        }

        val regParamL1 = $(elasticNetParam) * $(regParam)
        val regParamL2 = (1.0 - $(elasticNetParam)) * $(regParam)

        val bcFeaturesStd = instances.context.broadcast(featuresStd)
        val costFun = new LogisticCostFun(instances, numClasses, $(fitIntercept),
          $(standardization), bcFeaturesStd, regParamL2, multinomial = true, $(aggregationDepth))

        val optimizer = if ($(elasticNetParam) == 0.0 || $(regParam) == 0.0) {
          new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))
        } else {
          val standardizationParam = $(standardization)
          def regParamL1Fun = (index: Int) => {
            // Remove the L1 penalization on the intercept
            val isIntercept = $(fitIntercept) && ((index + 1) % numFeaturesPlusIntercept == 0)
            if (isIntercept) {
              0.0
            } else {
              if (standardizationParam) {
                regParamL1
              } else {
                val featureIndex = if ($(fitIntercept)) {
                  index % numFeaturesPlusIntercept
                } else {
                  index % numFeatures
                }
                // If `standardization` is false, we still standardize the data
                // to improve the rate of convergence; as a result, we have to
                // perform this reverse standardization by penalizing each component
                // differently to get effectively the same objective function when
                // the training dataset is not standardized.
                if (featuresStd(featureIndex) != 0.0) {
                  regParamL1 / featuresStd(featureIndex)
                } else {
                  0.0
                }
              }
            }
          }
          new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))
        }

        val initialCoefficientsWithIntercept = Vectors.zeros(numClasses * numFeaturesPlusIntercept)

        if ($(fitIntercept)) {
          /*
             For multinomial logistic regression, when we initialize the coefficients as zeros,
             it will converge faster if we initialize the intercepts such that
             it follows the distribution of the labels.
             {{{
               P(1) = \exp(b_1) / Z
               ...
               P(K) = \exp(b_K) / Z
               where Z = \sum_{k=1}^{K} \exp(b_k)
             }}}
             Since this doesn't have a unique solution, one of the solutions that satisfies the
             above equations is
             {{{
               \exp(b_k) = count_k * \exp(\lambda)
               b_k = \log(count_k) * \lambda
             }}}
             \lambda is a free parameter, so choose the phase \lambda such that the
             mean is centered. This yields
             {{{
               b_k = \log(count_k)
               b_k' = b_k - \mean(b_k)
             }}}
           */
          val rawIntercepts = histogram.map(c => math.log(c + 1)) // add 1 for smoothing
          val rawMean = rawIntercepts.sum / rawIntercepts.length
          rawIntercepts.indices.foreach { i =>
            initialCoefficientsWithIntercept.toArray(i * numFeaturesPlusIntercept + numFeatures) =
              rawIntercepts(i) - rawMean
          }
        }

        val states = optimizer.iterations(new CachedDiffFunction(costFun),
          initialCoefficientsWithIntercept.asBreeze.toDenseVector)

        /*
           Note that in Multinomial Logistic Regression, the objective history
           (loss + regularization) is log-likelihood which is invariant under feature
           standardization. As a result, the objective history from optimizer is the same as the
           one in the original space.
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
        bcFeaturesStd.destroy(blocking = false)

        /*
           The coefficients are trained in the scaled space; we're converting them back to
           the original space.
           Note that the intercept in scaled space and original space is the same;
           as a result, no scaling is needed.
         */
        val rawCoefficients = state.x.toArray
        val interceptsArray: Array[Double] = if ($(fitIntercept)) {
          Array.tabulate(numClasses) { i =>
            val coefIndex = (i + 1) * numFeaturesPlusIntercept - 1
            rawCoefficients(coefIndex)
          }
        } else {
          Array[Double]()
        }

        val coefficientArray: Array[Double] = Array.tabulate(numClasses * numFeatures) { i =>
          // flatIndex will loop though rawCoefficients, and skip the intercept terms.
          val flatIndex = if ($(fitIntercept)) i + i / numFeatures else i
          val featureIndex = i % numFeatures
          if (featuresStd(featureIndex) != 0.0) {
            rawCoefficients(flatIndex) / featuresStd(featureIndex)
          } else {
            0.0
          }
        }
        val coefficientMatrix =
          new DenseMatrix(numClasses, numFeatures, coefficientArray, isTransposed = true)

        /*
          When no regularization is applied, the coefficients lack identifiability because
          we do not use a pivot class. We can add any constant value to the coefficients and
          get the same likelihood. So here, we choose the mean centered coefficients for
          reproducibility. This method follows the approach in glmnet, described here:

          Friedman, et al. "Regularization Paths for Generalized Linear Models via
            Coordinate Descent," https://core.ac.uk/download/files/153/6287975.pdf
         */
        if ($(regParam) == 0.0) {
          val coefficientMean = coefficientMatrix.values.sum / (numClasses * numFeatures)
          coefficientMatrix.update(_ - coefficientMean)
        }
        /*
          The intercepts are never regularized, so we always center the mean.
         */
        val interceptVector = if (interceptsArray.nonEmpty) {
          val interceptMean = interceptsArray.sum / numClasses
          interceptsArray.indices.foreach { i => interceptsArray(i) -= interceptMean }
          Vectors.dense(interceptsArray)
        } else {
          Vectors.sparse(numClasses, Seq())
        }

        (coefficientMatrix, interceptVector, arrayBuilder.result())
      }
    }

    if (handlePersistence) instances.unpersist()

    val model = copyValues(
      new MultinomialLogisticRegressionModel(uid, coefficients, intercepts, numClasses))
    instr.logSuccess(model)
    model
  }

  @Since("2.1.0")
  override def copy(extra: ParamMap): MultinomialLogisticRegression = defaultCopy(extra)
}

@Since("2.1.0")
object MultinomialLogisticRegression extends DefaultParamsReadable[MultinomialLogisticRegression] {

  @Since("2.1.0")
  override def load(path: String): MultinomialLogisticRegression = super.load(path)
}

/**
 * :: Experimental ::
 * Model produced by [[MultinomialLogisticRegression]].
 */
@Since("2.1.0")
@Experimental
class MultinomialLogisticRegressionModel private[spark] (
    @Since("2.1.0") override val uid: String,
    @Since("2.1.0") val coefficients: Matrix,
    @Since("2.1.0") val intercepts: Vector,
    @Since("2.1.0") val numClasses: Int)
  extends ProbabilisticClassificationModel[Vector, MultinomialLogisticRegressionModel]
    with MultinomialLogisticRegressionParams with MLWritable {

  @Since("2.1.0")
  override def setThresholds(value: Array[Double]): this.type = super.setThresholds(value)

  @Since("2.1.0")
  override def getThresholds: Array[Double] = super.getThresholds

  @Since("2.1.0")
  override val numFeatures: Int = coefficients.numCols

  /** Margin (rawPrediction) for each class label. */
  private val margins: Vector => Vector = (features) => {
    val m = intercepts.toDense.copy
    BLAS.gemv(1.0, coefficients, features, 1.0, m)
    m
  }

  /** Score (probability) for each class label. */
  private val scores: Vector => Vector = (features) => {
    val m = margins(features)
    val maxMarginIndex = m.argmax
    val marginArray = m.toArray
    val maxMargin = marginArray(maxMarginIndex)

    // adjust margins for overflow
    val sum = {
      var temp = 0.0
      var k = 0
      while (k < numClasses) {
        marginArray(k) = if (maxMargin > 0) {
          math.exp(marginArray(k) - maxMargin)
        } else {
          math.exp(marginArray(k))
        }
        temp += marginArray(k)
        k += 1
      }
      temp
    }

    val scores = Vectors.dense(marginArray)
    BLAS.scal(1 / sum, scores)
    scores
  }

  /**
   * Predict label for the given feature vector.
   * The behavior of this can be adjusted using [[thresholds]].
   */
  override protected def predict(features: Vector): Double = {
    if (isDefined(thresholds)) {
      val thresholds: Array[Double] = getThresholds
      val probabilities = scores(features).toArray
      var argMax = 0
      var max = Double.NegativeInfinity
      var i = 0
      while (i < numClasses) {
        if (thresholds(i) == 0.0) {
          max = Double.PositiveInfinity
          argMax = i
        } else {
          val scaled = probabilities(i) / thresholds(i)
          if (scaled > max) {
            max = scaled
            argMax = i
          }
        }
        i += 1
      }
      argMax
    } else {
      scores(features).argmax
    }
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        val size = dv.size
        val values = dv.values

        // get the maximum margin
        val maxMarginIndex = rawPrediction.argmax
        val maxMargin = rawPrediction(maxMarginIndex)

        if (maxMargin == Double.PositiveInfinity) {
          var k = 0
          while (k < size) {
            values(k) = if (k == maxMarginIndex) 1.0 else 0.0
            k += 1
          }
        } else {
          val sum = {
            var temp = 0.0
            var k = 0
            while (k < numClasses) {
              values(k) = if (maxMargin > 0) {
                math.exp(values(k) - maxMargin)
              } else {
                math.exp(values(k))
              }
              temp += values(k)
              k += 1
            }
            temp
          }
          BLAS.scal(1 / sum, dv)
        }
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in MultinomialLogisticRegressionModel:" +
          " raw2probabilitiesInPlace encountered SparseVector")
    }
  }

  override protected def predictRaw(features: Vector): Vector = margins(features)

  @Since("2.1.0")
  override def copy(extra: ParamMap): MultinomialLogisticRegressionModel = {
    val newModel =
      copyValues(
        new MultinomialLogisticRegressionModel(uid, coefficients, intercepts, numClasses), extra)
    newModel.setParent(parent)
  }

  /**
   * Returns a [[org.apache.spark.ml.util.MLWriter]] instance for this ML instance.
   *
   * This does not save the [[parent]] currently.
   */
  @Since("2.1.0")
  override def write: MLWriter =
    new MultinomialLogisticRegressionModel.MultinomialLogisticRegressionModelWriter(this)
}


@Since("2.1.0")
object MultinomialLogisticRegressionModel extends MLReadable[MultinomialLogisticRegressionModel] {

  @Since("2.1.0")
  override def read: MLReader[MultinomialLogisticRegressionModel] =
    new MultinomialLogisticRegressionModelReader

  @Since("2.1.0")
  override def load(path: String): MultinomialLogisticRegressionModel = super.load(path)

  /** [[MLWriter]] instance for [[MultinomialLogisticRegressionModel]] */
  private[MultinomialLogisticRegressionModel]
  class MultinomialLogisticRegressionModelWriter(instance: MultinomialLogisticRegressionModel)
    extends MLWriter with Logging {

    private case class Data(
        numClasses: Int,
        numFeatures: Int,
        intercepts: Vector,
        coefficients: Matrix)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: numClasses, numFeatures, intercept, coefficients
      val data = Data(instance.numClasses, instance.numFeatures, instance.intercepts,
        instance.coefficients)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class MultinomialLogisticRegressionModelReader
    extends MLReader[MultinomialLogisticRegressionModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[MultinomialLogisticRegressionModel].getName

    override def load(path: String): MultinomialLogisticRegressionModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.format("parquet").load(dataPath)
        .select("numClasses", "numFeatures", "intercepts", "coefficients").head()
      val numClasses = data.getAs[Int](data.fieldIndex("numClasses"))
      val intercepts = data.getAs[Vector](data.fieldIndex("intercepts"))
      val coefficients = data.getAs[Matrix](data.fieldIndex("coefficients"))
      val model =
        new MultinomialLogisticRegressionModel(metadata.uid, coefficients, intercepts, numClasses)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}
