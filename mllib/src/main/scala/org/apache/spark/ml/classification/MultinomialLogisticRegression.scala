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
 * Params for multinomial logistic regression.
 */
private[classification] trait MultinomialLogisticRegressionParams
  extends ProbabilisticClassifierParams with HasRegParam with HasElasticNetParam with HasMaxIter
    with HasFitIntercept with HasTol with HasStandardization with HasWeightCol {

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
 * Multinomial Logistic regression.
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

  override protected[spark] def train(dataset: Dataset[_]): MultinomialLogisticRegressionModel = {
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    train(dataset, handlePersistence)
  }

  protected[spark] def train(
      dataset: Dataset[_],
      handlePersistence: Boolean): MultinomialLogisticRegressionModel = {
    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Instance] =
      dataset.select(col($(labelCol)).cast(DoubleType), w, col($(featuresCol))).rdd.map {
        case Row(label: Double, weight: Double, features: Vector) =>
          Instance(label, weight, features)
      }

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

      val labelIsConstant = histogram.count(_ != 0) == 1

      if ($(fitIntercept) && labelIsConstant) {
        // we want to produce a model that will always predict the constant label
        (Matrices.sparse(numClasses, numFeatures, Array.fill(numFeatures + 1)(0), Array(), Array()),
          Vectors.sparse(numClasses, Seq((numClasses - 1, Double.PositiveInfinity))),
          Array.empty[Double])
      } else {
        if (!$(fitIntercept) && labelIsConstant) {
          logWarning(s"All labels belong to a single class and fitIntercept=false. It's" +
            s"a dangerous ground, so the algorithm may not converge.")
        }

        val featuresStd = summarizer.variance.toArray.map(math.sqrt)
        val standardizedInstances = instances.map { case Instance(label, weight, features) =>
          val f = features match {
            case DenseVector(vs) =>
              val values = vs.clone()
              val size = values.length
              var i = 0
              while (i < size) {
                values(i) *= (if (featuresStd(i) != 0.0) 1.0 / featuresStd(i) else 0.0)
                i += 1
              }
              Vectors.dense(values)
            case SparseVector(size, indices, vs) =>
              val values = vs.clone()
              val nnz = values.length
              var i = 0
              while (i < nnz) {
                values(i) *= (if (featuresStd(indices(i)) != 0.0) {
                  1.0 / featuresStd(indices(i))
                } else {
                  0.0
                })
                i += 1
              }
              Vectors.sparse(size, indices, values)
            case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
          }
          Instance(label, weight, f)
        }
        if (handlePersistence) standardizedInstances.persist(StorageLevel.MEMORY_AND_DISK)

        val regParamL1 = $(elasticNetParam) * $(regParam)
        val regParamL2 = (1.0 - $(elasticNetParam)) * $(regParam)

        val bcFeaturesStd = instances.context.broadcast(featuresStd)
        val costFun = new LogisticCostFun(standardizedInstances, numClasses, $(fitIntercept),
          $(standardization), bcFeaturesStd, regParamL2, multinomial = true, standardize = false)

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
               P(0) = \exp(b_0) / (\sum_{k=1}^K \exp(b_k))
               ...
               P(K) = \exp(b_K) / (\sum_{k=1}^K \exp(b_k))
             }}}
             The solution to this is not identifiable, so choose the solution with minimum
             L2 penalty (i.e. subtract the mean). Hence,
             {{{
               b_k = \log{count_k / count_0}
               b_k' = b_k - \frac{1}{K} \sum b_k
             }}}
           */
          val referenceCoef = histogram.indices.map { i =>
            if (histogram(i) > 0) {
              math.log(histogram(i) / (histogram(0) + 1)) // add 1 for smoothing
            } else {
              0.0
            }
          }
          val referenceMean = referenceCoef.sum / referenceCoef.length
          histogram.indices.foreach { i =>
            initialCoefficientsWithIntercept.toArray(i * numFeaturesPlusIntercept + numFeatures) =
              referenceCoef(i) - referenceMean
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
        if (handlePersistence) standardizedInstances.unpersist()

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
        var interceptSum = 0.0
        var coefSum = 0.0
        val rawCoefficients = state.x.toArray.clone()
        val coefArray = Array.ofDim[Double](numFeatures * numClasses)
        val interceptArray = Array.ofDim[Double](if (getFitIntercept) numClasses else 0)
        (0 until numClasses).foreach { k =>
          var i = 0
          while (i < numFeatures) {
            val rawValue = rawCoefficients(k * numFeaturesPlusIntercept + i)
            val unscaledCoef =
              rawValue * { if (featuresStd(i) != 0.0) 1.0 / featuresStd(i) else 0.0 }
            coefArray(k * numFeatures + i) = unscaledCoef
            coefSum += unscaledCoef
            i += 1
          }
          if (getFitIntercept) {
            val intercept = rawCoefficients(k * numFeaturesPlusIntercept + numFeatures)
            interceptArray(k) = intercept
            interceptSum += intercept
          }
        }

        val _coefficients = {
          /*
            When no regularization is applied, the coefficients lack identifiability because
            we do not use a pivot class. We can add any constant value to the coefficients and
            get the same likelihood. So here, we choose the mean centered coefficients for
            reproducibility. This method follows the approach in glmnet, described here:

            Friedman, et al. "Regularization Paths for Generalized Linear Models via
              Coordinate Descent," https://core.ac.uk/download/files/153/6287975.pdf
           */
          if ($(regParam) == 0) {
            val coefficientMean = coefSum / (numClasses * numFeatures)
            var i = 0
            while (i < coefArray.length) {
              coefArray(i) -= coefficientMean
              i += 1
            }
          }
          new DenseMatrix(numClasses, numFeatures, coefArray, isTransposed = true)
        }

        val _intercepts = if (getFitIntercept) {
          /*
            The intercepts are never regularized, so we always center the mean.
          */
          val interceptMean = interceptSum / numClasses
          var k = 0
          while (k < interceptArray.length) {
            interceptArray(k) -= interceptMean
            k += 1
          }
          Vectors.dense(interceptArray)
        } else {
          Vectors.sparse(numClasses, Seq())
        }

        (_coefficients, _intercepts, arrayBuilder.result())
      }
    }

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
    val m = margins(features).toDense
    val maxMarginIndex = m.argmax
    val maxMargin = m(maxMarginIndex)

    // adjust margins for overflow
    val sum = {
      var temp = 0.0
      if (maxMargin > 0) {
        for (i <- 0 until numClasses) {
          m.toArray(i) -= maxMargin
          temp += math.exp(m(i))
        }
      } else {
        for (i <- 0 until numClasses ) {
          temp += math.exp(m(i))
        }
      }
      temp
    }

    var i = 0
    while (i < m.size) {
      m.values(i) = math.exp(m.values(i)) / sum
      i += 1
    }
    m
  }

  /**
   * Predict label for the given feature vector.
   * The behavior of this can be adjusted using [[thresholds]].
   */
  override protected def predict(features: Vector): Double = {
    if (isDefined(thresholds)) {
      val thresholds: Array[Double] = getThresholds
      val scaledProbability: Array[Double] =
        scores(features).toArray.zip(thresholds).map { case (p, t) =>
          if (t == 0.0) Double.PositiveInfinity else p / t
        }
      Vectors.dense(scaledProbability).argmax
    } else {
      scores(features).argmax
    }
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        val size = dv.size

        // get the maximum margin
        val maxMarginIndex = rawPrediction.argmax
        val maxMargin = rawPrediction(maxMarginIndex)

        if (maxMargin == Double.PositiveInfinity) {
          for (j <- 0 until size) {
            if (j == maxMarginIndex) {
              dv.values(j) = 1.0
            } else {
              dv.values(j) = 0.0
            }
          }
        } else {
          val sum = {
            var temp = 0.0
            if (maxMargin > 0) {
              // adjust margins for overflow
              for (j <- 0 until numClasses) {
                dv.values(j) -= maxMargin
                temp += math.exp(dv.values(j))
              }
            } else {
              for (j <- 0 until numClasses) {
                temp += math.exp(dv.values(j))
              }
            }
            temp
          }

          // update in place
          var i = 0
          while (i < size) {
            dv.values(i) = math.exp(dv.values(i)) / sum
            i += 1
          }
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
        intercept: Vector,
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
        .select("numClasses", "numFeatures", "intercept", "coefficients").head()
      val numClasses = data.getInt(0)
      val intercepts = data.getAs[Vector](2)
      val coefficients = data.getAs[Matrix](3)
      val model =
        new MultinomialLogisticRegressionModel(metadata.uid, coefficients, intercepts, numClasses)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}
