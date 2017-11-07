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

package org.apache.spark.ml.summary

import java.util.Locale

import breeze.stats.{distributions => dist}
import breeze.stats.distributions.StudentsT
import org.apache.commons.lang3.StringUtils

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, GeneralizedLinearRegressionModel,
  LinearRegressionModel}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
 * :: Experimental ::
 *
 * Summary of [[GeneralizedLinearRegression]] model and predictions.
 *
 * @param dataset Dataset to be summarized.
 * @param origModel Model to be summarized.  This is copied to create an internal
 *                  model which cannot be modified from outside.
 */
@Since("2.0.0")
@Experimental
class GeneralizedLinearRegressionSummary private[ml] (
    dataset: Dataset[_],
    origModel: GeneralizedLinearRegressionModel) extends Summary {

  import GeneralizedLinearRegression._

  /**
   * Field in "predictions" which gives the predicted value of each instance.
   * This is set to a new column name if the original model's `predictionCol` is not set.
   */
  @Since("2.0.0")
  override val predictionCol: String = {
    if (origModel.isDefined(origModel.predictionCol) && origModel.getPredictionCol.nonEmpty) {
      origModel.getPredictionCol
    } else {
      "prediction_" + java.util.UUID.randomUUID.toString
    }
  }

  /**
   * Private copy of model to ensure Params are not modified outside this class.
   * Coefficients is not a deep copy, but that is acceptable.
   *
   * @note [[predictionCol]] must be set correctly before the value of [[model]] is set,
   * and [[model]] must be set before [[predictions]] is set!
   */
  protected val model: GeneralizedLinearRegressionModel =
    origModel.copy(ParamMap.empty).setPredictionCol(predictionCol)

  /**
   * Predictions output by the model's `transform` method.
   */
  @Since("2.0.0") @transient override val predictions: DataFrame = model.transform(dataset)

  @Since("2.3.0")
  override def featuresCol: String = model.getFeaturesCol

  private[ml] lazy val familyLink: FamilyAndLink = FamilyAndLink(model)

  private[ml] lazy val family: Family = familyLink.family

  private[ml] lazy val link: Link = familyLink.link

  /** Number of instances in DataFrame predictions. */
  @Since("2.2.0")
  lazy val numInstances: Long = predictions.count()


  /**
   * Name of features. If the name cannot be retrieved from attributes,
   * set default names to feature column name with numbered suffix "_0", "_1", and so on.
   */
  private[ml] lazy val featureNames: Array[String] = {
    val featureAttrs = AttributeGroup.fromStructField(
      dataset.schema(model.getFeaturesCol)).attributes
    if (featureAttrs.isDefined) {
      featureAttrs.get.map(_.name.get)
    } else {
      Array.tabulate[String](origModel.numFeatures)((x: Int) => model.getFeaturesCol + "_" + x)
    }
  }

  /** The numeric rank of the fitted linear model. */
  @Since("2.0.0")
  lazy val rank: Long = if (model.getFitIntercept) {
    model.coefficients.size + 1
  } else {
    model.coefficients.size
  }

  /** Degrees of freedom. */
  @Since("2.0.0")
  lazy val degreesOfFreedom: Long = numInstances - rank

  /** The residual degrees of freedom. */
  @Since("2.0.0")
  lazy val residualDegreeOfFreedom: Long = degreesOfFreedom

  /** The residual degrees of freedom for the null model. */
  @Since("2.0.0")
  lazy val residualDegreeOfFreedomNull: Long = {
    if (model.getFitIntercept) numInstances - 1 else numInstances
  }

  private def label: Column = col(model.getLabelCol).cast(DoubleType)

  private def prediction: Column = col(predictionCol)

  private def weight: Column = {
    if (!model.hasWeightCol) lit(1.0) else col(model.getWeightCol)
  }

  private def offset: Column = {
    if (!model.hasOffsetCol) lit(0.0) else col(model.getOffsetCol).cast(DoubleType)
  }

  private[ml] lazy val devianceResiduals: DataFrame = {
    val drUDF = udf { (y: Double, mu: Double, weight: Double) =>
      val r = math.sqrt(math.max(family.deviance(y, mu, weight), 0.0))
      if (y > mu) r else -1.0 * r
    }
    predictions.select(
      drUDF(label, prediction, weight).as("devianceResiduals"))
  }

  private[ml] lazy val pearsonResiduals: DataFrame = {
    val prUDF = udf { mu: Double => family.variance(mu) }
    predictions.select(label.minus(prediction)
      .multiply(sqrt(weight)).divide(sqrt(prUDF(prediction))).as("pearsonResiduals"))
  }

  private[ml] lazy val workingResiduals: DataFrame = {
    val wrUDF = udf { (y: Double, mu: Double) => (y - mu) * link.deriv(mu) }
    predictions.select(wrUDF(label, prediction).as("workingResiduals"))
  }

  private[ml] lazy val responseResiduals: DataFrame = {
    predictions.select(label.minus(prediction).as("responseResiduals"))
  }

  /**
   * Get the default residuals (deviance residuals) of the fitted model.
   */
  @Since("2.0.0")
  def residuals(): DataFrame = devianceResiduals

  /**
   * Get the residuals of the fitted model by type.
   *
   * @param residualsType The type of residuals which should be returned.
   *                      Supported options: deviance, pearson, working and response.
   */
  @Since("2.0.0")
  def residuals(residualsType: String): DataFrame = {
    residualsType match {
      case "deviance" => devianceResiduals
      case "pearson" => pearsonResiduals
      case "working" => workingResiduals
      case "response" => responseResiduals
      case other => throw new UnsupportedOperationException(
        s"The residuals type $other is not supported by Generalized Linear Regression.")
    }
  }

  /**
   * The deviance for the null model.
   */
  @Since("2.0.0")
  lazy val nullDeviance: Double = {
    val intercept: Double = if (!model.getFitIntercept) {
      0.0
    } else {
      /*
        Estimate intercept analytically when there is no offset, or when there is offset but
        the model is Gaussian family with identity link. Otherwise, fit an intercept only model.
      */
      if (!model.hasOffsetCol ||
        (model.hasOffsetCol && family == Gaussian && link == Identity)) {
        val agg = predictions.agg(sum(weight.multiply(
          label.minus(offset))), sum(weight)).first()
        link.link(agg.getDouble(0) / agg.getDouble(1))
      } else {
        // Create empty feature column and fit intercept only model using param setting from model
        val featureNull = "feature_" + java.util.UUID.randomUUID.toString
        val paramMap = model.extractParamMap()
        paramMap.put(model.featuresCol, featureNull)
        if (family.name != "tweedie") {
          paramMap.remove(model.variancePower)
        }
        val emptyVectorUDF = udf{ () => Vectors.zeros(0) }
        model.parent.fit(
          dataset.withColumn(featureNull, emptyVectorUDF()), paramMap
        ).intercept
      }
    }
    predictions.select(label, offset, weight).rdd.map {
      case Row(y: Double, offset: Double, weight: Double) =>
        family.deviance(y, link.unlink(intercept + offset), weight)
    }.sum()
  }

  /**
   * The deviance for the fitted model.
   */
  @Since("2.0.0")
  lazy val deviance: Double = {
    predictions.select(label, prediction, weight).rdd.map {
      case Row(label: Double, pred: Double, weight: Double) =>
        family.deviance(label, pred, weight)
    }.sum()
  }

  /**
   * The dispersion of the fitted model.
   * It is taken as 1.0 for the "binomial" and "poisson" families, and otherwise
   * estimated by the residual Pearson's Chi-Squared statistic (which is defined as
   * sum of the squares of the Pearson residuals) divided by the residual degrees of freedom.
   */
  @Since("2.0.0")
  lazy val dispersion: Double = if (
    model.getFamily.toLowerCase(Locale.ROOT) == Binomial.name ||
      model.getFamily.toLowerCase(Locale.ROOT) == Poisson.name) {
    1.0
  } else {
    val rss = pearsonResiduals.agg(sum(pow(col("pearsonResiduals"), 2.0))).first().getDouble(0)
    rss / degreesOfFreedom
  }

  /** Akaike Information Criterion (AIC) for the fitted model. */
  @Since("2.0.0")
  lazy val aic: Double = {
    val weightSum = predictions.select(weight).agg(sum(weight)).first().getDouble(0)
    val t = predictions.select(
      label, prediction, weight).rdd.map {
      case Row(label: Double, pred: Double, weight: Double) =>
        (label, pred, weight)
    }
    family.aic(t, deviance, numInstances, weightSum) + 2 * rank
  }
}

/**
 * :: Experimental ::
 *
 * Summary of [[GeneralizedLinearRegression]] fitting and model.
 *
 * @param dataset Dataset to be summarized.
 * @param origModel Model to be summarized.  This is copied to create an internal
 *                  model which cannot be modified from outside.
 * @param diagInvAtWA diagonal of matrix (A^T * W * A)^-1 in the last iteration
 * @param numIterations number of iterations
 * @param solver the solver algorithm used for model training
 */
@Since("2.0.0")
@Experimental
class GeneralizedLinearRegressionTrainingSummary private[ml] (
    dataset: Dataset[_],
    origModel: GeneralizedLinearRegressionModel,
    private val diagInvAtWA: Array[Double],
    @Since("2.0.0") val numIterations: Int,
    @Since("2.0.0") val solver: String)
  extends GeneralizedLinearRegressionSummary(dataset, origModel) {

  import GeneralizedLinearRegression._

  /**
   * Whether the underlying `WeightedLeastSquares` using the "normal" solver.
   */
  private[ml] val isNormalSolver: Boolean = {
    diagInvAtWA.length != 1 || diagInvAtWA(0) != 0
  }

  /**
   * Standard error of estimated coefficients and intercept.
   * This value is only available when the underlying `WeightedLeastSquares`
   * using the "normal" solver.
   *
   * If `GeneralizedLinearRegression.fitIntercept` is set to true,
   * then the last element returned corresponds to the intercept.
   */
  @Since("2.0.0")
  lazy val coefficientStandardErrors: Array[Double] = {
    if (isNormalSolver) {
      diagInvAtWA.map(_ * dispersion).map(math.sqrt)
    } else {
      throw new UnsupportedOperationException(
        "No Std. Error of coefficients available for this GeneralizedLinearRegressionModel")
    }
  }

  /**
   * T-statistic of estimated coefficients and intercept.
   * This value is only available when the underlying `WeightedLeastSquares`
   * using the "normal" solver.
   *
   * If `GeneralizedLinearRegression.fitIntercept` is set to true,
   * then the last element returned corresponds to the intercept.
   */
  @Since("2.0.0")
  lazy val tValues: Array[Double] = {
    if (isNormalSolver) {
      val estimate = if (model.getFitIntercept) {
        Array.concat(model.coefficients.toArray, Array(model.intercept))
      } else {
        model.coefficients.toArray
      }
      estimate.zip(coefficientStandardErrors).map { x => x._1 / x._2 }
    } else {
      throw new UnsupportedOperationException(
        "No t-statistic available for this GeneralizedLinearRegressionModel")
    }
  }

  /**
   * Two-sided p-value of estimated coefficients and intercept.
   * This value is only available when the underlying `WeightedLeastSquares`
   * using the "normal" solver.
   *
   * If `GeneralizedLinearRegression.fitIntercept` is set to true,
   * then the last element returned corresponds to the intercept.
   */
  @Since("2.0.0")
  lazy val pValues: Array[Double] = {
    if (isNormalSolver) {
      if (model.getFamily.toLowerCase(Locale.ROOT) == Binomial.name ||
        model.getFamily.toLowerCase(Locale.ROOT) == Poisson.name) {
        tValues.map { x => 2.0 * (1.0 - dist.Gaussian(0.0, 1.0).cdf(math.abs(x))) }
      } else {
        tValues.map { x =>
          2.0 * (1.0 - dist.StudentsT(degreesOfFreedom.toDouble).cdf(math.abs(x)))
        }
      }
    } else {
      throw new UnsupportedOperationException(
        "No p-value available for this GeneralizedLinearRegressionModel")
    }
  }

  /**
   * Coefficients with statistics: feature name, coefficients, standard error, tValue and pValue.
   */
  private[ml] lazy val coefficientsWithStatistics: Array[
    (String, Double, Double, Double, Double)] = {
    var featureNamesLocal = featureNames
    var coefficientsArray = model.coefficients.toArray
    var index = Array.range(0, coefficientsArray.length)
    if (model.getFitIntercept) {
      featureNamesLocal = featureNamesLocal :+ "(Intercept)"
      coefficientsArray = coefficientsArray :+ model.intercept
      // Reorder so that intercept comes first
      index = (coefficientsArray.length - 1) +: index
    }
    index.map { i =>
      (featureNamesLocal(i), coefficientsArray(i), coefficientStandardErrors(i),
        tValues(i), pValues(i))
    }
  }

  override def toString: String = {
    if (isNormalSolver) {

      def round(x: Double): String = {
        BigDecimal(x).setScale(4, BigDecimal.RoundingMode.HALF_UP).toString
      }

      val colNames = Array("Feature", "Estimate", "Std Error", "T Value", "P Value")

      val data = coefficientsWithStatistics.map { row =>
        val strRow = row.productIterator.map { cell =>
          val str = cell match {
            case s: String => s
            case n: Double => round(n)
          }
          // Truncate if length > 20
          if (str.length > 20) {
            str.substring(0, 17) + "..."
          } else {
            str
          }
        }
        strRow.toArray
      }

      // Compute the width of each column
      val colWidths = colNames.map(_.length)
      data.foreach { strRow =>
        strRow.zipWithIndex.foreach { case (cell: String, i: Int) =>
          colWidths(i) = math.max(colWidths(i), cell.length)
        }
      }

      val sb = new StringBuilder

      // Output coefficients with statistics
      sb.append("Coefficients:\n")
      colNames.zipWithIndex.map { case (colName: String, i: Int) =>
        StringUtils.leftPad(colName, colWidths(i))
      }.addString(sb, "", " ", "\n")

      data.foreach { case strRow: Array[String] =>
        strRow.zipWithIndex.map { case (cell: String, i: Int) =>
          StringUtils.leftPad(cell.toString, colWidths(i))
        }.addString(sb, "", " ", "\n")
      }

      sb.append("\n")
      sb.append(s"(Dispersion parameter for ${family.name} family taken to be " +
        s"${round(dispersion)})")

      sb.append("\n")
      val nd = s"Null deviance: ${round(nullDeviance)} on $degreesOfFreedom degrees of freedom"
      val rd = s"Residual deviance: ${round(deviance)} on $residualDegreeOfFreedom degrees of " +
        "freedom"
      val l = math.max(nd.length, rd.length)
      sb.append(StringUtils.leftPad(nd, l))
      sb.append("\n")
      sb.append(StringUtils.leftPad(rd, l))

      if (family.name != "tweedie") {
        sb.append("\n")
        sb.append(s"AIC: " + round(aic))
      }

      sb.toString()
    } else {
      throw new UnsupportedOperationException(
        "No summary available for this GeneralizedLinearRegressionModel")
    }
  }
}

/**
 * :: Experimental ::
 * Linear regression results evaluated on a dataset.
 *
 * @param predictions predictions output by the model's `transform` method.
 * @param predictionCol Field in "predictions" which gives the predicted value of the label at
 *                      each instance.
 * @param labelCol Field in "predictions" which gives the true label of each instance.
 * @param featuresCol Field in "predictions" which gives the features of each instance as a vector.
 */
@Since("1.5.0")
@Experimental
class LinearRegressionSummary private[ml] (
    @transient override val predictions: DataFrame,
    override val predictionCol: String,
    val labelCol: String,
    override val featuresCol: String,
    private val privateModel: LinearRegressionModel,
    private val diagInvAtWA: Array[Double]) extends Summary {

  @transient private val metrics = new RegressionMetrics(
    predictions
      .select(col(predictionCol), col(labelCol).cast(DoubleType))
      .rdd
      .map { case Row(pred: Double, label: Double) => (pred, label) },
    !privateModel.getFitIntercept)

  /**
   * Returns the explained variance regression score.
   * explainedVariance = 1 - variance(y - \hat{y}) / variance(y)
   * Reference: <a href="http://en.wikipedia.org/wiki/Explained_variation">
   * Wikipedia explain variation</a>
   *
   * @note This ignores instance weights (setting all to 1.0) from `LinearRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
  val explainedVariance: Double = metrics.explainedVariance

  /**
   * Returns the mean absolute error, which is a risk function corresponding to the
   * expected value of the absolute error loss or l1-norm loss.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LinearRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
  val meanAbsoluteError: Double = metrics.meanAbsoluteError

  /**
   * Returns the mean squared error, which is a risk function corresponding to the
   * expected value of the squared error loss or quadratic loss.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LinearRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
  val meanSquaredError: Double = metrics.meanSquaredError

  /**
   * Returns the root mean squared error, which is defined as the square root of
   * the mean squared error.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LinearRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
  val rootMeanSquaredError: Double = metrics.rootMeanSquaredError

  /**
   * Returns R^2^, the coefficient of determination.
   * Reference: <a href="http://en.wikipedia.org/wiki/Coefficient_of_determination">
   * Wikipedia coefficient of determination</a>
   *
   * @note This ignores instance weights (setting all to 1.0) from `LinearRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
  val r2: Double = metrics.r2

  /** Residuals (label - predicted value) */
  @Since("1.5.0")
  @transient lazy val residuals: DataFrame = {
    val t = udf { (pred: Double, label: Double) => label - pred }
    predictions.select(t(col(predictionCol), col(labelCol)).as("residuals"))
  }

  /** Number of instances in DataFrame predictions */
  lazy val numInstances: Long = predictions.count()

  /** Degrees of freedom */
  @Since("2.2.0")
  val degreesOfFreedom: Long = if (privateModel.getFitIntercept) {
    numInstances - privateModel.coefficients.size - 1
  } else {
    numInstances - privateModel.coefficients.size
  }

  /**
   * The weighted residuals, the usual residuals rescaled by
   * the square root of the instance weights.
   */
  lazy val devianceResiduals: Array[Double] = {
    val weighted =
      if (!privateModel.isDefined(privateModel.weightCol) || privateModel.getWeightCol.isEmpty) {
        lit(1.0)
      } else {
        sqrt(col(privateModel.getWeightCol))
      }
    val dr = predictions
      .select(col(privateModel.getLabelCol).minus(col(privateModel.getPredictionCol))
        .multiply(weighted).as("weightedResiduals"))
      .select(min(col("weightedResiduals")).as("min"), max(col("weightedResiduals")).as("max"))
      .first()
    Array(dr.getDouble(0), dr.getDouble(1))
  }

  /**
   * Standard error of estimated coefficients and intercept.
   * This value is only available when using the "normal" solver.
   *
   * If `LinearRegression.fitIntercept` is set to true,
   * then the last element returned corresponds to the intercept.
   *
   * @see `LinearRegression.solver`
   */
  lazy val coefficientStandardErrors: Array[Double] = {
    if (diagInvAtWA.length == 1 && diagInvAtWA(0) == 0) {
      throw new UnsupportedOperationException(
        "No Std. Error of coefficients available for this LinearRegressionModel")
    } else {
      val rss =
        if (!privateModel.isDefined(privateModel.weightCol) || privateModel.getWeightCol.isEmpty) {
          meanSquaredError * numInstances
        } else {
          val t = udf { (pred: Double, label: Double, weight: Double) =>
            math.pow(label - pred, 2.0) * weight }
          predictions.select(t(col(privateModel.getPredictionCol), col(privateModel.getLabelCol),
            col(privateModel.getWeightCol)).as("wse")).agg(sum(col("wse"))).first().getDouble(0)
        }
      val sigma2 = rss / degreesOfFreedom
      diagInvAtWA.map(_ * sigma2).map(math.sqrt)
    }
  }

  /**
   * T-statistic of estimated coefficients and intercept.
   * This value is only available when using the "normal" solver.
   *
   * If `LinearRegression.fitIntercept` is set to true,
   * then the last element returned corresponds to the intercept.
   *
   * @see `LinearRegression.solver`
   */
  lazy val tValues: Array[Double] = {
    if (diagInvAtWA.length == 1 && diagInvAtWA(0) == 0) {
      throw new UnsupportedOperationException(
        "No t-statistic available for this LinearRegressionModel")
    } else {
      val estimate = if (privateModel.getFitIntercept) {
        Array.concat(privateModel.coefficients.toArray, Array(privateModel.intercept))
      } else {
        privateModel.coefficients.toArray
      }
      estimate.zip(coefficientStandardErrors).map { x => x._1 / x._2 }
    }
  }

  /**
   * Two-sided p-value of estimated coefficients and intercept.
   * This value is only available when using the "normal" solver.
   *
   * If `LinearRegression.fitIntercept` is set to true,
   * then the last element returned corresponds to the intercept.
   *
   * @see `LinearRegression.solver`
   */
  lazy val pValues: Array[Double] = {
    if (diagInvAtWA.length == 1 && diagInvAtWA(0) == 0) {
      throw new UnsupportedOperationException(
        "No p-value available for this LinearRegressionModel")
    } else {
      tValues.map { x => 2.0 * (1.0 - StudentsT(degreesOfFreedom.toDouble).cdf(math.abs(x))) }
    }
  }

}


/**
 * :: Experimental ::
 *
 * Linear regression training results. Currently, the training summary ignores the
 * training weights except for the objective trace.
 *
 * @param predictions predictions output by the model's `transform` method.
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
@Since("1.5.0")
@Experimental
class LinearRegressionTrainingSummary private[ml] (
    predictions: DataFrame,
    predictionCol: String,
    labelCol: String,
    featuresCol: String,
    model: LinearRegressionModel,
    diagInvAtWA: Array[Double],
    val objectiveHistory: Array[Double])
  extends LinearRegressionSummary(
    predictions,
    predictionCol,
    labelCol,
    featuresCol,
    model,
    diagInvAtWA) {

  /**
   * Number of training iterations until termination
   *
   * This value is only available when using the "l-bfgs" solver.
   *
   * @see `LinearRegression.solver`
   */
  @Since("1.5.0")
  val totalIterations = objectiveHistory.length

}
