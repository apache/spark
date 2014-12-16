package org.apache.spark.ml.classification

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.ml.LabeledPoint
import org.apache.spark.ml.evaluation.ClassificationEvaluator
import org.apache.spark.ml.param.{HasWeightCol, Param, ParamMap, HasMaxIter}
import org.apache.spark.ml.impl.estimator.{ProbabilisticClassificationModel, WeakLearner,
  IterativeEstimator, IterativeSolver}


private[classification] trait AdaBoostParams extends ClassifierParams
  with HasMaxIter with HasWeightCol {

  /** param for weak learner type */
  val weakLearner: Param[Classifier[_, _]] =
    new Param(this, "weakLearner", "weak learning algorithm")
  def getWeakLearner: Classifier[_, _] = get(weakLearner)

  /** param for weak learner param maps */
  val weakLearnerParamMap: Param[ParamMap] =
    new Param(this, "weakLearnerParamMap", "param map for the weak learner")
  def getWeakLearnerParamMap: ParamMap = get(weakLearnerParamMap)

  override def validate(paramMap: ParamMap): Unit = {
    // TODO: Check maxIter, weakLearner, weakLearnerParamMap, weightCol
    // Check: If the weak learner does not extend WeakLearner, then featuresColName should be
    //        castable to FeaturesType.
  }
}


/**
 * AdaBoost
 *
 * Developer notes:
 *  - If the weak learner implements the [[WeakLearner]]
 */
class AdaBoost extends Classifier[AdaBoost, AdaBoostModel]
  with AdaBoostParams
  with IterativeEstimator[AdaBoostModel] {

  def setMaxIter(value: Int): this.type = set(maxIter, value)
  def setWeightCol(value: String): this.type = set(weightCol, value)
  def setWeakLearner(value: Classifier[_, _]): this.type = set(weakLearner, value)
  def setWeakLearnerParamMap(value: ParamMap): this.type = set(weakLearnerParamMap, value)

  /**
   * Extract LabeledPoints, using the weak learner's native feature representation if possible.
   * @param paramMap  Complete paramMap (after combining with the internal paramMap)
   */
  private def extractLabeledPoints(dataset: SchemaRDD, paramMap: ParamMap): RDD[LabeledPoint] = {
    import dataset.sqlContext._
    val featuresColName = paramMap(featuresCol)
    val wl = paramMap(weakLearner)
    val featuresRDD: RDD[Vector] = wl match {
      case wlTagged: WeakLearner[_] =>
        val wlParamMap = paramMap(weakLearnerParamMap)
        val wlFeaturesColName = wlParamMap(wl.featuresCol)
        // TODO: How do I get this to use the string value of wlFeaturesColName?
        val origFeaturesRDD = dataset.select(featuresColName.attr).as('wlFeaturesColName)
        wlTagged.getNativeFeatureRDD(origFeaturesRDD, wlParamMap)
      case _ =>
        dataset.select(featuresColName.attr).map { case Row(features: Vector) => features }
    }

    val labelColName = paramMap(labelCol)
    if (paramMap.contains(weightCol)) {
      val weightColName = paramMap(weightCol)
      dataset.select(labelColName.attr, weightColName.attr)
        .zip(featuresRDD).map { case (Row(label: Double, weight: Double), features: Vector) =>
        LabeledPoint(label, features, weight)
      }
    } else {
      dataset.select(labelColName.attr)
        .zip(featuresRDD).map { case (Row(label: Double), features: Vector) =>
        LabeledPoint(label, features)
      }
    }
  }

  // From Classifier
  override def fit(dataset: SchemaRDD, paramMap: ParamMap): AdaBoostModel = {
    val map = this.paramMap ++ paramMap
    val labeledPoints: RDD[LabeledPoint] = extractLabeledPoints(dataset, map)
    train(labeledPoints, paramMap)
  }

  // From IterativeEstimator
  override private[ml] def createSolver(dataset: SchemaRDD, paramMap: ParamMap): AdaBoostSolver = {
    val map = this.paramMap ++ paramMap
    val labeledPoints: RDD[LabeledPoint] = extractLabeledPoints(dataset, map)
    new AdaBoostSolver(labeledPoints, this, map)
  }

  // From Predictor
  override def train(dataset: RDD[LabeledPoint], paramMap: ParamMap): AdaBoostModel = {
    val map = this.paramMap ++ paramMap
    val solver = new AdaBoostSolver(dataset, this, map)
    while (solver.step()) { }
    solver.currentModel
  }
}


class AdaBoostModel private[ml] (
    val weakHypotheses: Array[ClassificationModel[_]],
    val weakHypothesisWeights: Array[Double],
    override val parent: AdaBoost,
    override val fittingParamMap: ParamMap)
  extends ClassificationModel[AdaBoostModel]
  with ProbabilisticClassificationModel
  with AdaBoostParams {

  require(weakHypotheses.size != 0)
  require(weakHypotheses.size == weakHypothesisWeights.size)

  // From Classifier.Model:
  override val numClasses: Int = weakHypotheses(0).numClasses

  require(weakHypotheses.forall(_.numClasses == numClasses))

  private val margin: Vector => Double = (features) => {
    weakHypotheses.zip(weakHypothesisWeights)
      .foldLeft(0.0) { case (total: Double, (wh: ClassificationModel[_], weight: Double)) =>
      val pred = if (wh.predict(features) == 1.0) 1.0 else -1.0
      total + weight * pred
    }
  }

  private val score: Vector => Double = (features) => {
    val m = margin(features)
    1.0 / (1.0 + math.exp(-2.0 * m))
  }

  override def predictProbabilities(features: Vector): Vector = {
    val s = score(features)
    Vectors.dense(Array(1.0 - s, s))
  }

  override def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(Array(-m, m))
  }
}


private[ml] class AdaBoostSolver(
    val origData: RDD[LabeledPoint],
    val parent: AdaBoost,
    val paramMap: ParamMap) extends IterativeSolver[AdaBoostModel] {

  private val weakHypotheses = new ArrayBuffer[ClassificationModel[_]]
  private val weakHypothesisWeights = new ArrayBuffer[Double]

  private val wl: Classifier[_, _] = paramMap(parent.weakLearner)
  private val wlParamMap = paramMap(parent.weakLearnerParamMap)
  override val maxIterations: Int = paramMap(parent.maxIter)

  // TODO: Decide if this alg should cache data, or if that should be left to the user.

  // TODO: check for weights = 0
  // TODO: EDITING HERE NOW: switch to log weights
  private var logInstanceWeights: RDD[Double] = origData.map(lp => math.log(lp.weight))

  override def stepImpl(): Boolean = ??? /*{
    // Check if the weak learner takes instance weights.
    val wlDataset = wl match {
      case wlWeighted: HasWeightCol =>
        origData.zip(logInstanceWeights).map { case (lp: LabeledPoint, logWeight: Double) =>
          LabeledPoint(lp.label, lp.features, weight)
        }
      case _ =>
        // Subsample data to simulate the current instance weight distribution.
        // TODO: This needs to be done before AdaBoost is committed.
        throw new NotImplementedError(
          "AdaBoost currently requires that the weak learning algorithm accept instance weights.")
    }
    // Train the weak learning algorithm.
    val weakHypothesis: ClassificationModel[_] = wl match {
      case wlTagged: WeakLearner[_] =>
        // This lets the weak learner know that the features are in its native format.
        wlTagged.trainNative(wlDataset, wlParamMap).asInstanceOf[ClassificationModel[_]]
      case _ =>
        wl.train(wlDataset, wlParamMap).asInstanceOf[ClassificationModel[_]]
    }
    // Add the weighted weak hypothesis to the ensemble.
    // TODO: Handle instance weights.
    val predictionsAndLabels = wlDataset.map(lp => weakHypothesis.predict(lp.features))
      .zip(wlDataset.map(_.label))
    val eps = ClassificationEvaluator.computeMetric(predictionsAndLabels, "accuracy")
    val alpha = 0.5 * (math.log(1.0 - eps) - math.log(eps)) // TODO: handle eps near 0
    weakHypotheses += weakHypothesis
    weakHypothesisWeights += alpha
    // Update weights.
    val newInstanceWeights = instanceWeights.zip(predictionsAndLabels).map {
      case (weight: Double, (pred: Double, label: Double)) =>
        ???
    }

  }*/

  override def currentModel: AdaBoostModel = {
    new AdaBoostModel(weakHypotheses.toArray, weakHypothesisWeights.toArray, parent, paramMap)
  }
}
