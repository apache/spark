package org.apache.spark.mllib.feature

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

private[feature] trait FeatureFilter extends java.io.Serializable {
  /** methods for feature filtering based on statistics */
  protected def topForClass
}

private[feature] trait CombinationsCalculator extends java.io.Serializable {

  protected def indexByLabelMap(labeledData: RDD[LabeledPoint]) = {
    labeledData.map(labeledPoint =>
      labeledPoint.label).distinct.collect.zipWithIndex.toMap
  }

  protected def featureLabelCombinations(labeledData: RDD[LabeledPoint]) = {
    val indexByLabel = indexByLabelMap(labeledData)
    labeledData.flatMap {
      labeledPoint =>
        labeledPoint.features.toArray.zipWithIndex.map {
          case (featureValue, featureIndex) =>
            /** array of feature presence/absence in a class */
            val counts = Array.fill[(Int, Int)](indexByLabel.size)(0, 0)
            val label = labeledPoint.label
            counts(indexByLabel(label)) = if(featureValue != 0)  (1, 0) else (0, 1)
            (featureIndex, counts)
        }
    }.reduceByKey {
      case (x, y) =>
        x.zip(y).map { case ((a1, b1), (a2, b2)) =>
          (a1 + a2, b1 + b2)}
    }
  }
}

private[feature] class ChiSquared(labeledData: RDD[LabeledPoint])
  extends java.io.Serializable with CombinationsCalculator with FeatureSelection {

  private val indexByLabel = indexByLabelMap(labeledData)
  private val labelsByIndex = indexByLabel.map(_.swap)
  private val combinations = featureLabelCombinations(labeledData)

  /** not private for test purposes */
  lazy val chi2Data: RDD[((Int, Double), Double)] = combinations.flatMap {
    case (featureIndex, counts) =>
      val (featureClassCounts, notFeatureClassCounts) = counts.unzip
      val featureCount = featureClassCounts.sum
      val notFeatureCount = notFeatureClassCounts.sum
      val notFeatureNotClassCounts = notFeatureClassCounts.map(notFeatureCount - _)
      val featureNotClassCounts = featureClassCounts.map(featureCount - _)
      val iCounts = counts.zipWithIndex
      iCounts.map { case ((a, b), labelIndex) =>
        val n11 = featureClassCounts(labelIndex)
        val n10 = featureNotClassCounts(labelIndex)
        val n01 = notFeatureClassCounts(labelIndex)
        val n00 = notFeatureNotClassCounts(labelIndex)
        val chi2 = (n11 + n10 + n01 + n00) * sqr(n11 * n00 - n10 * n01).toDouble /
          ((n11 + n01) * (n11 + n10) * (n10 + n00) * (n01 + n00))
        ((featureIndex, labelsByIndex(labelIndex)), chi2)}
  }
  private def sqr(x: Int): Int = x * x

  override def select(data: RDD[LabeledPoint]): Set[Int] = {
    /** the actual selection process should run here*/
    Set(1, 2)
  }
}

object ChiSquared{
  def compute(labeledData: RDD[LabeledPoint]) = new ChiSquared(labeledData)
}