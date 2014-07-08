package org.apache.spark.mllib.feature

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

class ChiSquared(labeledData:RDD[LabeledPoint]) extends java.io.Serializable {

  val indexByLabel = labeledData.map(labeledPoint =>
    labeledPoint.label).distinct.collect.zipWithIndex.toMap
  val labelsByIndex = indexByLabel.map(_.swap)

  lazy val chi2Data: RDD[((Int, Double), Double)] = labeledData.flatMap {
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
  }.flatMap {
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
  private def sqr(x:Int):Int = x * x
}
