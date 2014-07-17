package org.apache.spark.mllib.feature

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

private[feature] trait ContingencyTableCalculator extends java.io.Serializable {

  private def indexByLabelMap(discreteData: RDD[LabeledPoint]) = {
    discreteData.map(labeledPoint =>
      labeledPoint.label).distinct.collect.sorted.zipWithIndex.toMap
  }

  private def enumerateValues(discreteData: RDD[LabeledPoint]) = {
    discreteData.flatMap(labeledPoint =>
      (0 to labeledPoint.features.size).zip(labeledPoint.features.toArray))
      .distinct()
      .combineByKey[List[Double]](
        createCombiner = (value: Double) => List(value),
        mergeValue = (c: List[Double], value: Double) => value :: c,
        mergeCombiners = (c1: List[Double], c2: List[Double]) => c1 ::: c2
      ).collectAsMap()
  }

  def tables(discreteData: RDD[LabeledPoint]): RDD[(Int, Array[Array[Int]])] = {
    val indexByLabel = indexByLabelMap(discreteData)
    val classesCount = indexByLabel.size
    val valuesByIndex = enumerateValues(discreteData)
    discreteData.flatMap {
      labeledPoint =>
        labeledPoint.features.toArray.zipWithIndex.map {
          case (featureValue, featureIndex) =>
            /** array of feature presence/absence in a class */
            val featureValues = valuesByIndex(featureIndex)
            val valuesCount = featureValues.size
            val featureValueIndex = featureValues.indexOf(featureValue)
            val labelIndex = indexByLabel(labeledPoint.label)
            val counts = Array.ofDim[Int](valuesCount, classesCount)
            counts(featureValueIndex)(labelIndex) = 1
            (featureIndex, counts)
        }
    }.reduceByKey {
      case (x, y) =>
        x.zip(y).map { case(row1, row2) => row1.zip(row2).map{ case(a, b) => a + b} }
    }
  }
}

private[feature] trait ChiSquared {

  def compute(contingencyTable: Array[Array[Int]]): Double = {
    /** probably use List instead of Array */
    val columns = contingencyTable(0).size
    val rowSums = contingencyTable.map( row => row.sum)
    val columnSums = contingencyTable.fold(Array.ofDim[Int](columns)){ case (ri, rj) =>
      ri.zip(rj).map { case (a1, b1)
      => a1 + b1}}
    val tableSum = rowSums.sum
    val rowRatios = rowSums.map( _.toDouble / tableSum)
    val expectedTable = rowRatios.map( a => columnSums.map(_ * a))
    val chi2 = contingencyTable.zip(expectedTable).foldLeft(0.0){ case(sum, (obsRow, expRow)) =>
      obsRow.zip(expRow).map{ case (oValue, eValue) =>
        sqr(oValue - eValue) / eValue}.sum + sum}
    chi2
  }

  private def sqr(x: Double): Double = x * x
}

class ChiSquaredFeatureSelection(labeledData: RDD[LabeledPoint], numTopFeatures: Int) extends java.io.Serializable
with LabeledPointFeatureFilter with ContingencyTableCalculator with ChiSquared {
  override def data: RDD[LabeledPoint] = labeledData

  override def select: Set[Int] = {
    tables(data).map { case (featureIndex, contTable) =>
      (featureIndex, compute(contTable))}.collect().sortBy(-_._2).take(numTopFeatures).unzip._1.toSet
  }
}