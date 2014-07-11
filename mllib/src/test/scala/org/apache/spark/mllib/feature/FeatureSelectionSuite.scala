package org.apache.spark.mllib.feature

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.LocalSparkContext
import org.scalatest.FunSuite

class FeatureSelectionSuite extends FunSuite with LocalSparkContext{

  test("FeatureSelection test") {
    val labeledData = sc.parallelize(
      Seq( new LabeledPoint(0.0, Vectors.sparse(3, Seq((0, 8.8),(1, 9.9)))),
        new LabeledPoint(1.0, Vectors.sparse(3, Seq((0, 1.1),(2, 3.3)))),
        new LabeledPoint(1.0, Vectors.sparse(3, Seq((1, 2.2),(2, 4.4))))
      ), 2)
    labeledData.foreach(println)
    val filteredData = new ChiSquared(labeledData).filter
    filteredData.foreach(println)
  }

}
