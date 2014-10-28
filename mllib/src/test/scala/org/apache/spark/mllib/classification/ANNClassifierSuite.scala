package org.apache.spark.mllib.classification

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.LocalSparkContext
import org.scalatest.FunSuite

class ANNClassifierSuite extends FunSuite with LocalSparkContext {

  test("ANN classifier test"){
    val inputs = Array[Array[Double]](
      Array[Double](0,0),
      Array[Double](0,1),
      Array[Double](1,0),
      Array[Double](1,1)
    )
    val outputs = Array[Double](0, 1, 1, 0)
    val data = inputs.zip(outputs).map{ case(input, output) =>
      new LabeledPoint(output, Vectors.dense(input))}
    val rddData = sc.parallelize(data, 2)
    val model = ANNClassifier.train(rddData)
    val predictionAndLabels = rddData.map(lp => (model.predict(lp.features), lp.label))
    predictionAndLabels.foreach(println)
  }

}
