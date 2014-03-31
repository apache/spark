package org.apache.spark.mllib.evaluation

import org.scalatest.FunSuite
import org.apache.spark.mllib.util.LocalSparkContext

class BinaryClassificationEvaluationSuite extends FunSuite with LocalSparkContext {
  test("test") {
    val data = Seq((0.0, 0.0), (0.1, 0.0), (0.1, 1.0), (0.4, 0.0), (0.6, 0.0), (0.6, 1.0), (0.6, 1.0), (0.8, 1.0), (0.9, 1.0))
    BinaryClassificationEvaluator.get(data)
    val rdd = sc.parallelize(data, 3)
    BinaryClassificationEvaluator.get(rdd)
  }
}
