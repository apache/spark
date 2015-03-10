package org.apache.spark.ml.regression.benchmark

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.FunSuite

class LinearRegressionBenchmarkSuite extends FunSuite with MLlibTestSparkContext {

  test("linear regression benchmark"){
    
    val lr = new LinearRegression()
      .setStepSize(0.00000001)
      .setMaxIter(70)
    
    RegressionBenchmarkCases.testLogisticInput(lr, sc)
    RegressionBenchmarkCases.testMachineData(lr, sc)
  }
  
}
