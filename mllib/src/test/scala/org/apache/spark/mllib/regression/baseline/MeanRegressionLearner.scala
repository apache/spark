package org.apache.spark.mllib.regression.baseline

import org.apache.spark.mllib.regression.{LabeledPoint, RegressionLearner, RegressionModel}
import org.apache.spark.rdd.{DoubleRDDFunctions, RDD}

class MeanRegressionLearner extends RegressionLearner {

  def train(trainData: RDD[LabeledPoint]): RegressionModel = {
    new MeanRegressionModel(new DoubleRDDFunctions(trainData.map(_.label)).mean())
  }

}
