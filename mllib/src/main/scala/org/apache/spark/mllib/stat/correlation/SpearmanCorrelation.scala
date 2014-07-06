package org.apache.spark.mllib.stat.correlation

import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.rdd.RDD

class SpearmanCorrelation extends Correlation{
  override def computeCorrelationMatrix(x: RDD[Double], y: RDD[Double]): Matrix = ???

  override def computeCorrelationMatrix(X: RDD[Vector]): Matrix = {
    //fork the RDD into a numCols RDDs
  }

}
