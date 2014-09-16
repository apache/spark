package org.apache.spark.mllib.clustering

import org.apache.spark.mllib.base.FP
import org.apache.spark.rdd.RDD

private[mllib] trait KMeansInitializer[P <: FP, C <: FP] extends Serializable {
  def init(data: RDD[P], seed: Int): Array[Array[C]]
}
