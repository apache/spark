package org.apache.spark.mllib.clustering

import org.apache.spark.Logging
import org.apache.spark.mllib.base.FP
import org.apache.spark.rdd.RDD


private[mllib] trait MultiKMeansClusterer[P <: FP, C <: FP] extends Serializable with Logging {
  def cluster(data: RDD[P], centers: Array[Array[C]]): (Double, GeneralizedKMeansModel[P, C])

}
