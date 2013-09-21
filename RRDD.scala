package org.apache.spark.api.r

import org.apache.spark.api.java.{JavaSparkContext, JavaRDD}
import scala.collection.JavaConversions._

object RRDD {

  def createRDDFromArray(jsc: JavaSparkContext, arr: Array[Array[Byte]]): JavaRDD[Array[Byte]] = {
    JavaRDD.fromRDD(jsc.sc.parallelize(arr, arr.length))
  }

}