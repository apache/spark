package org.apache.spark.mllib.grouped

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
 * Created by kellrott on 7/2/14.
 */
object GroupedUtils {

  def kFoldTrainingUnion[T: ClassTag] (input: Array[(RDD[T], RDD[T])]) : RDD[(Int,T)] = {
    if (input.length == 0) {
      return null
    }
    val sc = input(0)._1.context
    sc.union( input.zipWithIndex.map( x => x._1._1.map( y => (x._2, y)) ) )
  }

  def kFoldTestingUnion[T: ClassTag] (input: Array[(RDD[T], RDD[T])]) : RDD[(Int,T)] = {
    if (input.length == 0) {
      return null
    }
    val sc = input(0)._1.context
    sc.union( input.zipWithIndex.map( x => x._1._2.map( y => (x._2, y)) ) )
  }
}
