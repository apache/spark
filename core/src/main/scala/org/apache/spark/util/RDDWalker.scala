package org.apache.spark.util

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * This class allows execution of a function on an RDD and all of its dependencies. This is accomplished by
 * walking the object graph linking these RDDs. This is useful for debugging internal RDD references.
 * @param rddToWalk - The RDD to traverse along with its dependencies
 * @tparam T - The type of the RDD
 */
class RDDWalker[T : ClassTag] (rddToWalk : RDD[T]){
  val rdd = rddToWalk

  /**
   * Execute the passed function on the underlying RDD
   * @param func - The function to execute on
   */
  def walk(func : (RDD[T])=>Unit): Unit ={

  }
}
