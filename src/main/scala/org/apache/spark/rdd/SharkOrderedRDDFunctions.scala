package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark.Logging

import scala.language.implicitConversions

/**
 * Extra functions for Shark available on RDDs of (key, value) pairs where the key is
 * sortable through an implicit conversion.
 * Import `org.apache.spark.SharkOrderedRDDFunctions._` at the top of your program to
 * use these functions. They will work with any key type that has a `scala.math.Ordered`
 * implementation.
 */
class SharkOrderedRDDFunctions[K <% Ordered[K]: ClassTag,
                               V: ClassTag,
                               P <: Product2[K, V] : ClassTag](
    self: RDD[P])
  extends Logging with Serializable {

  /**
   * Sort data within a partition. This function will not introduce a
   * shuffling operation.
   */
  def sortByKeyLocally(ascending: Boolean = true): RDD[P] = {
    self.mapPartitions(iter => {
      val buf = iter.toArray
      if (ascending) {
        buf.sortWith((x, y) => x._1 < y._1).iterator
      } else {
        buf.sortWith((x, y) => x._1 > y._1).iterator
      }
    }, preservesPartitioning = true)
  }
}

object SharkOrderedRDDFunctions {
  implicit def rddToSharkOrderedRDDFunctions[K <% Ordered[K]: ClassTag, V: ClassTag](
      rdd: RDD[(K, V)]) =
    new SharkOrderedRDDFunctions[K, V, (K, V)](rdd)
}