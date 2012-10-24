package spark

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions._

/** A set of functions used to aggregate data.
  * 
  * @param createCombiner function to create the initial value of the aggregation.
  * @param mergeValue function to merge a new value into the aggregation result.
  * @param mergeCombiners function to merge outputs from multiple mergeValue function.
  */
case class Aggregator[K, V, C] (
    val createCombiner: V => C,
    val mergeValue: (C, V) => C,
    val mergeCombiners: (C, C) => C) {

  def combineValuesByKey(iter: Iterator[(K, V)]) : Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    for ((k, v) <- iter) {
      val oldC = combiners.get(k)
      if (oldC == null) {
        combiners.put(k, createCombiner(v))
      } else {
        combiners.put(k, mergeValue(oldC, v))
      }
    }
    combiners.iterator
  }

  def combineCombinersByKey(iter: Iterator[(K, C)]) : Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    for ((k, c) <- iter) {
      val oldC = combiners.get(k)
      if (oldC == null) {
        combiners.put(k, c)
      } else {
        combiners.put(k, mergeCombiners(oldC, c))
      }
    }
    combiners.iterator
  }
}

