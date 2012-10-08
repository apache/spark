package spark.partial

import java.util.{HashMap => JHashMap}
import java.util.{Map => JMap}

import scala.collection.Map
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions.mapAsScalaMap

import cern.jet.stat.Probability

import it.unimi.dsi.fastutil.objects.{Object2LongOpenHashMap => OLMap}

/**
 * An ApproximateEvaluator for counts by key. Returns a map of key to confidence interval.
 */
private[spark] class GroupedCountEvaluator[T](totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[OLMap[T], Map[T, BoundedDouble]] {

  var outputsMerged = 0
  var sums = new OLMap[T]   // Sum of counts for each key

  override def merge(outputId: Int, taskResult: OLMap[T]) {
    outputsMerged += 1
    val iter = taskResult.object2LongEntrySet.fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      sums.put(entry.getKey, sums.getLong(entry.getKey) + entry.getLongValue)
    }
  }

  override def currentResult(): Map[T, BoundedDouble] = {
    if (outputsMerged == totalOutputs) {
      val result = new JHashMap[T, BoundedDouble](sums.size)
      val iter = sums.object2LongEntrySet.fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val sum = entry.getLongValue()
        result(entry.getKey) = new BoundedDouble(sum, 1.0, sum, sum)
      }
      result
    } else if (outputsMerged == 0) {
      new HashMap[T, BoundedDouble]
    } else {
      val p = outputsMerged.toDouble / totalOutputs
      val confFactor = Probability.normalInverse(1 - (1 - confidence) / 2)
      val result = new JHashMap[T, BoundedDouble](sums.size)
      val iter = sums.object2LongEntrySet.fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val sum = entry.getLongValue
        val mean = (sum + 1 - p) / p
        val variance = (sum + 1) * (1 - p) / (p * p)
        val stdev = math.sqrt(variance)
        val low = mean - confFactor * stdev
        val high = mean + confFactor * stdev
        result(entry.getKey) = new BoundedDouble(mean, confidence, low, high)
      }
      result
    }
  }
}
