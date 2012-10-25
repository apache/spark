package spark.partial

import java.util.{HashMap => JHashMap}
import java.util.{Map => JMap}

import scala.collection.mutable.HashMap
import scala.collection.Map
import scala.collection.JavaConversions.mapAsScalaMap

import spark.util.StatCounter

/**
 * An ApproximateEvaluator for means by key. Returns a map of key to confidence interval.
 */
private[spark] class GroupedMeanEvaluator[T](totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[JHashMap[T, StatCounter], Map[T, BoundedDouble]] {

  var outputsMerged = 0
  var sums = new JHashMap[T, StatCounter]   // Sum of counts for each key

  override def merge(outputId: Int, taskResult: JHashMap[T, StatCounter]) {
    outputsMerged += 1
    val iter = taskResult.entrySet.iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val old = sums.get(entry.getKey)
      if (old != null) {
        old.merge(entry.getValue)
      } else {
        sums.put(entry.getKey, entry.getValue)
      }
    }
  }

  override def currentResult(): Map[T, BoundedDouble] = {
    if (outputsMerged == totalOutputs) {
      val result = new JHashMap[T, BoundedDouble](sums.size)
      val iter = sums.entrySet.iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val mean = entry.getValue.mean
        result(entry.getKey) = new BoundedDouble(mean, 1.0, mean, mean)
      }
      result
    } else if (outputsMerged == 0) {
      new HashMap[T, BoundedDouble]
    } else {
      val p = outputsMerged.toDouble / totalOutputs
      val studentTCacher = new StudentTCacher(confidence)
      val result = new JHashMap[T, BoundedDouble](sums.size)
      val iter = sums.entrySet.iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val counter = entry.getValue
        val mean = counter.mean
        val stdev = math.sqrt(counter.sampleVariance / counter.count)
        val confFactor = studentTCacher.get(counter.count)
        val low = mean - confFactor * stdev
        val high = mean + confFactor * stdev
        result(entry.getKey) = new BoundedDouble(mean, confidence, low, high)
      }
      result
    }
  }
}
