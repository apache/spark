package spark.partial

import java.util.{HashMap => JHashMap}
import java.util.{Map => JMap}

import scala.collection.mutable.HashMap
import scala.collection.Map
import scala.collection.JavaConversions.mapAsScalaMap

import spark.util.StatCounter

/**
 * An ApproximateEvaluator for sums by key. Returns a map of key to confidence interval.
 */
private[spark] class GroupedSumEvaluator[T](totalOutputs: Int, confidence: Double)
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
        val sum = entry.getValue.sum
        result(entry.getKey) = new BoundedDouble(sum, 1.0, sum, sum)
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
        val meanEstimate = counter.mean
        val meanVar = counter.sampleVariance / counter.count
        val countEstimate = (counter.count + 1 - p) / p
        val countVar = (counter.count + 1) * (1 - p) / (p * p)
        val sumEstimate = meanEstimate * countEstimate
        val sumVar = (meanEstimate * meanEstimate * countVar) +
                     (countEstimate * countEstimate * meanVar) +
                     (meanVar * countVar)
        val sumStdev = math.sqrt(sumVar)
        val confFactor = studentTCacher.get(counter.count)
        val low = sumEstimate - confFactor * sumStdev
        val high = sumEstimate + confFactor * sumStdev
        result(entry.getKey) = new BoundedDouble(sumEstimate, confidence, low, high)
      }
      result
    }
  }
}
