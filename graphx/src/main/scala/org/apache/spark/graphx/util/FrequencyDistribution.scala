package org.apache.spark.graphx.util

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 *
 */
trait FrequencyDistribution extends Serializable {
  protected var isSortedDisjointRanges: Boolean = false

  def compute(numbers: RDD[Int], max: Int, min: Int): Array[((Int, Int), Long)] = {
    assert(max >= min)
    val ranges = getRanges(max, min)
    assert(ranges.forall(range => range._1 <= range._2))
    val frequencies = numbers.mapPartitions(getFrequencyFunc(ranges, max, min)).reduce((a, b) => a.zip(b).map(pair => pair._1 + pair._2))
    ranges.zip(frequencies)
  }

  def compute(numbers: RDD[Int]): Array[((Int, Int), Long)] = {
    val (max, min) = numbers.mapPartitions { iter =>
      var (max: Int, min: Int) = (0, 0)
      if (iter.hasNext) {
        max = iter.next()
        min = max
      }
      while (iter.hasNext) {
        val num = iter.next()
        if (num > max) max = num
        else if (num < min) min = num
      }
      Iterator(Tuple2(max, min))
    }.reduce { (a, b) =>
      val max = if (a._1 > b._1) a._1 else b._1
      val min = if (a._2 < b._2) a._2 else b._2
      (max, min)
    }
    compute(numbers, max, min)
  }

  protected def getFrequencyFunc(ranges: Array[(Int, Int)], max: Int, min: Int): Iterator[Int] => Iterator[Array[Long]] = {
    if (isSortedDisjointRanges) {
      (iter: Iterator[Int]) => {
        val frequencies = new Array[Long](ranges.length)
        while (iter.hasNext) {
          val num = iter.next()
          val target = binaryRangeSearch(ranges, num)
          if (target != -1) frequencies(target) += 1
        }
        Iterator(frequencies)
      }
    } else {
      (iter: Iterator[Int]) => {
        val frequencies = new Array[Long](ranges.length)
        while (iter.hasNext) {
          val num = iter.next()
          var i = 0
          ranges.foreach { range =>
            if (num >= range._1 && num <= range._2) frequencies(i) += 1
            i += 1
          }
        }
        Iterator(frequencies)
      }
    }
  }

  private def binaryRangeSearch(ranges: Array[(Int, Int)], num: Int): Int = {
    var (left, right) = (0, ranges.length - 1)
    while (left <= right) {
      val middle = (left + right) >>> 1
      if (num > ranges(middle)._2) left = middle + 1
      else if (num < ranges(middle)._1) right = middle - 1
      else return middle
    }
    -1
  }

  protected def getRanges(max: Int, min: Int): Array[(Int, Int)]
}

object FrequencyDistribution {

  private case class UserDefinedRanges(ranges: Array[(Int, Int)], sortedDisjoint: Boolean = false) extends FrequencyDistribution {
    isSortedDisjointRanges = sortedDisjoint

    def getRanges(max: Int, min: Int): Array[(Int, Int)] = ranges
  }

  private case class DividedRanges(numRanges: Int) extends FrequencyDistribution {
    isSortedDisjointRanges = true

    def getRanges(max: Int, min: Int): Array[(Int, Int)] = {
      val span = max - min + 1
      val initSize = span / numRanges
      val sizes = Array.fill(numRanges)(initSize)
      val remainder = span - numRanges * initSize
      for (i <- 0 until remainder) {
        sizes(i) += 1
      }
      assert(sizes.reduce(_ + _) == span)
      val ranges = ArrayBuffer.empty[(Int, Int)]
      var start = min
      sizes.filter(_ > 0).foreach { size =>
        val end = start + size - 1
        ranges += Tuple2(start, end)
        start = end + 1
      }
      assert(start == max + 1)
      ranges.toArray
    }
  }

  private case class StepRanges(stepSize: Int, startFrom: Option[Int] = None) extends FrequencyDistribution {
    isSortedDisjointRanges = true

    def getRanges(max: Int, min: Int): Array[(Int, Int)] = {
      var start = startFrom.getOrElse(min)
      val ranges = ArrayBuffer.empty[(Int, Int)]
      while (start <= max) {
        val end = start + stepSize - 1
        ranges += Tuple2(start, end)
        start = end + 1
      }
      ranges.toArray
    }
  }

  private case class SlidingWindows(windowSize: Int, slidingSize: Int, startFrom: Option[Int] = None) extends FrequencyDistribution {
    def getRanges(max: Int, min: Int): Array[(Int, Int)] = {
      var start = startFrom.getOrElse(min)
      val ranges = ArrayBuffer.empty[(Int, Int)]
      while (start <= max) {
        val end = start + windowSize - 1
        ranges += Tuple2(start, end)
        start += slidingSize
      }
      ranges.toArray
    }
  }

  private case object Log10 extends FrequencyDistribution {
    isSortedDisjointRanges = true

    def getRanges(max: Int, min: Int): Array[(Int, Int)] = {
      val ranges = ArrayBuffer.empty[(Int, Int)]
      ranges += Tuple2(0, 9)
      var start = 10
      while (start <= max) {
        val end = start * 10 - 1
        ranges += Tuple2(start, end)
        start = end + 1
      }
      ranges.toArray
    }
  }

  def apply(ranges: Array[(Int, Int)], sortedDisjoint: Boolean = false): FrequencyDistribution = {
    new UserDefinedRanges(ranges, sortedDisjoint)
  }

  def log10(): FrequencyDistribution = Log10

  def divide(numRanges: Int): FrequencyDistribution = {
    assert(numRanges > 0)
    new DividedRanges(numRanges)
  }

  def step(stepSize: Int): FrequencyDistribution = {
    assert(stepSize > 0)
    new StepRanges(stepSize)
  }

  def step(stepSize: Int, startFrom: Int): FrequencyDistribution = {
    assert(stepSize > 0)
    new StepRanges(stepSize, Some(startFrom))
  }


  def sliding(windowSize: Int, slidingSize: Int): FrequencyDistribution ={
    new SlidingWindows(windowSize, slidingSize)
  }

  def sliding(windowSize: Int, slidingSize: Int, startFrom: Int): FrequencyDistribution ={
    new SlidingWindows(windowSize, slidingSize, Some(startFrom))
  }
}
