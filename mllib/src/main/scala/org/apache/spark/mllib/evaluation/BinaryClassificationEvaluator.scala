package org.apache.spark.mllib.evaluation

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class BinaryClassificationEvaluator(scoreAndLabel: RDD[(Double, Double)]) {
  
}

object BinaryClassificationEvaluator {

  def get(rdd: RDD[(Double, Double)]) {
    // Create a bin for each distinct score value, count positives and negatives within each bin,
    // and then sort by score values in descending order.
    val counts = rdd.combineByKey(
      createCombiner = (label: Double) => new Counter(0L, 0L) += label,
      mergeValue = (c: Counter, label: Double) => c += label,
      mergeCombiners = (c1: Counter, c2: Counter) => c1 += c2
    ).sortByKey(ascending = false)
    println(counts.collect().toList)
    val agg = counts.values.mapPartitions((iter: Iterator[Counter]) => {
      val agg = new Counter()
      iter.foreach(agg += _)
      Iterator(agg)
    }, preservesPartitioning = true).collect()
    println(agg.toList)
    val cum = agg.scanLeft(new Counter())((agg: Counter, c: Counter) => agg + c)
    val total = cum.last
    println(total)
    println(cum.toList)
    val cumCountsRdd = counts.mapPartitionsWithIndex((index: Int, iter: Iterator[(Double, Counter)]) => {
      val cumCount = cum(index)
      iter.map { case (score, c) =>
        cumCount += c
        (score, cumCount.clone())
      }
    }, preservesPartitioning = true)
    println("cum: " + cumCountsRdd.collect().toList)
    val rocAUC = AreaUnderCurve.of(cumCountsRdd.values.map((c: Counter) => {
      (1.0 * c.numNegatives / total.numNegatives,
        1.0 * c.numPositives / total.numPositives)
    }))
    println(rocAUC)
    val prAUC = AreaUnderCurve.of(cumCountsRdd.values.map((c: Counter) => {
      (1.0 * c.numPositives / total.numPositives,
        1.0 * c.numPositives / (c.numPositives + c.numNegatives))
    }))
    println(prAUC)
  }

  def get(data: Iterable[(Double, Double)]) {
    val counts = data.groupBy(_._1).mapValues { s =>
      val c = new Counter()
      s.foreach(c += _._2)
      c
    }.toSeq.sortBy(- _._1)
    println("counts: " + counts.toList)
    val total = new Counter()
    val cum = counts.map { s =>
      total += s._2
      (s._1, total.clone())
    }
    println("cum: " + cum.toList)
    val roc = cum.map { case (s, c) =>
      (1.0 * c.numNegatives / total.numNegatives, 1.0 * c.numPositives / total.numPositives)
    }
    val rocAUC = AreaUnderCurve.of(roc)
    println(rocAUC)
    val pr = cum.map { case (s, c) =>
      (1.0 * c.numPositives / total.numPositives,
        1.0 * c.numPositives / (c.numPositives + c.numNegatives))
    }
    val prAUC = AreaUnderCurve.of(pr)
    println(prAUC)
  }
}

class Counter(var numPositives: Long = 0L, var numNegatives: Long = 0L) extends Serializable {

  def +=(label: Double): Counter = {
    // Though we assume 1.0 for positive and 0.0 for negative, the following check will handle
    // -1.0 for negative as well.
    if (label > 0.5) numPositives += 1L else numNegatives += 1L
    this
  }

  def +=(other: Counter): Counter = {
    numPositives += other.numPositives
    numNegatives += other.numNegatives
    this
  }

  def +(label: Double): Counter = {
    this.clone() += label
  }

  def +(other: Counter): Counter = {
    this.clone() += other
  }

  def sum: Long = numPositives + numNegatives

  override def clone(): Counter = {
    new Counter(numPositives, numNegatives)
  }

  override def toString(): String = s"[$numPositives,$numNegatives]"
}

