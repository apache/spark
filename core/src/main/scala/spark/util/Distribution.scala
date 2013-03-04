package spark.util

import java.io.PrintStream

/**
 * Util for getting some stats from a small sample of numeric values, with some handy summary functions.
 *
 * Entirely in memory, not intended as a good way to compute stats over large data sets.
 *
 * Assumes you are giving it a non-empty set of data
 */
class Distribution(val data: Array[Double], val startIdx: Int, val endIdx: Int) {
  require(startIdx < endIdx)
  def this(data: Traversable[Double]) = this(data.toArray, 0, data.size)
  java.util.Arrays.sort(data, startIdx, endIdx)
  val length = endIdx - startIdx

  val defaultProbabilities = Array(0,0.25,0.5,0.75,1.0)

  /**
   * Get the value of the distribution at the given probabilities.  Probabilities should be
   * given from 0 to 1
   * @param probabilities
   */
  def getQuantiles(probabilities: Traversable[Double] = defaultProbabilities) = {
    probabilities.toIndexedSeq.map{p:Double => data(closestIndex(p))}
  }

  private def closestIndex(p: Double) = {
    math.min((p * length).toInt + startIdx, endIdx - 1)
  }

  def showQuantiles(out: PrintStream = System.out) = {
    out.println("min\t25%\t50%\t75%\tmax")
    getQuantiles(defaultProbabilities).foreach{q => out.print(q + "\t")}
    out.println
  }

  def statCounter = StatCounter(data.slice(startIdx, endIdx))

  /**
   * print a summary of this distribution to the given PrintStream.
   * @param out
   */
  def summary(out: PrintStream = System.out) {
    out.println(statCounter)
    showQuantiles(out)
  }
}

object Distribution {

  def apply(data: Traversable[Double]): Option[Distribution] = {
    if (data.size > 0)
      Some(new Distribution(data))
    else
      None
  }

  def showQuantiles(out: PrintStream = System.out, quantiles: Traversable[Double]) {
    out.println("min\t25%\t50%\t75%\tmax")
    quantiles.foreach{q => out.print(q + "\t")}
    out.println
  }
}