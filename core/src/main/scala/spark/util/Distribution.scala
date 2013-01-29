package spark.util

import java.io.PrintStream

/**
 * util for getting some stats from a small sample of numeric values, with some handy summary functions
 *
 * Entirely in memory, not intended as a good way to compute stats over large data sets.
 */
class Distribution(val data: Array[Double], val startIdx: Int, val endIdx: Int) {
  def this(data: Traversable[Double]) = this(data.toArray, 0, data.size)
  java.util.Arrays.sort(data, startIdx, endIdx)
  val length = endIdx - startIdx

  val defaultProbabilities = Array(0,0.25,0.5,0.75,1.0)

  /**
   * Get the value of the distribution at the given probabilities.  Probabilities should be
   * given from 0 to 1
   * @param probabilities
   */
  def getQuantiles(probabilities: Traversable[Double]) = {
    probabilities.map{q =>data((q * length).toInt + startIdx)}
  }

  def showQuantiles(out: PrintStream = System.out, probabilities: Traversable[Double] = defaultProbabilities) = {
    out.println("min\t25%\t50%\t75%max")
    probabilities.foreach{q => out.print(q + "\t")}
    out.println
  }

  def summary : (StatCounter, Traversable[Double]) = {
    (StatCounter(data), getQuantiles(defaultProbabilities))
  }

  /**
   * print a summary of this distribution to the given PrintStream.
   * @param out
   */
  def summary(out: PrintStream = System.out) {
    val (statCounter, quantiles) = summary
    out.println(statCounter)
    Distribution.showQuantiles(out, quantiles)
  }
}

object Distribution {
  def showQuantiles(out: PrintStream = System.out, quantiles: Traversable[Double]) {
    out.println("min\t25%\t50%\t75%max")
    quantiles.foreach{q => out.print(q + "\t")}
    out.println
  }
}