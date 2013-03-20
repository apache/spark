package spark.util

/**
 * A utility for tracking the total time an iterator takes to iterate through its elements.
 *
 * In general, this should only be used if you expect it to take a considerable amount of time
 * (eg. milliseconds) to get each element -- otherwise, the timing won't be very accurate,
 * and you are probably just adding more overhead
 */
class TimedIterator[+A](val sub: Iterator[A]) extends Iterator[A] {
  private var netMillis = 0l
  private var nElems = 0
  def hasNext = {
    val start = System.currentTimeMillis()
    val r = sub.hasNext
    val end = System.currentTimeMillis()
    netMillis += (end - start)
    r
  }
  def next = {
    val start = System.currentTimeMillis()
    val r = sub.next
    val end = System.currentTimeMillis()
    netMillis += (end - start)
    nElems += 1
    r
  }

  def getNetMillis = netMillis
  def getAverageTimePerItem = netMillis / nElems.toDouble

}
