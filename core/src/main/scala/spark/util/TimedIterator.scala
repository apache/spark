package spark.util

/**
 * A utility for tracking the the time an iterator takes to iterate through its elements.
 */
trait TimedIterator {
  def getNetMillis: Option[Long]
  def getAverageTimePerItem: Option[Double]
}

/**
 * A TimedIterator which uses System.currentTimeMillis() on every call to next().
 *
 * In general, this should only be used if you expect it to take a considerable amount of time
 * (eg. milliseconds) to get each element -- otherwise, the timing won't be very accurate,
 * and you are probably just adding more overhead
 */
class SystemTimedIterator[+A](val sub: Iterator[A]) extends Iterator[A] with TimedIterator {
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

  def getNetMillis = Some(netMillis)
  def getAverageTimePerItem = Some(netMillis / nElems.toDouble)

}

/**
 * A TimedIterator which doesn't perform any timing measurements.
 */
class NoOpTimedIterator[+A](val sub: Iterator[A]) extends Iterator[A] with TimedIterator {
  def hasNext = sub.hasNext
  def next = sub.next
  def getNetMillis = None
  def getAverageTimePerItem = None
}
