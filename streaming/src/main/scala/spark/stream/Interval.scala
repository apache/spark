package spark.stream

case class Interval (val beginTime: Time, val endTime: Time) {
  
  def this(beginMs: Long, endMs: Long) = this(new LongTime(beginMs), new LongTime(endMs))
  
  def duration(): Time = endTime - beginTime
  
  def += (time: Time) {
    beginTime += time
    endTime += time
    this
  }

  def + (time: Time): Interval = {
    new Interval(beginTime + time, endTime + time) 
  }

  def < (that: Interval): Boolean = {
    if (this.duration != that.duration) {
      throw new Exception("Comparing two intervals with different durations [" + this + ", " + that + "]")
    }
    this.endTime < that.endTime
  }

  def <= (that: Interval) = (this < that || this == that)
 
  def > (that: Interval) = !(this <= that)
  
  def >= (that: Interval) = !(this < that)

  def next(): Interval = {
    this + (endTime - beginTime)
  }

  def isZero() = (beginTime.isZero && endTime.isZero)

  def toFormattedString = beginTime.toFormattedString + "-" + endTime.toFormattedString

  override def toString = "[" + beginTime + ", " + endTime + "]" 
}

object Interval {

  /*
  implicit def longTupleToInterval (longTuple: (Long, Long)) =
    Interval(longTuple._1, longTuple._2)
  
  implicit def intTupleToInterval (intTuple: (Int, Int)) =
    Interval(intTuple._1, intTuple._2)
  
  implicit def string2Interval (str: String): Interval = {
    val parts = str.split(",")
    if (parts.length == 1)
      return Interval.zero
    return Interval (parts(0).toInt, parts(1).toInt)
  }
  
  def getInterval (timeMs: Long, intervalDurationMs: Long): Interval = {
    val intervalBeginMs = timeMs / intervalDurationMs * intervalDurationMs
    Interval(intervalBeginMs, intervalBeginMs + intervalDurationMs)
  }
  */

  def zero() = new Interval (Time.zero, Time.zero)

  def currentInterval(intervalDuration: LongTime): Interval  = {
    val time = LongTime(System.currentTimeMillis)
    val intervalBegin = time.floor(intervalDuration) 
    Interval(intervalBegin, intervalBegin + intervalDuration) 
  }

}


