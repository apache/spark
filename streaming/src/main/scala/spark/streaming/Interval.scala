package spark.streaming

private[streaming]
case class Interval(beginTime: Time, endTime: Time) {
  def this(beginMs: Long, endMs: Long) = this(Time(beginMs), new Time(endMs))
  
  def duration(): Time = endTime - beginTime

  def + (time: Time): Interval = {
    new Interval(beginTime + time, endTime + time) 
  }

  def - (time: Time): Interval = {
    new Interval(beginTime - time, endTime - time)
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

  def isZero = (beginTime.isZero && endTime.isZero)

  def toFormattedString = beginTime.toFormattedString + "-" + endTime.toFormattedString

  override def toString = "[" + beginTime + ", " + endTime + "]" 
}

object Interval {
  def zero() = new Interval (Time.zero, Time.zero)

  def currentInterval(intervalDuration: Time): Interval  = {
    val time = Time(System.currentTimeMillis)
    val intervalBegin = time.floor(intervalDuration) 
    Interval(intervalBegin, intervalBegin + intervalDuration) 
  }
}


