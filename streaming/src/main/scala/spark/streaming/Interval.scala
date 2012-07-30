package spark.streaming

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

  def zero() = new Interval (Time.zero, Time.zero)

  def currentInterval(intervalDuration: LongTime): Interval  = {
    val time = LongTime(System.currentTimeMillis)
    val intervalBegin = time.floor(intervalDuration) 
    Interval(intervalBegin, intervalBegin + intervalDuration) 
  }

}


