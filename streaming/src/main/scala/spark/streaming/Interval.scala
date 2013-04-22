package spark.streaming

private[streaming]
class Interval(val beginTime: Time, val endTime: Time) {
  def this(beginMs: Long, endMs: Long) = this(new Time(beginMs), new Time(endMs))
  
  def duration(): Duration = endTime - beginTime

  def + (time: Duration): Interval = {
    new Interval(beginTime + time, endTime + time) 
  }

  def - (time: Duration): Interval = {
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

  override def toString = "[" + beginTime + ", " + endTime + "]"
}

private[streaming]
object Interval {
  def currentInterval(duration: Duration): Interval  = {
    val time = new Time(System.currentTimeMillis)
    val intervalBegin = time.floor(duration)
    new Interval(intervalBegin, intervalBegin + duration)
  }
}


