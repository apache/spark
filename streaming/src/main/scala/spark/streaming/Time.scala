package spark.streaming

/**
 * This is a simple class that represents time. Internally, it represents time as UTC.
 * The recommended way to create instances of Time is to use helper objects
 * [[spark.streaming.Milliseconds]], [[spark.streaming.Seconds]], and [[spark.streaming.Minutes]].
 * @param millis Time in UTC.
 */

case class Time(private val millis: Long) {
  
  def < (that: Time): Boolean = (this.millis < that.millis)

  def <= (that: Time): Boolean = (this.millis <= that.millis)

  def > (that: Time): Boolean = (this.millis > that.millis)
  
  def >= (that: Time): Boolean = (this.millis >= that.millis)

  def + (that: Time): Time = Time(millis + that.millis)
  
  def - (that: Time): Time = Time(millis - that.millis)
  
  def * (times: Int): Time = Time(millis * times)

  def / (that: Time): Long = millis / that.millis

  def floor(that: Time): Time = {
    val t = that.millis
    val m = math.floor(this.millis / t).toLong 
    Time(m * t)
  }

  def isMultipleOf(that: Time): Boolean = 
    (this.millis % that.millis == 0)

  def min(that: Time): Time = if (this < that) this else that

  def max(that: Time): Time = if (this > that) this else that

  def isZero: Boolean = (this.millis == 0)

  override def toString: String = (millis.toString + " ms")

  def toFormattedString: String = millis.toString
  
  def milliseconds: Long = millis
}

private[streaming] object Time {
  val zero = Time(0)

  implicit def toTime(long: Long) = Time(long)
}

/**
 * Helper object that creates instance of [[spark.streaming.Time]] representing
 * a given number of milliseconds.
 */
object Milliseconds {
  def apply(milliseconds: Long) = Time(milliseconds)
}

/**
 * Helper object that creates instance of [[spark.streaming.Time]] representing
 * a given number of seconds.
 */
object Seconds {
  def apply(seconds: Long) = Time(seconds * 1000)
}

/**
 * Helper object that creates instance of [[spark.streaming.Time]] representing
 * a given number of minutes.
 */
object Minutes {
  def apply(minutes: Long) = Time(minutes * 60000)
}

