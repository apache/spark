package spark.streaming

/**
 * This is a simple class that represents time. Internally, it represents time as UTC.
 * The recommended way to create instances of Time is to use helper objects
 * [[spark.streaming.Milliseconds]], [[spark.streaming.Seconds]], and [[spark.streaming.Minutes]].
 * @param millis Time in UTC.
 */

class Time(private val millis: Long) {
  
  def < (that: Time): Boolean = (this.millis < that.millis)

  def <= (that: Time): Boolean = (this.millis <= that.millis)

  def > (that: Time): Boolean = (this.millis > that.millis)
  
  def >= (that: Time): Boolean = (this.millis >= that.millis)

  def + (that: Duration): Time = new Time(millis + that.milliseconds)

  def - (that: Time): Duration = new Duration(millis - that.millis)

  def - (that: Duration): Time = new Time(millis - that.milliseconds)

  def floor(that: Duration): Time = {
    val t = that.milliseconds
    val m = math.floor(this.millis / t).toLong 
    new Time(m * t)
  }

  def isMultipleOf(that: Duration): Boolean =
    (this.millis % that.milliseconds == 0)

  def min(that: Time): Time = if (this < that) this else that

  def max(that: Time): Time = if (this > that) this else that

  override def toString: String = (millis.toString + " ms")

  def milliseconds: Long = millis
}

/*private[streaming] object Time {
  implicit def toTime(long: Long) = Time(long)
}
*/

