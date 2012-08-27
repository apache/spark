package spark.streaming

case class Time(millis: Long) {
  def zero = Time.zero

  def < (that: Time): Boolean = (this.millis < that.millis)
 
  def <= (that: Time) = (this.millis <= that.millis)
  
  def > (that: Time) = (this.millis > that.millis)
  
  def >= (that: Time) = (this.millis >= that.millis)

  def + (that: Time) = new Time(millis + that.millis)
  
  def - (that: Time) = new Time(millis - that.millis)
  
  def * (times: Int) = new Time(millis * times)
  
  def floor(that: Time): Time = {
    val t = that.millis
    val m = math.floor(this.millis / t).toLong 
    new Time(m * t)
  }

  def isMultipleOf(that: Time): Boolean = 
    (this.millis % that.millis == 0)

  def isZero = (this.millis == 0)

  override def toString = (millis.toString + " ms")

  def toFormattedString = millis.toString
  
  def milliseconds = millis
}

object Time {
  val zero = new Time(0)

  implicit def toTime(long: Long) = Time(long)
  
  implicit def toLong(time: Time) = time.milliseconds  
}

object Milliseconds {
  def apply(milliseconds: Long) = Time(milliseconds)
}

object Seconds {
  def apply(seconds: Long) = Time(seconds * 1000)
}  

object Minutes { 
  def apply(minutes: Long) = Time(minutes * 60000)
}

