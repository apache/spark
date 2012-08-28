package spark.streaming

case class Time(millis: Long) extends Serializable {
  
  def < (that: Time): Boolean = (this.millis < that.millis)
 
  def <= (that: Time): Boolean = (this.millis <= that.millis)
  
  def > (that: Time): Boolean = (this.millis > that.millis)
  
  def >= (that: Time): Boolean = (this.millis >= that.millis)

  def + (that: Time): Time = Time(millis + that.millis)
  
  def - (that: Time): Time = Time(millis - that.millis)
  
  def * (times: Int): Time = Time(millis * times)
  
  def floor(that: Time): Time = {
    val t = that.millis
    val m = math.floor(this.millis / t).toLong 
    Time(m * t)
  }

  def isMultipleOf(that: Time): Boolean = 
    (this.millis % that.millis == 0)

  def isZero: Boolean = (this.millis == 0)

  override def toString: String = (millis.toString + " ms")

  def toFormattedString: String = millis.toString
  
  def milliseconds: Long = millis
}

object Time {
  val zero = Time(0)

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

