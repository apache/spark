package spark.streaming

class Time(private var millis: Long) {

  def copy() = new Time(this.millis) 
 
  def zero = Time.zero

  def < (that: Time): Boolean =
    (this.millis < that.millis)
 
  def <= (that: Time) = (this < that || this == that)
  
  def > (that: Time) = !(this <= that)
  
  def >= (that: Time) = !(this < that)  
  
  def += (that: Time): Time = {
    this.millis += that.millis
    this
  }
  
  def -= (that: Time): Time = {
    this.millis -= that.millis
    this
  }

  def + (that: Time) = this.copy() += that
  
  def - (that: Time) = this.copy() -= that
  
  def * (times: Int) = {
    var count = 0
    var result = this.copy()
    while (count < times) {
      result += this
      count += 1
    }
    result
  }
  
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
  
  def apply(milliseconds: Long) = new Time(milliseconds)
  
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

