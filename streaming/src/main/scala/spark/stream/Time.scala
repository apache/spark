package spark.stream

abstract case class Time {

  // basic operations that must be overridden
  def copy(): Time
  def zero: Time
  def < (that: Time): Boolean
  def += (that: Time): Time
  def -= (that: Time): Time
  def floor(that: Time): Time
  def isMultipleOf(that: Time): Boolean

  // derived operations composed of basic operations
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
  def <= (that: Time) = (this < that || this == that)
  def > (that: Time) = !(this <= that)
  def >= (that: Time) = !(this < that)
  def isZero = (this == zero)
  def toFormattedString = toString
}

object Time {
  def Milliseconds(milliseconds: Long) = LongTime(milliseconds)

  def zero = LongTime(0)
}

case class LongTime(var milliseconds: Long) extends Time {
  
  override def copy() = LongTime(this.milliseconds) 
 
  override def zero = LongTime(0)

  override def < (that: Time): Boolean =
    (this.milliseconds < that.asInstanceOf[LongTime].milliseconds)
 
  override def += (that: Time): Time = {
    this.milliseconds += that.asInstanceOf[LongTime].milliseconds
    this
  }
  
  override def -= (that: Time): Time = {
    this.milliseconds -= that.asInstanceOf[LongTime].milliseconds
    this
  }

  override def floor(that: Time): Time = {
    val t = that.asInstanceOf[LongTime].milliseconds
    val m = this.milliseconds / t 
    LongTime(m.toLong * t)
  }

  override def isMultipleOf(that: Time): Boolean = 
    (this.milliseconds % that.asInstanceOf[LongTime].milliseconds == 0)

  override def isZero = (this.milliseconds == 0)

  override def toString = (milliseconds.toString + "ms")

  override def toFormattedString = milliseconds.toString 
}

object Milliseconds {
  def apply(milliseconds: Long) = LongTime(milliseconds)
}

object Seconds {
  def apply(seconds: Long) = LongTime(seconds * 1000)
}  

object Minutes { 
  def apply(minutes: Long) = LongTime(minutes * 60000)
}

